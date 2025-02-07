import threading
import random
import time
import pandas as pd
import numpy as np
from queue import Queue

def mock_database_query(query):
    time.sleep(random.uniform(0.5, 2))
    return pd.DataFrame({
        'id': np.arange(1, 101),
        'value': np.random.randn(100)
    })

class DataProcessor(threading.Thread):
    def __init__(self, data_queue, processed_queue):
        super().__init__()
        self.data_queue = data_queue
        self.processed_queue = processed_queue

    def run(self):
        while True:
            data = self.data_queue.get()
            if data is None:
                break
            processed_data = data.copy()
            processed_data['value'] = processed_data['value'].apply(self.process_value)
            self.processed_queue.put(processed_data)

    def process_value(self, value):
        return value * random.uniform(0.5, 1.5)

def generate_report(processed_data):
    try:
        report = processed_data.groupby('id').agg(
            total_value=pd.NamedAgg(column='value', aggfunc='sum')
        )
        report['avg_value'] = report['total_value'] / len(report)
        return report
    except Exception as e:
        print(f"Error in report generation: {e}")
        return None

def main():
    print("Fetching data from the database...")
    data = mock_database_query("SELECT * FROM data")
    print("Data fetched!")

    data_queue = Queue()
    processed_queue = Queue()

    num_threads = 4
    workers = [DataProcessor(data_queue, processed_queue) for _ in range(num_threads)]
    for worker in workers:
        worker.start()

    for i in range(0, len(data), 25):
        data_chunk = data.iloc[i:i+25]
        data_queue.put(data_chunk)

    for _ in range(num_threads):
        data_queue.put(None)

    processed_data = []
    while len(processed_data) < len(data):
        processed_chunk = processed_queue.get()
        processed_data.append(processed_chunk)

    final_report = generate_report(pd.concat(processed_data))

    if final_report is not None:
        print("\nFinal Report:")
        print(final_report.head())

if __name__ == "__main__":
    main()
