import sys
from clickhouse_driver import Client


def run(database):
    client = Client("localhost",user="default",password="")
    client.execute(f"CREATE TABLE IF NOT EXISTS {database}.test (x Int32) ENGINE = Memory")
    client.execute(f"INSERT INTO {database}.test (x) VALUES", [{"x": 100}])
    result = client.execute(f"SELECT * FROM {database}.test")
    print(result)


if __name__ == "__main__":
    database = sys.argv[1]
    run(database)
