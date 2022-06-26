#!/usr/bin/env python3

import duckdb
import timeit
import psutil

con = duckdb.connect(database='my-db.duckdb', read_only=False)
# See https://github.com/duckdb/duckdb/issues/3969
con.execute("PRAGMA memory_limit='{}b'".format(psutil.virtual_memory().total / 4))
con.execute("PRAGMA threads={}".format(psutil.cpu_count(logical=False)))

queries_file = open('queries.sql', 'r')
queries = queries_file.readlines()

print("Will load the data")

start = timeit.default_timer()
con.execute(open('create.sql').read())
con.execute("INSERT INTO hits SELECT * FROM read_csv_auto('hits.csv')")
end = timeit.default_timer()
print(end - start)

for query in queries:
    print(query)
    for try_num in range(3):
        start = timeit.default_timer()
        con.execute(query)
        end = timeit.default_timer()
        print(end - start)
