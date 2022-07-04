#!/usr/bin/env python3

import duckdb
import timeit
import psutil
import sys

query = sys.stdin.read()
print(query)

con = duckdb.connect(database='my-db.duckdb', read_only=False)
# See https://github.com/duckdb/duckdb/issues/3969
con.execute("PRAGMA memory_limit='{}b'".format(psutil.virtual_memory().total / 4))
con.execute("PRAGMA threads={}".format(psutil.cpu_count(logical=False)))

for try_num in range(3):
    start = timeit.default_timer()
    con.execute(query)
    end = timeit.default_timer()
    print(end - start)
