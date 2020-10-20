## ClickHouse performance tests

This directory contains `.xml`-files with performance tests for @akuzm tool.

### How to write performance test

First of all you should check existing tests don't cover your case. If there are no such tests than you should write your own.

You have to specify `preconditions`. It contains table names. Only `hits_100m_single`, `hits_10m_single`, `test.hits` are available in CI.

You can use `substitions`, `create`, `fill` and `drop` queries to prepare test. You can find examples in this folder.

Take into account, that these tests will run in CI which consists of 56-cores and 512 RAM machines. Queries will be executed much faster than on local laptop.

If your test continued more than 10 minutes, please, add tag `long` to have an opportunity to run all tests and skip long ones.

### How to run performance test

TODO @akuzm

### How to validate single test

```
pip3 install clickhouse_driver
../../docker/test/performance-comparison/perf.py --runs 1 insert_parallel.xml
```
