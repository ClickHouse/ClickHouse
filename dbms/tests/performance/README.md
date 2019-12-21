## ClickHouse performance tests

This directory contains `.xml`-files with performance tests for `clickhouse-performance-test` tool.

### How to write performance test

First of all you should check existing tests don't cover your case. If there are no such tests than you should write your own.

There two types of performance tests:
* First is executed in loop, and have tag `<type>loop</type>` in config.
* Second one is executed only once and have tag `<type>once</type>` in config.

Type `once` should be used only for endless queries. Even if your query really long (10 seconds+), it's better to choose `loop` test.

After you have choosen type, you have to specify `preconditions`. It contains table names. Only `hits_100m_single`, `hits_10m_single`, `test.hits` are available in CI.

The most important part of test is `stop_conditions`. For `loop` test you should always use `min_time_not_changing_for_ms` stop condition. For `once` test you can choose between `average_speed_not_changing_for_ms` and `max_speed_not_changing_for_ms`, but first is preferable. Also you should always specify `total_time_ms` metric. Endless tests will be ignored by CI.

`metrics` and `main_metric` settings are not important and can be ommited, because `loop` tests are always compared by `min_time` metric and `once` tests compared by `max_rows_per_second`.

You can use `substitions`, `create`, `fill` and `drop` queries to prepare test. You can find examples in this folder.

Take into account, that these tests will run in CI which consists of 56-cores and 512 RAM machines. Queries will be executed much faster than on local laptop.

If your test continued more than 10 minutes, please, add tag `long` to have an opportunity to run all tests and skip long ones.

### How to run performance test

You have to run clickhouse-server and after you can start testing:

```
$ clickhouse-performance-test --input-file my_lovely_test1.xml --input-file my_lovely_test2.xml
$ clickhouse-performance-test --input-file /my_lovely_test_dir/
```
