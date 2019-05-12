## ClickHouse release 19.5.3.8, 2019-04-18

### Bug fixes
* Fixed type of setting `max_partitions_per_insert_block` from boolean to UInt64. [#5028](https://github.com/yandex/ClickHouse/pull/5028) ([Mohammad Hossein Sekhavat](https://github.com/mhsekhavat))

## ClickHouse release 19.5.2.6, 2019-04-15

### New Features

* [Hyperscan](https://github.com/intel/hyperscan) multiple regular expression matching was added (functions `multiMatchAny`, `multiMatchAnyIndex`, `multiFuzzyMatchAny`, `multiFuzzyMatchAnyIndex`). [#4780](https://github.com/yandex/ClickHouse/pull/4780), [#4841](https://github.com/yandex/ClickHouse/pull/4841) ([Danila Kutenin](https://github.com/danlark1))
* `multiSearchFirstPosition` function was added. [#4780](https://github.com/yandex/ClickHouse/pull/4780) ([Danila Kutenin](https://github.com/danlark1))
* Implement the predefined expression filter per row for tables. [#4792](https://github.com/yandex/ClickHouse/pull/4792) ([Ivan](https://github.com/abyss7))
* A new type of data skipping indices based on bloom filters (can be used for `equal`, `in` and `like` functions). [#4499](https://github.com/yandex/ClickHouse/pull/4499) ([Nikita Vasilev](https://github.com/nikvas0))
* Added `ASOF JOIN` which allows to run queries that join to the most recent value known. [#4774](https://github.com/yandex/ClickHouse/pull/4774) [#4867](https://github.com/yandex/ClickHouse/pull/4867) [#4863](https://github.com/yandex/ClickHouse/pull/4863) [#4875](https://github.com/yandex/ClickHouse/pull/4875) ([Martijn Bakker](https://github.com/Gladdy), [Artem Zuikov](https://github.com/4ertus2))
* Rewrite multiple `COMMA JOIN` to `CROSS JOIN`. Then rewrite them to `INNER JOIN` if possible. [#4661](https://github.com/yandex/ClickHouse/pull/4661) ([Artem Zuikov](https://github.com/4ertus2))

### Improvement

* `topK` and `topKWeighted` now supports custom `loadFactor` (fixes issue [#4252](https://github.com/yandex/ClickHouse/issues/4252)). [#4634](https://github.com/yandex/ClickHouse/pull/4634) ([Kirill Danshin](https://github.com/kirillDanshin))
* Allow to use `parallel_replicas_count > 1` even for tables without sampling (the setting is simply ignored for them). In previous versions it was lead to exception. [#4637](https://github.com/yandex/ClickHouse/pull/4637) ([Alexey Elymanov](https://github.com/digitalist))
* Support for `CREATE OR REPLACE VIEW`. Allow to create a view or set a new definition in a single statement. [#4654](https://github.com/yandex/ClickHouse/pull/4654) ([Boris Granveaud](https://github.com/bgranvea))
* `Buffer` table engine now supports `PREWHERE`. [#4671](https://github.com/yandex/ClickHouse/pull/4671) ([Yangkuan Liu](https://github.com/LiuYangkuan))
* Add ability to start replicated table without metadata in zookeeper in `readonly` mode. [#4691](https://github.com/yandex/ClickHouse/pull/4691) ([alesapin](https://github.com/alesapin))
* Fixed flicker of progress bar in clickhouse-client. The issue was most noticeable when using `FORMAT Null` with streaming queries. [#4811](https://github.com/yandex/ClickHouse/pull/4811) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Allow to disable functions with `hyperscan` library on per user basis to limit potentially excessive and uncontrolled resource usage. [#4816](https://github.com/yandex/ClickHouse/pull/4816) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Add version number logging in all errors. [#4824](https://github.com/yandex/ClickHouse/pull/4824) ([proller](https://github.com/proller))
* Added restriction to the `multiMatch` functions which requires string size to fit into `unsigned int`. Also added the number of arguments limit to the `multiSearch` functions. [#4834](https://github.com/yandex/ClickHouse/pull/4834) ([Danila Kutenin](https://github.com/danlark1))
* Improved usage of scratch space and error handling in Hyperscan. [#4866](https://github.com/yandex/ClickHouse/pull/4866) ([Danila Kutenin](https://github.com/danlark1))
* Fill `system.graphite_detentions` from a table config of `*GraphiteMergeTree` engine tables. [#4584](https://github.com/yandex/ClickHouse/pull/4584) ([Mikhail f. Shiryaev](https://github.com/Felixoid))
* Rename `trigramDistance` function to `ngramDistance` and add more functions with `CaseInsensitive` and `UTF`. [#4602](https://github.com/yandex/ClickHouse/pull/4602) ([Danila Kutenin](https://github.com/danlark1))
* Improved data skipping indices calculation. [#4640](https://github.com/yandex/ClickHouse/pull/4640) ([Nikita Vasilev](https://github.com/nikvas0))
* Keep ordinary, `DEFAULT`, `MATERIALIZED` and `ALIAS` columns in a single list (fixes issue [#2867](https://github.com/yandex/ClickHouse/issues/2867)). [#4707](https://github.com/yandex/ClickHouse/pull/4707) ([Alex Zatelepin](https://github.com/ztlpn))

### Bug Fix

* Avoid `std::terminate` in case of memory allocation failure. Now `std::bad_alloc` exception is thrown as expected. [#4665](https://github.com/yandex/ClickHouse/pull/4665) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixes capnproto reading from buffer. Sometimes files wasn't loaded successfully by HTTP. [#4674](https://github.com/yandex/ClickHouse/pull/4674) ([Vladislav](https://github.com/smirnov-vs))
* Fix error `Unknown log entry type: 0` after `OPTIMIZE TABLE FINAL` query. [#4683](https://github.com/yandex/ClickHouse/pull/4683) ([Amos Bird](https://github.com/amosbird))
* Wrong arguments to `hasAny` or `hasAll` functions may lead to segfault. [#4698](https://github.com/yandex/ClickHouse/pull/4698) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Deadlock may happen while executing `DROP DATABASE dictionary` query. [#4701](https://github.com/yandex/ClickHouse/pull/4701) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix undefinied behavior in `median` and `quantile` functions. [#4702](https://github.com/yandex/ClickHouse/pull/4702) ([hcz](https://github.com/hczhcz))
* Fix compression level detection when `network_compression_method` in lowercase. Broken in v19.1. [#4706](https://github.com/yandex/ClickHouse/pull/4706) ([proller](https://github.com/proller))
* Fixed ignorance of `<timezone>UTC</timezone>` setting (fixes issue [#4658](https://github.com/yandex/ClickHouse/issues/4658)). [#4718](https://github.com/yandex/ClickHouse/pull/4718) ([proller](https://github.com/proller))
* Fix `histogram` function behaviour with `Distributed` tables. [#4741](https://github.com/yandex/ClickHouse/pull/4741) ([olegkv](https://github.com/olegkv))
* Fixed tsan report `destroy of a locked mutex`. [#4742](https://github.com/yandex/ClickHouse/pull/4742) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed TSan report on shutdown due to race condition in system logs usage. Fixed potential use-after-free on shutdown when part_log is enabled. [#4758](https://github.com/yandex/ClickHouse/pull/4758) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix recheck parts in `ReplicatedMergeTreeAlterThread` in case of error. [#4772](https://github.com/yandex/ClickHouse/pull/4772) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Arithmetic operations on intermediate aggregate function states were not working for constant arguments (such as subquery results). [#4776](https://github.com/yandex/ClickHouse/pull/4776) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Always backquote column names in metadata. Otherwise it's impossible to create a table with column named `index` (server won't restart due to malformed `ATTACH` query in metadata). [#4782](https://github.com/yandex/ClickHouse/pull/4782) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix crash in `ALTER ... MODIFY ORDER BY` on `Distributed` table. [#4790](https://github.com/yandex/ClickHouse/pull/4790) ([TCeason](https://github.com/TCeason))
* Fix segfault in `JOIN ON` with enabled `enable_optimize_predicate_expression`. [#4794](https://github.com/yandex/ClickHouse/pull/4794) ([Winter Zhang](https://github.com/zhang2014))
* Fix bug with adding an extraneous row after consuming a protobuf message from Kafka. [#4808](https://github.com/yandex/ClickHouse/pull/4808) ([Vitaly Baranov](https://github.com/vitlibar))
* Fix crash of `JOIN` on not-nullable vs nullable column. Fix `NULLs` in right keys in `ANY JOIN` + `join_use_nulls`. [#4815](https://github.com/yandex/ClickHouse/pull/4815) ([Artem Zuikov](https://github.com/4ertus2))
* Fix segmentation fault in `clickhouse-copier`. [#4835](https://github.com/yandex/ClickHouse/pull/4835) ([proller](https://github.com/proller))
* Fixed race condition in `SELECT` from `system.tables` if the table is renamed or altered concurrently. [#4836](https://github.com/yandex/ClickHouse/pull/4836) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed data race when fetching data part that is already obsolete. [#4839](https://github.com/yandex/ClickHouse/pull/4839) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed rare data race that can happen during `RENAME` table of MergeTree family. [#4844](https://github.com/yandex/ClickHouse/pull/4844) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed segmentation fault in function `arrayIntersect`. Segmentation fault could happen if function was called with mixed constant and ordinary arguments. [#4847](https://github.com/yandex/ClickHouse/pull/4847) ([Lixiang Qian](https://github.com/fancyqlx))
* Fixed reading from `Array(LowCardinality)` column in rare case when column contained a long sequence of empty arrays. [#4850](https://github.com/yandex/ClickHouse/pull/4850) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Fix crash in `FULL/RIGHT JOIN` when we joining on nullable vs not nullable. [#4855](https://github.com/yandex/ClickHouse/pull/4855) ([Artem Zuikov](https://github.com/4ertus2))
* Fix `No message received` exception while fetching parts between replicas. [#4856](https://github.com/yandex/ClickHouse/pull/4856) ([alesapin](https://github.com/alesapin))
* Fixed `arrayIntersect` function wrong result in case of several repeated values in single array. [#4871](https://github.com/yandex/ClickHouse/pull/4871) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Fix a race condition during concurrent `ALTER COLUMN` queries that could lead to a server crash (fixes issue [#3421](https://github.com/yandex/ClickHouse/issues/3421)). [#4592](https://github.com/yandex/ClickHouse/pull/4592) ([Alex Zatelepin](https://github.com/ztlpn))
* Fix incorrect result in `FULL/RIGHT JOIN` with const column. [#4723](https://github.com/yandex/ClickHouse/pull/4723) ([Artem Zuikov](https://github.com/4ertus2))
* Fix duplicates in `GLOBAL JOIN` with asterisk. [#4705](https://github.com/yandex/ClickHouse/pull/4705) ([Artem Zuikov](https://github.com/4ertus2))
* Fix parameter deduction in `ALTER MODIFY` of column `CODEC` when column type is not specified. [#4883](https://github.com/yandex/ClickHouse/pull/4883) ([alesapin](https://github.com/alesapin))
* Functions `cutQueryStringAndFragment()` and `queryStringAndFragment()` now works correctly when `URL` contains a fragment and no query. [#4894](https://github.com/yandex/ClickHouse/pull/4894) ([Vitaly Baranov](https://github.com/vitlibar))
* Fix rare bug when setting `min_bytes_to_use_direct_io` is greater than zero, which occures when thread have to seek backward in column file. [#4897](https://github.com/yandex/ClickHouse/pull/4897) ([alesapin](https://github.com/alesapin))
* Fix wrong argument types for aggregate functions with `LowCardinality` arguments (fixes issue [#4919](https://github.com/yandex/ClickHouse/issues/4919)). [#4922](https://github.com/yandex/ClickHouse/pull/4922) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Fix wrong name qualification in `GLOBAL JOIN`. [#4969](https://github.com/yandex/ClickHouse/pull/4969) ([Artem Zuikov](https://github.com/4ertus2))
* Fix function `toISOWeek` result for year 1970. [#4988](https://github.com/yandex/ClickHouse/pull/4988) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix `DROP`, `TRUNCATE` and `OPTIMIZE` queries duplication, when executed on `ON CLUSTER` for `ReplicatedMergeTree*` tables family. [#4991](https://github.com/yandex/ClickHouse/pull/4991) ([alesapin](https://github.com/alesapin))

### Backward Incompatible Change

* Rename setting `insert_sample_with_metadata` to setting `input_format_defaults_for_omitted_fields`. [#4771](https://github.com/yandex/ClickHouse/pull/4771) ([Artem Zuikov](https://github.com/4ertus2))
* Added setting `max_partitions_per_insert_block` (with value 100 by default). If inserted block contains larger number of partitions, an exception is thrown. Set it to 0 if you want to remove the limit (not recommended). [#4845](https://github.com/yandex/ClickHouse/pull/4845) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Multi-search functions were renamed (`multiPosition` to `multiSearchAllPositions`, `multiSearch` to `multiSearchAny`, `firstMatch` to `multiSearchFirstIndex`). [#4780](https://github.com/yandex/ClickHouse/pull/4780) ([Danila Kutenin](https://github.com/danlark1))

### Performance Improvement

* Optimize Volnitsky searcher by inlining, giving about 5-10% search improvement for queries with many needles or many similar bigrams. [#4862](https://github.com/yandex/ClickHouse/pull/4862) ([Danila Kutenin](https://github.com/danlark1))
* Fix performance issue when setting `use_uncompressed_cache` is greater than zero, which appeared when all read data contained in cache. [#4913](https://github.com/yandex/ClickHouse/pull/4913) ([alesapin](https://github.com/alesapin))


### Build/Testing/Packaging Improvement

* Hardening debug build: more granular memory mappings and ASLR; add memory protection for mark cache and index. This allows to find more memory stomping bugs in case when ASan and MSan cannot do it. [#4632](https://github.com/yandex/ClickHouse/pull/4632) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Add support for cmake variables `ENABLE_PROTOBUF`, `ENABLE_PARQUET` and `ENABLE_BROTLI` which allows to enable/disable the above features (same as we can do for librdkafka, mysql, etc). [#4669](https://github.com/yandex/ClickHouse/pull/4669) ([Silviu Caragea](https://github.com/silviucpp))
* Add ability to print process list and stacktraces of all threads if some queries are hung after test run. [#4675](https://github.com/yandex/ClickHouse/pull/4675) ([alesapin](https://github.com/alesapin))
* Add retries on `Connection loss` error in `clickhouse-test`. [#4682](https://github.com/yandex/ClickHouse/pull/4682) ([alesapin](https://github.com/alesapin))
* Add freebsd build with vagrant and build with thread sanitizer to packager script. [#4712](https://github.com/yandex/ClickHouse/pull/4712) [#4748](https://github.com/yandex/ClickHouse/pull/4748) ([alesapin](https://github.com/alesapin))
* Now user asked for password for user `'default'` during installation. [#4725](https://github.com/yandex/ClickHouse/pull/4725) ([proller](https://github.com/proller))
* Suppress warning in `rdkafka` library. [#4740](https://github.com/yandex/ClickHouse/pull/4740) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Allow ability to build without ssl. [#4750](https://github.com/yandex/ClickHouse/pull/4750) ([proller](https://github.com/proller))
* Add a way to launch clickhouse-server image from a custom user. [#4753](https://github.com/yandex/ClickHouse/pull/4753) ([Mikhail f. Shiryaev](https://github.com/Felixoid))
* Upgrade contrib boost to 1.69. [#4793](https://github.com/yandex/ClickHouse/pull/4793) ([proller](https://github.com/proller))
* Disable usage of `mremap` when compiled with Thread Sanitizer. Surprisingly enough, TSan does not intercept `mremap` (though it does intercept `mmap`, `munmap`) that leads to false positives. Fixed TSan report in stateful tests. [#4859](https://github.com/yandex/ClickHouse/pull/4859) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Add test checking using format schema via HTTP interface. [#4864](https://github.com/yandex/ClickHouse/pull/4864) ([Vitaly Baranov](https://github.com/vitlibar))

## ClickHouse release 19.4.4.33, 2019-04-17

### Bug Fixes

* Avoid `std::terminate` in case of memory allocation failure. Now `std::bad_alloc` exception is thrown as expected. [#4665](https://github.com/yandex/ClickHouse/pull/4665) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixes capnproto reading from buffer. Sometimes files wasn't loaded successfully by HTTP. [#4674](https://github.com/yandex/ClickHouse/pull/4674) ([Vladislav](https://github.com/smirnov-vs))
* Fix error `Unknown log entry type: 0` after `OPTIMIZE TABLE FINAL` query. [#4683](https://github.com/yandex/ClickHouse/pull/4683) ([Amos Bird](https://github.com/amosbird))
* Wrong arguments to `hasAny` or `hasAll` functions may lead to segfault. [#4698](https://github.com/yandex/ClickHouse/pull/4698) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Deadlock may happen while executing `DROP DATABASE dictionary` query. [#4701](https://github.com/yandex/ClickHouse/pull/4701) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix undefinied behavior in `median` and `quantile` functions. [#4702](https://github.com/yandex/ClickHouse/pull/4702) ([hcz](https://github.com/hczhcz))
* Fix compression level detection when `network_compression_method` in lowercase. Broken in v19.1. [#4706](https://github.com/yandex/ClickHouse/pull/4706) ([proller](https://github.com/proller))
* Fixed ignorance of `<timezone>UTC</timezone>` setting (fixes issue [#4658](https://github.com/yandex/ClickHouse/issues/4658)). [#4718](https://github.com/yandex/ClickHouse/pull/4718) ([proller](https://github.com/proller))
* Fix `histogram` function behaviour with `Distributed` tables. [#4741](https://github.com/yandex/ClickHouse/pull/4741) ([olegkv](https://github.com/olegkv))
* Fixed tsan report `destroy of a locked mutex`. [#4742](https://github.com/yandex/ClickHouse/pull/4742) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed TSan report on shutdown due to race condition in system logs usage. Fixed potential use-after-free on shutdown when part_log is enabled. [#4758](https://github.com/yandex/ClickHouse/pull/4758) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix recheck parts in `ReplicatedMergeTreeAlterThread` in case of error. [#4772](https://github.com/yandex/ClickHouse/pull/4772) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Arithmetic operations on intermediate aggregate function states were not working for constant arguments (such as subquery results). [#4776](https://github.com/yandex/ClickHouse/pull/4776) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Always backquote column names in metadata. Otherwise it's impossible to create a table with column named `index` (server won't restart due to malformed `ATTACH` query in metadata). [#4782](https://github.com/yandex/ClickHouse/pull/4782) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix crash in `ALTER ... MODIFY ORDER BY` on `Distributed` table. [#4790](https://github.com/yandex/ClickHouse/pull/4790) ([TCeason](https://github.com/TCeason))
* Fix segfault in `JOIN ON` with enabled `enable_optimize_predicate_expression`. [#4794](https://github.com/yandex/ClickHouse/pull/4794) ([Winter Zhang](https://github.com/zhang2014))
* Fix bug with adding an extraneous row after consuming a protobuf message from Kafka. [#4808](https://github.com/yandex/ClickHouse/pull/4808) ([Vitaly Baranov](https://github.com/vitlibar))
* Fix segmentation fault in `clickhouse-copier`. [#4835](https://github.com/yandex/ClickHouse/pull/4835) ([proller](https://github.com/proller))
* Fixed race condition in `SELECT` from `system.tables` if the table is renamed or altered concurrently. [#4836](https://github.com/yandex/ClickHouse/pull/4836) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed data race when fetching data part that is already obsolete. [#4839](https://github.com/yandex/ClickHouse/pull/4839) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed rare data race that can happen during `RENAME` table of MergeTree family. [#4844](https://github.com/yandex/ClickHouse/pull/4844) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed segmentation fault in function `arrayIntersect`. Segmentation fault could happen if function was called with mixed constant and ordinary arguments. [#4847](https://github.com/yandex/ClickHouse/pull/4847) ([Lixiang Qian](https://github.com/fancyqlx))
* Fixed reading from `Array(LowCardinality)` column in rare case when column contained a long sequence of empty arrays. [#4850](https://github.com/yandex/ClickHouse/pull/4850) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Fix `No message received` exception while fetching parts between replicas. [#4856](https://github.com/yandex/ClickHouse/pull/4856) ([alesapin](https://github.com/alesapin))
* Fixed `arrayIntersect` function wrong result in case of several repeated values in single array. [#4871](https://github.com/yandex/ClickHouse/pull/4871) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Fix a race condition during concurrent `ALTER COLUMN` queries that could lead to a server crash (fixes issue [#3421](https://github.com/yandex/ClickHouse/issues/3421)). [#4592](https://github.com/yandex/ClickHouse/pull/4592) ([Alex Zatelepin](https://github.com/ztlpn))
* Fix parameter deduction in `ALTER MODIFY` of column `CODEC` when column type is not specified. [#4883](https://github.com/yandex/ClickHouse/pull/4883) ([alesapin](https://github.com/alesapin))
* Functions `cutQueryStringAndFragment()` and `queryStringAndFragment()` now works correctly when `URL` contains a fragment and no query. [#4894](https://github.com/yandex/ClickHouse/pull/4894) ([Vitaly Baranov](https://github.com/vitlibar))
* Fix rare bug when setting `min_bytes_to_use_direct_io` is greater than zero, which occures when thread have to seek backward in column file. [#4897](https://github.com/yandex/ClickHouse/pull/4897) ([alesapin](https://github.com/alesapin))
* Fix wrong argument types for aggregate functions with `LowCardinality` arguments (fixes issue [#4919](https://github.com/yandex/ClickHouse/issues/4919)). [#4922](https://github.com/yandex/ClickHouse/pull/4922) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Fix function `toISOWeek` result for year 1970. [#4988](https://github.com/yandex/ClickHouse/pull/4988) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix `DROP`, `TRUNCATE` and `OPTIMIZE` queries duplication, when executed on `ON CLUSTER` for `ReplicatedMergeTree*` tables family. [#4991](https://github.com/yandex/ClickHouse/pull/4991) ([alesapin](https://github.com/alesapin))

### Improvements

* Keep ordinary, `DEFAULT`, `MATERIALIZED` and `ALIAS` columns in a single list (fixes issue [#2867](https://github.com/yandex/ClickHouse/issues/2867)). [#4707](https://github.com/yandex/ClickHouse/pull/4707) ([Alex Zatelepin](https://github.com/ztlpn))

## ClickHouse release 19.4.3.11, 2019-04-02

### Bug Fixes

* Fix crash in `FULL/RIGHT JOIN` when we joining on nullable vs not nullable. [#4855](https://github.com/yandex/ClickHouse/pull/4855) ([Artem Zuikov](https://github.com/4ertus2))
* Fix segmentation fault in `clickhouse-copier`. [#4835](https://github.com/yandex/ClickHouse/pull/4835) ([proller](https://github.com/proller))

### Build/Testing/Packaging Improvement

* Add a way to launch clickhouse-server image from a custom user. [#4753](https://github.com/yandex/ClickHouse/pull/4753) ([Mikhail f. Shiryaev](https://github.com/Felixoid))

## ClickHouse release 19.4.2.7, 2019-03-30

### Bug Fixes
* Fixed reading from `Array(LowCardinality)` column in rare case when column contained a long sequence of empty arrays. [#4850](https://github.com/yandex/ClickHouse/pull/4850) ([Nikolai Kochetov](https://github.com/KochetovNicolai))

## ClickHouse release 19.4.1.3, 2019-03-19

### Bug Fixes
* Fixed remote queries which contain both `LIMIT BY` and `LIMIT`. Previously, if `LIMIT BY` and `LIMIT` were used for remote query, `LIMIT` could happen before `LIMIT BY`, which led to too filtered result. [#4708](https://github.com/yandex/ClickHouse/pull/4708) ([Constantin S. Pan](https://github.com/kvap))

## ClickHouse release 19.4.0.49, 2019-03-09

### New Features
* Added full support for `Protobuf` format (input and output, nested data structures). [#4174](https://github.com/yandex/ClickHouse/pull/4174) [#4493](https://github.com/yandex/ClickHouse/pull/4493) ([Vitaly Baranov](https://github.com/vitlibar))
* Added bitmap functions with Roaring Bitmaps. [#4207](https://github.com/yandex/ClickHouse/pull/4207) ([Andy Yang](https://github.com/andyyzh)) [#4568](https://github.com/yandex/ClickHouse/pull/4568) ([Vitaly Baranov](https://github.com/vitlibar))
* Parquet format support. [#4448](https://github.com/yandex/ClickHouse/pull/4448) ([proller](https://github.com/proller))
* N-gram distance was added for fuzzy string comparison. It is similar to q-gram metrics in R language. [#4466](https://github.com/yandex/ClickHouse/pull/4466) ([Danila Kutenin](https://github.com/danlark1))
* Combine rules for graphite rollup from dedicated aggregation and retention patterns. [#4426](https://github.com/yandex/ClickHouse/pull/4426) ([Mikhail f. Shiryaev](https://github.com/Felixoid))
* Added `max_execution_speed` and `max_execution_speed_bytes` to limit resource usage. Added `min_execution_speed_bytes` setting to complement the `min_execution_speed`. [#4430](https://github.com/yandex/ClickHouse/pull/4430) ([Winter Zhang](https://github.com/zhang2014))
* Implemented function `flatten`. [#4555](https://github.com/yandex/ClickHouse/pull/4555) [#4409](https://github.com/yandex/ClickHouse/pull/4409) ([alexey-milovidov](https://github.com/alexey-milovidov), [kzon](https://github.com/kzon))
* Added functions `arrayEnumerateDenseRanked` and `arrayEnumerateUniqRanked` (it's like `arrayEnumerateUniq` but allows to fine tune array depth to look inside multidimensional arrays). [#4475](https://github.com/yandex/ClickHouse/pull/4475) ([proller](https://github.com/proller)) [#4601](https://github.com/yandex/ClickHouse/pull/4601) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Multiple JOINS with some restrictions: no asterisks, no complex aliases in ON/WHERE/GROUP BY/... [#4462](https://github.com/yandex/ClickHouse/pull/4462) ([Artem Zuikov](https://github.com/4ertus2))

### Bug Fixes
* This release also contains all bug fixes from 19.3 and 19.1.
* Fixed bug in data skipping indices: order of granules after INSERT was incorrect. [#4407](https://github.com/yandex/ClickHouse/pull/4407) ([Nikita Vasilev](https://github.com/nikvas0))
* Fixed `set` index for `Nullable` and `LowCardinality` columns. Before it, `set` index with `Nullable` or `LowCardinality` column led to error `Data type must be deserialized with multiple streams` while selecting. [#4594](https://github.com/yandex/ClickHouse/pull/4594) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Correctly set update_time on full `executable` dictionary update. [#4551](https://github.com/yandex/ClickHouse/pull/4551) ([Tema Novikov](https://github.com/temoon))
* Fix broken progress bar in 19.3. [#4627](https://github.com/yandex/ClickHouse/pull/4627) ([filimonov](https://github.com/filimonov))
* Fixed inconsistent values of MemoryTracker when memory region was shrinked, in certain cases. [#4619](https://github.com/yandex/ClickHouse/pull/4619) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed undefined behaviour in ThreadPool. [#4612](https://github.com/yandex/ClickHouse/pull/4612) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed a very rare crash with the message `mutex lock failed: Invalid argument` that could happen when a MergeTree table was dropped concurrently with a SELECT. [#4608](https://github.com/yandex/ClickHouse/pull/4608) ([Alex Zatelepin](https://github.com/ztlpn))
* ODBC driver compatibility with `LowCardinality` data type. [#4381](https://github.com/yandex/ClickHouse/pull/4381) ([proller](https://github.com/proller))
* FreeBSD: Fixup for `AIOcontextPool: Found io_event with unknown id 0` error. [#4438](https://github.com/yandex/ClickHouse/pull/4438) ([urgordeadbeef](https://github.com/urgordeadbeef))
* `system.part_log` table was created regardless to configuration. [#4483](https://github.com/yandex/ClickHouse/pull/4483) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix undefined behaviour in `dictIsIn` function for cache dictionaries. [#4515](https://github.com/yandex/ClickHouse/pull/4515) ([alesapin](https://github.com/alesapin))
* Fixed a deadlock when a SELECT query locks the same table multiple times (e.g. from different threads or when executing multiple subqueries) and there is a concurrent DDL query. [#4535](https://github.com/yandex/ClickHouse/pull/4535) ([Alex Zatelepin](https://github.com/ztlpn))
* Disable compile_expressions by default until we get own `llvm` contrib and can test it with `clang` and `asan`. [#4579](https://github.com/yandex/ClickHouse/pull/4579) ([alesapin](https://github.com/alesapin))
* Prevent `std::terminate` when `invalidate_query` for `clickhouse` external dictionary source has returned wrong resultset (empty or more than one row or more than one column). Fixed issue when the `invalidate_query` was performed every five seconds regardless to the `lifetime`. [#4583](https://github.com/yandex/ClickHouse/pull/4583) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Avoid deadlock when the `invalidate_query` for a dictionary with `clickhouse` source was involving `system.dictionaries` table or `Dictionaries` database (rare case). [#4599](https://github.com/yandex/ClickHouse/pull/4599) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixes for CROSS JOIN with empty WHERE. [#4598](https://github.com/yandex/ClickHouse/pull/4598) ([Artem Zuikov](https://github.com/4ertus2))
* Fixed segfault in function "replicate" when constant argument is passed. [#4603](https://github.com/yandex/ClickHouse/pull/4603) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix lambda function with predicate optimizer. [#4408](https://github.com/yandex/ClickHouse/pull/4408) ([Winter Zhang](https://github.com/zhang2014))
* Multiple JOINs multiple fixes. [#4595](https://github.com/yandex/ClickHouse/pull/4595) ([Artem Zuikov](https://github.com/4ertus2))

### Improvements
* Support aliases in JOIN ON section for right table columns. [#4412](https://github.com/yandex/ClickHouse/pull/4412) ([Artem Zuikov](https://github.com/4ertus2))
* Result of multiple JOINs need correct result names to be used in subselects. Replace flat aliases with source names in result. [#4474](https://github.com/yandex/ClickHouse/pull/4474) ([Artem Zuikov](https://github.com/4ertus2))
* Improve push-down logic for joined statements. [#4387](https://github.com/yandex/ClickHouse/pull/4387) ([Ivan](https://github.com/abyss7))

### Performance Improvements
* Improved heuristics of "move to PREWHERE" optimization. [#4405](https://github.com/yandex/ClickHouse/pull/4405) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Use proper lookup tables that uses HashTable's API for 8-bit and 16-bit keys. [#4536](https://github.com/yandex/ClickHouse/pull/4536) ([Amos Bird](https://github.com/amosbird))
* Improved performance of string comparison. [#4564](https://github.com/yandex/ClickHouse/pull/4564) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Cleanup distributed DDL queue in a separate thread so that it doesn't slow down the main loop that processes distributed DDL tasks. [#4502](https://github.com/yandex/ClickHouse/pull/4502) ([Alex Zatelepin](https://github.com/ztlpn))
* When `min_bytes_to_use_direct_io` is set to 1, not every file was opened with O_DIRECT mode because the data size to read was sometimes underestimated by the size of one compressed block. [#4526](https://github.com/yandex/ClickHouse/pull/4526) ([alexey-milovidov](https://github.com/alexey-milovidov))

### Build/Testing/Packaging Improvement
* Added support for clang-9 [#4604](https://github.com/yandex/ClickHouse/pull/4604) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix wrong `__asm__` instructions (again) [#4621](https://github.com/yandex/ClickHouse/pull/4621) ([Konstantin Podshumok](https://github.com/podshumok))
* Add ability to specify settings for `clickhouse-performance-test` from command line. [#4437](https://github.com/yandex/ClickHouse/pull/4437) ([alesapin](https://github.com/alesapin))
* Add dictionaries tests to integration tests. [#4477](https://github.com/yandex/ClickHouse/pull/4477) ([alesapin](https://github.com/alesapin))
* Added queries from the benchmark on the website to automated performance tests. [#4496](https://github.com/yandex/ClickHouse/pull/4496) ([alexey-milovidov](https://github.com/alexey-milovidov))
* `xxhash.h` does not exist in external lz4 because it is an implementation detail and its symbols are namespaced with `XXH_NAMESPACE` macro.  When lz4 is external, xxHash has to be external too, and the dependents have to link to it. [#4495](https://github.com/yandex/ClickHouse/pull/4495) ([Orivej Desh](https://github.com/orivej))
* Fixed a case when `quantileTiming` aggregate function can be called with negative or floating point argument (this fixes fuzz test with undefined behaviour sanitizer). [#4506](https://github.com/yandex/ClickHouse/pull/4506) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Spelling error correction. [#4531](https://github.com/yandex/ClickHouse/pull/4531) ([sdk2](https://github.com/sdk2))
* Fix compilation on Mac. [#4371](https://github.com/yandex/ClickHouse/pull/4371) ([Vitaly Baranov](https://github.com/vitlibar))
* Build fixes for FreeBSD and various unusual build configurations. [#4444](https://github.com/yandex/ClickHouse/pull/4444) ([proller](https://github.com/proller))

## ClickHouse release 19.3.9.1, 2019-04-02

### Bug Fixes

* Fix crash in `FULL/RIGHT JOIN` when we joining on nullable vs not nullable. [#4855](https://github.com/yandex/ClickHouse/pull/4855) ([Artem Zuikov](https://github.com/4ertus2))
* Fix segmentation fault in `clickhouse-copier`. [#4835](https://github.com/yandex/ClickHouse/pull/4835) ([proller](https://github.com/proller))
* Fixed reading from `Array(LowCardinality)` column in rare case when column contained a long sequence of empty arrays. [#4850](https://github.com/yandex/ClickHouse/pull/4850) ([Nikolai Kochetov](https://github.com/KochetovNicolai))

### Build/Testing/Packaging Improvement

* Add a way to launch clickhouse-server image from a custom user [#4753](https://github.com/yandex/ClickHouse/pull/4753) ([Mikhail f. Shiryaev](https://github.com/Felixoid))


## ClickHouse release 19.3.7, 2019-03-12

### Bug fixes

* Fixed error in #3920. This error manifestate itself as random cache corruption (messages `Unknown codec family code`, `Cannot seek through file`) and segfaults. This bug first appeared in version 19.1 and is present in versions up to 19.1.10 and 19.3.6. [#4623](https://github.com/yandex/ClickHouse/pull/4623) ([alexey-milovidov](https://github.com/alexey-milovidov))


## ClickHouse release 19.3.6, 2019-03-02

### Bug fixes

* When there are more than 1000 threads in a thread pool, `std::terminate` may happen on thread exit. [Azat Khuzhin](https://github.com/azat) [#4485](https://github.com/yandex/ClickHouse/pull/4485) [#4505](https://github.com/yandex/ClickHouse/pull/4505) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Now it's possible to create `ReplicatedMergeTree*` tables with comments on columns without defaults and tables with columns codecs without comments and defaults. Also fix comparison of codecs. [#4523](https://github.com/yandex/ClickHouse/pull/4523) ([alesapin](https://github.com/alesapin))
* Fixed crash on JOIN with array or tuple. [#4552](https://github.com/yandex/ClickHouse/pull/4552) ([Artem Zuikov](https://github.com/4ertus2))
* Fixed crash in clickhouse-copier with the message `ThreadStatus not created`. [#4540](https://github.com/yandex/ClickHouse/pull/4540) ([Artem Zuikov](https://github.com/4ertus2))
* Fixed hangup on server shutdown if distributed DDLs were used. [#4472](https://github.com/yandex/ClickHouse/pull/4472) ([Alex Zatelepin](https://github.com/ztlpn))
* Incorrect column numbers were printed in error message about text format parsing for columns with number greater than 10. [#4484](https://github.com/yandex/ClickHouse/pull/4484) ([alexey-milovidov](https://github.com/alexey-milovidov))

### Build/Testing/Packaging Improvements

* Fixed build with AVX enabled. [#4527](https://github.com/yandex/ClickHouse/pull/4527) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Enable extended accounting and IO accounting based on good known version instead of kernel under which it is compiled. [#4541](https://github.com/yandex/ClickHouse/pull/4541) ([nvartolomei](https://github.com/nvartolomei))
* Allow to skip setting of core_dump.size_limit, warning instead of throw if limit set fail. [#4473](https://github.com/yandex/ClickHouse/pull/4473) ([proller](https://github.com/proller))
* Removed the `inline` tags of `void readBinary(...)` in `Field.cpp`. Also merged redundant `namespace DB` blocks. [#4530](https://github.com/yandex/ClickHouse/pull/4530) ([hcz](https://github.com/hczhcz))


## ClickHouse release 19.3.5, 2019-02-21

### Bug fixes
* Fixed bug with large http insert queries processing. [#4454](https://github.com/yandex/ClickHouse/pull/4454) ([alesapin](https://github.com/alesapin))
* Fixed backward incompatibility with old versions due to wrong implementation of `send_logs_level` setting. [#4445](https://github.com/yandex/ClickHouse/pull/4445) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed backward incompatibility of table function `remote` introduced with column comments. [#4446](https://github.com/yandex/ClickHouse/pull/4446) ([alexey-milovidov](https://github.com/alexey-milovidov))

## ClickHouse release 19.3.4, 2019-02-16

### Improvements
* Table index size is not accounted for memory limits when doing `ATTACH TABLE` query. Avoided the possibility that a table cannot be attached after being detached. [#4396](https://github.com/yandex/ClickHouse/pull/4396) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Slightly raised up the limit on max string and array size received from ZooKeeper. It allows to continue to work with increased size of `CLIENT_JVMFLAGS=-Djute.maxbuffer=...` on ZooKeeper. [#4398](https://github.com/yandex/ClickHouse/pull/4398) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Allow to repair abandoned replica even if it already has huge number of nodes in its queue. [#4399](https://github.com/yandex/ClickHouse/pull/4399) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Add one required argument to `SET` index (max stored rows number). [#4386](https://github.com/yandex/ClickHouse/pull/4386) ([Nikita Vasilev](https://github.com/nikvas0))

### Bug Fixes
* Fixed `WITH ROLLUP` result for group by single `LowCardinality` key. [#4384](https://github.com/yandex/ClickHouse/pull/4384) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Fixed bug in the set index (dropping a granule if it contains more than `max_rows` rows). [#4386](https://github.com/yandex/ClickHouse/pull/4386) ([Nikita Vasilev](https://github.com/nikvas0))
* A lot of FreeBSD build fixes. [#4397](https://github.com/yandex/ClickHouse/pull/4397) ([proller](https://github.com/proller))
* Fixed aliases substitution in queries with subquery containing same alias (issue [#4110](https://github.com/yandex/ClickHouse/issues/4110)). [#4351](https://github.com/yandex/ClickHouse/pull/4351) ([Artem Zuikov](https://github.com/4ertus2))

### Build/Testing/Packaging Improvements
* Add ability to run `clickhouse-server` for stateless tests in docker image. [#4347](https://github.com/yandex/ClickHouse/pull/4347) ([Vasily Nemkov](https://github.com/Enmk))

## ClickHouse release 19.3.3, 2019-02-13

### New Features
* Added the `KILL MUTATION` statement that allows removing mutations that are for some reasons stuck. Added `latest_failed_part`, `latest_fail_time`, `latest_fail_reason` fields to the `system.mutations` table for easier troubleshooting. [#4287](https://github.com/yandex/ClickHouse/pull/4287) ([Alex Zatelepin](https://github.com/ztlpn))
* Added aggregate function `entropy` which computes Shannon entropy. [#4238](https://github.com/yandex/ClickHouse/pull/4238) ([Quid37](https://github.com/Quid37))
* Added ability to send queries `INSERT INTO tbl VALUES (....` to server without splitting on `query` and `data` parts. [#4301](https://github.com/yandex/ClickHouse/pull/4301) ([alesapin](https://github.com/alesapin))
* Generic implementation of `arrayWithConstant` function was added. [#4322](https://github.com/yandex/ClickHouse/pull/4322) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Implented `NOT BETWEEN` comparison operator. [#4228](https://github.com/yandex/ClickHouse/pull/4228) ([Dmitry Naumov](https://github.com/nezed))
* Implement `sumMapFiltered` in order to be able to limit the number of keys for which values will be summed by `sumMap`. [#4129](https://github.com/yandex/ClickHouse/pull/4129) ([Léo Ercolanelli](https://github.com/ercolanelli-leo))
* Added support of `Nullable` types in `mysql` table function. [#4198](https://github.com/yandex/ClickHouse/pull/4198) ([Emmanuel Donin de Rosière](https://github.com/edonin))
* Support for arbitrary constant expressions in `LIMIT` clause. [#4246](https://github.com/yandex/ClickHouse/pull/4246) ([k3box](https://github.com/k3box))
* Added `topKWeighted` aggregate function that takes additional argument with (unsigned integer) weight. [#4245](https://github.com/yandex/ClickHouse/pull/4245) ([Andrew Golman](https://github.com/andrewgolman))
* `StorageJoin` now supports `join_overwrite` setting that allows overwriting existing values of the same key. [#3973](https://github.com/yandex/ClickHouse/pull/3973) ([Amos Bird](https://github.com/amosbird)
* Added function `toStartOfInterval`. [#4304](https://github.com/yandex/ClickHouse/pull/4304) ([Vitaly Baranov](https://github.com/vitlibar))
* Added `RowBinaryWithNamesAndTypes` format. [#4200](https://github.com/yandex/ClickHouse/pull/4200) ([Oleg V. Kozlyuk](https://github.com/DarkWanderer))
* Added `IPv4` and `IPv6` data types. More effective implementations of `IPv*` functions. [#3669](https://github.com/yandex/ClickHouse/pull/3669) ([Vasily Nemkov](https://github.com/Enmk))
* Added function `toStartOfTenMinutes()`. [#4298](https://github.com/yandex/ClickHouse/pull/4298) ([Vitaly Baranov](https://github.com/vitlibar))
* Added `Protobuf` output format. [#4005](https://github.com/yandex/ClickHouse/pull/4005) [#4158](https://github.com/yandex/ClickHouse/pull/4158) ([Vitaly Baranov](https://github.com/vitlibar))
* Added brotli support for HTTP interface for data import (INSERTs). [#4235](https://github.com/yandex/ClickHouse/pull/4235) ([Mikhail ](https://github.com/fandyushin))
* Added hints while user make typo in function name or type in command line client. [#4239](https://github.com/yandex/ClickHouse/pull/4239) ([Danila Kutenin](https://github.com/danlark1))
* Added `Query-Id` to Server's HTTP Response header. [#4231](https://github.com/yandex/ClickHouse/pull/4231) ([Mikhail ](https://github.com/fandyushin))

### Experimental features
* Added `minmax` and `set` data skipping indices for MergeTree table engines family. [#4143](https://github.com/yandex/ClickHouse/pull/4143) ([Nikita Vasilev](https://github.com/nikvas0))
* Added conversion of `CROSS JOIN` to `INNER JOIN` if possible. [#4221](https://github.com/yandex/ClickHouse/pull/4221) [#4266](https://github.com/yandex/ClickHouse/pull/4266) ([Artem Zuikov](https://github.com/4ertus2))

### Bug Fixes
* Fixed `Not found column` for duplicate columns in `JOIN ON` section. [#4279](https://github.com/yandex/ClickHouse/pull/4279) ([Artem Zuikov](https://github.com/4ertus2))
* Make `START REPLICATED SENDS` command start replicated sends. [#4229](https://github.com/yandex/ClickHouse/pull/4229) ([nvartolomei](https://github.com/nvartolomei))
* Fixed aggregate functions execution with `Array(LowCardinality)` arguments. [#4055](https://github.com/yandex/ClickHouse/pull/4055) ([KochetovNicolai](https://github.com/KochetovNicolai))
* Fixed wrong behaviour when doing `INSERT ... SELECT ... FROM file(...)` query and file has `CSVWithNames` or `TSVWIthNames` format and the first data row is missing. [#4297](https://github.com/yandex/ClickHouse/pull/4297) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed crash on dictionary reload if dictionary not available. This bug was appeared in 19.1.6. [#4188](https://github.com/yandex/ClickHouse/pull/4188) ([proller](https://github.com/proller))
* Fixed `ALL JOIN` with duplicates in right table. [#4184](https://github.com/yandex/ClickHouse/pull/4184) ([Artem Zuikov](https://github.com/4ertus2))
* Fixed segmentation fault with `use_uncompressed_cache=1` and exception with wrong uncompressed size. This bug was appeared in 19.1.6. [#4186](https://github.com/yandex/ClickHouse/pull/4186) ([alesapin](https://github.com/alesapin))
* Fixed `compile_expressions` bug with comparison of big (more than int16) dates. [#4341](https://github.com/yandex/ClickHouse/pull/4341) ([alesapin](https://github.com/alesapin))
* Fixed infinite loop when selecting from table function `numbers(0)`. [#4280](https://github.com/yandex/ClickHouse/pull/4280) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Temporarily disable predicate optimization for `ORDER BY`. [#3890](https://github.com/yandex/ClickHouse/pull/3890) ([Winter Zhang](https://github.com/zhang2014))
* Fixed `Illegal instruction` error when using base64 functions on old CPUs. This error has been reproduced only when ClickHouse was compiled with gcc-8. [#4275](https://github.com/yandex/ClickHouse/pull/4275) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed `No message received` error when interacting with PostgreSQL ODBC Driver through TLS connection. Also fixes segfault when using MySQL ODBC Driver. [#4170](https://github.com/yandex/ClickHouse/pull/4170) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed incorrect result when `Date` and `DateTime` arguments are used in branches of conditional operator (function `if`). Added generic case for function `if`. [#4243](https://github.com/yandex/ClickHouse/pull/4243) ([alexey-milovidov](https://github.com/alexey-milovidov))
* ClickHouse dictionaries now load within `clickhouse` process. [#4166](https://github.com/yandex/ClickHouse/pull/4166) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed deadlock when `SELECT` from a table with `File` engine was retried after `No such file or directory` error. [#4161](https://github.com/yandex/ClickHouse/pull/4161) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed race condition when selecting from `system.tables` may give `table doesn't exist` error. [#4313](https://github.com/yandex/ClickHouse/pull/4313) ([alexey-milovidov](https://github.com/alexey-milovidov))
* `clickhouse-client` can segfault on exit while loading data for command line suggestions if it was run in interactive mode. [#4317](https://github.com/yandex/ClickHouse/pull/4317) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed a bug when the execution of mutations containing `IN` operators was producing incorrect results. [#4099](https://github.com/yandex/ClickHouse/pull/4099) ([Alex Zatelepin](https://github.com/ztlpn))
* Fixed error: if there is a database with `Dictionary` engine, all dictionaries forced to load at server startup, and if there is a dictionary with ClickHouse source from localhost, the dictionary cannot load. [#4255](https://github.com/yandex/ClickHouse/pull/4255) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed error when system logs are tried to create again at server shutdown. [#4254](https://github.com/yandex/ClickHouse/pull/4254) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Correctly return the right type and properly handle locks in `joinGet` function. [#4153](https://github.com/yandex/ClickHouse/pull/4153) ([Amos Bird](https://github.com/amosbird))
* Added `sumMapWithOverflow` function. [#4151](https://github.com/yandex/ClickHouse/pull/4151) ([Léo Ercolanelli](https://github.com/ercolanelli-leo))
* Fixed segfault with `allow_experimental_multiple_joins_emulation`. [52de2c](https://github.com/yandex/ClickHouse/commit/52de2cd927f7b5257dd67e175f0a5560a48840d0) ([Artem Zuikov](https://github.com/4ertus2))
* Fixed bug with incorrect `Date` and `DateTime` comparison. [#4237](https://github.com/yandex/ClickHouse/pull/4237) ([valexey](https://github.com/valexey))
* Fixed fuzz test under undefined behavior sanitizer: added parameter type check for `quantile*Weighted` family of functions. [#4145](https://github.com/yandex/ClickHouse/pull/4145) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed rare race condition when removing of old data parts can fail with `File not found` error. [#4378](https://github.com/yandex/ClickHouse/pull/4378) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix install package with missing /etc/clickhouse-server/config.xml. [#4343](https://github.com/yandex/ClickHouse/pull/4343) ([proller](https://github.com/proller))


### Build/Testing/Packaging Improvements
* Debian package: correct /etc/clickhouse-server/preprocessed link according to config. [#4205](https://github.com/yandex/ClickHouse/pull/4205) ([proller](https://github.com/proller))
* Various build fixes for FreeBSD. [#4225](https://github.com/yandex/ClickHouse/pull/4225) ([proller](https://github.com/proller))
* Added ability to create, fill and drop tables in perftest. [#4220](https://github.com/yandex/ClickHouse/pull/4220) ([alesapin](https://github.com/alesapin))
* Added a script to check for duplicate includes. [#4326](https://github.com/yandex/ClickHouse/pull/4326) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Added ability to run queries by index in performance test. [#4264](https://github.com/yandex/ClickHouse/pull/4264) ([alesapin](https://github.com/alesapin))
* Package with debug symbols is suggested to be installed. [#4274](https://github.com/yandex/ClickHouse/pull/4274) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Refactoring of performance-test. Better logging and signals handling. [#4171](https://github.com/yandex/ClickHouse/pull/4171) ([alesapin](https://github.com/alesapin))
* Added docs to anonymized Yandex.Metrika datasets. [#4164](https://github.com/yandex/ClickHouse/pull/4164) ([alesapin](https://github.com/alesapin))
* Аdded tool for converting an old month-partitioned part to the custom-partitioned format. [#4195](https://github.com/yandex/ClickHouse/pull/4195) ([Alex Zatelepin](https://github.com/ztlpn))
* Added docs about two datasets in s3. [#4144](https://github.com/yandex/ClickHouse/pull/4144) ([alesapin](https://github.com/alesapin))
* Added script which creates changelog from pull requests description. [#4169](https://github.com/yandex/ClickHouse/pull/4169) [#4173](https://github.com/yandex/ClickHouse/pull/4173) ([KochetovNicolai](https://github.com/KochetovNicolai)) ([KochetovNicolai](https://github.com/KochetovNicolai))
* Added puppet module for Clickhouse. [#4182](https://github.com/yandex/ClickHouse/pull/4182) ([Maxim Fedotov](https://github.com/MaxFedotov))
* Added docs for a group of undocumented functions. [#4168](https://github.com/yandex/ClickHouse/pull/4168) ([Winter Zhang](https://github.com/zhang2014))
* ARM build fixes. [#4210](https://github.com/yandex/ClickHouse/pull/4210)[#4306](https://github.com/yandex/ClickHouse/pull/4306) [#4291](https://github.com/yandex/ClickHouse/pull/4291) ([proller](https://github.com/proller)) ([proller](https://github.com/proller))
* Dictionary tests now able to run from `ctest`.  [#4189](https://github.com/yandex/ClickHouse/pull/4189) ([proller](https://github.com/proller))
* Now `/etc/ssl` is used as default directory with SSL certificates. [#4167](https://github.com/yandex/ClickHouse/pull/4167) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Added checking SSE and AVX instruction at start. [#4234](https://github.com/yandex/ClickHouse/pull/4234) ([Igr](https://github.com/igron99))
* Init script will wait server until start. [#4281](https://github.com/yandex/ClickHouse/pull/4281) ([proller](https://github.com/proller))

### Backward Incompatible Changes
* Removed `allow_experimental_low_cardinality_type` setting. `LowCardinality` data types are production ready. [#4323](https://github.com/yandex/ClickHouse/pull/4323) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Reduce mark cache size and uncompressed cache size accordingly to available memory amount. [#4240](https://github.com/yandex/ClickHouse/pull/4240) ([Lopatin Konstantin](https://github.com/k-lopatin)
* Added keyword `INDEX` in `CREATE TABLE` query. A column with name `index` must be quoted with backticks or double quotes: `` `index` ``.  [#4143](https://github.com/yandex/ClickHouse/pull/4143) ([Nikita Vasilev](https://github.com/nikvas0))
* `sumMap` now promote result type instead of overflow. The old `sumMap` behavior can be obtained by using `sumMapWithOverflow` function. [#4151](https://github.com/yandex/ClickHouse/pull/4151) ([Léo Ercolanelli](https://github.com/ercolanelli-leo))

### Performance Impovements
* `std::sort` replaced by `pdqsort` for queries without `LIMIT`. [#4236](https://github.com/yandex/ClickHouse/pull/4236) ([Evgenii Pravda](https://github.com/kvinty))
* Now server reuse threads from global thread pool. This affects performance in some corner cases. [#4150](https://github.com/yandex/ClickHouse/pull/4150) ([alexey-milovidov](https://github.com/alexey-milovidov))

### Improvements
* Implemented AIO support for FreeBSD. [#4305](https://github.com/yandex/ClickHouse/pull/4305) ([urgordeadbeef](https://github.com/urgordeadbeef))
* `SELECT * FROM a JOIN b USING a, b` now return `a` and `b` columns only from the left table. [#4141](https://github.com/yandex/ClickHouse/pull/4141) ([Artem Zuikov](https://github.com/4ertus2))
* Allow `-C` option of client to work as `-c` option. [#4232](https://github.com/yandex/ClickHouse/pull/4232) ([syominsergey](https://github.com/syominsergey))
* Now option `--password` used without value requires password from stdin. [#4230](https://github.com/yandex/ClickHouse/pull/4230) ([BSD_Conqueror](https://github.com/bsd-conqueror))
* Added highlighting of unescaped metacharacters in string literals that contain `LIKE` expressions or regexps. [#4327](https://github.com/yandex/ClickHouse/pull/4327) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Added cancelling of HTTP read only queries if client socket goes away. [#4213](https://github.com/yandex/ClickHouse/pull/4213) ([nvartolomei](https://github.com/nvartolomei))
* Now server reports progress to keep client connections alive. [#4215](https://github.com/yandex/ClickHouse/pull/4215) ([Ivan](https://github.com/abyss7))
* Slightly better message with reason for OPTIMIZE query with `optimize_throw_if_noop` setting enabled. [#4294](https://github.com/yandex/ClickHouse/pull/4294) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Added support of `--version` option for clickhouse server.  [#4251](https://github.com/yandex/ClickHouse/pull/4251) ([Lopatin Konstantin](https://github.com/k-lopatin))
* Added `--help/-h` option to `clickhouse-server`. [#4233](https://github.com/yandex/ClickHouse/pull/4233) ([Yuriy Baranov](https://github.com/yurriy))
* Added support for scalar subqueries with aggregate function state result. [#4348](https://github.com/yandex/ClickHouse/pull/4348) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Improved server shutdown time and ALTERs waiting time. [#4372](https://github.com/yandex/ClickHouse/pull/4372) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Added info about the replicated_can_become_leader setting to system.replicas and add logging if the replica won't try to become leader. [#4379](https://github.com/yandex/ClickHouse/pull/4379) ([Alex Zatelepin](https://github.com/ztlpn))


## ClickHouse release 19.1.14, 2019-03-14

* Fixed error `Column ... queried more than once` that may happen if the setting `asterisk_left_columns_only` is set to 1 in case of using `GLOBAL JOIN` with `SELECT *` (rare case). The issue does not exist in 19.3 and newer. [6bac7d8d](https://github.com/yandex/ClickHouse/pull/4692/commits/6bac7d8d11a9b0d6de0b32b53c47eb2f6f8e7062) ([Artem Zuikov](https://github.com/4ertus2))

## ClickHouse release 19.1.13, 2019-03-12

This release contains exactly the same set of patches as 19.3.7.

## ClickHouse release 19.1.10, 2019-03-03

This release contains exactly the same set of patches as 19.3.6.


## ClickHouse release 19.1.9, 2019-02-21

### Bug fixes
* Fixed backward incompatibility with old versions due to wrong implementation of `send_logs_level` setting. [#4445](https://github.com/yandex/ClickHouse/pull/4445) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed backward incompatibility of table function `remote` introduced with column comments. [#4446](https://github.com/yandex/ClickHouse/pull/4446) ([alexey-milovidov](https://github.com/alexey-milovidov))

## ClickHouse release 19.1.8, 2019-02-16

### Bug Fixes
* Fix install package with missing /etc/clickhouse-server/config.xml. [#4343](https://github.com/yandex/ClickHouse/pull/4343) ([proller](https://github.com/proller))


## ClickHouse release 19.1.7, 2019-02-15

### Bug Fixes
* Correctly return the right type and properly handle locks in `joinGet` function. [#4153](https://github.com/yandex/ClickHouse/pull/4153) ([Amos Bird](https://github.com/amosbird))
* Fixed error when system logs are tried to create again at server shutdown. [#4254](https://github.com/yandex/ClickHouse/pull/4254) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed error: if there is a database with `Dictionary` engine, all dictionaries forced to load at server startup, and if there is a dictionary with ClickHouse source from localhost, the dictionary cannot load. [#4255](https://github.com/yandex/ClickHouse/pull/4255) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed a bug when the execution of mutations containing `IN` operators was producing incorrect results. [#4099](https://github.com/yandex/ClickHouse/pull/4099) ([Alex Zatelepin](https://github.com/ztlpn))
* `clickhouse-client` can segfault on exit while loading data for command line suggestions if it was run in interactive mode. [#4317](https://github.com/yandex/ClickHouse/pull/4317) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed race condition when selecting from `system.tables` may give `table doesn't exist` error. [#4313](https://github.com/yandex/ClickHouse/pull/4313) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed deadlock when `SELECT` from a table with `File` engine was retried after `No such file or directory` error. [#4161](https://github.com/yandex/ClickHouse/pull/4161) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed an issue: local ClickHouse dictionaries are loaded via TCP, but should load within process. [#4166](https://github.com/yandex/ClickHouse/pull/4166) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed `No message received` error when interacting with PostgreSQL ODBC Driver through TLS connection. Also fixes segfault when using MySQL ODBC Driver. [#4170](https://github.com/yandex/ClickHouse/pull/4170) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Temporarily disable predicate optimization for `ORDER BY`. [#3890](https://github.com/yandex/ClickHouse/pull/3890) ([Winter Zhang](https://github.com/zhang2014))
* Fixed infinite loop when selecting from table function `numbers(0)`. [#4280](https://github.com/yandex/ClickHouse/pull/4280) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed `compile_expressions` bug with comparison of big (more than int16) dates. [#4341](https://github.com/yandex/ClickHouse/pull/4341) ([alesapin](https://github.com/alesapin))
* Fixed segmentation fault with `uncompressed_cache=1` and exception with wrong uncompressed size. [#4186](https://github.com/yandex/ClickHouse/pull/4186) ([alesapin](https://github.com/alesapin))
* Fixed `ALL JOIN` with duplicates in right table. [#4184](https://github.com/yandex/ClickHouse/pull/4184) ([Artem Zuikov](https://github.com/4ertus2))
* Fixed wrong behaviour when doing `INSERT ... SELECT ... FROM file(...)` query and file has `CSVWithNames` or `TSVWIthNames` format and the first data row is missing. [#4297](https://github.com/yandex/ClickHouse/pull/4297) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed aggregate functions execution with `Array(LowCardinality)` arguments. [#4055](https://github.com/yandex/ClickHouse/pull/4055) ([KochetovNicolai](https://github.com/KochetovNicolai))
* Debian package: correct /etc/clickhouse-server/preprocessed link according to config. [#4205](https://github.com/yandex/ClickHouse/pull/4205) ([proller](https://github.com/proller))
* Fixed fuzz test under undefined behavior sanitizer: added parameter type check for `quantile*Weighted` family of functions. [#4145](https://github.com/yandex/ClickHouse/pull/4145) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Make `START REPLICATED SENDS` command start replicated sends. [#4229](https://github.com/yandex/ClickHouse/pull/4229) ([nvartolomei](https://github.com/nvartolomei))
* Fixed `Not found column` for duplicate columns in JOIN ON section. [#4279](https://github.com/yandex/ClickHouse/pull/4279) ([Artem Zuikov](https://github.com/4ertus2))
* Now `/etc/ssl` is used as default directory with SSL certificates. [#4167](https://github.com/yandex/ClickHouse/pull/4167) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed crash on dictionary reload if dictionary not available. [#4188](https://github.com/yandex/ClickHouse/pull/4188) ([proller](https://github.com/proller))
* Fixed bug with incorrect `Date` and `DateTime` comparison. [#4237](https://github.com/yandex/ClickHouse/pull/4237) ([valexey](https://github.com/valexey))
* Fixed incorrect result when `Date` and `DateTime` arguments are used in branches of conditional operator (function `if`). Added generic case for function `if`. [#4243](https://github.com/yandex/ClickHouse/pull/4243) ([alexey-milovidov](https://github.com/alexey-milovidov))

## ClickHouse release 19.1.6, 2019-01-24

### New Features

* Custom per column compression codecs for tables. [#3899](https://github.com/yandex/ClickHouse/pull/3899) [#4111](https://github.com/yandex/ClickHouse/pull/4111) ([alesapin](https://github.com/alesapin), [Winter Zhang](https://github.com/zhang2014), [Anatoly](https://github.com/Sindbag))
* Added compression codec `Delta`. [#4052](https://github.com/yandex/ClickHouse/pull/4052) ([alesapin](https://github.com/alesapin))
* Allow to `ALTER` compression codecs. [#4054](https://github.com/yandex/ClickHouse/pull/4054) ([alesapin](https://github.com/alesapin))
* Added functions `left`, `right`, `trim`, `ltrim`, `rtrim`, `timestampadd`, `timestampsub` for SQL standard compatibility. [#3826](https://github.com/yandex/ClickHouse/pull/3826) ([Ivan Blinkov](https://github.com/blinkov))
* Support for write in `HDFS` tables and `hdfs` table function. [#4084](https://github.com/yandex/ClickHouse/pull/4084) ([alesapin](https://github.com/alesapin))
* Added functions to search for multiple constant strings from big haystack: `multiPosition`, `multiSearch` ,`firstMatch` also with `-UTF8`, `-CaseInsensitive`, and `-CaseInsensitiveUTF8` variants. [#4053](https://github.com/yandex/ClickHouse/pull/4053) ([Danila Kutenin](https://github.com/danlark1))
* Pruning of unused shards if `SELECT` query filters by sharding key (setting `optimize_skip_unused_shards`). [#3851](https://github.com/yandex/ClickHouse/pull/3851) ([Gleb Kanterov](https://github.com/kanterov), [Ivan](https://github.com/abyss7))
* Allow `Kafka` engine to ignore some number of parsing errors per block. [#4094](https://github.com/yandex/ClickHouse/pull/4094) ([Ivan](https://github.com/abyss7))
* Added support for `CatBoost` multiclass models evaluation. Function `modelEvaluate` returns tuple with per-class raw predictions for multiclass models. `libcatboostmodel.so` should be built with [#607](https://github.com/catboost/catboost/pull/607). [#3959](https://github.com/yandex/ClickHouse/pull/3959) ([KochetovNicolai](https://github.com/KochetovNicolai))
* Added functions `filesystemAvailable`, `filesystemFree`, `filesystemCapacity`. [#4097](https://github.com/yandex/ClickHouse/pull/4097) ([Boris Granveaud](https://github.com/bgranvea))
* Added hashing functions `xxHash64` and `xxHash32`. [#3905](https://github.com/yandex/ClickHouse/pull/3905) ([filimonov](https://github.com/filimonov))
* Added `gccMurmurHash` hashing function (GCC flavoured Murmur hash) which uses the same hash seed as [gcc](https://github.com/gcc-mirror/gcc/blob/41d6b10e96a1de98e90a7c0378437c3255814b16/libstdc%2B%2B-v3/include/bits/functional_hash.h#L191) [#4000](https://github.com/yandex/ClickHouse/pull/4000) ([sundyli](https://github.com/sundy-li))
* Added hashing functions `javaHash`, `hiveHash`. [#3811](https://github.com/yandex/ClickHouse/pull/3811) ([shangshujie365](https://github.com/shangshujie365))
* Added table function `remoteSecure`. Function works as `remote`, but uses secure connection. [#4088](https://github.com/yandex/ClickHouse/pull/4088) ([proller](https://github.com/proller))


### Experimental features

* Added multiple JOINs emulation (`allow_experimental_multiple_joins_emulation` setting). [#3946](https://github.com/yandex/ClickHouse/pull/3946) ([Artem Zuikov](https://github.com/4ertus2))


### Bug Fixes

* Make `compiled_expression_cache_size` setting limited by default to lower memory consumption. [#4041](https://github.com/yandex/ClickHouse/pull/4041) ([alesapin](https://github.com/alesapin))
* Fix a bug that led to hangups in threads that perform ALTERs of Replicated tables and in the thread that updates configuration from ZooKeeper. [#2947](https://github.com/yandex/ClickHouse/issues/2947) [#3891](https://github.com/yandex/ClickHouse/issues/3891) [#3934](https://github.com/yandex/ClickHouse/pull/3934) ([Alex Zatelepin](https://github.com/ztlpn))
* Fixed a race condition when executing a distributed ALTER task. The race condition led to more than one replica trying to execute the task and all replicas except one failing with a ZooKeeper error. [#3904](https://github.com/yandex/ClickHouse/pull/3904) ([Alex Zatelepin](https://github.com/ztlpn))
* Fix a bug when `from_zk` config elements weren't refreshed after a request to ZooKeeper timed out. [#2947](https://github.com/yandex/ClickHouse/issues/2947) [#3947](https://github.com/yandex/ClickHouse/pull/3947) ([Alex Zatelepin](https://github.com/ztlpn))
* Fix bug with wrong prefix for IPv4 subnet masks. [#3945](https://github.com/yandex/ClickHouse/pull/3945) ([alesapin](https://github.com/alesapin))
* Fixed crash (`std::terminate`) in rare cases when a new thread cannot be created due to exhausted resources. [#3956](https://github.com/yandex/ClickHouse/pull/3956) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix bug when in `remote` table function execution when wrong restrictions were used for in `getStructureOfRemoteTable`. [#4009](https://github.com/yandex/ClickHouse/pull/4009) ([alesapin](https://github.com/alesapin))
* Fix a leak of netlink sockets. They were placed in a pool where they were never deleted and new sockets were created at the start of a new thread when all current sockets were in use. [#4017](https://github.com/yandex/ClickHouse/pull/4017) ([Alex Zatelepin](https://github.com/ztlpn))
* Fix bug with closing `/proc/self/fd` directory earlier than all fds were read from `/proc` after forking `odbc-bridge` subprocess. [#4120](https://github.com/yandex/ClickHouse/pull/4120) ([alesapin](https://github.com/alesapin))
* Fixed String to UInt monotonic conversion in case of usage String in primary key. [#3870](https://github.com/yandex/ClickHouse/pull/3870) ([Winter Zhang](https://github.com/zhang2014))
* Fixed error in calculation of integer conversion function monotonicity. [#3921](https://github.com/yandex/ClickHouse/pull/3921) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed segfault in `arrayEnumerateUniq`, `arrayEnumerateDense` functions in case of some invalid arguments. [#3909](https://github.com/yandex/ClickHouse/pull/3909) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix UB in StorageMerge. [#3910](https://github.com/yandex/ClickHouse/pull/3910) ([Amos Bird](https://github.com/amosbird))
* Fixed segfault in functions `addDays`, `subtractDays`. [#3913](https://github.com/yandex/ClickHouse/pull/3913) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed error: functions `round`, `floor`, `trunc`, `ceil` may return bogus result when executed on integer argument and large negative scale. [#3914](https://github.com/yandex/ClickHouse/pull/3914) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed a bug induced by 'kill query sync' which leads to a core dump. [#3916](https://github.com/yandex/ClickHouse/pull/3916) ([muVulDeePecker](https://github.com/fancyqlx))
* Fix bug with long delay after empty replication queue. [#3928](https://github.com/yandex/ClickHouse/pull/3928) [#3932](https://github.com/yandex/ClickHouse/pull/3932) ([alesapin](https://github.com/alesapin))
* Fixed excessive memory usage in case of inserting into table with `LowCardinality` primary key. [#3955](https://github.com/yandex/ClickHouse/pull/3955) ([KochetovNicolai](https://github.com/KochetovNicolai))
* Fixed `LowCardinality` serialization for `Native` format in case of empty arrays. [#3907](https://github.com/yandex/ClickHouse/issues/3907) [#4011](https://github.com/yandex/ClickHouse/pull/4011) ([KochetovNicolai](https://github.com/KochetovNicolai))
* Fixed incorrect result while using distinct by single LowCardinality numeric column. [#3895](https://github.com/yandex/ClickHouse/issues/3895) [#4012](https://github.com/yandex/ClickHouse/pull/4012) ([KochetovNicolai](https://github.com/KochetovNicolai))
* Fixed specialized aggregation with LowCardinality key (in case when `compile` setting is enabled). [#3886](https://github.com/yandex/ClickHouse/pull/3886) ([KochetovNicolai](https://github.com/KochetovNicolai))
* Fix user and password forwarding for replicated tables queries. [#3957](https://github.com/yandex/ClickHouse/pull/3957) ([alesapin](https://github.com/alesapin)) ([小路](https://github.com/nicelulu))
* Fixed very rare race condition that can happen when listing tables in Dictionary database while reloading dictionaries. [#3970](https://github.com/yandex/ClickHouse/pull/3970) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed incorrect result when HAVING was used with ROLLUP or CUBE. [#3756](https://github.com/yandex/ClickHouse/issues/3756) [#3837](https://github.com/yandex/ClickHouse/pull/3837) ([Sam Chou](https://github.com/reflection))
* Fixed column aliases for query with `JOIN ON` syntax and distributed tables. [#3980](https://github.com/yandex/ClickHouse/pull/3980) ([Winter Zhang](https://github.com/zhang2014))
* Fixed error in internal implementation of `quantileTDigest` (found by Artem Vakhrushev). This error never happens in ClickHouse and was relevant only for those who use ClickHouse codebase as a library directly. [#3935](https://github.com/yandex/ClickHouse/pull/3935) ([alexey-milovidov](https://github.com/alexey-milovidov))

### Improvements

* Support for `IF NOT EXISTS` in `ALTER TABLE ADD COLUMN` statements along with `IF EXISTS` in `DROP/MODIFY/CLEAR/COMMENT COLUMN`. [#3900](https://github.com/yandex/ClickHouse/pull/3900) ([Boris Granveaud](https://github.com/bgranvea))
* Function `parseDateTimeBestEffort`: support for formats `DD.MM.YYYY`, `DD.MM.YY`, `DD-MM-YYYY`, `DD-Mon-YYYY`, `DD/Month/YYYY` and similar. [#3922](https://github.com/yandex/ClickHouse/pull/3922) ([alexey-milovidov](https://github.com/alexey-milovidov))
* `CapnProtoInputStream` now support jagged structures. [#4063](https://github.com/yandex/ClickHouse/pull/4063) ([Odin Hultgren Van Der Horst](https://github.com/Miniwoffer))
* Usability improvement: added a check that server process is started from the data directory's owner. Do not allow to start server from root if the data belongs to non-root user. [#3785](https://github.com/yandex/ClickHouse/pull/3785) ([sergey-v-galtsev](https://github.com/sergey-v-galtsev))
* Better logic of checking required columns during analysis of queries with JOINs. [#3930](https://github.com/yandex/ClickHouse/pull/3930) ([Artem Zuikov](https://github.com/4ertus2))
* Decreased the number of connections in case of large number of Distributed tables in a single server. [#3726](https://github.com/yandex/ClickHouse/pull/3726) ([Winter Zhang](https://github.com/zhang2014))
* Supported totals row for `WITH TOTALS` query for ODBC driver. [#3836](https://github.com/yandex/ClickHouse/pull/3836) ([Maksim Koritckiy](https://github.com/nightweb))
* Allowed to use `Enum`s as integers inside if function. [#3875](https://github.com/yandex/ClickHouse/pull/3875) ([Ivan](https://github.com/abyss7))
* Added `low_cardinality_allow_in_native_format` setting. If disabled, do not use `LowCadrinality` type in `Native` format. [#3879](https://github.com/yandex/ClickHouse/pull/3879) ([KochetovNicolai](https://github.com/KochetovNicolai))
* Removed some redundant objects from compiled expressions cache to lower memory usage. [#4042](https://github.com/yandex/ClickHouse/pull/4042) ([alesapin](https://github.com/alesapin))
* Add check that `SET send_logs_level = 'value'` query accept appropriate value. [#3873](https://github.com/yandex/ClickHouse/pull/3873) ([Sabyanin Maxim](https://github.com/s-mx))
* Fixed data type check in type conversion functions. [#3896](https://github.com/yandex/ClickHouse/pull/3896) ([Winter Zhang](https://github.com/zhang2014))

### Performance Improvements

* Add a MergeTree setting `use_minimalistic_part_header_in_zookeeper`. If enabled, Replicated tables will store compact part metadata in a single part znode. This can dramatically reduce ZooKeeper snapshot size (especially if the tables have a lot of columns). Note that after enabling this setting you will not be able to downgrade to a version that doesn't support it. [#3960](https://github.com/yandex/ClickHouse/pull/3960) ([Alex Zatelepin](https://github.com/ztlpn))
* Add an DFA-based implementation for functions `sequenceMatch` and `sequenceCount` in case pattern doesn't contain time. [#4004](https://github.com/yandex/ClickHouse/pull/4004) ([Léo Ercolanelli](https://github.com/ercolanelli-leo))
* Performance improvement for integer numbers serialization. [#3968](https://github.com/yandex/ClickHouse/pull/3968) ([Amos Bird](https://github.com/amosbird))
* Zero left padding PODArray so that -1 element is always valid and zeroed. It's used for branchless calculation of offsets. [#3920](https://github.com/yandex/ClickHouse/pull/3920) ([Amos Bird](https://github.com/amosbird))
* Reverted `jemalloc` version which lead to performance degradation. [#4018](https://github.com/yandex/ClickHouse/pull/4018) ([alexey-milovidov](https://github.com/alexey-milovidov))

### Backward Incompatible Changes

* Removed undocumented feature `ALTER MODIFY PRIMARY KEY` because it was superseded by the `ALTER MODIFY ORDER BY` command. [#3887](https://github.com/yandex/ClickHouse/pull/3887) ([Alex Zatelepin](https://github.com/ztlpn))
* Removed function `shardByHash`. [#3833](https://github.com/yandex/ClickHouse/pull/3833) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Forbid using scalar subqueries with result of type `AggregateFunction`. [#3865](https://github.com/yandex/ClickHouse/pull/3865) ([Ivan](https://github.com/abyss7))

### Build/Testing/Packaging Improvements

* Added support for PowerPC (`ppc64le`) build. [#4132](https://github.com/yandex/ClickHouse/pull/4132) ([Danila Kutenin](https://github.com/danlark1))
* Stateful functional tests are run on public available dataset. [#3969](https://github.com/yandex/ClickHouse/pull/3969) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed error when the server cannot start with the `bash: /usr/bin/clickhouse-extract-from-config: Operation not permitted` message within Docker or systemd-nspawn. [#4136](https://github.com/yandex/ClickHouse/pull/4136) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Updated `rdkafka` library to v1.0.0-RC5. Used cppkafka instead of raw C interface. [#4025](https://github.com/yandex/ClickHouse/pull/4025) ([Ivan](https://github.com/abyss7))
* Updated `mariadb-client` library. Fixed one of issues found by UBSan. [#3924](https://github.com/yandex/ClickHouse/pull/3924) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Some fixes for UBSan builds. [#3926](https://github.com/yandex/ClickHouse/pull/3926) [#3021](https://github.com/yandex/ClickHouse/pull/3021) [#3948](https://github.com/yandex/ClickHouse/pull/3948) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Added per-commit runs of tests with UBSan build.
* Added per-commit runs of PVS-Studio static analyzer.
* Fixed bugs found by PVS-Studio. [#4013](https://github.com/yandex/ClickHouse/pull/4013) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed glibc compatibility issues. [#4100](https://github.com/yandex/ClickHouse/pull/4100) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Move Docker images to 18.10 and add compatibility file for glibc >= 2.28 [#3965](https://github.com/yandex/ClickHouse/pull/3965) ([alesapin](https://github.com/alesapin))
* Add env variable if user don't want to chown directories in server Docker image. [#3967](https://github.com/yandex/ClickHouse/pull/3967) ([alesapin](https://github.com/alesapin))
* Enabled most of the warnings from `-Weverything` in clang. Enabled `-Wpedantic`. [#3986](https://github.com/yandex/ClickHouse/pull/3986) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Added a few more warnings that are available only in clang 8. [#3993](https://github.com/yandex/ClickHouse/pull/3993) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Link to `libLLVM` rather than to individual LLVM libs when using shared linking. [#3989](https://github.com/yandex/ClickHouse/pull/3989) ([Orivej Desh](https://github.com/orivej))
* Added sanitizer variables for test images. [#4072](https://github.com/yandex/ClickHouse/pull/4072) ([alesapin](https://github.com/alesapin))
* `clickhouse-server` debian package will recommend `libcap2-bin` package to use `setcap` tool for setting capabilities. This is optional. [#4093](https://github.com/yandex/ClickHouse/pull/4093) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Improved compilation time, fixed includes. [#3898](https://github.com/yandex/ClickHouse/pull/3898) ([proller](https://github.com/proller))
* Added performance tests for hash functions. [#3918](https://github.com/yandex/ClickHouse/pull/3918) ([filimonov](https://github.com/filimonov))
* Fixed cyclic library dependences. [#3958](https://github.com/yandex/ClickHouse/pull/3958) ([proller](https://github.com/proller))
* Improved compilation with low available memory. [#4030](https://github.com/yandex/ClickHouse/pull/4030) ([proller](https://github.com/proller))
* Added test script to reproduce performance degradation in `jemalloc`. [#4036](https://github.com/yandex/ClickHouse/pull/4036) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed misspells in comments and string literals under `dbms`. [#4122](https://github.com/yandex/ClickHouse/pull/4122) ([maiha](https://github.com/maiha))
* Fixed typos in comments. [#4089](https://github.com/yandex/ClickHouse/pull/4089) ([Evgenii Pravda](https://github.com/kvinty))


## ClickHouse release 18.16.1, 2018-12-21

### Bug fixes:

* Fixed an error that led to problems with updating dictionaries with the ODBC source. [#3825](https://github.com/yandex/ClickHouse/issues/3825), [#3829](https://github.com/yandex/ClickHouse/issues/3829)
* JIT compilation of aggregate functions now works with LowCardinality columns. [#3838](https://github.com/yandex/ClickHouse/issues/3838)

### Improvements:

* Added the `low_cardinality_allow_in_native_format` setting (enabled by default). When disabled, LowCardinality columns will be converted to ordinary columns for SELECT queries and ordinary columns will be expected for INSERT queries. [#3879](https://github.com/yandex/ClickHouse/pull/3879)

### Build improvements:

* Fixes for builds on macOS and ARM.

## ClickHouse release 18.16.0, 2018-12-14

### New features:

* `DEFAULT` expressions are evaluated for missing fields when loading data in semi-structured input formats (`JSONEachRow`, `TSKV`). The feature is enabled with the `insert_sample_with_metadata` setting. [#3555](https://github.com/yandex/ClickHouse/pull/3555)
* The `ALTER TABLE` query now has the `MODIFY ORDER BY` action for changing the sorting key when adding or removing a table column. This is useful for tables in the `MergeTree` family that perform additional tasks when merging based on this sorting key, such as `SummingMergeTree`, `AggregatingMergeTree`, and so on. [#3581](https://github.com/yandex/ClickHouse/pull/3581) [#3755](https://github.com/yandex/ClickHouse/pull/3755)
* For tables in the `MergeTree` family, now you can specify a different sorting key (`ORDER BY`) and index (`PRIMARY KEY`). The sorting key can be longer than the index. [#3581](https://github.com/yandex/ClickHouse/pull/3581)
* Added the `hdfs` table function and the `HDFS` table engine for importing and exporting data to HDFS. [chenxing-xc](https://github.com/yandex/ClickHouse/pull/3617)
* Added functions for working with base64: `base64Encode`, `base64Decode`, `tryBase64Decode`. [Alexander Krasheninnikov](https://github.com/yandex/ClickHouse/pull/3350)
* Now you can use a parameter to configure the precision of the `uniqCombined` aggregate function (select the number of HyperLogLog cells). [#3406](https://github.com/yandex/ClickHouse/pull/3406)
* Added the `system.contributors` table that contains the names of everyone who made commits in ClickHouse. [#3452](https://github.com/yandex/ClickHouse/pull/3452)
* Added the ability to omit the partition for the `ALTER TABLE ... FREEZE` query in order to back up all partitions at once. [#3514](https://github.com/yandex/ClickHouse/pull/3514)
* Added `dictGet` and `dictGetOrDefault` functions that don't require specifying the type of return value. The type is determined automatically from the dictionary description. [Amos Bird](https://github.com/yandex/ClickHouse/pull/3564)
* Now you can specify comments for a column in the table description and change it using `ALTER`. [#3377](https://github.com/yandex/ClickHouse/pull/3377)
* Reading is supported for `Join` type tables with simple keys. [Amos Bird](https://github.com/yandex/ClickHouse/pull/3728)
* Now you can specify the options `join_use_nulls`, `max_rows_in_join`, `max_bytes_in_join`, and `join_overflow_mode` when creating a `Join` type table. [Amos Bird](https://github.com/yandex/ClickHouse/pull/3728)
* Added the `joinGet` function that allows you to use a `Join` type table like a dictionary. [Amos Bird](https://github.com/yandex/ClickHouse/pull/3728)
* Added the `partition_key`, `sorting_key`, `primary_key`, and `sampling_key` columns to the `system.tables` table in order to provide information about table keys. [#3609](https://github.com/yandex/ClickHouse/pull/3609)
* Added the `is_in_partition_key`, `is_in_sorting_key`, `is_in_primary_key`, and `is_in_sampling_key` columns to the `system.columns` table. [#3609](https://github.com/yandex/ClickHouse/pull/3609)
* Added the `min_time` and `max_time`  columns to the `system.parts` table. These columns are populated when the partitioning key is an expression consisting of `DateTime` columns. [Emmanuel Donin de Rosière](https://github.com/yandex/ClickHouse/pull/3800)

### Bug fixes:

* Fixes and performance improvements for the `LowCardinality` data type. `GROUP BY` using `LowCardinality(Nullable(...))`. Getting the values of `extremes`. Processing high-order functions. `LEFT ARRAY JOIN`. Distributed `GROUP BY`. Functions that return `Array`. Execution of `ORDER BY`. Writing to `Distributed` tables (nicelulu). Backward compatibility for `INSERT` queries from old clients that implement the `Native` protocol. Support for `LowCardinality` for `JOIN`. Improved performance when working in a single stream. [#3823](https://github.com/yandex/ClickHouse/pull/3823) [#3803](https://github.com/yandex/ClickHouse/pull/3803) [#3799](https://github.com/yandex/ClickHouse/pull/3799) [#3769](https://github.com/yandex/ClickHouse/pull/3769) [#3744](https://github.com/yandex/ClickHouse/pull/3744) [#3681](https://github.com/yandex/ClickHouse/pull/3681) [#3651](https://github.com/yandex/ClickHouse/pull/3651) [#3649](https://github.com/yandex/ClickHouse/pull/3649) [#3641](https://github.com/yandex/ClickHouse/pull/3641) [#3632](https://github.com/yandex/ClickHouse/pull/3632) [#3568](https://github.com/yandex/ClickHouse/pull/3568) [#3523](https://github.com/yandex/ClickHouse/pull/3523) [#3518](https://github.com/yandex/ClickHouse/pull/3518)
* Fixed how the `select_sequential_consistency` option works. Previously, when this setting was enabled, an incomplete result was sometimes returned after beginning to write to a new partition. [#2863](https://github.com/yandex/ClickHouse/pull/2863)
* Databases are correctly specified when executing DDL `ON CLUSTER` queries and `ALTER UPDATE/DELETE`. [#3772](https://github.com/yandex/ClickHouse/pull/3772) [#3460](https://github.com/yandex/ClickHouse/pull/3460)
* Databases are correctly specified for subqueries inside a VIEW. [#3521](https://github.com/yandex/ClickHouse/pull/3521)
* Fixed a bug in `PREWHERE` with `FINAL` for `VersionedCollapsingMergeTree`. [7167bfd7](https://github.com/yandex/ClickHouse/commit/7167bfd7b365538f7a91c4307ad77e552ab4e8c1)
* Now you can use `KILL QUERY` to cancel queries that have not started yet because they are waiting for the table to be locked. [#3517](https://github.com/yandex/ClickHouse/pull/3517)
* Corrected date and time calculations if the clocks were moved back at midnight (this happens in Iran, and happened in Moscow from 1981 to 1983). Previously, this led to the time being reset a day earlier than necessary, and also caused incorrect formatting of the date and time in text format. [#3819](https://github.com/yandex/ClickHouse/pull/3819)
* Fixed bugs in some cases of `VIEW` and subqueries that omit the database. [Winter Zhang](https://github.com/yandex/ClickHouse/pull/3521)
* Fixed a race condition when simultaneously reading from a `MATERIALIZED VIEW` and deleting a `MATERIALIZED VIEW` due to not locking the internal `MATERIALIZED VIEW`. [#3404](https://github.com/yandex/ClickHouse/pull/3404) [#3694](https://github.com/yandex/ClickHouse/pull/3694)
* Fixed the error `Lock handler cannot be nullptr.` [#3689](https://github.com/yandex/ClickHouse/pull/3689)
* Fixed query processing when the `compile_expressions` option is enabled (it's enabled by default). Nondeterministic constant expressions like the `now` function are no longer unfolded. [#3457](https://github.com/yandex/ClickHouse/pull/3457)
* Fixed a crash when specifying a non-constant scale argument in `toDecimal32/64/128` functions.
* Fixed an error when trying to insert an array with `NULL` elements in the `Values` format into a column of type `Array` without `Nullable` (if `input_format_values_interpret_expressions` = 1). [#3487](https://github.com/yandex/ClickHouse/pull/3487) [#3503](https://github.com/yandex/ClickHouse/pull/3503)
* Fixed continuous error logging in `DDLWorker` if ZooKeeper is not available. [8f50c620](https://github.com/yandex/ClickHouse/commit/8f50c620334988b28018213ec0092fe6423847e2)
* Fixed the return type for `quantile*` functions from `Date` and `DateTime` types of arguments. [#3580](https://github.com/yandex/ClickHouse/pull/3580)
* Fixed the `WITH` clause if it specifies a simple alias without expressions. [#3570](https://github.com/yandex/ClickHouse/pull/3570)
* Fixed processing of queries with named sub-queries and qualified column names when `enable_optimize_predicate_expression` is enabled. [Winter Zhang](https://github.com/yandex/ClickHouse/pull/3588)
* Fixed the error `Attempt to attach to nullptr thread group` when working with materialized views. [Marek Vavruša](https://github.com/yandex/ClickHouse/pull/3623)
* Fixed a crash when passing certain incorrect arguments to the `arrayReverse` function. [73e3a7b6](https://github.com/yandex/ClickHouse/commit/73e3a7b662161d6005e7727d8a711b930386b871)
* Fixed the buffer overflow in the `extractURLParameter` function. Improved performance. Added correct processing of strings containing zero bytes. [141e9799](https://github.com/yandex/ClickHouse/commit/141e9799e49201d84ea8e951d1bed4fb6d3dacb5)
* Fixed buffer overflow in the `lowerUTF8` and `upperUTF8` functions. Removed the ability to execute these functions over `FixedString` type arguments. [#3662](https://github.com/yandex/ClickHouse/pull/3662)
* Fixed a rare race condition when deleting `MergeTree` tables. [#3680](https://github.com/yandex/ClickHouse/pull/3680)
* Fixed a race condition when reading from `Buffer` tables and simultaneously performing `ALTER` or `DROP` on the target tables. [#3719](https://github.com/yandex/ClickHouse/pull/3719)
* Fixed a segfault if the `max_temporary_non_const_columns` limit was exceeded. [#3788](https://github.com/yandex/ClickHouse/pull/3788)

### Improvements:

* The server does not write the processed configuration files to the `/etc/clickhouse-server/` directory. Instead, it saves them in the `preprocessed_configs` directory inside `path`. This means that the `/etc/clickhouse-server/` directory doesn't have write access for the `clickhouse` user, which improves security. [#2443](https://github.com/yandex/ClickHouse/pull/2443)
* The `min_merge_bytes_to_use_direct_io` option is set to 10 GiB by default. A merge that forms large parts of tables from the MergeTree family will be performed in `O_DIRECT` mode, which prevents excessive page cache eviction. [#3504](https://github.com/yandex/ClickHouse/pull/3504)
* Accelerated server start when there is a very large number of tables. [#3398](https://github.com/yandex/ClickHouse/pull/3398)
* Added a connection pool and HTTP `Keep-Alive` for connections between replicas. [#3594](https://github.com/yandex/ClickHouse/pull/3594)
* If the query syntax is invalid, the `400 Bad Request` code is returned in the `HTTP` interface (500 was returned previously). [31bc680a](https://github.com/yandex/ClickHouse/commit/31bc680ac5f4bb1d0360a8ba4696fa84bb47d6ab)
* The `join_default_strictness` option is set to `ALL` by default for compatibility. [120e2cbe](https://github.com/yandex/ClickHouse/commit/120e2cbe2ff4fbad626c28042d9b28781c805afe)
* Removed logging to `stderr` from the `re2` library for invalid or complex regular expressions. [#3723](https://github.com/yandex/ClickHouse/pull/3723)
* Added for the `Kafka` table engine: checks for subscriptions before beginning to read from Kafka; the kafka_max_block_size setting for the table. [Marek Vavruša](https://github.com/yandex/ClickHouse/pull/3396)
* The `cityHash64`, `farmHash64`, `metroHash64`, `sipHash64`, `halfMD5`, `murmurHash2_32`, `murmurHash2_64`, `murmurHash3_32`, and `murmurHash3_64` functions now work for any number of arguments and for arguments in the form of tuples. [#3451](https://github.com/yandex/ClickHouse/pull/3451) [#3519](https://github.com/yandex/ClickHouse/pull/3519)
* The `arrayReverse` function now works with any types of arrays. [73e3a7b6](https://github.com/yandex/ClickHouse/commit/73e3a7b662161d6005e7727d8a711b930386b871)
* Added an optional parameter: the slot size for the `timeSlots` function. [Kirill Shvakov](https://github.com/yandex/ClickHouse/pull/3724)
* For `FULL` and `RIGHT JOIN`, the `max_block_size` setting is used for a stream of non-joined data from the right table. [Amos Bird](https://github.com/yandex/ClickHouse/pull/3699)
* Added the `--secure` command line parameter in  `clickhouse-benchmark` and `clickhouse-performance-test` to enable TLS. [#3688](https://github.com/yandex/ClickHouse/pull/3688) [#3690](https://github.com/yandex/ClickHouse/pull/3690)
* Type conversion when the structure of a `Buffer` type table does not match the structure of the destination table. [Vitaly Baranov](https://github.com/yandex/ClickHouse/pull/3603)
* Added the `tcp_keep_alive_timeout` option to enable keep-alive packets after inactivity for the specified time interval. [#3441](https://github.com/yandex/ClickHouse/pull/3441)
* Removed unnecessary quoting of values for the partition key in the `system.parts` table if it consists of a single column. [#3652](https://github.com/yandex/ClickHouse/pull/3652)
* The modulo function works for `Date` and `DateTime` data types. [#3385](https://github.com/yandex/ClickHouse/pull/3385)
* Added synonyms for the `POWER`, `LN`, `LCASE`, `UCASE`, `REPLACE`, `LOCATE`, `SUBSTR`, and `MID` functions. [#3774](https://github.com/yandex/ClickHouse/pull/3774) [#3763](https://github.com/yandex/ClickHouse/pull/3763) Some function names are case-insensitive for compatibility with the SQL standard. Added syntactic sugar `SUBSTRING(expr FROM start FOR length)` for compatibility with SQL. [#3804](https://github.com/yandex/ClickHouse/pull/3804)
* Added the ability to `mlock` memory pages corresponding to `clickhouse-server`  executable code to prevent it from being forced out of memory. This feature is disabled by default. [#3553](https://github.com/yandex/ClickHouse/pull/3553)
* Improved performance when reading from `O_DIRECT` (with the `min_bytes_to_use_direct_io` option enabled). [#3405](https://github.com/yandex/ClickHouse/pull/3405)
* Improved performance of the `dictGet...OrDefault` function for a constant key argument and a non-constant default argument. [Amos Bird](https://github.com/yandex/ClickHouse/pull/3563)
* The `firstSignificantSubdomain` function now processes the domains `gov`, `mil`, and `edu`. [Igor Hatarist](https://github.com/yandex/ClickHouse/pull/3601) Improved performance. [#3628](https://github.com/yandex/ClickHouse/pull/3628)
* Ability to specify custom environment variables for starting `clickhouse-server` using the `SYS-V init.d` script by defining `CLICKHOUSE_PROGRAM_ENV` in `/etc/default/clickhouse`.
[Pavlo Bashynskyi](https://github.com/yandex/ClickHouse/pull/3612)
* Correct return code for the clickhouse-server init script. [#3516](https://github.com/yandex/ClickHouse/pull/3516)
* The `system.metrics` table now has the `VersionInteger` metric, and `system.build_options` has the added line `VERSION_INTEGER`, which contains the numeric form of the ClickHouse version, such as  `18016000`. [#3644](https://github.com/yandex/ClickHouse/pull/3644)
* Removed the ability to compare the `Date` type with a number to avoid potential errors like `date = 2018-12-17`, where quotes around the date are omitted by mistake. [#3687](https://github.com/yandex/ClickHouse/pull/3687)
* Fixed the behavior of stateful functions like `rowNumberInAllBlocks`. They previously output a result that was one number larger due to starting during query analysis. [Amos Bird](https://github.com/yandex/ClickHouse/pull/3729)
* If the `force_restore_data` file can't be deleted, an error message is displayed. [Amos Bird](https://github.com/yandex/ClickHouse/pull/3794)

### Build improvements:

* Updated the `jemalloc` library, which fixes a potential memory leak. [Amos Bird](https://github.com/yandex/ClickHouse/pull/3557)
* Profiling with `jemalloc` is enabled by default in order to debug builds. [2cc82f5c](https://github.com/yandex/ClickHouse/commit/2cc82f5cbe266421cd4c1165286c2c47e5ffcb15)
* Added the ability to run integration tests when only `Docker` is installed on the system. [#3650](https://github.com/yandex/ClickHouse/pull/3650)
* Added the fuzz expression test in SELECT queries. [#3442](https://github.com/yandex/ClickHouse/pull/3442)
* Added a stress test for commits, which performs functional tests in parallel and in random order to detect more race conditions. [#3438](https://github.com/yandex/ClickHouse/pull/3438)
* Improved the method for starting clickhouse-server in a Docker image. [Elghazal Ahmed](https://github.com/yandex/ClickHouse/pull/3663)
* For a Docker image, added support for initializing databases using files in the `/docker-entrypoint-initdb.d` directory. [Konstantin Lebedev](https://github.com/yandex/ClickHouse/pull/3695)
* Fixes for builds on ARM. [#3709](https://github.com/yandex/ClickHouse/pull/3709)

### Backward incompatible changes:

* Removed the ability to compare the `Date` type with a number. Instead of `toDate('2018-12-18') = 17883`, you must use explicit type conversion `= toDate(17883)` [#3687](https://github.com/yandex/ClickHouse/pull/3687)

## ClickHouse release 18.14.19, 2018-12-19

### Bug fixes:

* Fixed an error that led to problems with updating dictionaries with the ODBC source. [#3825](https://github.com/yandex/ClickHouse/issues/3825), [#3829](https://github.com/yandex/ClickHouse/issues/3829)
* Databases are correctly specified when executing DDL `ON CLUSTER` queries. [#3460](https://github.com/yandex/ClickHouse/pull/3460)
* Fixed a segfault if the `max_temporary_non_const_columns` limit was exceeded. [#3788](https://github.com/yandex/ClickHouse/pull/3788)

### Build improvements:

* Fixes for builds on ARM.

## ClickHouse release 18.14.18, 2018-12-04

### Bug fixes:
* Fixed error in `dictGet...` function for dictionaries of type `range`, if one of the arguments is constant and other is not. [#3751](https://github.com/yandex/ClickHouse/pull/3751)
* Fixed error that caused messages `netlink: '...': attribute type 1 has an invalid length` to be printed in Linux kernel log, that was happening only on fresh enough versions of Linux kernel. [#3749](https://github.com/yandex/ClickHouse/pull/3749)
* Fixed segfault in function `empty` for argument of `FixedString` type. [Daniel, Dao Quang Minh](https://github.com/yandex/ClickHouse/pull/3703)
* Fixed excessive memory allocation when using large value of `max_query_size` setting (a memory chunk of `max_query_size` bytes was preallocated at once). [#3720](https://github.com/yandex/ClickHouse/pull/3720)

### Build changes:
* Fixed build with LLVM/Clang libraries of version 7 from the OS packages (these libraries are used for runtime query compilation). [#3582](https://github.com/yandex/ClickHouse/pull/3582)

## ClickHouse release 18.14.17, 2018-11-30

### Bug fixes:
* Fixed cases when the ODBC bridge process did not terminate with the main server process. [#3642](https://github.com/yandex/ClickHouse/pull/3642)
* Fixed synchronous insertion into the `Distributed` table with a columns list that differs from the column list of the remote table. [#3673](https://github.com/yandex/ClickHouse/pull/3673)
* Fixed a rare race condition that can lead to a crash when dropping a MergeTree table. [#3643](https://github.com/yandex/ClickHouse/pull/3643)
* Fixed a query deadlock in case when query thread creation fails with the `Resource temporarily unavailable` error. [#3643](https://github.com/yandex/ClickHouse/pull/3643)
* Fixed parsing of the `ENGINE` clause when the `CREATE AS table` syntax was used and the `ENGINE` clause was specified before the `AS table` (the error resulted in ignoring the specified engine). [#3692](https://github.com/yandex/ClickHouse/pull/3692)

## ClickHouse release 18.14.15, 2018-11-21

### Bug fixes:
* The size of memory chunk was overestimated while deserializing the column of type `Array(String)` that leads to "Memory limit exceeded" errors. The issue appeared in version 18.12.13. [#3589](https://github.com/yandex/ClickHouse/issues/3589)

## ClickHouse release 18.14.14, 2018-11-20

### Bug fixes:
* Fixed `ON CLUSTER` queries when cluster configured as secure (flag `<secure>`). [#3599](https://github.com/yandex/ClickHouse/pull/3599)

### Build changes:
* Fixed problems (llvm-7 from system, macos) [#3582](https://github.com/yandex/ClickHouse/pull/3582)

## ClickHouse release 18.14.13, 2018-11-08

### Bug fixes:
* Fixed the `Block structure mismatch in MergingSorted stream` error. [#3162](https://github.com/yandex/ClickHouse/issues/3162)
* Fixed `ON CLUSTER` queries in case when secure connections were turned on in the cluster config (the `<secure>` flag). [#3465](https://github.com/yandex/ClickHouse/pull/3465)
* Fixed an error in queries that used `SAMPLE`, `PREWHERE` and alias columns. [#3543](https://github.com/yandex/ClickHouse/pull/3543)
* Fixed a rare `unknown compression method` error when the `min_bytes_to_use_direct_io` setting was enabled. [3544](https://github.com/yandex/ClickHouse/pull/3544)

### Performance improvements:
* Fixed performance regression of queries with `GROUP BY` of columns of UInt16 or Date type when executing on AMD EPYC processors. [Igor Lapko](https://github.com/yandex/ClickHouse/pull/3512)
* Fixed performance regression of queries that process long strings. [#3530](https://github.com/yandex/ClickHouse/pull/3530)

### Build improvements:
* Improvements for simplifying the Arcadia build. [#3475](https://github.com/yandex/ClickHouse/pull/3475), [#3535](https://github.com/yandex/ClickHouse/pull/3535)

## ClickHouse release 18.14.12, 2018-11-02

### Bug fixes:

* Fixed a crash on joining two unnamed subqueries. [#3505](https://github.com/yandex/ClickHouse/pull/3505)
* Fixed generating incorrect queries (with an empty `WHERE` clause) when querying external databases. [hotid](https://github.com/yandex/ClickHouse/pull/3477)
* Fixed using an incorrect timeout value in ODBC dictionaries. [Marek Vavruša](https://github.com/yandex/ClickHouse/pull/3511)

## ClickHouse release 18.14.11, 2018-10-29

### Bug fixes:

* Fixed the error `Block structure mismatch in UNION stream: different number of columns` in LIMIT queries. [#2156](https://github.com/yandex/ClickHouse/issues/2156)
* Fixed errors when merging data in tables containing arrays inside Nested structures. [#3397](https://github.com/yandex/ClickHouse/pull/3397)
* Fixed incorrect query results if the `merge_tree_uniform_read_distribution` setting is disabled (it is enabled by default). [#3429](https://github.com/yandex/ClickHouse/pull/3429)
* Fixed an error on inserts to a Distributed table in Native format. [#3411](https://github.com/yandex/ClickHouse/issues/3411)

## ClickHouse release 18.14.10, 2018-10-23

* The `compile_expressions` setting (JIT compilation of expressions) is disabled by default. [#3410](https://github.com/yandex/ClickHouse/pull/3410)
* The `enable_optimize_predicate_expression` setting is disabled by default.

## ClickHouse release 18.14.9, 2018-10-16

### New features:

* The `WITH CUBE` modifier for `GROUP BY` (the alternative syntax `GROUP BY CUBE(...)` is also available). [#3172](https://github.com/yandex/ClickHouse/pull/3172)
* Added the `formatDateTime` function. [Alexandr Krasheninnikov](https://github.com/yandex/ClickHouse/pull/2770)
* Added the `JDBC`  table engine and `jdbc`  table function (requires installing clickhouse-jdbc-bridge). [Alexandr Krasheninnikov](https://github.com/yandex/ClickHouse/pull/3210)
* Added functions for working with the ISO week number: `toISOWeek`, `toISOYear`, `toStartOfISOYear`, and `toDayOfYear`. [#3146](https://github.com/yandex/ClickHouse/pull/3146)
* Now you can use `Nullable` columns for `MySQL` and `ODBC` tables. [#3362](https://github.com/yandex/ClickHouse/pull/3362)
* Nested data structures can be read as nested objects in `JSONEachRow` format. Added the `input_format_import_nested_json` setting. [Veloman Yunkan](https://github.com/yandex/ClickHouse/pull/3144)
* Parallel processing is available for many `MATERIALIZED VIEW`s when inserting data. See the `parallel_view_processing` setting. [Marek Vavruša](https://github.com/yandex/ClickHouse/pull/3208)
* Added the `SYSTEM FLUSH LOGS` query (forced log flushes to system tables such as `query_log`) [#3321](https://github.com/yandex/ClickHouse/pull/3321)
* Now you can use pre-defined `database` and `table` macros when declaring `Replicated` tables. [#3251](https://github.com/yandex/ClickHouse/pull/3251)
* Added the ability to read `Decimal`  type values in engineering notation (indicating powers of ten). [#3153](https://github.com/yandex/ClickHouse/pull/3153)

### Experimental features:

* Optimization of the GROUP BY clause for `LowCardinality data types.` [#3138](https://github.com/yandex/ClickHouse/pull/3138)
* Optimized calculation of expressions for `LowCardinality data types.` [#3200](https://github.com/yandex/ClickHouse/pull/3200)

### Improvements:

* Significantly reduced memory consumption for queries with `ORDER BY` and `LIMIT`. See the `max_bytes_before_remerge_sort` setting. [#3205](https://github.com/yandex/ClickHouse/pull/3205)
* In the absence of `JOIN` (`LEFT`, `INNER`, ...), `INNER JOIN` is assumed. [#3147](https://github.com/yandex/ClickHouse/pull/3147)
* Qualified asterisks work correctly in queries with `JOIN`. [Winter Zhang](https://github.com/yandex/ClickHouse/pull/3202)
* The `ODBC` table engine correctly chooses the method for quoting identifiers in the SQL dialect of a remote database. [Alexandr Krasheninnikov](https://github.com/yandex/ClickHouse/pull/3210)
* The `compile_expressions` setting (JIT compilation of expressions) is enabled by default.
* Fixed behavior for simultaneous DROP DATABASE/TABLE IF EXISTS and CREATE DATABASE/TABLE IF NOT EXISTS. Previously, a `CREATE DATABASE ... IF NOT EXISTS` query could return the error message "File ... already exists", and the `CREATE TABLE ... IF NOT EXISTS` and `DROP TABLE IF EXISTS` queries could return `Table ... is creating or attaching right now`. [#3101](https://github.com/yandex/ClickHouse/pull/3101)
* LIKE and IN expressions with a constant right half are passed to the remote server when querying from MySQL or ODBC tables. [#3182](https://github.com/yandex/ClickHouse/pull/3182)
* Comparisons with constant expressions in a WHERE clause are passed to the remote server when querying from MySQL and ODBC tables. Previously, only comparisons with constants were passed. [#3182](https://github.com/yandex/ClickHouse/pull/3182)
* Correct calculation of row width in the terminal for `Pretty` formats, including strings with hieroglyphs. [Amos Bird](https://github.com/yandex/ClickHouse/pull/3257).
* `ON CLUSTER` can be specified for `ALTER UPDATE` queries.
* Improved performance for reading data in `JSONEachRow` format. [#3332](https://github.com/yandex/ClickHouse/pull/3332)
* Added synonyms for the `LENGTH` and `CHARACTER_LENGTH` functions for compatibility. The `CONCAT` function is no longer case-sensitive. [#3306](https://github.com/yandex/ClickHouse/pull/3306)
* Added the `TIMESTAMP` synonym for the `DateTime` type. [#3390](https://github.com/yandex/ClickHouse/pull/3390)
* There is always space reserved for query_id in the server logs, even if the log line is not related to a query. This makes it easier to parse server text logs with third-party tools.
* Memory consumption by a query is logged when it exceeds the next level of an integer number of gigabytes. [#3205](https://github.com/yandex/ClickHouse/pull/3205)
* Added compatibility mode for the case when the client library that uses the Native protocol sends fewer columns by mistake than the server expects for the INSERT query. This scenario was possible when using the clickhouse-cpp library. Previously, this scenario caused the server to crash. [#3171](https://github.com/yandex/ClickHouse/pull/3171)
* In a user-defined WHERE expression in `clickhouse-copier`, you can now use a `partition_key` alias (for additional filtering by source table partition). This is useful if the partitioning scheme changes during copying, but only changes slightly. [#3166](https://github.com/yandex/ClickHouse/pull/3166)
* The workflow of the `Kafka` engine has been moved to a background thread pool in order to automatically reduce the speed of data reading at high loads. [Marek Vavruša](https://github.com/yandex/ClickHouse/pull/3215).
* Support for reading `Tuple` and `Nested` values of structures like `struct` in the `Cap'n'Proto format`. [Marek Vavruša](https://github.com/yandex/ClickHouse/pull/3216)
* The list of top-level domains for the `firstSignificantSubdomain` function now includes the domain `biz`. [decaseal](https://github.com/yandex/ClickHouse/pull/3219)
* In the configuration of external dictionaries, `null_value` is interpreted as the value of the default data type. [#3330](https://github.com/yandex/ClickHouse/pull/3330)
* Support for the `intDiv` and `intDivOrZero` functions for `Decimal`. [b48402e8](https://github.com/yandex/ClickHouse/commit/b48402e8712e2b9b151e0eef8193811d433a1264)
* Support for the `Date`, `DateTime`, `UUID`, and `Decimal` types as a key for the `sumMap` aggregate function. [#3281](https://github.com/yandex/ClickHouse/pull/3281)
* Support for the `Decimal` data type in external dictionaries. [#3324](https://github.com/yandex/ClickHouse/pull/3324)
* Support for the `Decimal` data type in `SummingMergeTree` tables. [#3348](https://github.com/yandex/ClickHouse/pull/3348)
* Added specializations for `UUID` in `if`. [#3366](https://github.com/yandex/ClickHouse/pull/3366)
* Reduced the number of `open` and `close` system calls when reading from a `MergeTree table`. [#3283](https://github.com/yandex/ClickHouse/pull/3283)
* A `TRUNCATE TABLE` query can be executed on any replica (the query is passed to the leader replica). [Kirill Shvakov](https://github.com/yandex/ClickHouse/pull/3375)

### Bug fixes:

* Fixed an issue with `Dictionary` tables for `range_hashed` dictionaries. This error occurred in version 18.12.17. [#1702](https://github.com/yandex/ClickHouse/pull/1702)
* Fixed an error when loading `range_hashed` dictionaries (the message `Unsupported type Nullable (...)`). This error occurred in version 18.12.17. [#3362](https://github.com/yandex/ClickHouse/pull/3362)
* Fixed errors in the `pointInPolygon` function due to the accumulation of inaccurate calculations for polygons with a large number of vertices located close to each other. [#3331](https://github.com/yandex/ClickHouse/pull/3331) [#3341](https://github.com/yandex/ClickHouse/pull/3341)
* If after merging data parts, the checksum for the resulting part differs from the result of the same merge in another replica, the result of the merge is deleted and the data part is downloaded from the other replica (this is the correct behavior). But after downloading the data part, it couldn't be added to the working set because of an error that the part already exists (because the data part was deleted with some delay after the merge). This led to cyclical attempts to download the same data. [#3194](https://github.com/yandex/ClickHouse/pull/3194)
* Fixed incorrect calculation of total memory consumption by queries (because of incorrect calculation, the `max_memory_usage_for_all_queries` setting worked incorrectly and the `MemoryTracking` metric had an incorrect value). This error occurred in version 18.12.13. [Marek Vavruša](https://github.com/yandex/ClickHouse/pull/3344)
* Fixed the functionality of `CREATE TABLE ... ON CLUSTER ... AS SELECT ...` This error occurred in version 18.12.13. [#3247](https://github.com/yandex/ClickHouse/pull/3247)
* Fixed unnecessary preparation of data structures for `JOIN`s on the server that initiates the query if the `JOIN` is only performed on remote servers. [#3340](https://github.com/yandex/ClickHouse/pull/3340)
* Fixed bugs in the `Kafka` engine: deadlocks after exceptions when starting to read data, and locks upon completion [Marek Vavruša](https://github.com/yandex/ClickHouse/pull/3215).
* For `Kafka` tables, the optional `schema` parameter was not passed  (the schema of the `Cap'n'Proto` format). [Vojtech Splichal](https://github.com/yandex/ClickHouse/pull/3150)
* If the ensemble of ZooKeeper servers has servers that accept the connection but then immediately close it instead of responding to the handshake, ClickHouse chooses to connect another server. Previously, this produced the error `Cannot read all data. Bytes read: 0. Bytes expected: 4.` and the server couldn't start. [8218cf3a](https://github.com/yandex/ClickHouse/commit/8218cf3a5f39a43401953769d6d12a0bb8d29da9)
* If the ensemble of ZooKeeper servers contains servers for which the DNS query returns an error, these servers are ignored. [17b8e209](https://github.com/yandex/ClickHouse/commit/17b8e209221061325ad7ba0539f03c6e65f87f29)
* Fixed type conversion between `Date` and `DateTime` when inserting data in the `VALUES` format (if `input_format_values_interpret_expressions = 1`). Previously, the conversion was performed between the numerical value of the number of days in Unix Epoch time and the Unix timestamp, which led to unexpected results. [#3229](https://github.com/yandex/ClickHouse/pull/3229)
* Corrected type conversion between `Decimal` and integer numbers. [#3211](https://github.com/yandex/ClickHouse/pull/3211)
* Fixed errors in the `enable_optimize_predicate_expression` setting. [Winter Zhang](https://github.com/yandex/ClickHouse/pull/3231)
* Fixed a parsing error in CSV format with floating-point numbers if a non-default CSV separator is used, such as `;` [#3155](https://github.com/yandex/ClickHouse/pull/3155)
* Fixed the `arrayCumSumNonNegative` function (it does not accumulate negative values if the accumulator is less than zero). [Aleksey Studnev](https://github.com/yandex/ClickHouse/pull/3163)
* Fixed how `Merge` tables work on top of `Distributed` tables when using `PREWHERE`. [#3165](https://github.com/yandex/ClickHouse/pull/3165)
* Bug fixes in the `ALTER UPDATE` query.
* Fixed bugs in the `odbc` table function that appeared in version 18.12. [#3197](https://github.com/yandex/ClickHouse/pull/3197)
* Fixed the operation of aggregate functions with `StateArray` combinators. [#3188](https://github.com/yandex/ClickHouse/pull/3188)
* Fixed a crash when dividing a `Decimal` value by zero. [69dd6609](https://github.com/yandex/ClickHouse/commit/69dd6609193beb4e7acd3e6ad216eca0ccfb8179)
* Fixed output of types for operations using `Decimal` and integer arguments. [#3224](https://github.com/yandex/ClickHouse/pull/3224)
* Fixed the segfault during `GROUP BY` on `Decimal128`. [3359ba06](https://github.com/yandex/ClickHouse/commit/3359ba06c39fcd05bfdb87d6c64154819621e13a)
* The `log_query_threads` setting (logging information about each thread of query execution) now takes effect only if the `log_queries` option (logging information about queries) is set to 1. Since the `log_query_threads` option is enabled by default, information about threads was previously logged even if query logging was disabled. [#3241](https://github.com/yandex/ClickHouse/pull/3241)
* Fixed an error in the distributed operation of the quantiles aggregate function (the error message `Not found column quantile...`). [292a8855](https://github.com/yandex/ClickHouse/commit/292a885533b8e3b41ce8993867069d14cbd5a664)
* Fixed the compatibility problem when working on a cluster of version 18.12.17 servers and older servers at the same time. For distributed queries with GROUP BY keys of both fixed and non-fixed length, if there was a large amount of data to aggregate, the returned data was not always fully aggregated (two different rows contained the same aggregation keys). [#3254](https://github.com/yandex/ClickHouse/pull/3254)
* Fixed handling of substitutions in `clickhouse-performance-test`, if the query contains only part of the substitutions declared in the test. [#3263](https://github.com/yandex/ClickHouse/pull/3263)
* Fixed an error when using `FINAL` with `PREWHERE`. [#3298](https://github.com/yandex/ClickHouse/pull/3298)
* Fixed an error when using `PREWHERE` over columns that were added during `ALTER`. [#3298](https://github.com/yandex/ClickHouse/pull/3298)
* Added a check for the absence of `arrayJoin` for `DEFAULT` and `MATERIALIZED` expressions. Previously, `arrayJoin` led to an error when inserting data. [#3337](https://github.com/yandex/ClickHouse/pull/3337)
* Added a check for the absence of `arrayJoin` in a `PREWHERE` clause. Previously, this led to messages like `Size ... doesn't match` or `Unknown compression method` when executing queries. [#3357](https://github.com/yandex/ClickHouse/pull/3357)
* Fixed segfault that could occur in rare cases after optimization that replaced AND chains from equality evaluations with the corresponding IN expression. [liuyimin-bytedance](https://github.com/yandex/ClickHouse/pull/3339)
* Minor corrections to `clickhouse-benchmark`: previously, client information was not sent to the server; now the number of queries executed is calculated more accurately when shutting down and for limiting the number of iterations. [#3351](https://github.com/yandex/ClickHouse/pull/3351) [#3352](https://github.com/yandex/ClickHouse/pull/3352)

### Backward incompatible changes:

* Removed the `allow_experimental_decimal_type` option. The `Decimal` data type is available for default use. [#3329](https://github.com/yandex/ClickHouse/pull/3329)

## ClickHouse release 18.12.17, 2018-09-16

### New features:

* `invalidate_query` (the ability to specify a query to check whether an external dictionary needs to be updated) is implemented for the `clickhouse` source. [#3126](https://github.com/yandex/ClickHouse/pull/3126)
* Added the ability to use `UInt*`, `Int*`, and `DateTime`  data types (along with the `Date` type) as a `range_hashed` external dictionary key that defines the boundaries of ranges. Now `NULL` can be used to designate an open range. [Vasily Nemkov](https://github.com/yandex/ClickHouse/pull/3123)
* The `Decimal` type now supports `var*` and `stddev*` aggregate functions. [#3129](https://github.com/yandex/ClickHouse/pull/3129)
* The `Decimal` type now supports mathematical functions (`exp`, `sin` and so on.) [#3129](https://github.com/yandex/ClickHouse/pull/3129)
* The `system.part_log` table now has the `partition_id` column. [#3089](https://github.com/yandex/ClickHouse/pull/3089)

### Bug fixes:

* `Merge` now works correctly on `Distributed` tables. [Winter Zhang](https://github.com/yandex/ClickHouse/pull/3159)
* Fixed incompatibility (unnecessary dependency on the `glibc` version) that made it impossible to run ClickHouse on `Ubuntu Precise` and older versions. The incompatibility arose in version 18.12.13. [#3130](https://github.com/yandex/ClickHouse/pull/3130)
* Fixed errors in the `enable_optimize_predicate_expression` setting. [Winter Zhang](https://github.com/yandex/ClickHouse/pull/3107)
* Fixed a minor issue with backwards compatibility that appeared when working with a cluster of replicas on versions earlier than 18.12.13 and simultaneously creating a new replica of a table on a server with a newer version (shown in the message `Can not clone replica, because the ... updated to new ClickHouse version`, which is logical, but shouldn't happen). [#3122](https://github.com/yandex/ClickHouse/pull/3122)

### Backward incompatible changes:

* The `enable_optimize_predicate_expression` option is enabled by default (which is rather optimistic). If query analysis errors occur that are related to searching for the column names, set `enable_optimize_predicate_expression` to 0. [Winter Zhang](https://github.com/yandex/ClickHouse/pull/3107)

## ClickHouse release 18.12.14, 2018-09-13

### New features:

* Added support for `ALTER UPDATE` queries. [#3035](https://github.com/yandex/ClickHouse/pull/3035)
* Added the `allow_ddl` option, which restricts the user's access to DDL queries. [#3104](https://github.com/yandex/ClickHouse/pull/3104)
* Added the `min_merge_bytes_to_use_direct_io` option for `MergeTree` engines, which allows you to set a threshold for the total size of the merge (when above the threshold, data part files will be handled using O_DIRECT). [#3117](https://github.com/yandex/ClickHouse/pull/3117)
* The `system.merges` system table now contains the `partition_id` column. [#3099](https://github.com/yandex/ClickHouse/pull/3099)

### Improvements

* If a data part remains unchanged during mutation, it isn't downloaded by replicas. [#3103](https://github.com/yandex/ClickHouse/pull/3103)
* Autocomplete is available for names of settings when working with `clickhouse-client`. [#3106](https://github.com/yandex/ClickHouse/pull/3106)

### Bug fixes:

* Added a check for the sizes of arrays that are elements of `Nested` type fields when inserting. [#3118](https://github.com/yandex/ClickHouse/pull/3118)
* Fixed an error updating external dictionaries with the `ODBC` source and `hashed` storage. This error occurred in version 18.12.13.
* Fixed a crash when creating a temporary table from a query with an `IN` condition. [Winter Zhang](https://github.com/yandex/ClickHouse/pull/3098)
* Fixed an error in aggregate functions for arrays that can have `NULL` elements. [Winter Zhang](https://github.com/yandex/ClickHouse/pull/3097)


## ClickHouse release 18.12.13, 2018-09-10

### New features:

* Added the `DECIMAL(digits, scale)` data type (`Decimal32(scale)`, `Decimal64(scale)`, `Decimal128(scale)`). To enable it, use the setting `allow_experimental_decimal_type`. [#2846](https://github.com/yandex/ClickHouse/pull/2846) [#2970](https://github.com/yandex/ClickHouse/pull/2970) [#3008](https://github.com/yandex/ClickHouse/pull/3008) [#3047](https://github.com/yandex/ClickHouse/pull/3047)
* New `WITH ROLLUP` modifier for `GROUP BY` (alternative syntax: `GROUP BY ROLLUP(...)`). [#2948](https://github.com/yandex/ClickHouse/pull/2948)
* In queries with JOIN, the star character expands to a list of columns in all tables, in compliance with the SQL standard. You can restore the old behavior by setting `asterisk_left_columns_only` to 1 on the user configuration level. [Winter Zhang](https://github.com/yandex/ClickHouse/pull/2787)
* Added support for JOIN with table functions. [Winter Zhang](https://github.com/yandex/ClickHouse/pull/2907)
* Autocomplete by pressing Tab in clickhouse-client. [Sergey Shcherbin](https://github.com/yandex/ClickHouse/pull/2447)
* Ctrl+C in clickhouse-client clears a query that was entered. [#2877](https://github.com/yandex/ClickHouse/pull/2877)
* Added the `join_default_strictness` setting (values: `"`, `'any'`, `'all'`). This allows you to not specify `ANY` or `ALL` for `JOIN`. [#2982](https://github.com/yandex/ClickHouse/pull/2982)
* Each line of the server log related to query processing shows the query ID. [#2482](https://github.com/yandex/ClickHouse/pull/2482)
* Now you can get query execution logs in clickhouse-client (use the `send_logs_level` setting). With distributed query processing, logs are cascaded from all the servers. [#2482](https://github.com/yandex/ClickHouse/pull/2482)
* The `system.query_log` and `system.processes` (`SHOW PROCESSLIST`) tables now have information about all changed settings when you run a query (the nested structure of the `Settings` data). Added the `log_query_settings` setting. [#2482](https://github.com/yandex/ClickHouse/pull/2482)
* The `system.query_log` and `system.processes` tables now show information about the number of threads that are participating in query execution (see the `thread_numbers` column). [#2482](https://github.com/yandex/ClickHouse/pull/2482)
* Added `ProfileEvents` counters that measure the time spent on reading and writing over the network and reading and writing to disk, the number of network errors, and the time spent waiting when network bandwidth is limited. [#2482](https://github.com/yandex/ClickHouse/pull/2482)
* Added `ProfileEvents`counters that contain the system metrics from rusage (you can use them to get information about CPU usage in userspace and the kernel, page faults, and context switches), as well as taskstats metrics (use these to obtain information about I/O wait time, CPU wait time, and the amount of data read and recorded, both with and without page cache). [#2482](https://github.com/yandex/ClickHouse/pull/2482)
* The `ProfileEvents` counters are applied globally and for each query, as well as for each query execution thread, which allows you to profile resource consumption by query  in detail. [#2482](https://github.com/yandex/ClickHouse/pull/2482)
* Added the `system.query_thread_log` table, which contains information about each query execution thread. Added the `log_query_threads` setting. [#2482](https://github.com/yandex/ClickHouse/pull/2482)
* The `system.metrics` and `system.events` tables now have built-in documentation. [#3016](https://github.com/yandex/ClickHouse/pull/3016)
* Added the `arrayEnumerateDense` function. [Amos Bird](https://github.com/yandex/ClickHouse/pull/2975)
* Added the `arrayCumSumNonNegative` and `arrayDifference` functions. [Aleksey Studnev](https://github.com/yandex/ClickHouse/pull/2942)
* Added the `retention` aggregate function. [Sundy Li](https://github.com/yandex/ClickHouse/pull/2887)
* Now you can add (merge) states of aggregate functions by using the plus operator, and multiply the states of aggregate functions by a nonnegative constant. [#3062](https://github.com/yandex/ClickHouse/pull/3062) [#3034](https://github.com/yandex/ClickHouse/pull/3034)
* Tables in the MergeTree family now have the virtual column `_partition_id`. [#3089](https://github.com/yandex/ClickHouse/pull/3089)

### Experimental features:

* Added the `LowCardinality(T)` data type. This data type automatically creates a local dictionary of values and allows data processing without unpacking the dictionary. [#2830](https://github.com/yandex/ClickHouse/pull/2830)
* Added a cache of JIT-compiled functions and a counter for the number of uses before compiling. To JIT compile expressions, enable the `compile_expressions` setting. [#2990](https://github.com/yandex/ClickHouse/pull/2990) [#3077](https://github.com/yandex/ClickHouse/pull/3077)

### Improvements:

* Fixed the problem with unlimited accumulation of the replication log when there are abandoned replicas. Added an effective recovery mode for replicas with a long lag.
* Improved performance of `GROUP BY` with multiple aggregation fields when one of them is string and the others are fixed length.
* Improved performance when using `PREWHERE` and with implicit transfer of expressions in `PREWHERE`.
* Improved parsing performance for text formats (`CSV`, `TSV`). [Amos Bird](https://github.com/yandex/ClickHouse/pull/2977) [#2980](https://github.com/yandex/ClickHouse/pull/2980)
* Improved performance of reading strings and arrays in binary formats. [Amos Bird](https://github.com/yandex/ClickHouse/pull/2955)
* Increased performance and reduced memory consumption for queries to `system.tables` and `system.columns` when there is a very large number of tables on a single server. [#2953](https://github.com/yandex/ClickHouse/pull/2953)
* Fixed a performance problem in the case of a large stream of queries that result in an error (the ` _dl_addr` function is visible in `perf top`, but the server isn't using much CPU). [#2938](https://github.com/yandex/ClickHouse/pull/2938)
* Conditions are cast into the View (when `enable_optimize_predicate_expression` is enabled). [Winter Zhang](https://github.com/yandex/ClickHouse/pull/2907)
* Improvements to the functionality for the `UUID` data type. [#3074](https://github.com/yandex/ClickHouse/pull/3074) [#2985](https://github.com/yandex/ClickHouse/pull/2985)
* The `UUID` data type is supported in The-Alchemist dictionaries. [#2822](https://github.com/yandex/ClickHouse/pull/2822)
* The `visitParamExtractRaw` function works correctly with nested structures. [Winter Zhang](https://github.com/yandex/ClickHouse/pull/2974)
* When the `input_format_skip_unknown_fields` setting is enabled, object fields in `JSONEachRow` format are skipped correctly. [BlahGeek](https://github.com/yandex/ClickHouse/pull/2958)
* For a `CASE` expression with conditions, you can now omit `ELSE`, which is equivalent to `ELSE NULL`. [#2920](https://github.com/yandex/ClickHouse/pull/2920)
* The operation timeout can now be configured when working with ZooKeeper. [urykhy](https://github.com/yandex/ClickHouse/pull/2971)
* You can specify an offset for `LIMIT n, m` as `LIMIT n OFFSET m`. [#2840](https://github.com/yandex/ClickHouse/pull/2840)
* You can use the `SELECT TOP n` syntax as an alternative for `LIMIT`. [#2840](https://github.com/yandex/ClickHouse/pull/2840)
* Increased the size of the queue to write to system tables, so the `SystemLog parameter queue is full` error doesn't happen as often.
* The `windowFunnel` aggregate function now supports events that meet multiple conditions. [Amos Bird](https://github.com/yandex/ClickHouse/pull/2801)
* Duplicate columns can be used in a `USING` clause for `JOIN`. [#3006](https://github.com/yandex/ClickHouse/pull/3006)
* `Pretty` formats now have a limit on column alignment by width. Use the `output_format_pretty_max_column_pad_width` setting. If a value is wider, it will still be displayed in its entirety, but the other cells in the table will not be too wide. [#3003](https://github.com/yandex/ClickHouse/pull/3003)
* The `odbc` table function now allows you to specify the database/schema name. [Amos Bird](https://github.com/yandex/ClickHouse/pull/2885)
* Added the ability to use a username specified in the `clickhouse-client` config file. [Vladimir Kozbin](https://github.com/yandex/ClickHouse/pull/2909)
* The `ZooKeeperExceptions` counter has been split into three counters: `ZooKeeperUserExceptions`, `ZooKeeperHardwareExceptions`, and `ZooKeeperOtherExceptions`.
* `ALTER DELETE` queries work for materialized views.
* Added randomization when running the cleanup thread periodically for `ReplicatedMergeTree` tables in order to avoid periodic load spikes when there are a very large number of `ReplicatedMergeTree` tables.
* Support for `ATTACH TABLE ... ON CLUSTER` queries. [#3025](https://github.com/yandex/ClickHouse/pull/3025)

### Bug fixes:

* Fixed an issue with `Dictionary` tables (throws the `Size of offsets doesn't match size of column` or `Unknown compression method` exception). This bug appeared in version 18.10.3. [#2913](https://github.com/yandex/ClickHouse/issues/2913)
* Fixed a bug when merging `CollapsingMergeTree` tables if one of the data parts is empty (these parts are formed during merge or `ALTER DELETE` if all data was deleted), and the `vertical` algorithm was used for the merge. [#3049](https://github.com/yandex/ClickHouse/pull/3049)
* Fixed a race condition during `DROP` or `TRUNCATE` for `Memory` tables with a simultaneous `SELECT`, which could lead to server crashes. This bug appeared in version 1.1.54388. [#3038](https://github.com/yandex/ClickHouse/pull/3038)
* Fixed the possibility of data loss when inserting in `Replicated` tables if the `Session is expired` error is returned (data loss can be detected by the `ReplicatedDataLoss` metric). This error occurred in version 1.1.54378. [#2939](https://github.com/yandex/ClickHouse/pull/2939) [#2949](https://github.com/yandex/ClickHouse/pull/2949) [#2964](https://github.com/yandex/ClickHouse/pull/2964)
* Fixed a segfault during `JOIN ... ON`. [#3000](https://github.com/yandex/ClickHouse/pull/3000)
* Fixed the error searching column names when the `WHERE` expression consists entirely of a qualified column name, such as `WHERE table.column`. [#2994](https://github.com/yandex/ClickHouse/pull/2994)
* Fixed the "Not found column" error that occurred when executing distributed queries if a single column consisting of an IN expression with a subquery is requested from a remote server. [#3087](https://github.com/yandex/ClickHouse/pull/3087)
* Fixed the `Block structure mismatch in UNION stream: different number of columns` error that occurred for distributed queries if one of the shards is local and the other is not, and optimization of the move to `PREWHERE` is triggered. [#2226](https://github.com/yandex/ClickHouse/pull/2226) [#3037](https://github.com/yandex/ClickHouse/pull/3037) [#3055](https://github.com/yandex/ClickHouse/pull/3055) [#3065](https://github.com/yandex/ClickHouse/pull/3065) [#3073](https://github.com/yandex/ClickHouse/pull/3073) [#3090](https://github.com/yandex/ClickHouse/pull/3090) [#3093](https://github.com/yandex/ClickHouse/pull/3093)
* Fixed the `pointInPolygon` function for certain cases of non-convex polygons. [#2910](https://github.com/yandex/ClickHouse/pull/2910)
* Fixed the incorrect result when comparing `nan` with integers. [#3024](https://github.com/yandex/ClickHouse/pull/3024)
* Fixed an error in the `zlib-ng` library that could lead to segfault in rare cases. [#2854](https://github.com/yandex/ClickHouse/pull/2854)
* Fixed a memory leak when inserting into a table with `AggregateFunction` columns, if the state of the aggregate function is not simple (allocates memory separately), and if a single insertion request results in multiple small blocks. [#3084](https://github.com/yandex/ClickHouse/pull/3084)
* Fixed a race condition when creating and deleting the same `Buffer` or `MergeTree` table simultaneously.
* Fixed the possibility of a segfault when comparing tuples made up of certain non-trivial types, such as tuples. [#2989](https://github.com/yandex/ClickHouse/pull/2989)
* Fixed the possibility of a segfault when running certain `ON CLUSTER` queries. [Winter Zhang](https://github.com/yandex/ClickHouse/pull/2960)
* Fixed an error in the `arrayDistinct` function for `Nullable` array elements. [#2845](https://github.com/yandex/ClickHouse/pull/2845) [#2937](https://github.com/yandex/ClickHouse/pull/2937)
* The `enable_optimize_predicate_expression` option now correctly supports cases with `SELECT *`. [Winter Zhang](https://github.com/yandex/ClickHouse/pull/2929)
* Fixed the segfault when re-initializing the ZooKeeper session. [#2917](https://github.com/yandex/ClickHouse/pull/2917)
* Fixed potential blocking when working with ZooKeeper.
* Fixed incorrect code for adding nested data structures in a `SummingMergeTree`.
* When allocating memory for states of aggregate functions, alignment is correctly taken into account, which makes it possible to use operations that require alignment when implementing states of aggregate functions. [chenxing-xc](https://github.com/yandex/ClickHouse/pull/2808)

### Security fix:

* Safe use of ODBC data sources. Interaction with ODBC drivers uses a separate `clickhouse-odbc-bridge` process. Errors in third-party ODBC drivers no longer cause problems with server stability or vulnerabilities. [#2828](https://github.com/yandex/ClickHouse/pull/2828) [#2879](https://github.com/yandex/ClickHouse/pull/2879) [#2886](https://github.com/yandex/ClickHouse/pull/2886) [#2893](https://github.com/yandex/ClickHouse/pull/2893) [#2921](https://github.com/yandex/ClickHouse/pull/2921)
* Fixed incorrect validation of the file path in the `catBoostPool` table function. [#2894](https://github.com/yandex/ClickHouse/pull/2894)
* The contents of system tables (`tables`, `databases`, `parts`, `columns`, `parts_columns`, `merges`, `mutations`, `replicas`, and `replication_queue`) are filtered according to the user's configured access to databases (`allow_databases`). [Winter Zhang](https://github.com/yandex/ClickHouse/pull/2856)

### Backward incompatible changes:

* In queries with JOIN, the star character expands to a list of columns in all tables, in compliance with the SQL standard. You can restore the old behavior by setting `asterisk_left_columns_only` to 1 on the user configuration level.

### Build changes:

* Most integration tests can now be run by commit.
* Code style checks can also be run by commit.
* The `memcpy` implementation is chosen correctly when building on CentOS7/Fedora. [Etienne Champetier](https://github.com/yandex/ClickHouse/pull/2912)
* When using clang to build, some warnings from `-Weverything` have been added, in addition to the regular `-Wall-Wextra -Werror`. [#2957](https://github.com/yandex/ClickHouse/pull/2957)
* Debugging the build uses the `jemalloc` debug option.
* The interface of the library for interacting with ZooKeeper is declared abstract. [#2950](https://github.com/yandex/ClickHouse/pull/2950)

## ClickHouse release 18.10.3, 2018-08-13

### New features:

* HTTPS can be used for replication. [#2760](https://github.com/yandex/ClickHouse/pull/2760)
* Added the functions `murmurHash2_64`, `murmurHash3_32`, `murmurHash3_64`, and `murmurHash3_128` in addition to the existing `murmurHash2_32`. [#2791](https://github.com/yandex/ClickHouse/pull/2791)
* Support for Nullable types in the ClickHouse ODBC driver (`ODBCDriver2` output format). [#2834](https://github.com/yandex/ClickHouse/pull/2834)
* Support for `UUID` in the key columns.

### Improvements:

* Clusters can be removed without restarting the server when they are deleted from the config files. [#2777](https://github.com/yandex/ClickHouse/pull/2777)
* External dictionaries can be removed without restarting the server when they are removed from config files. [#2779](https://github.com/yandex/ClickHouse/pull/2779)
* Added `SETTINGS` support for the `Kafka` table engine. [Alexander Marshalov](https://github.com/yandex/ClickHouse/pull/2781)
* Improvements for the `UUID` data type (not yet complete). [#2618](https://github.com/yandex/ClickHouse/pull/2618)
* Support for empty parts after merges in the `SummingMergeTree`, `CollapsingMergeTree` and `VersionedCollapsingMergeTree` engines. [#2815](https://github.com/yandex/ClickHouse/pull/2815)
* Old records of completed mutations are deleted (`ALTER DELETE`). [#2784](https://github.com/yandex/ClickHouse/pull/2784)
* Added the `system.merge_tree_settings` table. [Kirill Shvakov](https://github.com/yandex/ClickHouse/pull/2841)
* The `system.tables` table now has dependency columns: `dependencies_database` and `dependencies_table`. [Winter Zhang](https://github.com/yandex/ClickHouse/pull/2851)
* Added the `max_partition_size_to_drop` config option. [#2782](https://github.com/yandex/ClickHouse/pull/2782)
* Added the `output_format_json_escape_forward_slashes` option. [Alexander Bocharov](https://github.com/yandex/ClickHouse/pull/2812)
* Added the `max_fetch_partition_retries_count` setting. [#2831](https://github.com/yandex/ClickHouse/pull/2831)
* Added the `prefer_localhost_replica` setting for disabling the preference for a local replica and going to a local replica without inter-process interaction. [#2832](https://github.com/yandex/ClickHouse/pull/2832)
* The `quantileExact` aggregate function returns `nan` in the case of aggregation on an empty `Float32` or `Float64` set. [Sundy Li](https://github.com/yandex/ClickHouse/pull/2855)

### Bug fixes:

* Removed unnecessary escaping of the connection string parameters for ODBC, which made it impossible to establish a connection. This error occurred in version 18.6.0.
* Fixed the logic for processing `REPLACE PARTITION` commands in the replication queue. If there are two `REPLACE` commands for the same partition, the incorrect logic could cause one of them to remain in the replication queue and not be executed. [#2814](https://github.com/yandex/ClickHouse/pull/2814)
* Fixed a merge bug when all data parts were empty (parts that were formed from a merge or from `ALTER DELETE` if all data was deleted). This bug appeared in version 18.1.0. [#2930](https://github.com/yandex/ClickHouse/pull/2930)
* Fixed an error for concurrent `Set` or `Join`. [Amos Bird](https://github.com/yandex/ClickHouse/pull/2823)
* Fixed the `Block structure mismatch in UNION stream: different number of columns` error that occurred for `UNION ALL` queries inside a sub-query if one of the `SELECT` queries contains duplicate column names. [Winter Zhang](https://github.com/yandex/ClickHouse/pull/2094)
* Fixed a memory leak if an exception occurred when connecting to a MySQL server.
* Fixed incorrect clickhouse-client response code in case of a query error.
* Fixed incorrect behavior of materialized views containing DISTINCT. [#2795](https://github.com/yandex/ClickHouse/issues/2795)

### Backward incompatible changes

* Removed support for CHECK TABLE queries for Distributed tables.

### Build changes:

* The allocator has been replaced: `jemalloc` is now used instead of `tcmalloc`. In some scenarios, this increases speed up to 20%. However, there are queries that have slowed by up to 20%. Memory consumption has been reduced by approximately 10% in some scenarios, with improved stability. With highly competitive loads, CPU usage in userspace and in system shows just a slight increase. [#2773](https://github.com/yandex/ClickHouse/pull/2773)
* Use of libressl from a submodule. [#1983](https://github.com/yandex/ClickHouse/pull/1983) [#2807](https://github.com/yandex/ClickHouse/pull/2807)
* Use of unixodbc from a submodule. [#2789](https://github.com/yandex/ClickHouse/pull/2789)
* Use of mariadb-connector-c from a submodule. [#2785](https://github.com/yandex/ClickHouse/pull/2785)
* Added functional test files to the repository that depend on the availability of test data (for the time being, without the test data itself).

## ClickHouse release 18.6.0, 2018-08-02

### New features:

* Added support for ON expressions for the JOIN ON syntax:
`JOIN ON Expr([table.]column ...) = Expr([table.]column, ...) [AND Expr([table.]column, ...) = Expr([table.]column, ...) ...]`
The expression must be a chain of equalities joined by the AND operator. Each side of the equality can be an arbitrary expression over the columns of one of the tables. The use of fully qualified column names is supported (`table.name`, `database.table.name`, `table_alias.name`, `subquery_alias.name`) for the right table. [#2742](https://github.com/yandex/ClickHouse/pull/2742)
* HTTPS can be enabled for replication. [#2760](https://github.com/yandex/ClickHouse/pull/2760)

### Improvements:

* The server passes the patch component of its version to the client. Data about the patch version component is in `system.processes` and `query_log`. [#2646](https://github.com/yandex/ClickHouse/pull/2646)

## ClickHouse release 18.5.1, 2018-07-31

### New features:

* Added the hash function `murmurHash2_32` [#2756](https://github.com/yandex/ClickHouse/pull/2756).

### Improvements:

* Now you can use the `from_env` [#2741](https://github.com/yandex/ClickHouse/pull/2741) attribute to set values in config files from environment variables.
* Added case-insensitive versions of the `coalesce`, `ifNull`, and `nullIf functions` [#2752](https://github.com/yandex/ClickHouse/pull/2752).

### Bug fixes:

* Fixed a possible bug when starting a replica [#2759](https://github.com/yandex/ClickHouse/pull/2759).

## ClickHouse release 18.4.0, 2018-07-28

### New features:

* Added system tables: `formats`, `data_type_families`, `aggregate_function_combinators`, `table_functions`, `table_engines`, `collations` [#2721](https://github.com/yandex/ClickHouse/pull/2721).
* Added the ability to use a table function instead of a table as an argument of a `remote` or `cluster table function` [#2708](https://github.com/yandex/ClickHouse/pull/2708).
* Support for `HTTP Basic` authentication in the replication protocol [#2727](https://github.com/yandex/ClickHouse/pull/2727).
* The `has` function now allows searching for a numeric value in an array of `Enum` values [Maxim Khrisanfov](https://github.com/yandex/ClickHouse/pull/2699).
* Support for adding arbitrary message separators when reading from `Kafka` [Amos Bird](https://github.com/yandex/ClickHouse/pull/2701).

### Improvements:

* The `ALTER TABLE t DELETE WHERE` query does not rewrite data parts that were not affected by the WHERE condition [#2694](https://github.com/yandex/ClickHouse/pull/2694).
* The `use_minimalistic_checksums_in_zookeeper` option for `ReplicatedMergeTree` tables is enabled by default. This setting was added in version 1.1.54378, 2018-04-16. Versions that are older than 1.1.54378 can no longer be installed.
* Support for running `KILL` and `OPTIMIZE` queries that specify `ON CLUSTER` [Winter Zhang](https://github.com/yandex/ClickHouse/pull/2689).

### Bug fixes:

* Fixed the error `Column ... is not under an aggregate function and not in GROUP BY` for aggregation with an IN expression. This bug appeared in version 18.1.0. ([bbdd780b](https://github.com/yandex/ClickHouse/commit/bbdd780be0be06a0f336775941cdd536878dd2c2))
* Fixed a bug in the `windowFunnel aggregate function` [Winter Zhang](https://github.com/yandex/ClickHouse/pull/2735).
* Fixed a bug in the `anyHeavy`  aggregate function ([a2101df2](https://github.com/yandex/ClickHouse/commit/a2101df25a6a0fba99aa71f8793d762af2b801ee))
* Fixed server crash when using the `countArray()` aggregate function.

### Backward incompatible changes:

* Parameters for `Kafka` engine was changed from `Kafka(kafka_broker_list, kafka_topic_list, kafka_group_name, kafka_format[, kafka_schema, kafka_num_consumers])` to `Kafka(kafka_broker_list, kafka_topic_list, kafka_group_name, kafka_format[, kafka_row_delimiter, kafka_schema, kafka_num_consumers])`. If your tables use `kafka_schema` or `kafka_num_consumers` parameters, you have to manually edit the metadata files `path/metadata/database/table.sql` and add `kafka_row_delimiter` parameter with `''` value.

## ClickHouse release 18.1.0, 2018-07-23

### New features:

* Support for the `ALTER TABLE t DELETE WHERE` query for non-replicated MergeTree tables ([#2634](https://github.com/yandex/ClickHouse/pull/2634)).
* Support for arbitrary types for the `uniq*` family of aggregate functions ([#2010](https://github.com/yandex/ClickHouse/issues/2010)).
* Support for arbitrary types in comparison operators ([#2026](https://github.com/yandex/ClickHouse/issues/2026)).
* The `users.xml` file allows setting a subnet mask in the format `10.0.0.1/255.255.255.0`. This is necessary for using masks for IPv6 networks with zeros in the middle ([#2637](https://github.com/yandex/ClickHouse/pull/2637)).
* Added the `arrayDistinct` function ([#2670](https://github.com/yandex/ClickHouse/pull/2670)).
* The SummingMergeTree engine can now work with AggregateFunction type columns ([Constantin S. Pan](https://github.com/yandex/ClickHouse/pull/2566)).

### Improvements:

* Changed the numbering scheme for release versions. Now the first part contains the year of release (A.D., Moscow timezone, minus 2000), the second part contains the number for major changes (increases for most releases), and the third part is the patch version. Releases are still backwards compatible, unless otherwise stated in the changelog.
* Faster conversions of floating-point numbers to a string ([Amos Bird](https://github.com/yandex/ClickHouse/pull/2664)).
* If some rows were skipped during an insert due to parsing errors (this is possible with the `input_allow_errors_num` and `input_allow_errors_ratio` settings enabled), the number of skipped rows is now written to the server log ([Leonardo Cecchi](https://github.com/yandex/ClickHouse/pull/2669)).

### Bug fixes:

* Fixed the TRUNCATE command for temporary tables ([Amos Bird](https://github.com/yandex/ClickHouse/pull/2624)).
* Fixed a rare deadlock in the ZooKeeper client library that occurred when there was a network error while reading the response ([c315200](https://github.com/yandex/ClickHouse/commit/c315200e64b87e44bdf740707fc857d1fdf7e947)).
* Fixed an error during a CAST to Nullable types ([#1322](https://github.com/yandex/ClickHouse/issues/1322)).
* Fixed the incorrect result of the `maxIntersection()` function when the boundaries of intervals coincided ([Michael Furmur](https://github.com/yandex/ClickHouse/pull/2657)).
* Fixed incorrect transformation of the OR expression chain in a function argument ([chenxing-xc](https://github.com/yandex/ClickHouse/pull/2663)).
* Fixed performance degradation for queries containing `IN (subquery)` expressions inside another subquery ([#2571](https://github.com/yandex/ClickHouse/issues/2571)).
* Fixed incompatibility between servers with different versions in distributed queries that use a `CAST` function that isn't in uppercase letters ([fe8c4d6](https://github.com/yandex/ClickHouse/commit/fe8c4d64e434cacd4ceef34faa9005129f2190a5)).
* Added missing quoting of identifiers for queries to an external DBMS ([#2635](https://github.com/yandex/ClickHouse/issues/2635)).

### Backward incompatible changes:

* Converting a string containing the number zero to DateTime does not work. Example: `SELECT toDateTime('0')`. This is also the reason that `DateTime DEFAULT '0'` does not work in tables, as well as `<null_value>0</null_value>` in dictionaries. Solution: replace `0` with `0000-00-00 00:00:00`.

## ClickHouse release 1.1.54394, 2018-07-12

### New features:

* Added the `histogram` aggregate function ([Mikhail Surin](https://github.com/yandex/ClickHouse/pull/2521)).
* Now `OPTIMIZE TABLE ... FINAL` can be used without specifying partitions for `ReplicatedMergeTree` ([Amos Bird](https://github.com/yandex/ClickHouse/pull/2600)).

### Bug fixes:

* Fixed a problem with a very small timeout for sockets (one second) for reading and writing when sending and downloading replicated data, which made it impossible to download larger parts if there is a load on the network or disk (it resulted in cyclical attempts to download parts). This error occurred in version 1.1.54388.
* Fixed issues when using chroot in ZooKeeper if you inserted duplicate data blocks in the table.
* The `has` function now works correctly for an array with Nullable elements ([#2115](https://github.com/yandex/ClickHouse/issues/2115)).
* The `system.tables` table now works correctly when used in distributed queries. The `metadata_modification_time` and `engine_full` columns are now non-virtual. Fixed an error that occurred if only these columns were queried from the table.
* Fixed how an empty `TinyLog` table works after inserting an empty data block ([#2563](https://github.com/yandex/ClickHouse/issues/2563)).
* The `system.zookeeper` table works if the value of the node in ZooKeeper is NULL.

## ClickHouse release 1.1.54390, 2018-07-06

### New features:

* Queries can be sent in `multipart/form-data` format (in the `query` field), which is useful if external data is also sent for query processing ([Olga Hvostikova](https://github.com/yandex/ClickHouse/pull/2490)).
* Added the ability to enable or disable processing single or double quotes when reading data in CSV format. You can configure this in the `format_csv_allow_single_quotes` and `format_csv_allow_double_quotes` settings ([Amos Bird](https://github.com/yandex/ClickHouse/pull/2574)).
* Now `OPTIMIZE TABLE ... FINAL` can be used without specifying the partition for non-replicated variants of `MergeTree` ([Amos Bird](https://github.com/yandex/ClickHouse/pull/2599)).

### Improvements:

* Improved performance, reduced memory consumption, and correct memory consumption tracking with use of the IN operator when a table index could be used ([#2584](https://github.com/yandex/ClickHouse/pull/2584)).
* Removed redundant checking of checksums when adding a data part. This is important when there are a large number of replicas, because in these cases the total number of checks was equal to N^2.
* Added support for `Array(Tuple(...))`  arguments for the `arrayEnumerateUniq` function ([#2573](https://github.com/yandex/ClickHouse/pull/2573)).
* Added `Nullable` support for the `runningDifference` function ([#2594](https://github.com/yandex/ClickHouse/pull/2594)).
* Improved query analysis performance when there is a very large number of expressions ([#2572](https://github.com/yandex/ClickHouse/pull/2572)).
* Faster selection of data parts for merging in `ReplicatedMergeTree` tables. Faster recovery of the ZooKeeper session ([#2597](https://github.com/yandex/ClickHouse/pull/2597)).
* The `format_version.txt` file for `MergeTree` tables is re-created if it is missing, which makes sense if ClickHouse is launched after copying the directory structure without files ([Ciprian Hacman](https://github.com/yandex/ClickHouse/pull/2593)).

### Bug fixes:

* Fixed a bug when working with ZooKeeper that could make it impossible to recover the session and readonly states of tables before restarting the server.
* Fixed a bug when working with ZooKeeper that could result in old nodes not being deleted if the session is interrupted.
* Fixed an error in the `quantileTDigest` function for Float arguments (this bug was introduced in version 1.1.54388) ([Mikhail Surin](https://github.com/yandex/ClickHouse/pull/2553)).
* Fixed a bug in the index for MergeTree tables if the primary key column is located inside the function for converting types between signed and unsigned integers of the same size ([#2603](https://github.com/yandex/ClickHouse/pull/2603)).
* Fixed segfault if `macros` are used but they aren't in the config file ([#2570](https://github.com/yandex/ClickHouse/pull/2570)).
* Fixed switching to the default database when reconnecting the client ([#2583](https://github.com/yandex/ClickHouse/pull/2583)).
* Fixed a bug that occurred when the `use_index_for_in_with_subqueries` setting was disabled.

### Security fix:

* Sending files is no longer possible when connected to MySQL (`LOAD DATA LOCAL INFILE`).

## ClickHouse release 1.1.54388, 2018-06-28

### New features:

* Support for the `ALTER TABLE t DELETE WHERE` query for replicated tables. Added the `system.mutations` table to track progress of this type of queries.
* Support for the `ALTER TABLE t [REPLACE|ATTACH] PARTITION` query for \*MergeTree tables.
* Support for the `TRUNCATE TABLE` query ([Winter Zhang](https://github.com/yandex/ClickHouse/pull/2260))
* Several new `SYSTEM` queries for replicated tables (`RESTART REPLICAS`, `SYNC REPLICA`, `[STOP|START] [MERGES|FETCHES|SENDS REPLICATED|REPLICATION QUEUES]`).
* Added the ability to write to a table with the MySQL engine and the corresponding table function ([sundy-li](https://github.com/yandex/ClickHouse/pull/2294)).
* Added the `url()` table function and the `URL` table engine ([Alexander Sapin](https://github.com/yandex/ClickHouse/pull/2501)).
* Added the `windowFunnel`  aggregate function ([sundy-li](https://github.com/yandex/ClickHouse/pull/2352)).
* New `startsWith` and `endsWith` functions for strings ([Vadim Plakhtinsky](https://github.com/yandex/ClickHouse/pull/2429)).
* The `numbers()` table function now allows you to specify the offset ([Winter Zhang](https://github.com/yandex/ClickHouse/pull/2535)).
* The password to `clickhouse-client` can be entered interactively.
* Server logs can now be sent to syslog ([Alexander Krasheninnikov](https://github.com/yandex/ClickHouse/pull/2459)).
* Support for logging in dictionaries with a shared library source ([Alexander Sapin](https://github.com/yandex/ClickHouse/pull/2472)).
* Support for custom CSV delimiters ([Ivan Zhukov](https://github.com/yandex/ClickHouse/pull/2263))
* Added the `date_time_input_format` setting. If you switch this setting to `'best_effort'`, DateTime values will be read in a wide range of formats.
* Added the `clickhouse-obfuscator` utility for data obfuscation. Usage example: publishing data used in performance tests.

### Experimental features:

* Added the ability to calculate `and` arguments only where they are needed ([Anastasia Tsarkova](https://github.com/yandex/ClickHouse/pull/2272))
* JIT compilation to native code is now available for some expressions ([pyos](https://github.com/yandex/ClickHouse/pull/2277)).

### Bug fixes:

* Duplicates no longer appear for a query with `DISTINCT` and `ORDER BY`.
* Queries with `ARRAY JOIN` and `arrayFilter` no longer return an incorrect result.
* Fixed an error when reading an array column from a Nested structure ([#2066](https://github.com/yandex/ClickHouse/issues/2066)).
* Fixed an error when analyzing queries with a HAVING clause like `HAVING tuple IN (...)`.
* Fixed an error when analyzing queries with recursive aliases.
* Fixed an error when reading from ReplacingMergeTree with a condition in PREWHERE that filters all rows ([#2525](https://github.com/yandex/ClickHouse/issues/2525)).
* User profile settings were not applied when using sessions in the HTTP interface.
* Fixed how settings are applied from the command line parameters in clickhouse-local.
* The ZooKeeper client library now uses the session timeout received from the server.
* Fixed a bug in the ZooKeeper client library when the client waited for the server response longer than the timeout.
* Fixed pruning of parts for queries with conditions on partition key columns ([#2342](https://github.com/yandex/ClickHouse/issues/2342)).
* Merges are now possible after `CLEAR COLUMN IN PARTITION` ([#2315](https://github.com/yandex/ClickHouse/issues/2315)).
* Type mapping in the ODBC table function has been fixed ([sundy-li](https://github.com/yandex/ClickHouse/pull/2268)).
* Type comparisons have been fixed for `DateTime` with and without the time zone ([Alexander Bocharov](https://github.com/yandex/ClickHouse/pull/2400)).
* Fixed syntactic parsing and formatting of the `CAST` operator.
* Fixed insertion into a materialized view for the Distributed table engine ([Babacar Diassé](https://github.com/yandex/ClickHouse/pull/2411)).
* Fixed a race condition when writing data from the `Kafka` engine to materialized views ([Yangkuan Liu](https://github.com/yandex/ClickHouse/pull/2448)).
* Fixed SSRF in the remote() table function.
* Fixed exit behavior of `clickhouse-client` in multiline mode ([#2510](https://github.com/yandex/ClickHouse/issues/2510)).

### Improvements:

* Background tasks in replicated tables are now performed in a thread pool instead of in separate threads ([Silviu Caragea](https://github.com/yandex/ClickHouse/pull/1722)).
* Improved LZ4 compression performance.
* Faster analysis for queries with a large number of JOINs and sub-queries.
* The DNS cache is now updated automatically when there are too many network errors.
* Table inserts no longer occur if the insert into one of the materialized views is not possible because it has too many parts.
* Corrected the discrepancy in the event counters `Query`, `SelectQuery`, and `InsertQuery`.
* Expressions like `tuple IN (SELECT tuple)` are allowed if the tuple types match.
* A server with replicated tables can start even if you haven't configured ZooKeeper.
* When calculating the number of available CPU cores, limits on cgroups are now taken into account ([Atri Sharma](https://github.com/yandex/ClickHouse/pull/2325)).
* Added chown for config directories in the systemd config file ([Mikhail Shiryaev](https://github.com/yandex/ClickHouse/pull/2421)).

### Build changes:

* The gcc8 compiler can be used for builds.
* Added the ability to build llvm from submodule.
* The version of the librdkafka library has been updated to v0.11.4.
* Added the ability to use the system libcpuid library. The library version has been updated to 0.4.0.
* Fixed the build using the vectorclass library ([Babacar Diassé](https://github.com/yandex/ClickHouse/pull/2274)).
* Cmake now generates files for ninja by default (like when using `-G Ninja`).
* Added the ability to use the libtinfo library instead of libtermcap ([Georgy Kondratiev](https://github.com/yandex/ClickHouse/pull/2519)).
* Fixed a header file conflict in Fedora Rawhide ([#2520](https://github.com/yandex/ClickHouse/issues/2520)).

### Backward incompatible changes:

* Removed escaping in `Vertical` and `Pretty*` formats and deleted the `VerticalRaw` format.
* If servers with version 1.1.54388 (or newer) and servers with an older version are used simultaneously in a distributed query and the query has the `cast(x, 'Type')` expression without the `AS` keyword and doesn't have the word `cast` in uppercase, an exception will be thrown with a message like `Not found column cast(0, 'UInt8') in block`. Solution: Update the server on the entire cluster.

## ClickHouse release 1.1.54385, 2018-06-01

### Bug fixes:

* Fixed an error that in some cases caused ZooKeeper operations to block.

## ClickHouse release 1.1.54383, 2018-05-22

### Bug fixes:

* Fixed a slowdown of replication queue if a table has many replicas.

## ClickHouse release 1.1.54381, 2018-05-14

### Bug fixes:

* Fixed a nodes leak in ZooKeeper when ClickHouse loses connection to ZooKeeper server.

## ClickHouse release 1.1.54380, 2018-04-21

### New features:

* Added the table function `file(path, format, structure)`. An example reading bytes from `/dev/urandom`: `ln -s /dev/urandom /var/lib/clickhouse/user_files/random``clickhouse-client -q "SELECT * FROM file('random', 'RowBinary', 'd UInt8') LIMIT 10"`.

### Improvements:

* Subqueries can be wrapped in `()` brackets to enhance query readability. For example: `(SELECT 1) UNION ALL (SELECT 1)`.
* Simple `SELECT` queries from the `system.processes` table are not included in the `max_concurrent_queries` limit.

### Bug fixes:

* Fixed incorrect behavior of the `IN` operator when select from `MATERIALIZED VIEW`.
* Fixed incorrect filtering by partition index in expressions like `partition_key_column IN (...)`.
* Fixed inability to execute `OPTIMIZE` query on non-leader replica if `REANAME` was performed on the table.
* Fixed the authorization error when executing `OPTIMIZE` or `ALTER` queries on a non-leader replica.
* Fixed freezing of `KILL QUERY`.
* Fixed an error in ZooKeeper client library which led to loss of watches, freezing of distributed DDL queue, and slowdowns in the replication queue if a non-empty `chroot` prefix is used in the ZooKeeper configuration.

### Backward incompatible changes:

* Removed support for expressions like `(a, b) IN (SELECT (a, b))` (you can use the equivalent expression `(a, b) IN (SELECT a, b)`). In previous releases, these expressions led to undetermined `WHERE` filtering or caused errors.

## ClickHouse release 1.1.54378, 2018-04-16

### New features:

* Logging level can be changed without restarting the server.
* Added the `SHOW CREATE DATABASE` query.
* The `query_id` can be passed to `clickhouse-client` (elBroom).
* New setting: `max_network_bandwidth_for_all_users`.
* Added support for `ALTER TABLE ... PARTITION ... ` for `MATERIALIZED VIEW`.
* Added information about the size of data parts in uncompressed form in the system table.
* Server-to-server encryption support for distributed tables (`<secure>1</secure>` in the replica config in `<remote_servers>`).
* Configuration of the table level for the `ReplicatedMergeTree` family in order to minimize the amount of data stored in Zookeeper: : `use_minimalistic_checksums_in_zookeeper = 1`
* Configuration of the `clickhouse-client` prompt. By default, server names are now output to the prompt. The server's display name can be changed. It's also sent in the `X-ClickHouse-Display-Name` HTTP header (Kirill Shvakov).
* Multiple comma-separated `topics` can be specified for the `Kafka` engine  (Tobias Adamson)
* When a query is stopped by `KILL QUERY` or `replace_running_query`, the client receives the `Query was cancelled` exception instead of an incomplete result.

### Improvements:

* `ALTER TABLE ... DROP/DETACH PARTITION` queries are run at the front of the replication queue.
* `SELECT ... FINAL` and `OPTIMIZE ... FINAL` can be used even when the table has a single data part.
* A `query_log` table is recreated on the fly if it was deleted manually (Kirill Shvakov).
* The `lengthUTF8` function runs faster (zhang2014).
* Improved performance of synchronous inserts in `Distributed` tables (`insert_distributed_sync = 1`) when there is a very large number of shards.
* The server accepts the `send_timeout` and `receive_timeout` settings from the client and applies them when connecting to the client (they are applied in reverse order: the server socket's `send_timeout` is set to the `receive_timeout` value received from the client, and vice versa).
* More robust crash recovery for asynchronous insertion into `Distributed` tables.
* The return type of the `countEqual` function changed from `UInt32` to `UInt64` (谢磊).

### Bug fixes:

* Fixed an error with `IN` when the left side of the expression is `Nullable`.
* Correct results are now returned when using tuples with `IN` when some of the tuple components are in the table index.
* The `max_execution_time` limit now works correctly with distributed queries.
* Fixed errors when calculating the size of composite columns in the `system.columns` table.
* Fixed an error when creating a temporary table `CREATE TEMPORARY TABLE IF NOT EXISTS.`
* Fixed errors in `StorageKafka` (##2075)
* Fixed server crashes from invalid arguments of certain aggregate functions.
* Fixed the error that prevented the `DETACH DATABASE` query from stopping background tasks for `ReplicatedMergeTree` tables.
* `Too many parts` state is less likely to happen when inserting into aggregated materialized views (##2084).
* Corrected recursive handling of substitutions in the config if a substitution must be followed by another substitution on the same level.
* Corrected the syntax in the metadata file when creating a `VIEW` that uses a query with `UNION ALL`.
* `SummingMergeTree` now works correctly for summation of nested data structures with a composite key.
* Fixed the possibility of a race condition when choosing the leader for `ReplicatedMergeTree` tables.

### Build changes:

* The build supports `ninja` instead of `make` and uses `ninja` by default for building releases.
* Renamed packages: `clickhouse-server-base` in `clickhouse-common-static`; `clickhouse-server-common` in `clickhouse-server`; `clickhouse-common-dbg` in `clickhouse-common-static-dbg`. To install, use `clickhouse-server clickhouse-client`. Packages with the old names will still load in the repositories for backward compatibility.

### Backward incompatible changes:

* Removed the special interpretation of an IN expression if an array is specified on the left side. Previously, the expression `arr IN (set)` was interpreted as "at least one `arr` element belongs to the `set`". To get the same behavior in the new version, write `arrayExists(x -> x IN (set), arr)`.
* Disabled the incorrect use of the socket option `SO_REUSEPORT`, which was incorrectly enabled by default in the Poco library. Note that on Linux there is no longer any reason to simultaneously specify the addresses `::` and `0.0.0.0` for listen – use just `::`, which allows listening to the connection both over IPv4 and IPv6 (with the default kernel config settings). You can also revert to the behavior from previous versions by specifying `<listen_reuse_port>1</listen_reuse_port>` in the config.

## ClickHouse release 1.1.54370, 2018-03-16

### New features:

* Added the `system.macros` table and auto updating of macros when the config file is changed.
* Added the `SYSTEM RELOAD CONFIG` query.
* Added the `maxIntersections(left_col, right_col)` aggregate function, which returns the maximum number of simultaneously intersecting intervals `[left; right]`. The `maxIntersectionsPosition(left, right)` function returns the beginning of the "maximum" interval. ([Michael Furmur](https://github.com/yandex/ClickHouse/pull/2012)).

### Improvements:

* When inserting data in a `Replicated` table, fewer requests are made to `ZooKeeper` (and most of the user-level errors have disappeared from the `ZooKeeper` log).
* Added the ability to create aliases for data sets. Example: `WITH (1, 2, 3) AS set SELECT number IN set FROM system.numbers LIMIT 10`.

### Bug fixes:

* Fixed the `Illegal PREWHERE` error when reading from Merge tables for `Distributed`tables.
* Added fixes that allow you to start clickhouse-server in IPv4-only Docker containers.
* Fixed a race condition when reading from system `system.parts_columns tables.`
* Removed double buffering during a synchronous insert to a `Distributed` table, which could have caused the connection to timeout.
* Fixed a bug that caused excessively long waits for an unavailable replica before beginning a `SELECT` query.
* Fixed incorrect dates in the `system.parts` table.
* Fixed a bug that made it impossible to insert data in a `Replicated` table if `chroot` was non-empty in the configuration of the `ZooKeeper` cluster.
* Fixed the vertical merging algorithm for an empty `ORDER BY` table.
* Restored the ability to use dictionaries in queries to remote tables, even if these dictionaries are not present on the requestor server. This functionality was lost in release 1.1.54362.
* Restored the behavior for queries like `SELECT * FROM remote('server2', default.table) WHERE col IN (SELECT col2 FROM default.table)` when the right side of the `IN` should use a remote `default.table` instead of a local one. This behavior was broken in version 1.1.54358.
* Removed extraneous error-level logging of `Not found column ... in block`.

## Clickhouse Release 1.1.54362, 2018-03-11

### New features:

* Aggregation without `GROUP BY` for an empty set (such as `SELECT count(*) FROM table WHERE 0`) now returns a result with one row with null values for aggregate functions, in compliance with the SQL standard. To restore the old behavior (return an empty result), set `empty_result_for_aggregation_by_empty_set` to 1.
* Added type conversion for `UNION ALL`. Different alias names are allowed in `SELECT` positions in `UNION ALL`, in compliance with the SQL standard.
* Arbitrary expressions are supported in `LIMIT BY` clauses. Previously, it was only possible to use columns resulting from `SELECT`.
* An index of `MergeTree` tables is used when `IN` is applied to a tuple of expressions from the columns of the primary key. Example: `WHERE (UserID, EventDate) IN ((123, '2000-01-01'), ...)` (Anastasiya Tsarkova).
* Added the `clickhouse-copier` tool for copying between clusters and resharding data (beta).
* Added consistent hashing functions: `yandexConsistentHash`, `jumpConsistentHash`, `sumburConsistentHash`. They can be used as a sharding key in order to reduce the amount of network traffic during subsequent reshardings.
* Added functions: `arrayAny`, `arrayAll`, `hasAny`, `hasAll`, `arrayIntersect`, `arrayResize`.
* Added the `arrayCumSum` function (Javi Santana).
* Added the `parseDateTimeBestEffort`, `parseDateTimeBestEffortOrZero`, and `parseDateTimeBestEffortOrNull` functions to read the DateTime from a string containing text in a wide variety of possible formats.
* Data can be partially reloaded from external dictionaries during updating (load just the records in which the value of the specified field greater than in the previous download) (Arsen Hakobyan).
* Added the `cluster` table function. Example: `cluster(cluster_name, db, table)`. The `remote` table function can accept the cluster name as the first argument, if it is specified as an identifier.
* The `remote` and `cluster` table functions can be used in `INSERT` queries.
* Added the `create_table_query` and `engine_full` virtual columns to the `system.tables`table . The `metadata_modification_time` column is virtual.
* Added the `data_path` and `metadata_path` columns to `system.tables`and` system.databases` tables, and added the `path` column to the `system.parts` and `system.parts_columns` tables.
* Added additional information about merges in the `system.part_log` table.
* An arbitrary partitioning key can be used for the `system.query_log` table (Kirill Shvakov).
* The `SHOW TABLES` query now also shows temporary tables. Added temporary tables and the `is_temporary` column to `system.tables` (zhang2014).
* Added `DROP TEMPORARY TABLE` and `EXISTS TEMPORARY TABLE` queries (zhang2014).
* Support for `SHOW CREATE TABLE` for temporary tables (zhang2014).
* Added the `system_profile` configuration parameter for the settings used by internal processes.
* Support for loading `object_id` as an attribute in `MongoDB` dictionaries (Pavel Litvinenko).
* Reading `null` as the default value when loading data for an external dictionary with the `MongoDB` source (Pavel Litvinenko).
* Reading `DateTime` values in the `Values` format from a Unix timestamp without single quotes.
* Failover is supported in `remote` table functions for cases when some of the replicas are missing the requested table.
* Configuration settings can be overridden in the command line when you run `clickhouse-server`. Example: `clickhouse-server -- --logger.level=information`.
* Implemented the `empty` function from a `FixedString` argument: the function returns 1 if the string consists entirely of null bytes (zhang2014).
* Added the `listen_try`configuration parameter for listening to at least one of the listen addresses without quitting, if some of the addresses can't be listened to (useful for systems with disabled support for IPv4 or IPv6).
* Added the `VersionedCollapsingMergeTree` table engine.
* Support for rows and arbitrary numeric types for the `library` dictionary source.
* `MergeTree` tables can be used without a primary key (you need to specify `ORDER BY tuple()`).
* A `Nullable` type can be `CAST` to a non-`Nullable` type if the argument is not `NULL`.
* `RENAME TABLE` can be performed for `VIEW`.
* Added the `throwIf` function.
* Added the `odbc_default_field_size` option, which allows you to extend the maximum size of the value loaded from an ODBC source (by default, it is 1024).
* The `system.processes` table and `SHOW PROCESSLIST` now have the `is_cancelled` and `peak_memory_usage` columns.

### Improvements:

* Limits and quotas on the result are no longer applied to intermediate data for `INSERT SELECT` queries or for `SELECT` subqueries.
* Fewer false triggers of `force_restore_data` when checking the status of `Replicated` tables when the server starts.
* Added the `allow_distributed_ddl` option.
* Nondeterministic functions are not allowed in expressions for `MergeTree` table keys.
* Files with substitutions from `config.d` directories are loaded in alphabetical order.
* Improved performance of the `arrayElement` function in the case of a constant multidimensional array with an empty array as one of the elements. Example: `[[1], []][x]`.
* The server starts faster now when using configuration files with very large substitutions (for instance, very large lists of IP networks).
* When running a query, table valued functions run once. Previously, `remote` and `mysql`  table valued functions performed the same query twice to retrieve the table structure from a remote server.
* The `MkDocs` documentation generator is used.
* When you try to delete a table column that `DEFAULT`/`MATERIALIZED` expressions of other columns depend on, an exception is thrown (zhang2014).
* Added the ability to parse an empty line in text formats as the number 0 for `Float` data types. This feature was previously available but was lost in release 1.1.54342.
* `Enum` values can be used in `min`, `max`, `sum` and some other functions. In these cases, it uses the corresponding numeric values. This feature was previously available but was lost in the release 1.1.54337.
* Added `max_expanded_ast_elements` to restrict the size of the AST after recursively expanding aliases.

### Bug fixes:

* Fixed cases when unnecessary columns were removed from subqueries in error, or not removed from subqueries containing `UNION ALL`.
* Fixed a bug in merges for `ReplacingMergeTree` tables.
* Fixed synchronous insertions in `Distributed` tables (`insert_distributed_sync = 1`).
* Fixed segfault for certain uses of `FULL` and `RIGHT JOIN` with duplicate columns in subqueries.
* Fixed segfault for certain uses of `replace_running_query` and `KILL QUERY`.
* Fixed the order of the `source` and `last_exception` columns in the `system.dictionaries` table.
* Fixed a bug when the `DROP DATABASE` query did not delete the file with metadata.
* Fixed the `DROP DATABASE` query for `Dictionary` databases.
* Fixed the low precision of `uniqHLL12` and `uniqCombined` functions for cardinalities greater than 100 million items (Alex Bocharov).
* Fixed the calculation of implicit default values when necessary to simultaneously calculate default explicit expressions in `INSERT` queries (zhang2014).
* Fixed a rare case when a query to a `MergeTree` table couldn't finish (chenxing-xc).
* Fixed a crash that occurred when running a `CHECK` query for `Distributed` tables if all shards are local (chenxing.xc).
* Fixed a slight performance regression with functions that use regular expressions.
* Fixed a performance regression when creating multidimensional arrays from complex expressions.
* Fixed a bug that could cause an extra `FORMAT` section to appear in an `.sql` file with metadata.
* Fixed a bug that caused the `max_table_size_to_drop` limit to apply when trying to delete a `MATERIALIZED VIEW` looking at an explicitly specified table.
* Fixed incompatibility with old clients (old clients were sometimes sent data with the `DateTime('timezone')` type, which they do not understand).
* Fixed a bug when reading `Nested` column elements of structures that were added using `ALTER` but that are empty for the old partitions, when the conditions for these columns moved to `PREWHERE`.
* Fixed a bug when filtering tables by virtual `_table` columns in queries to `Merge` tables.
* Fixed a bug when using `ALIAS` columns in `Distributed` tables.
* Fixed a bug that made dynamic compilation impossible for queries with aggregate functions from the `quantile` family.
* Fixed a race condition in the query execution pipeline that occurred in very rare cases when using `Merge` tables with a large number of tables, and when using `GLOBAL` subqueries.
* Fixed a crash when passing arrays of different sizes to an `arrayReduce` function when using aggregate functions from multiple arguments.
* Prohibited the use of queries with `UNION ALL` in a `MATERIALIZED VIEW`.
* Fixed an error during initialization of the `part_log`  system table when the server starts (by default, `part_log` is disabled).

### Backward incompatible changes:

* Removed the `distributed_ddl_allow_replicated_alter` option. This behavior is enabled by default.
* Removed the `strict_insert_defaults` setting. If you were using this functionality, write to `clickhouse-feedback@yandex-team.com`.
* Removed the `UnsortedMergeTree` engine.

## Clickhouse Release 1.1.54343, 2018-02-05

* Added macros support for defining cluster names in distributed DDL queries and constructors of Distributed tables: `CREATE TABLE distr ON CLUSTER '{cluster}' (...) ENGINE = Distributed('{cluster}', 'db', 'table')`.
* Now queries like `SELECT ... FROM table WHERE expr IN (subquery)` are processed using the `table` index.
* Improved processing of duplicates when inserting to Replicated tables, so they no longer slow down execution of the replication queue.

## Clickhouse Release 1.1.54342, 2018-01-22

This release contains bug fixes for the previous release 1.1.54337:

* Fixed a regression in 1.1.54337: if the default user has readonly access, then the server refuses to start up with the message  `Cannot create database in readonly mode`.
* Fixed a regression in 1.1.54337: on systems with systemd, logs are always written to syslog regardless of the configuration; the watchdog script still uses init.d.
* Fixed a regression in 1.1.54337: wrong default configuration in the Docker image.
* Fixed nondeterministic behavior of GraphiteMergeTree (you can see it in log messages `Data after merge is not byte-identical to the data on another replicas`).
* Fixed a bug that may lead to inconsistent merges after OPTIMIZE query to Replicated tables (you may see it in log messages `Part ... intersects the previous part`).
* Buffer tables now work correctly when MATERIALIZED columns are present in the destination table (by zhang2014).
* Fixed a bug in implementation of NULL.

## Clickhouse Release 1.1.54337, 2018-01-18

### New features:

* Added support for storage of multi-dimensional arrays and tuples (`Tuple` data type) in tables.
* Support for table functions for `DESCRIBE` and `INSERT` queries. Added support for subqueries in  `DESCRIBE`. Examples: `DESC TABLE remote('host', default.hits)`; `DESC TABLE (SELECT 1)`; `INSERT INTO TABLE FUNCTION remote('host', default.hits)`. Support for `INSERT INTO TABLE` in addition to `INSERT INTO`.
* Improved support for time zones. The `DateTime` data type can be annotated with the timezone that is used for parsing and formatting in text formats. Example: `DateTime('Europe/Moscow')`. When timezones are specified in functions for `DateTime` arguments, the return type will track the timezone, and the value will be displayed as expected.
* Added the functions `toTimeZone`, `timeDiff`, `toQuarter`, `toRelativeQuarterNum`. The `toRelativeHour`/`Minute`/`Second` functions can take a value of type `Date` as an argument. The `now` function name is case-sensitive.
* Added the `toStartOfFifteenMinutes` function (Kirill Shvakov).
* Added the `clickhouse format` tool for formatting queries.
* Added the `format_schema_path`  configuration parameter (Marek Vavruşa). It is used for specifying a schema in `Cap'n Proto` format. Schema files can be located only in the specified directory.
* Added support for config substitutions (`incl` and `conf.d`) for configuration of external dictionaries and models (Pavel Yakunin).
* Added a column with documentation for the  `system.settings` table (Kirill Shvakov).
* Added the `system.parts_columns` table with information about column sizes in each data part of `MergeTree` tables.
* Added the `system.models` table with information about loaded `CatBoost` machine learning models.
* Added the `mysql` and `odbc` table function and corresponding `MySQL` and `ODBC` table engines for accessing remote databases. This functionality is in the beta stage.
* Added the possibility to pass an argument of type `AggregateFunction`  for the `groupArray`  aggregate function (so you can create an array of states of some aggregate function).
* Removed restrictions on various combinations of aggregate function combinators. For example, you can use `avgForEachIf`  as well as `avgIfForEach`  aggregate functions, which have different behaviors.
* The `-ForEach`  aggregate function combinator is extended for the case of aggregate functions of multiple arguments.
* Added support for aggregate functions of `Nullable`  arguments even for cases when the function returns a non-`Nullable`  result (added with the contribution of Silviu Caragea). Example: `groupArray`, `groupUniqArray`, `topK`.
* Added the `max_client_network_bandwidth` for `clickhouse-client` (Kirill Shvakov).
* Users with the ` readonly = 2`  setting are allowed to work with TEMPORARY tables (CREATE, DROP, INSERT...) (Kirill Shvakov).
* Added support for using multiple consumers with the `Kafka`  engine.  Extended configuration options for `Kafka`  (Marek Vavruša).
* Added the `intExp3`  and `intExp4`  functions.
* Added the `sumKahan`  aggregate function.
* Added the to * Number* OrNull functions, where * Number*  is a numeric type.
* Added support for `WITH` clauses for an `INSERT SELECT` query (author: zhang2014).
* Added settings: `http_connection_timeout`, `http_send_timeout`, `http_receive_timeout`. In particular, these settings are used for downloading data parts for replication. Changing these settings allows for faster failover if the network is overloaded.
* Added support for `ALTER` for tables of type `Null` (Anastasiya Tsarkova).
* The `reinterpretAsString`  function is extended for all data types that are stored contiguously in memory.
* Added the `--silent`  option for the `clickhouse-local`  tool. It suppresses printing query execution info in stderr.
* Added support for reading values of type `Date`  from text in a format where the month and/or day of the month is specified using a single digit instead of two digits (Amos Bird).

### Performance optimizations:

* Improved performance of aggregate functions `min`, `max`, `any`, `anyLast`, `anyHeavy`, `argMin`, `argMax` from string arguments.
* Improved performance of the functions `isInfinite`, `isFinite`, `isNaN`, `roundToExp2`.
* Improved performance of parsing and formatting `Date` and `DateTime` type values  in text format.
* Improved performance and precision of parsing floating point numbers.
* Lowered memory usage for `JOIN`  in the case when the left and right parts have columns with identical names that are not contained in `USING` .
* Improved performance of aggregate functions `varSamp`, `varPop`, `stddevSamp`, `stddevPop`, `covarSamp`, `covarPop`, `corr` by reducing computational stability. The old functions are available under the names `varSampStable`, `varPopStable`, `stddevSampStable`, `stddevPopStable`, `covarSampStable`, `covarPopStable`, `corrStable`.

### Bug fixes:

* Fixed data deduplication after running a `DROP` or `DETACH PARTITION` query. In the previous version, dropping a partition and inserting the same data again was not working because inserted blocks were considered duplicates.
* Fixed a bug that could lead to incorrect interpretation of the `WHERE`  clause for ` CREATE MATERIALIZED VIEW`  queries with `POPULATE` .
* Fixed a bug in using the `root_path` parameter in the `zookeeper_servers` configuration.
* Fixed unexpected results of passing the `Date`  argument to `toStartOfDay` .
* Fixed the `addMonths` and `subtractMonths`  functions and the arithmetic for ` INTERVAL n MONTH`  in cases when the result has the previous year.
* Added missing support for the `UUID`  data type for `DISTINCT` , `JOIN` , and `uniq`  aggregate functions and external dictionaries (Evgeniy Ivanov). Support for `UUID`  is still incomplete.
* Fixed `SummingMergeTree`  behavior in cases when the rows summed to zero.
* Various fixes for the `Kafka`  engine (Marek Vavruša).
* Fixed incorrect behavior of the `Join`  table engine (Amos Bird).
* Fixed incorrect allocator behavior under FreeBSD and OS X.
* The `extractAll`  function now supports empty matches.
* Fixed an error that blocked usage of `libressl`  instead of `openssl` .
* Fixed the ` CREATE TABLE AS SELECT`  query from temporary tables.
* Fixed non-atomicity of updating the replication queue. This could lead to replicas being out of sync until the server restarts.
* Fixed possible overflow in `gcd` , `lcm`  and `modulo`  (`%` operator) (Maks Skorokhod).
* `-preprocessed`  files are now created after changing `umask`  (`umask`  can be changed in the config).
* Fixed a bug in the background check of parts (`MergeTreePartChecker` ) when using a custom partition key.
* Fixed parsing of tuples (values of the `Tuple`  data type) in text formats.
* Improved error messages about incompatible types passed to `multiIf` , `array`  and some other functions.
* Redesigned support for `Nullable` types. Fixed bugs that may lead to a server crash. Fixed almost all other bugs related to `  NULL`  support: incorrect type conversions in INSERT SELECT, insufficient support for Nullable in HAVING and PREWHERE, `join_use_nulls`  mode, Nullable types as arguments of `OR`  operator, etc.
* Fixed various bugs related to internal semantics of data types. Examples: unnecessary summing of `Enum`  type fields in `SummingMergeTree` ; alignment of `Enum`  types in `Pretty`  formats, etc.
* Stricter checks for allowed combinations of composite columns.
* Fixed the overflow when specifying a very large parameter for the `FixedString`  data type.
*  Fixed a bug in the `topK`  aggregate function in a generic case.
* Added the missing check for equality of array sizes in arguments of n-ary variants of aggregate functions with an `-Array`  combinator.
* Fixed a bug in `--pager` for `clickhouse-client` (author: ks1322).
* Fixed the precision of the `exp10`  function.
* Fixed the behavior of the `visitParamExtract`  function for better compliance with documentation.
* Fixed the crash when incorrect data types are specified.
* Fixed the behavior of `DISTINCT`  in the case when all columns are constants.
* Fixed query formatting in the case of using the `tupleElement`  function with a complex constant expression as the tuple element index.
* Fixed a bug in `Dictionary` tables for `range_hashed` dictionaries.
* Fixed a bug that leads to excessive rows in the result of `FULL`  and ` RIGHT JOIN`  (Amos Bird).
* Fixed a server crash when creating and removing temporary files in `config.d`  directories during config reload.
* Fixed the ` SYSTEM DROP DNS CACHE`  query: the cache was flushed but addresses of cluster nodes were not updated.
* Fixed the behavior of ` MATERIALIZED VIEW`  after executing ` DETACH TABLE`  for the table under the view (Marek Vavruša).

### Build improvements:

* The `pbuilder` tool is used for builds. The build process is almost completely independent of the build host environment.
* A single build is used for different OS versions. Packages and binaries have been made compatible with a wide range of Linux systems.
* Added the `clickhouse-test`  package. It can be used to run functional tests.
* The source tarball can now be published to the repository. It can be used to reproduce the build without using GitHub.
* Added limited integration with Travis CI. Due to limits on build time in Travis, only the debug build is tested and a limited subset of tests are run.
* Added support for `Cap'n'Proto`  in the default build.
* Changed the format of documentation sources from `Restricted Text` to `Markdown`.
* Added support for `systemd` (Vladimir Smirnov). It is disabled by default due to incompatibility with some OS images and can be enabled manually.
* For dynamic code generation, `clang`  and `lld`  are embedded into the `clickhouse`  binary. They can also be invoked as ` clickhouse clang`  and ` clickhouse lld` .
* Removed usage of GNU extensions from the code. Enabled the `-Wextra`  option. When building with `clang` the default is `libc++` instead of `libstdc++`.
* Extracted `clickhouse_parsers`  and `clickhouse_common_io`  libraries to speed up builds of various tools.

### Backward incompatible changes:

* The format for marks in `Log`  type tables that contain `Nullable`  columns was changed in a backward incompatible way. If you have these tables, you should convert them to the `TinyLog`  type before starting up the new server version. To do this, replace `ENGINE = Log` with `ENGINE = TinyLog` in the corresponding `.sql` file in the `metadata` directory. If your table doesn't have `Nullable` columns or if the type of your table is not `Log`, then you don't need to do anything.
* Removed the `experimental_allow_extended_storage_definition_syntax` setting. Now this feature is enabled by default.
* The `runningIncome` function was renamed to `runningDifferenceStartingWithFirstvalue` to avoid confusion.
* Removed the ` FROM ARRAY JOIN arr`  syntax when ARRAY JOIN is specified directly after FROM with no table (Amos Bird).
* Removed the `BlockTabSeparated`  format that was used solely for demonstration purposes.
* Changed the state format for aggregate functions `varSamp`, `varPop`, `stddevSamp`, `stddevPop`, `covarSamp`, `covarPop`, `corr`. If you have stored states of these aggregate functions in tables (using the `AggregateFunction`  data type or materialized views with corresponding states), please write to clickhouse-feedback@yandex-team.com.
* In previous server versions there was an undocumented feature: if an aggregate function depends on parameters, you can still specify it without parameters in the AggregateFunction data type. Example: `AggregateFunction(quantiles, UInt64)` instead of `AggregateFunction(quantiles(0.5, 0.9), UInt64)`. This feature was lost. Although it was undocumented, we plan to support it again in future releases.
* Enum data types cannot be used in min/max aggregate functions. This ability will be returned in the next release.

### Please note when upgrading:

* When doing a rolling update on a cluster, at the point when some of the replicas are running the old version of ClickHouse and some are running the new version, replication is temporarily stopped and the message ` unknown parameter 'shard'`  appears in the log. Replication will continue after all replicas of the cluster are updated.
* If different versions of ClickHouse are running on the cluster servers, it is possible that distributed queries using the following functions will have incorrect results: `varSamp`, `varPop`, `stddevSamp`, `stddevPop`, `covarSamp`, `covarPop`, `corr`. You should update all cluster nodes.

## ClickHouse release 1.1.54327, 2017-12-21

This release contains bug fixes for the previous release 1.1.54318:

* Fixed bug with possible race condition in replication that could lead to data loss. This issue affects versions 1.1.54310 and 1.1.54318. If you use one of these versions with Replicated tables, the update is strongly recommended. This issue shows in logs in Warning messages like ` Part ... from own log doesn't exist.`  The issue is relevant even if you don't see these messages in logs.

## ClickHouse release 1.1.54318, 2017-11-30

This release contains bug fixes for the previous release 1.1.54310:

* Fixed incorrect row deletions during merges in the SummingMergeTree engine
* Fixed a memory leak in unreplicated MergeTree engines
* Fixed performance degradation with frequent inserts in MergeTree engines
* Fixed an issue that was causing the replication queue to stop running
* Fixed rotation and archiving of server logs

## ClickHouse release 1.1.54310, 2017-11-01

### New features:

* Custom partitioning key for the MergeTree family of table engines.
* [Kafka](https://clickhouse.yandex/docs/en/operations/table_engines/kafka/)  table engine.
* Added support for loading [CatBoost](https://catboost.yandex/)  models and applying them to data stored in ClickHouse.
* Added support for time zones with non-integer offsets from UTC.
* Added support for arithmetic operations with time intervals.
* The range of values for the Date and DateTime types is extended to the year 2105.
* Added the ` CREATE MATERIALIZED VIEW x TO y`  query (specifies an existing table for storing the data of a materialized view).
* Added the `ATTACH TABLE` query without arguments.
* The processing logic for Nested columns with names ending in -Map in a SummingMergeTree table was extracted to the sumMap aggregate function. You can now specify such columns explicitly.
* Max size of the IP trie dictionary is increased to 128M entries.
* Added the getSizeOfEnumType function.
* Added the sumWithOverflow aggregate function.
* Added support for the Cap'n Proto input format.
* You can now customize compression level when using the zstd algorithm.

### Backward incompatible changes:

* Creation of temporary tables with an engine other than Memory is not allowed.
* Explicit creation of tables with the View or MaterializedView engine is not allowed.
* During table creation, a new check verifies that the sampling key expression is included in the primary key.

### Bug fixes:

* Fixed hangups when synchronously inserting into a Distributed table.
* Fixed nonatomic adding and removing of parts in Replicated tables.
* Data inserted into a materialized view is not subjected to unnecessary deduplication.
* Executing a query to a Distributed table for which the local replica is lagging and remote replicas are unavailable does not result in an error anymore.
*  Users don't need access permissions to the `default`  database to create temporary tables anymore.
* Fixed crashing when specifying the Array type without arguments.
* Fixed hangups when the disk volume containing server logs is full.
* Fixed an overflow in the toRelativeWeekNum function for the first week of the Unix epoch.

### Build improvements:

* Several third-party libraries (notably Poco) were updated and converted to git submodules.

## ClickHouse release 1.1.54304, 2017-10-19

### New features:

* TLS support in the native protocol (to enable, set `tcp_ssl_port`  in `config.xml` ).

### Bug fixes:

* `ALTER` for replicated tables now tries to start running as soon as possible.
* Fixed crashing when reading data with the setting `preferred_block_size_bytes=0.`
* Fixed crashes of `clickhouse-client` when pressing ` Page Down`
* Correct interpretation of certain complex queries with `GLOBAL IN` and `UNION ALL`
*  `FREEZE PARTITION` always works atomically now.
* Empty POST requests now return a response with code 411.
* Fixed interpretation errors for expressions like `CAST(1 AS Nullable(UInt8)).`
* Fixed an error when reading `Array(Nullable(String))` columns from `MergeTree` tables.
* Fixed crashing when parsing queries like `SELECT dummy AS dummy, dummy AS b`
* Users are updated correctly with invalid `users.xml`
* Correct handling when an executable dictionary returns a non-zero response code.

## ClickHouse release 1.1.54292, 2017-09-20

### New features:

* Added the  `pointInPolygon` function for working with coordinates on a coordinate plane.
* Added the `sumMap` aggregate function for calculating the sum of arrays, similar to  `SummingMergeTree`.
* Added the `trunc` function. Improved performance of the rounding functions (`round`, `floor`, `ceil`, `roundToExp2`) and corrected the logic of how they work. Changed the logic of the `roundToExp2`  function for fractions and negative numbers.
* The ClickHouse executable file is now less dependent on the libc version. The same ClickHouse executable file can run on a wide variety of Linux systems. There is still a dependency when using compiled queries (with the setting ` compile = 1` , which is not used by default).
* Reduced the time needed for dynamic compilation of queries.

### Bug fixes:

* Fixed an error that sometimes produced ` part ... intersects previous part`  messages and weakened replica consistency.
* Fixed an error that caused the server to lock up if ZooKeeper was unavailable during shutdown.
* Removed excessive logging when restoring replicas.
* Fixed an error in the UNION ALL implementation.
* Fixed an error in the concat function that occurred if the first column in a block has the Array type.
* Progress is now displayed correctly in the system.merges table.

## ClickHouse release 1.1.54289, 2017-09-13

### New features:

* `SYSTEM` queries for server administration: `SYSTEM RELOAD DICTIONARY`, `SYSTEM RELOAD DICTIONARIES`, `SYSTEM DROP DNS CACHE`, `SYSTEM SHUTDOWN`, `SYSTEM KILL`.
* Added functions for working with arrays: `concat`, `arraySlice`, `arrayPushBack`, `arrayPushFront`, `arrayPopBack`, `arrayPopFront`.
* Added `root` and `identity` parameters for the ZooKeeper configuration. This allows you to isolate individual users on the same ZooKeeper cluster.
* Added aggregate functions `groupBitAnd`, `groupBitOr`, and `groupBitXor` (for compatibility, they are also available under the names `BIT_AND`, `BIT_OR`, and `BIT_XOR`).
* External dictionaries can be loaded from MySQL by specifying a socket in the filesystem.
* External dictionaries can be loaded from MySQL over SSL (`ssl_cert`, `ssl_key`, `ssl_ca`  parameters).
* Added the `max_network_bandwidth_for_user` setting to restrict the overall bandwidth use for queries per user.
* Support for `DROP TABLE` for temporary tables.
* Support for reading `DateTime`  values in Unix timestamp format from the `CSV`  and `JSONEachRow`  formats.
* Lagging replicas in distributed queries are now excluded by default (the default threshold is 5 minutes).
* FIFO locking is used during ALTER: an ALTER query isn't blocked indefinitely for continuously running queries.
* Option to set `umask`  in the config file.
* Improved performance for queries with `DISTINCT` .

### Bug fixes:

* Improved the process for deleting old nodes in ZooKeeper. Previously, old nodes sometimes didn't get deleted if there were very frequent inserts, which caused the server to be slow to shut down, among other things.
* Fixed randomization when choosing hosts for the connection to ZooKeeper.
* Fixed the exclusion of lagging replicas in distributed queries if the replica is localhost.
* Fixed an error where a data part in a `ReplicatedMergeTree`  table could be broken after running ` ALTER MODIFY`  on an element in a `Nested`  structure.
* Fixed an error that could cause SELECT queries to "hang".
* Improvements to distributed DDL queries.
* Fixed the query `CREATE TABLE ... AS <materialized view>`.
* Resolved the deadlock in the ` ALTER ... CLEAR COLUMN IN PARTITION`  query for `Buffer`  tables.
* Fixed the invalid default value for `Enum` s (0 instead of the minimum) when using the `JSONEachRow`  and `TSKV`  formats.
* Resolved the appearance of zombie processes when using a dictionary with an `executable`  source.
* Fixed segfault for the HEAD query.

### Improved workflow for developing and assembling ClickHouse:

* You can use `pbuilder`  to build ClickHouse.
* You can use `libc++`  instead of `libstdc++` for builds on Linux.
* Added instructions for using static code analysis tools: `Coverage`, `clang-tidy`, `cppcheck`.

### Please note when upgrading:

* There is now a higher default value for the MergeTree setting `max_bytes_to_merge_at_max_space_in_pool`  (the maximum total size of data parts to merge, in bytes): it has increased from 100 GiB to 150 GiB. This might result in large merges running after the server upgrade, which could cause an increased load on the disk subsystem. If the free space available on the server is less than twice the total amount of the merges that are running, this will cause all other merges to stop running, including merges of small data parts. As a result, INSERT queries will fail with the message "Merges are processing significantly slower than inserts." Use the ` SELECT * FROM system.merges`  query to monitor the situation. You can also check the `DiskSpaceReservedForMerge`  metric in the `system.metrics`  table, or in Graphite. You don't need to do anything to fix this, since the issue will resolve itself once the large merges finish. If you find this unacceptable, you can restore the previous value for the `max_bytes_to_merge_at_max_space_in_pool`  setting. To do this, go to the <merge_tree> section in config.xml, set `<merge_tree>``<max_bytes_to_merge_at_max_space_in_pool>107374182400</max_bytes_to_merge_at_max_space_in_pool>` and restart the server.

## ClickHouse release 1.1.54284, 2017-08-29

* This is a bugfix release for the previous 1.1.54282 release. It fixes leaks in the parts directory in ZooKeeper.

## ClickHouse release 1.1.54282, 2017-08-23

This release contains bug fixes for the previous release 1.1.54276:

* Fixed `DB::Exception: Assertion violation: !_path.empty()` when inserting into a Distributed table.
* Fixed parsing when inserting in RowBinary format if input data starts with';'.
* Errors during runtime compilation of certain aggregate functions (e.g. `groupArray()`).

## Clickhouse Release 1.1.54276, 2017-08-16

### New features:

* Added an optional WITH section for a SELECT query. Example query: `WITH 1+1 AS a SELECT a, a*a`
* INSERT can be performed synchronously in a Distributed table: OK is returned only after all the data is saved on all the shards. This is activated by the setting insert_distributed_sync=1.
* Added the UUID data type for working with 16-byte identifiers.
* Added aliases of CHAR, FLOAT and other types for compatibility with the Tableau.
* Added the functions toYYYYMM, toYYYYMMDD, and toYYYYMMDDhhmmss for converting time into numbers.
* You can use IP addresses (together with the hostname) to identify servers for clustered DDL queries.
* Added support for non-constant arguments and negative offsets in the function `substring(str, pos, len).`
* Added the max_size parameter for the `groupArray(max_size)(column)` aggregate function, and optimized its performance.

### Main changes:

* Security improvements: all server files are created with 0640 permissions (can be changed via <umask> config parameter).
* Improved error messages for queries with invalid syntax.
* Significantly reduced memory consumption and improved performance when merging large sections of MergeTree data.
* Significantly increased the performance of data merges for the ReplacingMergeTree engine.
* Improved performance for asynchronous inserts from a Distributed table by combining multiple source inserts. To enable this functionality, use the setting distributed_directory_monitor_batch_inserts=1.

### Backward incompatible changes:

* Changed the binary format of aggregate states of `groupArray(array_column)` functions for arrays.

### Complete list of changes:

* Added the `output_format_json_quote_denormals` setting, which enables outputting nan and inf values in JSON format.
* Optimized stream allocation when reading from a Distributed table.
* Settings can be configured in readonly mode if the value doesn't change.
* Added the ability to retrieve non-integer granules of the MergeTree engine in order to meet restrictions on the block size specified in the preferred_block_size_bytes setting. The purpose is to reduce the consumption of RAM and increase cache locality when processing queries from tables with large columns.
* Efficient use of indexes that contain expressions like `toStartOfHour(x)` for conditions like `toStartOfHour(x) op сonstexpr.`
* Added new settings for MergeTree engines (the merge_tree section in config.xml):
  - replicated_deduplication_window_seconds sets the number of seconds allowed for deduplicating inserts in Replicated tables.
  - cleanup_delay_period sets how often to start cleanup to remove outdated data.
  - replicated_can_become_leader can prevent a replica from becoming the leader (and assigning merges).
* Accelerated cleanup to remove outdated data from ZooKeeper.
* Multiple improvements and fixes for clustered DDL queries. Of particular interest is the new setting distributed_ddl_task_timeout, which limits the time to wait for a response from the servers in the cluster.
* Improved display of stack traces in the server logs.
* Added the "none" value for the compression method.
* You can use multiple dictionaries_config sections in config.xml.
* It is possible to connect to MySQL through a socket in the file system.
* The system.parts table has a new column with information about the size of marks, in bytes.

### Bug fixes:

* Distributed tables using a Merge table now work correctly for a SELECT query with a condition on the  `_table` field.
* Fixed a rare race condition in ReplicatedMergeTree when checking data parts.
* Fixed possible freezing on "leader election" when starting a server.
* The max_replica_delay_for_distributed_queries setting was ignored when using a local replica of the data source. This has been fixed.
* Fixed incorrect behavior of `ALTER TABLE CLEAR COLUMN IN PARTITION` when attempting to clean a non-existing column.
* Fixed an exception in the multiIf function when using empty arrays or strings.
* Fixed excessive memory allocations when deserializing Native format.
* Fixed incorrect auto-update of Trie dictionaries.
* Fixed an exception when running queries with a GROUP BY clause from a Merge table when using SAMPLE.
* Fixed a crash of GROUP BY when  using distributed_aggregation_memory_efficient=1.
* Now you can specify the database.table in the right side of IN and JOIN.
* Too many threads were used for parallel aggregation. This has been fixed.
* Fixed how the "if" function works with FixedString arguments.
* SELECT worked incorrectly from a Distributed table for shards with a weight of 0. This has been fixed.
* Running `CREATE VIEW IF EXISTS no longer causes crashes.`
* Fixed incorrect behavior when input_format_skip_unknown_fields=1 is set and there are negative numbers.
* Fixed an infinite loop in the `dictGetHierarchy()` function if there is some invalid data in the dictionary.
* Fixed `Syntax error: unexpected (...)` errors  when running distributed queries with subqueries in an IN or JOIN clause and Merge tables.
* Fixed an incorrect interpretation of a SELECT query from Dictionary tables.
* Fixed the "Cannot mremap" error when using arrays in IN and JOIN clauses with more than 2 billion elements.
* Fixed the failover for dictionaries with MySQL as the source.

### Improved workflow for developing and assembling ClickHouse:

* Builds can be assembled in Arcadia.
* You can use gcc 7 to compile ClickHouse.
* Parallel builds using ccache+distcc are faster now.

## ClickHouse release 1.1.54245, 2017-07-04

### New features:

* Distributed DDL (for example, `CREATE TABLE ON CLUSTER`)
* The replicated query `ALTER TABLE CLEAR COLUMN IN PARTITION.`
* The engine for Dictionary tables (access to dictionary data in the form of a table).
* Dictionary database engine (this type of database automatically has Dictionary tables available for all the connected external dictionaries).
* You can check for updates to the dictionary by sending a request to the source.
* Qualified column names
* Quoting identifiers using double quotation marks.
* Sessions in the HTTP interface.
* The OPTIMIZE query for a Replicated table can can run not only on the leader.

### Backward incompatible changes:

* Removed SET GLOBAL.

### Minor changes:

* Now after an alert is triggered, the log prints the full stack trace.
* Relaxed the verification of the number of damaged/extra data parts at startup (there were too many false positives).

### Bug fixes:

* Fixed a bad connection "sticking" when inserting into a Distributed table.
* GLOBAL IN now works for a query from a Merge table that looks at a Distributed table.
* The incorrect number of cores was detected on a Google Compute Engine virtual machine. This has been fixed.
* Changes in how an executable source of cached external dictionaries works.
* Fixed the comparison of strings containing null characters.
* Fixed the comparison of Float32 primary key fields with constants.
* Previously, an incorrect estimate of the size of a field could lead to overly large allocations.
* Fixed a crash when querying a Nullable column added to a table using ALTER.
* Fixed a crash when sorting by a Nullable column, if the number of rows is less than LIMIT.
* Fixed an ORDER BY subquery consisting of only constant values.
* Previously, a Replicated table could remain in the invalid state after a failed DROP TABLE.
* Aliases for scalar subqueries with empty results are no longer lost.
* Now a query that used compilation does not fail with an error if the .so file gets damaged.
