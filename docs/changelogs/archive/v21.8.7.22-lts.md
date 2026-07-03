---
sidebar_position: 1
sidebar_label: 2022
---

# 2022 Changelog

### ClickHouse release v21.8.7.22-lts FIXME as compared to v21.8.6.15-lts

#### Bug Fix (user-visible misbehaviour in official stable release)

* Backported in [#29121](https://github.com/ClickHouse/ClickHouse/issues/29121): Better check for connection usability and also catch any exception in RabbitMQ shutdown just in case. [#28797](https://github.com/ClickHouse/ClickHouse/pull/28797) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Backported in [#29027](https://github.com/ClickHouse/ClickHouse/issues/29027): Fix the number of threads used in `GLOBAL IN` subquery (it was executed in single threads since [#19414](https://github.com/ClickHouse/ClickHouse/issues/19414) bugfix). [#28997](https://github.com/ClickHouse/ClickHouse/pull/28997) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Backported in [#29193](https://github.com/ClickHouse/ClickHouse/issues/29193): Fix segfault while inserting into column with type LowCardinality(Nullable) in Avro input format. [#29132](https://github.com/ClickHouse/ClickHouse/pull/29132) ([Kruglov Pavel](https://github.com/Avogar)).

#### NOT FOR CHANGELOG / INSIGNIFICANT

* Do not add const group by key for query with only having. [#28975](https://github.com/ClickHouse/ClickHouse/pull/28975) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Merging [#27963](https://github.com/ClickHouse/ClickHouse/issues/27963) [#29063](https://github.com/ClickHouse/ClickHouse/pull/29063) ([Maksim Kita](https://github.com/kitaisreal)).
* Fix terminate on uncaught exception [#29216](https://github.com/ClickHouse/ClickHouse/pull/29216) ([Alexander Tokmakov](https://github.com/tavplubix)).
