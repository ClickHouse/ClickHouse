---
sidebar_position: 1
sidebar_label: 2023
---

# 2023 Changelog

### ClickHouse release v23.4.4.16-stable (747ba4fc6a0) FIXME as compared to v23.4.3.48-stable (d9199f8d3cc)

#### Bug Fix (user-visible misbehavior in an official stable release)

* Fix crash when Pool::Entry::disconnect() is called [#50334](https://github.com/ClickHouse/ClickHouse/pull/50334) ([Val Doroshchuk](https://github.com/valbok)).
* Fix iceberg V2 optional metadata parsing [#50974](https://github.com/ClickHouse/ClickHouse/pull/50974) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Fix subtly broken copy-on-write of ColumnLowCardinality dictionary [#51064](https://github.com/ClickHouse/ClickHouse/pull/51064) ([Michael Kolupaev](https://github.com/al13n321)).
* Generate safe IVs [#51086](https://github.com/ClickHouse/ClickHouse/pull/51086) ([Salvatore Mesoraca](https://github.com/aiven-sal)).

#### NOT FOR CHANGELOG / INSIGNIFICANT

* Don't mark a part as broken on `Poco::TimeoutException` [#50811](https://github.com/ClickHouse/ClickHouse/pull/50811) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Fix a versions' tweak for tagged commits, improve version_helper [#51035](https://github.com/ClickHouse/ClickHouse/pull/51035) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Sqlancer has changed master to main [#51060](https://github.com/ClickHouse/ClickHouse/pull/51060) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).

