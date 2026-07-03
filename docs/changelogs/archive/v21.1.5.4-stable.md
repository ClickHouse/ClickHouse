---
sidebar_position: 1
sidebar_label: 2022
---

# 2022 Changelog

### ClickHouse release v21.1.5.4-stable FIXME as compared to v21.1.4.46-stable

#### Bug Fix
* Backported in [#20678](https://github.com/ClickHouse/ClickHouse/issues/20678): Mark distributed batch as broken in case of empty data block in one of files. [#19449](https://github.com/ClickHouse/ClickHouse/pull/19449) ([Azat Khuzhin](https://github.com/azat)).
* Backported in [#20646](https://github.com/ClickHouse/ClickHouse/issues/20646): The function `greatCircleAngle` returned inaccurate results in previous versions. This closes [#19769](https://github.com/ClickHouse/ClickHouse/issues/19769). [#19789](https://github.com/ClickHouse/ClickHouse/pull/19789) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Backported in [#20507](https://github.com/ClickHouse/ClickHouse/issues/20507): Fixed the behavior when in case of broken JSON we tried to read the whole file into memory which leads to exception from the allocator. Fixes [#19719](https://github.com/ClickHouse/ClickHouse/issues/19719). [#20286](https://github.com/ClickHouse/ClickHouse/pull/20286) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov)).
* Backported in [#20618](https://github.com/ClickHouse/ClickHouse/issues/20618): Check if table function `view` is used in expression list and throw an error. This fixes [#20342](https://github.com/ClickHouse/ClickHouse/issues/20342). [#20350](https://github.com/ClickHouse/ClickHouse/pull/20350) ([Amos Bird](https://github.com/amosbird)).
* Backported in [#20488](https://github.com/ClickHouse/ClickHouse/issues/20488): Fix `LOGICAL_ERROR` for `join_use_nulls=1` when JOIN contains const from SELECT. [#20461](https://github.com/ClickHouse/ClickHouse/pull/20461) ([Azat Khuzhin](https://github.com/azat)).
* Backported in [#20886](https://github.com/ClickHouse/ClickHouse/issues/20886): Fix subquery with union distinct and limit clause. close [#20597](https://github.com/ClickHouse/ClickHouse/issues/20597). [#20610](https://github.com/ClickHouse/ClickHouse/pull/20610) ([flynn](https://github.com/ucasfl)).
* Backported in [#20992](https://github.com/ClickHouse/ClickHouse/issues/20992): Fix usage of `-Distinct` combinator with `-State` combinator in aggregate functions. [#20866](https://github.com/ClickHouse/ClickHouse/pull/20866) ([Anton Popov](https://github.com/CurtizJ)).
* Backported in [#20988](https://github.com/ClickHouse/ClickHouse/issues/20988): `USE database;` query did not work when using MySQL 5.7 client to connect to ClickHouse server, it's fixed. Fixes [#18926](https://github.com/ClickHouse/ClickHouse/issues/18926). [#20878](https://github.com/ClickHouse/ClickHouse/pull/20878) ([Alexander Tokmakov](https://github.com/tavplubix)).
