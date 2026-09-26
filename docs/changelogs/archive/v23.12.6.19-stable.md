---
sidebar_position: 1
sidebar_label: 2024
---

# 2024 Changelog

### ClickHouse release v23.12.6.19-stable (40080a3c2a4) FIXME as compared to v23.12.5.81-stable (a0fbe3ae813)

#### Bug Fix (user-visible misbehavior in an official stable release)

* Improve isolation of query cache entries under re-created users or role switches [#58611](https://github.com/ClickHouse/ClickHouse/pull/58611) ([Robert Schulze](https://github.com/rschu1ze)).
* Fix possible incorrect result of aggregate function `uniqExact` [#61257](https://github.com/ClickHouse/ClickHouse/pull/61257) ([Anton Popov](https://github.com/CurtizJ)).
* Fix consecutive keys optimization for nullable keys [#61393](https://github.com/ClickHouse/ClickHouse/pull/61393) ([Anton Popov](https://github.com/CurtizJ)).
* Fix string search with const position [#61547](https://github.com/ClickHouse/ClickHouse/pull/61547) ([Antonio Andelic](https://github.com/antonio2368)).
* Fix crash in `multiSearchAllPositionsCaseInsensitiveUTF8` for incorrect UTF-8 [#61749](https://github.com/ClickHouse/ClickHouse/pull/61749) ([pufit](https://github.com/pufit)).

#### CI Fix or Improvement (changelog entry is not required)

* Backported in [#61429](https://github.com/ClickHouse/ClickHouse/issues/61429):. [#61374](https://github.com/ClickHouse/ClickHouse/pull/61374) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Backported in [#61486](https://github.com/ClickHouse/ClickHouse/issues/61486): ... [#61441](https://github.com/ClickHouse/ClickHouse/pull/61441) ([Max K.](https://github.com/maxknv)).
* Backported in [#61641](https://github.com/ClickHouse/ClickHouse/issues/61641):. [#61592](https://github.com/ClickHouse/ClickHouse/pull/61592) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Backported in [#61811](https://github.com/ClickHouse/ClickHouse/issues/61811): ![Screenshot_20240323_025055](https://github.com/ClickHouse/ClickHouse/assets/18581488/ccaab212-a1d3-4dfb-8d56-b1991760b6bf). [#61801](https://github.com/ClickHouse/ClickHouse/pull/61801) ([Alexey Milovidov](https://github.com/alexey-milovidov)).

