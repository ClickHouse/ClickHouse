---
sidebar_position: 1
sidebar_label: 2022
---

# 2022 Changelog

### ClickHouse release master (90f17a94400) FIXME as compared to v22.8.1.2097-lts (09a2ff88435)

#### Improvement
* Improve and fix dictionaries in Arrow format. [#40173](https://github.com/ClickHouse/ClickHouse/pull/40173) ([Kruglov Pavel](https://github.com/Avogar)).
* - Fix storage merge on view cannot use index. [#40233](https://github.com/ClickHouse/ClickHouse/pull/40233) ([Duc Canh Le](https://github.com/canhld94)).
* It is now possible to set a custom error code for the exception thrown by function "throwIf". [#40319](https://github.com/ClickHouse/ClickHouse/pull/40319) ([Robert Schulze](https://github.com/rschu1ze)).

#### NOT FOR CHANGELOG / INSIGNIFICANT

* use ROBOT_CLICKHOUSE_COMMIT_TOKEN for create-pull-request [#40067](https://github.com/ClickHouse/ClickHouse/pull/40067) ([Yakov Olkhovskiy](https://github.com/yakov-olkhovskiy)).
* Allow parallel execution of disjoint drop ranges [#40246](https://github.com/ClickHouse/ClickHouse/pull/40246) ([Alexander Tokmakov](https://github.com/tavplubix)).
* fix typo [#40337](https://github.com/ClickHouse/ClickHouse/pull/40337) ([Peiqi Yin](https://github.com/yinpeiqi)).
* Fix setting description [#40347](https://github.com/ClickHouse/ClickHouse/pull/40347) ([Kruglov Pavel](https://github.com/Avogar)).
* split concurrent_threads_soft_limit setting into two [#40348](https://github.com/ClickHouse/ClickHouse/pull/40348) ([Sergei Trifonov](https://github.com/serxa)).
* Fix "incorrect replica identifier" [#40352](https://github.com/ClickHouse/ClickHouse/pull/40352) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Make test for s3 schema inference cache better [#40355](https://github.com/ClickHouse/ClickHouse/pull/40355) ([Kruglov Pavel](https://github.com/Avogar)).
* Update version_date.tsv and changelogs after v22.8.1.2097-lts [#40358](https://github.com/ClickHouse/ClickHouse/pull/40358) ([github-actions[bot]](https://github.com/apps/github-actions)).
* Fix typo in the S3 download links for [#40359](https://github.com/ClickHouse/ClickHouse/pull/40359) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Reduce changelog verbosity in CI [#40360](https://github.com/ClickHouse/ClickHouse/pull/40360) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Fix wrong assignees argument passing [#40367](https://github.com/ClickHouse/ClickHouse/pull/40367) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* And the last tiny fix for assignees [#40373](https://github.com/ClickHouse/ClickHouse/pull/40373) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Disable flaky 02382_filesystem_cache_persistent_files [#40386](https://github.com/ClickHouse/ClickHouse/pull/40386) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Disable flaky 02293_part_log_has_merge_reason [#40387](https://github.com/ClickHouse/ClickHouse/pull/40387) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Suppress "Cannot find column <subquery> in ActionsDAG result" [#40388](https://github.com/ClickHouse/ClickHouse/pull/40388) ([Alexander Tokmakov](https://github.com/tavplubix)).

