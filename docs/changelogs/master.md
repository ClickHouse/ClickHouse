---
sidebar_position: 1
sidebar_label: 2022
---

# 2022 Changelog

### ClickHouse release master (d335f264d9d) FIXME as compared to v22.8.1.2097-lts (09a2ff88435)

#### Improvement
* Improve and fix dictionaries in Arrow format. [#40173](https://github.com/ClickHouse/ClickHouse/pull/40173) ([Kruglov Pavel](https://github.com/Avogar)).
* - Fix storage merge on view cannot use index. [#40233](https://github.com/ClickHouse/ClickHouse/pull/40233) ([Duc Canh Le](https://github.com/canhld94)).
* It is now possible to set a custom error code for the exception thrown by function "throwIf". [#40319](https://github.com/ClickHouse/ClickHouse/pull/40319) ([Robert Schulze](https://github.com/rschu1ze)).

#### Build/Testing/Packaging Improvement
* We will check all queries from the changed perf tests to ensure that all changed queries were tested. [#40322](https://github.com/ClickHouse/ClickHouse/pull/40322) ([Nikita Taranov](https://github.com/nickitat)).

#### NOT FOR CHANGELOG / INSIGNIFICANT

* use ROBOT_CLICKHOUSE_COMMIT_TOKEN for create-pull-request [#40067](https://github.com/ClickHouse/ClickHouse/pull/40067) ([Yakov Olkhovskiy](https://github.com/yakov-olkhovskiy)).
* Entrypoint to check for HTTPS_PORT if http_port is disabled. [#40074](https://github.com/ClickHouse/ClickHouse/pull/40074) ([Heena Bansal](https://github.com/HeenaBansal2009)).
* Allow parallel execution of disjoint drop ranges [#40246](https://github.com/ClickHouse/ClickHouse/pull/40246) ([Alexander Tokmakov](https://github.com/tavplubix)).
* fix heap buffer overflow by limiting http chunk size [#40292](https://github.com/ClickHouse/ClickHouse/pull/40292) ([Sema Checherinda](https://github.com/CheSema)).
* fix typo [#40337](https://github.com/ClickHouse/ClickHouse/pull/40337) ([Peiqi Yin](https://github.com/yinpeiqi)).
* Fix setting description [#40347](https://github.com/ClickHouse/ClickHouse/pull/40347) ([Kruglov Pavel](https://github.com/Avogar)).
* split concurrent_threads_soft_limit setting into two [#40348](https://github.com/ClickHouse/ClickHouse/pull/40348) ([Sergei Trifonov](https://github.com/serxa)).
* Fix "incorrect replica identifier" [#40352](https://github.com/ClickHouse/ClickHouse/pull/40352) ([Alexander Tokmakov](https://github.com/tavplubix)).
* tests/stress: use --privileged over --cap-add syslog to obtain dmesg [#40354](https://github.com/ClickHouse/ClickHouse/pull/40354) ([Azat Khuzhin](https://github.com/azat)).
* Make test for s3 schema inference cache better [#40355](https://github.com/ClickHouse/ClickHouse/pull/40355) ([Kruglov Pavel](https://github.com/Avogar)).
* Update version_date.tsv and changelogs after v22.8.1.2097-lts [#40358](https://github.com/ClickHouse/ClickHouse/pull/40358) ([github-actions[bot]](https://github.com/apps/github-actions)).
* Fix typo in the S3 download links for [#40359](https://github.com/ClickHouse/ClickHouse/pull/40359) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Reduce changelog verbosity in CI [#40360](https://github.com/ClickHouse/ClickHouse/pull/40360) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Fix wrong assignees argument passing [#40367](https://github.com/ClickHouse/ClickHouse/pull/40367) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* And the last tiny fix for assignees [#40373](https://github.com/ClickHouse/ClickHouse/pull/40373) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Disable flaky 02382_filesystem_cache_persistent_files [#40386](https://github.com/ClickHouse/ClickHouse/pull/40386) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Disable flaky 02293_part_log_has_merge_reason [#40387](https://github.com/ClickHouse/ClickHouse/pull/40387) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Suppress "Cannot find column <subquery> in ActionsDAG result" [#40388](https://github.com/ClickHouse/ClickHouse/pull/40388) ([Alexander Tokmakov](https://github.com/tavplubix)).
* FileCache.cpp tiny refactor [#40413](https://github.com/ClickHouse/ClickHouse/pull/40413) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Update CHANGELOG.md [#40415](https://github.com/ClickHouse/ClickHouse/pull/40415) ([Denny Crane](https://github.com/den-crane)).
* Fix obvious trash in Parallel replicas [#40419](https://github.com/ClickHouse/ClickHouse/pull/40419) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov)).
* Update CHANGELOG.md [#40425](https://github.com/ClickHouse/ClickHouse/pull/40425) ([Kruglov Pavel](https://github.com/Avogar)).

