---
sidebar_position: 1
sidebar_label: 2025
---

# 2025 Changelog

### ClickHouse release v25.4.3.22-stable (f23535c7722) FIXME as compared to v25.4.2.31-stable (e2ce00b6885)

#### Performance Improvement
* Backported in [#79924](https://github.com/ClickHouse/ClickHouse/issues/79924): Prevent `LogSeriesLimiter` from doing cleanup on every construction, avoiding lock contention and performance regressions in high-concurrency scenarios. [#79864](https://github.com/ClickHouse/ClickHouse/pull/79864) ([filimonov](https://github.com/filimonov)).

#### Improvement
* Backported in [#79904](https://github.com/ClickHouse/ClickHouse/issues/79904): Added `access_control_improvements.enable_user_name_access_type` setting. This setting allows enabling/disabling of precise grants for users/roles, introduced in https://github.com/ClickHouse/ClickHouse/pull/72246. You may want to turn this setting off in case you have a cluster with the replicas older than 25.1. [#79842](https://github.com/ClickHouse/ClickHouse/pull/79842) ([pufit](https://github.com/pufit)).
* Backported in [#79934](https://github.com/ClickHouse/ClickHouse/issues/79934): Proper implementation of `ASTSelectWithUnionQuery::clone()` method now takes into account `is_normalized` field as well. This might help with [#77569](https://github.com/ClickHouse/ClickHouse/issues/77569). [#79909](https://github.com/ClickHouse/ClickHouse/pull/79909) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov)).

#### Bug Fix (user-visible misbehavior in an official stable release)
* Backported in [#79820](https://github.com/ClickHouse/ClickHouse/issues/79820): Fix applying `async_insert` from server (via `apply_settings_from_server`) (previously leads to `Unknown packet 11 from server` errors on the client). [#77578](https://github.com/ClickHouse/ClickHouse/pull/77578) ([Azat Khuzhin](https://github.com/azat)).
* Backported in [#79785](https://github.com/ClickHouse/ClickHouse/issues/79785): Fixed refreshable materialized views breaking backups. [#77893](https://github.com/ClickHouse/ClickHouse/pull/77893) ([Michael Kolupaev](https://github.com/al13n321)).
* Backported in [#79741](https://github.com/ClickHouse/ClickHouse/issues/79741): Fix crash in dynamic subcolumns discovery in Wide parts in MergeTree. [#79466](https://github.com/ClickHouse/ClickHouse/pull/79466) ([Pavel Kruglov](https://github.com/Avogar)).
* Backported in [#79585](https://github.com/ClickHouse/ClickHouse/issues/79585): Verify the table name's length only for initial create queries. Do not verify this for secondary creates to avoid backward compatibility issues. [#79488](https://github.com/ClickHouse/ClickHouse/pull/79488) ([Miсhael Stetsyuk](https://github.com/mstetsyuk)).
* Backported in [#79973](https://github.com/ClickHouse/ClickHouse/issues/79973): Hide password for query `CREATE DATABASE datalake ENGINE = DataLakeCatalog(\'http://catalog:8181\', \'admin\', \'password\')`. [#79941](https://github.com/ClickHouse/ClickHouse/pull/79941) ([Han Fei](https://github.com/hanfei1991)).

#### NOT FOR CHANGELOG / INSIGNIFICANT

* Backported in [#80018](https://github.com/ClickHouse/ClickHouse/issues/80018): Fix assert in S3Queue. [#78792](https://github.com/ClickHouse/ClickHouse/pull/78792) ([Nikolay Degterinsky](https://github.com/evillique)).
* Backported in [#79775](https://github.com/ClickHouse/ClickHouse/issues/79775): Address the code review comments from @KochetovNicolai. [#79399](https://github.com/ClickHouse/ClickHouse/pull/79399) ([Dmitry Novik](https://github.com/novikd)).
* Disable settings introduced here: https://github.com/ClickHouse/ClickHouse/pull/63206 https://github.com/ClickHouse/ClickHouse/pull/78778 They are proven to be non-stable. [#79896](https://github.com/ClickHouse/ClickHouse/pull/79896) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov)).

