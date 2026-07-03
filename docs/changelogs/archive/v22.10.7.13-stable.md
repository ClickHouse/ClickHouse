---
sidebar_position: 1
sidebar_label: 2023
---

# 2023 Changelog

### ClickHouse release v22.10.7.13-stable (d261d9036cc) FIXME as compared to v22.10.6.3-stable (645a66d221f)

#### Bug Fix (user-visible misbehavior in official stable release)

* Backported in [#44998](https://github.com/ClickHouse/ClickHouse/issues/44998): Another fix for `Cannot read all data` error which could happen while reading `LowCardinality` dictionary from remote fs. Fixes [#44709](https://github.com/ClickHouse/ClickHouse/issues/44709). [#44875](https://github.com/ClickHouse/ClickHouse/pull/44875) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Backported in [#45551](https://github.com/ClickHouse/ClickHouse/issues/45551): Fix `SELECT ... FROM system.dictionaries` exception when there is a dictionary with a bad structure (e.g. incorrect type in xml config). [#45399](https://github.com/ClickHouse/ClickHouse/pull/45399) ([Aleksei Filatov](https://github.com/aalexfvk)).

#### NOT FOR CHANGELOG / INSIGNIFICANT

* Automatically merge green backport PRs and green approved PRs [#41110](https://github.com/ClickHouse/ClickHouse/pull/41110) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Improve release scripts [#45074](https://github.com/ClickHouse/ClickHouse/pull/45074) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Fix wrong approved_at, simplify conditions [#45302](https://github.com/ClickHouse/ClickHouse/pull/45302) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Get rid of artifactory in favor of r2 + ch-repos-manager [#45421](https://github.com/ClickHouse/ClickHouse/pull/45421) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
