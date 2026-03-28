---
sidebar_position: 1
sidebar_label: 2023
---

# 2023 Changelog

### ClickHouse release v23.9.5.29-stable (f8554c1a1ff) FIXME as compared to v23.9.4.11-stable (74c1f49dd6a)

#### Build/Testing/Packaging Improvement
* Backported in [#56631](https://github.com/ClickHouse/ClickHouse/issues/56631): In [#54043](https://github.com/ClickHouse/ClickHouse/issues/54043) the setup plan started to appear in the logs. It should be only in the `runner_get_all_tests.log` only. As well, send the failed infrastructure event to CI db. [#56214](https://github.com/ClickHouse/ClickHouse/pull/56214) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Backported in [#56735](https://github.com/ClickHouse/ClickHouse/issues/56735): Do not fetch changed submodules in the builder container. [#56689](https://github.com/ClickHouse/ClickHouse/pull/56689) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).

#### Bug Fix (user-visible misbehavior in an official stable release)

* Select from system tables when table based on table function. [#55540](https://github.com/ClickHouse/ClickHouse/pull/55540) ([MikhailBurdukov](https://github.com/MikhailBurdukov)).
* Fix incomplete query result for UNION in view() function. [#56274](https://github.com/ClickHouse/ClickHouse/pull/56274) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fix crash in case of adding a column with type Object(JSON) [#56307](https://github.com/ClickHouse/ClickHouse/pull/56307) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov)).
* Fix segfault during Kerberos initialization [#56401](https://github.com/ClickHouse/ClickHouse/pull/56401) ([Nikolay Degterinsky](https://github.com/evillique)).
* Fix: RabbitMQ OpenSSL dynamic loading issue [#56703](https://github.com/ClickHouse/ClickHouse/pull/56703) ([Igor Nikonov](https://github.com/devcrafter)).
* Fix crash in GCD codec in case when zeros present in data [#56704](https://github.com/ClickHouse/ClickHouse/pull/56704) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov)).
* Fix crash in FPC codec [#56795](https://github.com/ClickHouse/ClickHouse/pull/56795) ([Alexey Milovidov](https://github.com/alexey-milovidov)).

#### NO CL CATEGORY

* Backported in [#56603](https://github.com/ClickHouse/ClickHouse/issues/56603):. [#56598](https://github.com/ClickHouse/ClickHouse/pull/56598) ([Maksim Kita](https://github.com/kitaisreal)).

#### NOT FOR CHANGELOG / INSIGNIFICANT

* Improve enrich image [#55793](https://github.com/ClickHouse/ClickHouse/pull/55793) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Rewrite jobs to use callable workflow [#56385](https://github.com/ClickHouse/ClickHouse/pull/56385) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Continue rewriting workflows to reusable tests [#56501](https://github.com/ClickHouse/ClickHouse/pull/56501) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Better exception messages [#56854](https://github.com/ClickHouse/ClickHouse/pull/56854) ([Antonio Andelic](https://github.com/antonio2368)).

