---
sidebar_position: 1
sidebar_label: 2023
---

# 2023 Changelog

### ClickHouse release v23.3.15.29-lts (218336662e4) FIXME as compared to v23.3.14.78-lts (c8f4ba52c65)

#### Build/Testing/Packaging Improvement
* Backported in [#55671](https://github.com/ClickHouse/ClickHouse/issues/55671): If the database is already initialized, it doesn't need to be initialized again upon subsequent launches. This can potentially fix the issue of infinite container restarts when the database fails to load within 1000 attempts (relevant for very large databases and multi-node setups). [#50724](https://github.com/ClickHouse/ClickHouse/pull/50724) ([Alexander Nikolaev](https://github.com/AlexNik)).
* Backported in [#55734](https://github.com/ClickHouse/ClickHouse/issues/55734): Fix integration check python script to use gh api url - Add Readme for CI tests. [#55716](https://github.com/ClickHouse/ClickHouse/pull/55716) ([Max K.](https://github.com/mkaynov)).
* Backported in [#55829](https://github.com/ClickHouse/ClickHouse/issues/55829): Check sha512 for tgz; use a proper repository for keeper; write only filenames to TGZ.sha512 files for tarball packages. Prerequisite for [#31473](https://github.com/ClickHouse/ClickHouse/issues/31473). [#55717](https://github.com/ClickHouse/ClickHouse/pull/55717) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).

#### Bug Fix (user-visible misbehavior in an official stable release)

* Fix bug with inability to drop detached partition in replicated merge tree on top of S3 without zero copy [#55309](https://github.com/ClickHouse/ClickHouse/pull/55309) ([alesapin](https://github.com/alesapin)).
* Fix crash in QueryNormalizer with cyclic aliases [#55602](https://github.com/ClickHouse/ClickHouse/pull/55602) ([vdimir](https://github.com/vdimir)).
* Fix window functions in case of sparse columns. [#55895](https://github.com/ClickHouse/ClickHouse/pull/55895) ([János Benjamin Antal](https://github.com/antaljanosbenjamin)).

#### NO CL ENTRY

* NO CL ENTRY:  'Pin rust version to fix GLIBC compatibility check'. [#55788](https://github.com/ClickHouse/ClickHouse/pull/55788) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).

#### NOT FOR CHANGELOG / INSIGNIFICANT

* Fix incorrect createColumn call on join clause [#48998](https://github.com/ClickHouse/ClickHouse/pull/48998) ([Yi Sheng](https://github.com/ongkong)).
* Use `--filter` to reduce checkout time [#54857](https://github.com/ClickHouse/ClickHouse/pull/54857) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Improve enrich image [#55793](https://github.com/ClickHouse/ClickHouse/pull/55793) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* One final leftover in diff_urls of PRInfo [#55874](https://github.com/ClickHouse/ClickHouse/pull/55874) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).

