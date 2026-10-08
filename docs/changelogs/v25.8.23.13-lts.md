---
sidebar_position: 1
sidebar_label: 2026
---

# 2026 Changelog

### ClickHouse release v25.8.23.13-lts (443620d152e) FIXME as compared to v25.8.22.28-lts (6d294975256)

#### Bug Fix (user-visible misbehavior in an official stable release)
* Backported in [#103184](https://github.com/ClickHouse/ClickHouse/issues/103184): Fix using wrong extreams in min-max index created on JSON column leading to wrong query result. Closes [#101700](https://github.com/ClickHouse/ClickHouse/issues/101700). [#101918](https://github.com/ClickHouse/ClickHouse/pull/101918) ([Pavel Kruglov](https://github.com/Avogar)).
* Backported in [#102352](https://github.com/ClickHouse/ClickHouse/issues/102352): Fix SIGSEGV in `MergeTreeDataPartWriterWide::cancel` when a stream constructor throws during `addStreams`, leaving a null entry in `column_streams`. [#101936](https://github.com/ClickHouse/ClickHouse/pull/101936) ([Antonio Andelic](https://github.com/antonio2368)).
* Backported in [#102968](https://github.com/ClickHouse/ClickHouse/issues/102968): Fix wrong date data type inference in case of overflow after timezone adjustment. Closes [#102601](https://github.com/ClickHouse/ClickHouse/issues/102601). [#102674](https://github.com/ClickHouse/ClickHouse/pull/102674) ([Pavel Kruglov](https://github.com/Avogar)).
* Backported in [#103300](https://github.com/ClickHouse/ClickHouse/issues/103300): Fix flattened Dynamic type serialization with binary encoded data types. Closes [#101911](https://github.com/ClickHouse/ClickHouse/issues/101911). [#102692](https://github.com/ClickHouse/ClickHouse/pull/102692) ([Pavel Kruglov](https://github.com/Avogar)).
* Backported in [#103018](https://github.com/ClickHouse/ClickHouse/issues/103018): Optimize row policy OR-chains to IN in the new analyzer. [#102915](https://github.com/ClickHouse/ClickHouse/pull/102915) ([Azat Khuzhin](https://github.com/azat)).
* Backported in [#103663](https://github.com/ClickHouse/ClickHouse/issues/103663): Fix Parquet ColumnIndex stats min_value > max_value for String columns. [#103334](https://github.com/ClickHouse/ClickHouse/pull/103334) ([Saurabh Kumar Ojha](https://github.com/saurabhojha)).
* Backported in [#103408](https://github.com/ClickHouse/ClickHouse/issues/103408): Check for malformed flattened Dynamic data in Native format. [#103392](https://github.com/ClickHouse/ClickHouse/pull/103392) ([Pavel Kruglov](https://github.com/Avogar)).

#### Build/Testing/Packaging Improvement
* Backported in [#102819](https://github.com/ClickHouse/ClickHouse/issues/102819): Use `openssl` 3.5.6. [#102606](https://github.com/ClickHouse/ClickHouse/pull/102606) ([Konstantin Bogdanov](https://github.com/thevar1able)).
* Refresh distroless Docker base image to fix OpenSSL CVEs in `libssl3t64`. [#103582](https://github.com/ClickHouse/ClickHouse/pull/103582) ([Rahul Nair](https://github.com/motsc)).

#### NO CL ENTRY

* NO CL ENTRY:  'Upgrade distroless base image to cc-debian13 (25.8)'. [#103154](https://github.com/ClickHouse/ClickHouse/pull/103154) ([Rahul Nair](https://github.com/motsc)).

#### NOT FOR CHANGELOG / INSIGNIFICANT

* Backported in [#103175](https://github.com/ClickHouse/ClickHouse/issues/103175): CI: disable automerge for backport branches. [#103160](https://github.com/ClickHouse/ClickHouse/pull/103160) ([Max Kainov](https://github.com/maxknv)).
* Backported in [#103868](https://github.com/ClickHouse/ClickHouse/issues/103868): The releases were failing due to a timeout on keyserver.ubuntu.com. This PR fixes it permanently by using a chain of keyservers to reliably receive the GPG key. [#103834](https://github.com/ClickHouse/ClickHouse/pull/103834) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).

