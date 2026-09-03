---
sidebar_position: 1
sidebar_label: 2026
---

# 2026 Changelog

### ClickHouse release v26.3.3.20-lts (5342e88d453) FIXME as compared to v26.3.2.3-lts (a1a0cf365ce)

#### Bug Fix (user-visible misbehavior in an official stable release)
* Backported in [#101224](https://github.com/ClickHouse/ClickHouse/issues/101224): Fix async insert queries reporting zero `written_rows`, `read_rows`, and `result_rows` in `query_log` and client output. [#99879](https://github.com/ClickHouse/ClickHouse/pull/99879) ([Sema Checherinda](https://github.com/CheSema)).
* Backported in [#100891](https://github.com/ClickHouse/ClickHouse/issues/100891): Validate file entry paths in backup metadata to reject path traversal, absolute paths, and empty names during `RESTORE`. [#100483](https://github.com/ClickHouse/ClickHouse/pull/100483) ([Pablo Marcos](https://github.com/pamarcos)).
* Backported in [#101394](https://github.com/ClickHouse/ClickHouse/issues/101394): Fix `session_timezone` being ignored when parsing `DateTime` values during async inserts (TCP) and all inserts over HTTP. [#100647](https://github.com/ClickHouse/ClickHouse/pull/100647) ([Sema Checherinda](https://github.com/CheSema)).
* Backported in [#100922](https://github.com/ClickHouse/ClickHouse/issues/100922): Fix server crash when using `has()` function with PREWHERE/WHERE on a Tuple key containing LowCardinality elements. [#100760](https://github.com/ClickHouse/ClickHouse/pull/100760) ([Groene AI](https://github.com/groeneai)).
* Backported in [#101187](https://github.com/ClickHouse/ClickHouse/issues/101187): Fix null pointer dereference segfault when loading dictionaries during server shutdown. `Context::getUserDefinedSQLObjectsStorage` (dereferences `user_defined_sql_objects_storage`) is called by dictionary threads concurrently with the main thread calling `Context::shutdown` (sets `user_defined_sql_objects_storage` to null). We need to make sure we disable future updates in the dictionaries loader, kill the currently running dictionary queries and join the dictionary loading threads - all before running `Context::shutdown`. Similar to what we do with normal queries. [#100839](https://github.com/ClickHouse/ClickHouse/pull/100839) ([Miсhael Stetsyuk](https://github.com/mstetsyuk)).
* Backported in [#101091](https://github.com/ClickHouse/ClickHouse/issues/101091): Fix buffer overflow in `ULIDStringToDateTime` when input contains non-ASCII bytes. [#100843](https://github.com/ClickHouse/ClickHouse/pull/100843) ([Konstantin Bogdanov](https://github.com/thevar1able)).
* Backported in [#101015](https://github.com/ClickHouse/ClickHouse/issues/101015): Fix exception in `intDiv`/`intDivOrZero` on arrays of nullable tuples, e.g. `SELECT intDiv([divide((1, 2), ... AND NULL)], 2)`. [#100895](https://github.com/ClickHouse/ClickHouse/pull/100895) ([Raúl Marín](https://github.com/Algunenano)).
* Backported in [#101250](https://github.com/ClickHouse/ClickHouse/issues/101250): Evaluate engine arguments for `StorageAlias` before storing the definition, so that expressions like `currentDatabase()` are resolved to literals before being saved to the database. [#100902](https://github.com/ClickHouse/ClickHouse/pull/100902) ([Nikolay Degterinsky](https://github.com/evillique)).
* Backported in [#101140](https://github.com/ClickHouse/ClickHouse/issues/101140): Fixed a crash when using a text search index with an `IN` clause containing a tuple subquery, e.g. `WHERE (id, str) IN (SELECT (id, str) FROM ...)`, or when the number of columns in the subquery does not match the tuple on the left side of `IN`. [#100959](https://github.com/ClickHouse/ClickHouse/pull/100959) ([Anton Popov](https://github.com/CurtizJ)).
* Backported in [#101080](https://github.com/ClickHouse/ClickHouse/issues/101080): Fix `sumCountOrDefault` aggregate function with one or more `Nullable` argument not being able to read older serialized states after introduction of `Nullable(Tuple)`. Closes [#100882](https://github.com/ClickHouse/ClickHouse/issues/100882). [#101021](https://github.com/ClickHouse/ClickHouse/pull/101021) ([Nihal Z. Miaji](https://github.com/nihalzp)).
* Backported in [#101169](https://github.com/ClickHouse/ClickHouse/issues/101169): Fix exception in `optimizeLazyMaterialization` when a projection with PREWHERE is used with `ORDER BY ... LIMIT`. [#101115](https://github.com/ClickHouse/ClickHouse/pull/101115) ([Anton Popov](https://github.com/CurtizJ)).
* Backported in [#101349](https://github.com/ClickHouse/ClickHouse/issues/101349): Fix a case which can lead to `Having zero bytes, ...` logical error from cache arising from a remote object being overwritten in between `list` and `read` which previously resulted in a stale object metadata. [#101219](https://github.com/ClickHouse/ClickHouse/pull/101219) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Backported in [#101373](https://github.com/ClickHouse/ClickHouse/issues/101373): ... [#101230](https://github.com/ClickHouse/ClickHouse/pull/101230) ([Kseniia Sumarokova](https://github.com/kssenii)).

#### NO CL CATEGORY

* Backported in [#101392](https://github.com/ClickHouse/ClickHouse/issues/101392):. [#101241](https://github.com/ClickHouse/ClickHouse/pull/101241) ([Anton Popov](https://github.com/CurtizJ)).

#### NOT FOR CHANGELOG / INSIGNIFICANT

* Backported in [#101198](https://github.com/ClickHouse/ClickHouse/issues/101198): This patch fixes cache disk startup, which, before this patch, skipped the access check. [#99741](https://github.com/ClickHouse/ClickHouse/pull/99741) ([Mikhail Artemenko](https://github.com/Michicosun)).
* Backported in [#100970](https://github.com/ClickHouse/ClickHouse/issues/100970): Fixes [#99996](https://github.com/ClickHouse/ClickHouse/issues/99996). [#100064](https://github.com/ClickHouse/ClickHouse/pull/100064) ([Mikhail Artemenko](https://github.com/Michicosun)).
* Backported in [#100808](https://github.com/ClickHouse/ClickHouse/issues/100808): This patch changes the behaviour of `removeObjectsIfExist` from a sequential removal loop to batch removal. [#100522](https://github.com/ClickHouse/ClickHouse/pull/100522) ([Mikhail Artemenko](https://github.com/Michicosun)).
* Backported in [#101002](https://github.com/ClickHouse/ClickHouse/issues/101002): Properly recalculate locations after config reload. [#100907](https://github.com/ClickHouse/ClickHouse/pull/100907) ([Mikhail Artemenko](https://github.com/Michicosun)).
* Backported in [#101341](https://github.com/ClickHouse/ClickHouse/issues/101341): fix double count result rows. [#101226](https://github.com/ClickHouse/ClickHouse/pull/101226) ([Sema Checherinda](https://github.com/CheSema)).

