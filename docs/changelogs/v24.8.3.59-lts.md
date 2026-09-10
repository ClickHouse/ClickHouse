---
sidebar_position: 1
sidebar_label: 2024
---

# 2024 Changelog

### ClickHouse release v24.8.3.59-lts (e729b9fa40e) FIXME as compared to v24.8.2.3-lts (b54f79ed323)

#### New Feature
* Backported in [#68710](https://github.com/ClickHouse/ClickHouse/issues/68710): Query cache entries can now be dropped by tag. For example, the query cache entry created by `SELECT 1 SETTINGS use_query_cache = true, query_cache_tag = 'abc'` can now be dropped by `SYSTEM DROP QUERY CACHE TAG 'abc'` (or of course just: `SYSTEM DROP QUERY CACHE` which will clear the entire query cache). [#68477](https://github.com/ClickHouse/ClickHouse/pull/68477) ([Michał Tabaszewski](https://github.com/pinsvin00)).

#### Improvement
* Backported in [#69097](https://github.com/ClickHouse/ClickHouse/issues/69097): Support for the Spanish language in the embedded dictionaries. [#69035](https://github.com/ClickHouse/ClickHouse/pull/69035) ([Vasily Okunev](https://github.com/VOkunev)).

#### Bug Fix (user-visible misbehavior in an official stable release)
* Backported in [#68973](https://github.com/ClickHouse/ClickHouse/issues/68973): Fix the upper bound of the function `fromModifiedJulianDay`. It was supposed to be `9999-12-31` but was mistakenly set to `9999-01-01`. [#67583](https://github.com/ClickHouse/ClickHouse/pull/67583) ([PHO](https://github.com/depressed-pho)).
* Backported in [#68818](https://github.com/ClickHouse/ClickHouse/issues/68818): Fixed crash in Parquet filtering when data types in the file substantially differ from requested types (e.g. `... FROM file('a.parquet', Parquet, 'x String')`, but the file has `x Int64`). Without this fix, use `input_format_parquet_filter_push_down = 0` as a workaround. [#68131](https://github.com/ClickHouse/ClickHouse/pull/68131) ([Michael Kolupaev](https://github.com/al13n321)).
* Backported in [#68893](https://github.com/ClickHouse/ClickHouse/issues/68893): After https://github.com/ClickHouse/ClickHouse/pull/61984 `schema_inference_make_columns_nullable=0` still can make columns `Nullable` in Parquet/Arrow formats. The change was backward incompatible and users noticed the changes in the behaviour. This PR makes `schema_inference_make_columns_nullable=0` to work as before (no Nullable columns will be inferred) and introduces new value `auto` for this setting that will make columns `Nullable` only if data has information about nullability. [#68298](https://github.com/ClickHouse/ClickHouse/pull/68298) ([Kruglov Pavel](https://github.com/Avogar)).
* Backported in [#68721](https://github.com/ClickHouse/ClickHouse/issues/68721): Fixes [#50868](https://github.com/ClickHouse/ClickHouse/issues/50868). Small DateTime64 constant values returned by a nested subquery inside a distributed query were wrongly transformed to Nulls, thus causing errors and possible incorrect query results. [#68323](https://github.com/ClickHouse/ClickHouse/pull/68323) ([Shankar](https://github.com/shiyer7474)).
* Backported in [#69029](https://github.com/ClickHouse/ClickHouse/issues/69029): Added back virtual columns ` _table` and `_database` to distributed tables. They were available until version 24.3. [#68672](https://github.com/ClickHouse/ClickHouse/pull/68672) ([Anton Popov](https://github.com/CurtizJ)).
* Backported in [#68864](https://github.com/ClickHouse/ClickHouse/issues/68864): Fix possible error `Size of permutation (0) is less than required (...)` during Variant column permutation. [#68681](https://github.com/ClickHouse/ClickHouse/pull/68681) ([Kruglov Pavel](https://github.com/Avogar)).
* Backported in [#68854](https://github.com/ClickHouse/ClickHouse/issues/68854): Fix possible error `DB::Exception: Block structure mismatch in joined block stream: different columns:` with new JSON column. [#68686](https://github.com/ClickHouse/ClickHouse/pull/68686) ([Kruglov Pavel](https://github.com/Avogar)).
* Backported in [#68790](https://github.com/ClickHouse/ClickHouse/issues/68790): Fix issue with materialized constant keys when hashing maps with arrays as keys in functions `sipHash(64/128)Keyed`. [#68731](https://github.com/ClickHouse/ClickHouse/pull/68731) ([Salvatore Mesoraca](https://github.com/aiven-sal)).
* Backported in [#69108](https://github.com/ClickHouse/ClickHouse/issues/69108): TODO. [#68744](https://github.com/ClickHouse/ClickHouse/pull/68744) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Backported in [#68850](https://github.com/ClickHouse/ClickHouse/issues/68850): Fix resolving dynamic subcolumns from subqueries in analyzer. [#68824](https://github.com/ClickHouse/ClickHouse/pull/68824) ([Kruglov Pavel](https://github.com/Avogar)).
* Backported in [#68911](https://github.com/ClickHouse/ClickHouse/issues/68911): Fix complex types metadata parsing in DeltaLake. Closes [#68739](https://github.com/ClickHouse/ClickHouse/issues/68739). [#68836](https://github.com/ClickHouse/ClickHouse/pull/68836) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Backported in [#69160](https://github.com/ClickHouse/ClickHouse/issues/69160): Fix possible wrong result during anyHeavy state merge. [#68950](https://github.com/ClickHouse/ClickHouse/pull/68950) ([Raúl Marín](https://github.com/Algunenano)).
* Backported in [#69072](https://github.com/ClickHouse/ClickHouse/issues/69072): Fixed writing to Materialized Views with enabled setting `optimize_functions_to_subcolumns`. [#68951](https://github.com/ClickHouse/ClickHouse/pull/68951) ([Anton Popov](https://github.com/CurtizJ)).
* Backported in [#69016](https://github.com/ClickHouse/ClickHouse/issues/69016): Don't use serializations cache in const Dynamic column methods. It could let to use-of-unitialized value or even race condition during aggregations. [#68953](https://github.com/ClickHouse/ClickHouse/pull/68953) ([Kruglov Pavel](https://github.com/Avogar)).
* Backported in [#69120](https://github.com/ClickHouse/ClickHouse/issues/69120): Fix logical error when we have empty async insert. [#69080](https://github.com/ClickHouse/ClickHouse/pull/69080) ([Han Fei](https://github.com/hanfei1991)).

#### NO CL CATEGORY

* Backported in [#68947](https://github.com/ClickHouse/ClickHouse/issues/68947):. [#68897](https://github.com/ClickHouse/ClickHouse/pull/68897) ([Alexander Gololobov](https://github.com/davenger)).

#### NOT FOR CHANGELOG / INSIGNIFICANT

* Backported in [#68704](https://github.com/ClickHouse/ClickHouse/issues/68704): Fix enumerating dynamic subcolumns. [#68582](https://github.com/ClickHouse/ClickHouse/pull/68582) ([Kruglov Pavel](https://github.com/Avogar)).
* Backported in [#69000](https://github.com/ClickHouse/ClickHouse/issues/69000): Prioritizing of virtual columns in hive partitioning. [#68606](https://github.com/ClickHouse/ClickHouse/pull/68606) ([Yarik Briukhovetskyi](https://github.com/yariks5s)).
* Backported in [#68799](https://github.com/ClickHouse/ClickHouse/issues/68799): CI: Disable SQLLogic job. [#68654](https://github.com/ClickHouse/ClickHouse/pull/68654) ([Max K.](https://github.com/maxknv)).
* Backported in [#68834](https://github.com/ClickHouse/ClickHouse/issues/68834): Turn off fault injection for insert in `01396_inactive_replica_cleanup_nodes_zookeeper`. [#68715](https://github.com/ClickHouse/ClickHouse/pull/68715) ([alesapin](https://github.com/alesapin)).
* Backported in [#68781](https://github.com/ClickHouse/ClickHouse/issues/68781): Fix flaky test 00989_parallel_parts_loading. [#68737](https://github.com/ClickHouse/ClickHouse/pull/68737) ([alesapin](https://github.com/alesapin)).
* Backported in [#68762](https://github.com/ClickHouse/ClickHouse/issues/68762): To make patch release possible from every commit on release branch, package_debug build is required and must not be skipped. [#68750](https://github.com/ClickHouse/ClickHouse/pull/68750) ([Max K.](https://github.com/maxknv)).
* Backported in [#68810](https://github.com/ClickHouse/ClickHouse/issues/68810): Try to disable rerun check if job triggered manually. [#68751](https://github.com/ClickHouse/ClickHouse/pull/68751) ([Max K.](https://github.com/maxknv)).
* Backported in [#68962](https://github.com/ClickHouse/ClickHouse/issues/68962): Fix 2477 timeout. [#68752](https://github.com/ClickHouse/ClickHouse/pull/68752) ([jsc0218](https://github.com/jsc0218)).
* Backported in [#68977](https://github.com/ClickHouse/ClickHouse/issues/68977): Check setting use_json_alias_for_old_object_type in runtime. [#68793](https://github.com/ClickHouse/ClickHouse/pull/68793) ([Kruglov Pavel](https://github.com/Avogar)).
* Backported in [#68852](https://github.com/ClickHouse/ClickHouse/issues/68852): Make dynamic structure selection more consistent. [#68802](https://github.com/ClickHouse/ClickHouse/pull/68802) ([Kruglov Pavel](https://github.com/Avogar)).
* Backported in [#69052](https://github.com/ClickHouse/ClickHouse/issues/69052): Fix 01114_database_atomic flakiness. [#68930](https://github.com/ClickHouse/ClickHouse/pull/68930) ([Raúl Marín](https://github.com/Algunenano)).

