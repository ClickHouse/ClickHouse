---
sidebar_position: 1
sidebar_label: 2025
---

# 2025 Changelog

### ClickHouse release v25.9.4.58-stable (08afb4f28ee) FIXME as compared to v25.9.3.48-stable (9a8e40bbf36)

#### Improvement
* Backported in [#88480](https://github.com/ClickHouse/ClickHouse/issues/88480): Use S3 storage client with backup-specific settings (for example, backup_slow_all_threads_after_retryable_s3_error) for server-side (native) copy to a backup destination. Make s3_slow_all_threads_after_retryable_error obsolete. [#87660](https://github.com/ClickHouse/ClickHouse/pull/87660) ([Julia Kartseva](https://github.com/jkartseva)).
* Backported in [#88302](https://github.com/ClickHouse/ClickHouse/issues/88302): Update azure sdk to include 'Content-Length' fix that is seen with copy and create container functionalities. [#88278](https://github.com/ClickHouse/ClickHouse/pull/88278) ([Smita Kulkarni](https://github.com/SmitaRKulkarni)).
* Backported in [#88783](https://github.com/ClickHouse/ClickHouse/issues/88783): Fixes memory leak due to `GLOBAL IN` (https://github.com/ClickHouse/ClickHouse/issues/88615). [#88617](https://github.com/ClickHouse/ClickHouse/pull/88617) ([pranavmehta94](https://github.com/pranavmehta94)).
* Backported in [#88766](https://github.com/ClickHouse/ClickHouse/issues/88766): Add a step to postinstall script for `clickhouse-keeper` which enables starting on boot. [#88746](https://github.com/ClickHouse/ClickHouse/pull/88746) ([YenchangChan](https://github.com/YenchangChan)).

#### Bug Fix (user-visible misbehavior in an official stable release)
* Backported in [#88226](https://github.com/ClickHouse/ClickHouse/issues/88226): Fixes a crash where some valid user-submitted data to an `AggregateFunction(quantileDD)` column could cause merges to recurse infinitely. [#86560](https://github.com/ClickHouse/ClickHouse/pull/86560) ([Raphaël Thériault](https://github.com/raphael-theriault-swi)).
* Backported in [#88229](https://github.com/ClickHouse/ClickHouse/issues/88229): Fixed move-to-prewhere optimization, which did not work in the presence of row policy. Continuation of [#85118](https://github.com/ClickHouse/ClickHouse/issues/85118). Closes [#69777](https://github.com/ClickHouse/ClickHouse/issues/69777). Closes [#83748](https://github.com/ClickHouse/ClickHouse/issues/83748). [#87303](https://github.com/ClickHouse/ClickHouse/pull/87303) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Backported in [#88391](https://github.com/ClickHouse/ClickHouse/issues/88391): In AzureBlobStorage, updated to try native copy first and go to read & write on 'Unauthroized' error (In AzureBlobStorage, if storage accounts are different for source & destination we get 'Unauthorized' error). And fix applying "use_native_copy" when endpoint is defined in configuration. [#87826](https://github.com/ClickHouse/ClickHouse/pull/87826) ([Smita Kulkarni](https://github.com/SmitaRKulkarni)).
* Backported in [#88692](https://github.com/ClickHouse/ClickHouse/issues/88692): Don't remove injective functions from GROUP BY if arguments types are not allowed in GROUP BY. [#87958](https://github.com/ClickHouse/ClickHouse/pull/87958) ([Pavel Kruglov](https://github.com/Avogar)).
* Backported in [#88438](https://github.com/ClickHouse/ClickHouse/issues/88438): Fix for incorrect granules/partitions elimination for datetime-based keys, when using `session_timezone` setting in queries. [#87987](https://github.com/ClickHouse/ClickHouse/pull/87987) ([Eduard Karacharov](https://github.com/korowa)).
* Backported in [#88422](https://github.com/ClickHouse/ClickHouse/issues/88422): Applies URI normalization before evaluation for the grants check introduced by https://github.com/ClickHouse/ClickHouse/pull/84503. [#88089](https://github.com/ClickHouse/ClickHouse/pull/88089) ([pufit](https://github.com/pufit)).
* Backported in [#88176](https://github.com/ClickHouse/ClickHouse/issues/88176): Fix possible uncaught exception while reading `system.tables` with invalid tables in external databases (`PostgreSQL`/`SQLite`/...) attached. [#88105](https://github.com/ClickHouse/ClickHouse/pull/88105) ([Azat Khuzhin](https://github.com/azat)).
* Backported in [#88258](https://github.com/ClickHouse/ClickHouse/issues/88258): Now `ON CLUSTER` queries will take less time in case of inactive replicas in cluster. [#88153](https://github.com/ClickHouse/ClickHouse/pull/88153) ([alesapin](https://github.com/alesapin)).
* Backported in [#88208](https://github.com/ClickHouse/ClickHouse/issues/88208): Fix running ClickHouse w/o cgroups (accidentally cgroups became a requirement for asynchronous metrics). [#88164](https://github.com/ClickHouse/ClickHouse/pull/88164) ([Azat Khuzhin](https://github.com/azat)).
* Backported in [#88285](https://github.com/ClickHouse/ClickHouse/issues/88285): Fixed propagation of `is_shared` flag in `ColumnLowCardinality`. It may lead to a wrong group-by result if a new value is inserted in a column after hash values are already pre-calculated and cached in the `ReverseIndex`. [#88213](https://github.com/ClickHouse/ClickHouse/pull/88213) ([Nikita Taranov](https://github.com/nickitat)).
* Backported in [#88461](https://github.com/ClickHouse/ClickHouse/issues/88461): Fixes a workload setting `max_cpu_share`. Now it can be used without `max_cpus` workload setting being set. [#88217](https://github.com/ClickHouse/ClickHouse/pull/88217) ([Neerav](https://github.com/neeravsalaria)).
* Backported in [#88338](https://github.com/ClickHouse/ClickHouse/issues/88338): Now correlated subqueries will work with object storages. [#88290](https://github.com/ClickHouse/ClickHouse/pull/88290) ([alesapin](https://github.com/alesapin)).
* Backported in [#88358](https://github.com/ClickHouse/ClickHouse/issues/88358): Avoid trying to initialize DataLake databases while accessing `system.projections` and `system.data_skipping_indices`. [#88330](https://github.com/ClickHouse/ClickHouse/pull/88330) ([Azat Khuzhin](https://github.com/azat)).
* Backported in [#88432](https://github.com/ClickHouse/ClickHouse/issues/88432): Fix quadratic complexity in the `countMatches` function. Closes [#88400](https://github.com/ClickHouse/ClickHouse/issues/88400). [#88401](https://github.com/ClickHouse/ClickHouse/pull/88401) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Backported in [#88650](https://github.com/ClickHouse/ClickHouse/issues/88650): Big clusters with node numbers > 10 have a high probability of failing the restore with error `[941] 67c45db4-4df4-4879-87c5-25b8d1e0d414 <Trace>: RestoreCoordinationOnCluster The version of node /clickhouse/backups/restore-7c551a77-bd76-404c-bad0-3213618ac58e/stage/num_hosts changed (attempt #9), will try again`. The `num_hosts` node is overwritten by many hosts at the same time. The fix makes the setting to control attempts dynamic. Closes [#87721](https://github.com/ClickHouse/ClickHouse/issues/87721). [#88484](https://github.com/ClickHouse/ClickHouse/pull/88484) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Backported in [#88629](https://github.com/ClickHouse/ClickHouse/issues/88629): Fix coalescing merge tree for tuple types. This closes [#88469](https://github.com/ClickHouse/ClickHouse/issues/88469). [#88526](https://github.com/ClickHouse/ClickHouse/pull/88526) ([scanhex12](https://github.com/scanhex12)).
* Backported in [#88718](https://github.com/ClickHouse/ClickHouse/issues/88718): Fix potential crash caused by concurrent mutation of underlying const PREWHERE columns. [#88605](https://github.com/ClickHouse/ClickHouse/pull/88605) ([Azat Khuzhin](https://github.com/azat)).
* Backported in [#88837](https://github.com/ClickHouse/ClickHouse/issues/88837): Catch exceptions when async logging fails to prevent program aborts. [#88814](https://github.com/ClickHouse/ClickHouse/pull/88814) ([Raúl Marín](https://github.com/Algunenano)).

#### NO CL ENTRY

* NO CL ENTRY:  'Revert "Backport [#88526](https://github.com/ClickHouse/ClickHouse/issues/88526) to 25.9: Fix coalescing merge tree for tuple types"'. [#88664](https://github.com/ClickHouse/ClickHouse/pull/88664) ([Raúl Marín](https://github.com/Algunenano)).

#### NOT FOR CHANGELOG / INSIGNIFICANT

* Backported in [#88135](https://github.com/ClickHouse/ClickHouse/issues/88135): Update setting history for query_plan_optimize_join_order_limit. [#87612](https://github.com/ClickHouse/ClickHouse/pull/87612) ([Vladimir Cherkasov](https://github.com/vdimir)).
* Backported in [#88590](https://github.com/ClickHouse/ClickHouse/issues/88590): Add column descriptions in system.database_replicas. [#87625](https://github.com/ClickHouse/ClickHouse/pull/87625) ([Konstantin Morozov](https://github.com/k-morozov)).
* Backported in [#88453](https://github.com/ClickHouse/ClickHouse/issues/88453): fix threads count for inserts. [#88339](https://github.com/ClickHouse/ClickHouse/pull/88339) ([Sema Checherinda](https://github.com/CheSema)).
* Backported in [#88583](https://github.com/ClickHouse/ClickHouse/issues/88583): Fix `ColumnBLOB should be converted to a regular column before usage` from `CREATE AS SELECT`. [#88544](https://github.com/ClickHouse/ClickHouse/pull/88544) ([Nikita Taranov](https://github.com/nickitat)).

