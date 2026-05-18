---
title: 'BigQuery and ClickHouse: equivalent concepts'
slug: /migrations/bigquery/equivalent-concepts
description: 'Table-format reference mapping each core BigQuery concept to its ClickHouse equivalent'
keywords: ['BigQuery', 'migration', 'concept mapping', 'equivalent concepts', 'comparison']
sidebar_label: 'Equivalent concepts'
doc_type: 'reference'
---

The tables below map each BigQuery concept to its ClickHouse equivalent — what to use instead, and where the model differs. For function-by-function SQL syntax mapping, see the BigQuery → ClickHouse SQL translation reference. For the end-to-end migration walkthrough, see [Migrating from BigQuery to ClickHouse Cloud](./02_migrating-to-clickhouse-cloud.md).

## Resource hierarchy {#resource-hierarchy}

How the platform organizes accounts, logical containers for data, and where compute is provisioned.

| BigQuery | ClickHouse | Notes |
|---|---|---|
| Organization | [Organization](/cloud/security/console-roles#organization-roles) | Root node of the hierarchy in both. |
| Project | Service (region-scoped); [warehouse](/cloud/reference/warehouses) for grouping services with shared storage and independent compute | A ClickHouse service is one storage + one compute pool. Use a warehouse to group services that share storage but scale compute independently. |
| Dataset | [Database](/sql-reference/statements/create/database) | Logical container that organizes tables and scopes access. |
| Folder | [Warehouse](/cloud/reference/warehouses) grouping, or separate services per workload | ClickHouse has no folder primitive — grouping is at the service / warehouse level. |
| IAM permissions | [Console roles](/cloud/guides/sql-console/manage-sql-console-role-assignments) plus SQL [grants](/sql-reference/statements/grant) | Two-layer access: roles in `console.clickhouse.cloud` plus SQL grants in the database. Console users can also be granted DB roles for SQL Console use. |

## Compute, capacity, pricing {#compute-capacity}

How processing is allocated to a query, sized, and billed.

| BigQuery | ClickHouse | Notes |
|---|---|---|
| Slot | Replica (whole node); queries [parallelize](/optimize/query-parallelism) across replicas | A replica is the unit of compute in a ClickHouse service; queries run across all replicas of the service. See the callout below for the granularity difference with BigQuery slots. |
| Slot reservation | Vertical and horizontal [autoscaling](/cloud/features/autoscaling/vertical) bounds; [warehouses](/cloud/reference/warehouses) for workload isolation | ClickHouse uses bounds-based autoscaling rather than guaranteed-capacity reservations. |
| Quotas | [Workload classes](/operations/workload-scheduling) plus per-query limits | Covers memory, CPU, concurrency, and I/O scheduling. The two quota models don't map row-by-row, but concept-level coverage is similar. |
| On-demand pricing (per TB scanned) | Compute-time (replica-hours) plus storage and transfer — see [billing overview](/cloud/manage/billing/overview) | The two pricing models are not directly comparable. |
| Logical vs physical storage billing | Compressed storage only — see [billing overview](/cloud/manage/billing/overview) | ClickHouse Cloud bills compressed storage. The logical-vs-physical distinction does not apply. |

:::note Slot vs replica
A BigQuery slot is much finer-grained than a
ClickHouse replica — it's closer to a CPU thread within a replica
than to a whole replica. Both are the unit of compute being
allocated to a query, but with very different sizing.
:::

## Storage and tables {#storage-tables}

How tables are stored: engines, schema, partitioning, snapshots, and access primitives.

In ClickHouse, a table's behavior is set at creation time: the engine (MergeTree family) determines merge and storage semantics, and `ORDER BY` / `PARTITION BY` / `TTL` clauses configure physical layout and retention. Many BigQuery per-feature settings map to a clause in the ClickHouse `CREATE TABLE` statement.

| BigQuery | ClickHouse | Notes |
|---|---|---|
| Table | [MergeTree-family table](/engines/table-engines/mergetree-family/mergetree) | Engine choice determines storage and merge behavior — pick by access pattern ([`MergeTree`](/engines/table-engines/mergetree-family/mergetree) for append-mostly facts, [`ReplacingMergeTree`](/engines/table-engines/mergetree-family/replacingmergetree) for upserts, [`AggregatingMergeTree`](/engines/table-engines/mergetree-family/aggregatingmergetree) for pre-aggregations). |
| Column schema modes (`NULLABLE`, `REQUIRED`, `REPEATED`) | [`Nullable(T)`](/sql-reference/data-types/nullable) for optional; omit for required; [`Array(T)`](/sql-reference/data-types/array) for repeated; `Array(Tuple(...))` or [`Nested`](/sql-reference/data-types/nested-data-structures/nested) for repeated records | In ClickHouse, columns are non-nullable unless wrapped with `Nullable(T)`. Nullability has a small storage and query cost, so use it only when the column actually needs nulls. |
| Schema evolution (add / drop / modify columns) | [`ALTER TABLE ... ADD / DROP / MODIFY COLUMN`](/sql-reference/statements/alter/column) | Same DDL surface as BigQuery. Many column changes are metadata-only. |
| Partitioning | [`PARTITION BY`](/engines/table-engines/mergetree-family/custom-partitioning-key) clause on the table | Partitions are defined at table creation; a partition expression determines how rows are grouped into parts on disk. |
| Clustering | [`ORDER BY`](/guides/best-practices/sparse-primary-indexes) columns in the table definition | Defined as part of the table; data is physically sorted on disk by the `ORDER BY` columns. |
| External tables / BigLake | [`s3`](/sql-reference/table-functions/s3) / [`gcs`](/sql-reference/table-functions/gcs) / [`azureBlobStorage`](/sql-reference/table-functions/azureBlobStorage) table functions for direct file access; [Iceberg engine](/engines/table-engines/integrations/iceberg) for open catalogs | Object storage and open-table formats are read directly through these functions and engines. ClickHouse does not provide a unified-governance layer over external storage. |
| Object tables (SQL access to unstructured files) | [`s3`](/sql-reference/table-functions/s3) / [`gcs`](/sql-reference/table-functions/gcs) table functions over binary formats | ClickHouse treats unstructured-object access as a special case of external file reading via table functions, not as a dedicated table type. |
| Apache Iceberg | [Iceberg engine](/engines/table-engines/integrations/iceberg) (read-only) | Reads Iceberg tables stored in S3, Azure, HDFS, or local storage; writes are not supported. See the engine page for the current list of supported features. |
| Default table / partition / dataset expiration | [`TTL` clause](/sql-reference/statements/create/table#ttl-expression) on the table, column, or partition | Both support automatic deletion of data older than a configured window. `TTL` can be set at table creation or via [`ALTER TABLE ... MODIFY TTL`](/sql-reference/statements/alter/ttl). |
| Table snapshot | Service-level [backup](/cloud/manage/backups/overview) | See callout below — granularity differs significantly. |
| Time travel | Point-in-time [backup](/cloud/manage/backups/overview) restore into a new service | Backups are service-scoped, not table-scoped, so the restore unit is the whole service rather than a single table at a moment in time. |
| Authorized views | [View](/sql-reference/statements/create/view) with `SQL SECURITY DEFINER` (runs with the view-owner's privileges) | See [CREATE VIEW](/sql-reference/statements/create/view) for the syntax and the `INVOKER` / `DEFINER` / `NONE` modes. |
| Row-level security | [Row policy](/sql-reference/statements/create/row-policy) — a `WHERE`-style expression evaluated per user | Row policies apply transparently to every query against the table. |
| Wildcard tables (`_TABLE_SUFFIX`) | [`Merge`](/engines/table-engines/special/merge) table engine (persistent grouping) or [`merge()`](/sql-reference/table-functions/merge) function (inline) | Same idea, different syntax. `Merge` is a persistent table-of-tables; `merge()` is inline without creating one. |
| Table clone | [`CREATE TABLE ... AS SELECT`](/sql-reference/statements/create/table) copy, or [backup](/cloud/manage/backups/overview) restore into a new service | ClickHouse has no copy-on-write primitive — every copy reads the source data fully. |

:::note Backups
ClickHouse Cloud backups are per-service. Restoring
a backup creates a new service — a single table cannot be restored
back into the original service. Plan accordingly if your current
workflow relies on per-table snapshots.
:::

## Query model and performance {#query-model}

How queries run and are accelerated — indexes, materialized views, caches, and streaming inputs.

Query acceleration in ClickHouse comes from three layers: primary-key ordering (a sparse index over the on-disk sort order), secondary indexes on non-key columns, and materialized views — incremental or refreshable. The rows below map BigQuery's acceleration features onto these primitives.

| BigQuery | ClickHouse | Notes |
|---|---|---|
| Primary key (advisory) | Primary key — drives the on-disk sort order and the [sparse primary index](/guides/best-practices/sparse-primary-indexes) | Neither system enforces uniqueness; the optimizer uses the key to prune granules, avoid re-sorts, and short-circuit `LIMIT`. |
| Foreign key (advisory) | Wide tables or [dictionaries](/dictionary) for lookups | ClickHouse doesn't accept foreign-key declarations even as advisory hints. |
| Search index | [Full-text index](/engines/table-engines/mergetree-family/textindexes) | Token index over string columns. |
| Vector index | [Vector ANN index](/engines/table-engines/mergetree-family/annindexes) | Approximate nearest-neighbor lookups over embedding columns. |
| Materialized view | [Incremental MV](/materialized-view/incremental-materialized-view) (updates on every insert) or [refreshable MV](/materialized-view/refreshable-materialized-view) (runs on a schedule) | ClickHouse supports two MV models — see callout. |
| Scheduled query | [Refreshable MV](/materialized-view/refreshable-materialized-view) — runs the query on a schedule and maintains its result table | Refreshable MVs replace the scheduled-query-into-target-table pattern. |
| Streaming inserts | Native [`INSERT`](/sql-reference/statements/insert-into) over HTTP or the native protocol for direct ingest; [ClickPipes](/integrations/clickpipes) for managed streaming | ClickPipes covers Kafka, Kinesis, Pub/Sub, MySQL, Postgres, and object storage. |
| Continuous queries | Streaming [table engine](/engines/table-engines/integrations/kafka) (Kafka, Pub/Sub, etc.) feeding a materialized view that writes to a destination table | Same end-to-end model: ingest → transform → write. |
| Dry run | [`EXPLAIN ESTIMATE`](/sql-reference/statements/explain) — reports rows, parts, and marks the query would read | Other [`EXPLAIN`](/sql-reference/statements/explain) variants (`PLAN`, `PIPELINE`, `SYNTAX`) cover deeper plan inspection. |
| Federated queries (Spanner, Cloud SQL, AlloyDB) | External OLTP attached via [database engine](/engines/database-engines) (PostgreSQL, MySQL, MongoDB, SQLite) | Distinct from external tables in object storage — these attach a live source so its tables are queryable directly. |
| Cached results | [Query cache](/operations/query-cache) | Both transparently reuse results of recently executed queries. |
| Sessions / multi-statement queries | Per-statement execution; multi-step state managed in the client or an orchestrator | ClickHouse has no per-session variables or shared state. |

### Secondary indexes {#secondary-indexes}

Indexes on non-primary-key columns, used when queries filter by columns outside the sort order:

- [Bloom-filter](/engines/table-engines/mergetree-family/mergetree#bloom-filter) — equality lookups (`=`, `IN`)
- Token-bloom — substring search on tokenized text
- [Minmax](/engines/table-engines/mergetree-family/mergetree#minmax) — range pruning by per-part min/max

:::note Materialized view update model
ClickHouse has two MV models:
**incremental** MVs update on every base-table insert (cost
proportional to the insert) and **refreshable** MVs run on a
schedule. BigQuery materialized views correspond to the refreshable
model. Use incremental for high-throughput aggregations, refreshable
for periodic snapshots.
:::

## SQL and functions {#sql-and-functions}

The query-language surface: SQL coverage, UDFs, and the built-in function library.

ClickHouse SQL covers the standard `SELECT` / `JOIN` / `GROUP BY` / window-function surface. Function-by-function mapping (date, JSON, string, regex, window) lives in the BigQuery → ClickHouse SQL translation reference; the rows below are concept-level only.

| BigQuery | ClickHouse | Notes |
|---|---|---|
| Standard SQL | ClickHouse SQL — same `SELECT` / `JOIN` / `GROUP BY`, with [lambdas](/sql-reference/functions/overview#arrow-operator-and-lambda) and aggregate [combinators](/sql-reference/aggregate-functions/combinators) as additional language features | Compatible at the level of basic SQL. Lambdas and combinators are the two extensions worth getting familiar with. |
| Aggregate functions | [Aggregate functions](/sql-reference/aggregate-functions/reference) composable with [combinators](/sql-reference/aggregate-functions/combinators) (`-Array`, `-Map`, `-ForEach`, `-If`, …) | Combinators compose any aggregate with any input shape. |
| Array functions, `UNNEST` | [Array functions](/sql-reference/functions/array-functions) and lambdas | Common patterns: `arrayFilter`, `arrayMap`, `arrayZip`, `arrayReduce`. |
| SQL UDFs | [`CREATE FUNCTION`](/sql-reference/statements/create/function) (SQL expression) | Same model — function from a SQL expression. |
| JavaScript UDFs | [Executable UDF](/sql-reference/functions/udf) shelling out to a Python, shell, or other script | Different language and execution model, similar role. |
| Stored procedures | Client-side or orchestrator-side procedural logic ([dbt](/integrations/dbt), Airflow) | ClickHouse has no procedural SQL. |
| Multi-statement transactions | Per-insert and per-DDL atomic guarantees; application-layer grouping for multi-write batches | Multi-statement transactions are on the [roadmap](https://github.com/ClickHouse/ClickHouse/issues/58392). |
| Sketches (HLL, approximate quantiles) | [`uniqHLL12`](/sql-reference/aggregate-functions/reference/uniqhll12), [`quantileTDigest`](/sql-reference/aggregate-functions/reference/quantiletdigest), [`quantileDDSketch`](/sql-reference/aggregate-functions/reference/quantileddsketch), and others — composable via `-State`/`-Merge` combinators | Approximate aggregates that serialize as state and merge across queries. |

## Security and governance {#security-governance}

Access control, encryption, masking, and network boundaries.

Authorized views and row-level security are listed under [Storage and tables](#storage-tables).

| BigQuery | ClickHouse | Notes |
|---|---|---|
| Policy tags / column-level access control | Column-level [grants](/sql-reference/statements/grant) on specific columns of a table | Grants apply at the column level. BigQuery's centralized taxonomy/policy-tag governance has no direct equivalent. |
| Data masking | Views, [row policies](/sql-reference/statements/create/row-policy), or function-based transforms — see [data masking patterns](/cloud/guides/data-masking) | No column-mask primitive yet; patterns are SQL-level. |
| Customer-managed encryption keys (CMEK) | [CMEK](/cloud/security/cmek) on the service | BYOK in AWS KMS, with rotation and revocation. |
| AEAD / SQL-level encryption functions | [Encryption functions](/sql-reference/functions/encryption-functions) (`encrypt` / `decrypt`) | Covers AES-128/256-CBC/GCM and AEAD modes. |
| Differential privacy | External noise application, or via a [UDF](/sql-reference/functions/udf) | No built-in differential privacy in ClickHouse. |
| VPC Service Controls | [PrivateLink](/manage/security/aws-privatelink) (AWS / Azure) and IP allowlists for ingress restriction | Boundary semantics are narrower than VPC SC. |

## Data sharing {#data-sharing}

Cross-organization data exchange and clean-room patterns.

| BigQuery | ClickHouse | Notes |
|---|---|---|
| Analytics Hub / data exchanges / listings | Read access to a shared database, or a dedicated service with consumer-specific [row policies](/sql-reference/statements/create/row-policy) | ClickHouse has no in-product data marketplace; sharing uses standard access primitives. |
| Data clean rooms | [Row policies](/sql-reference/statements/create/row-policy) and [authorized views](/sql-reference/statements/create/view) — assembled per use case | No managed clean-room product. |

## Operations and ecosystem {#operations}

Day-2 concerns: ingestion, ML/BI integration, observability, metadata, and disaster recovery.

ClickHouse surfaces operational state through `system.*` tables (queries, sessions, replication, parts, metrics) and the cloud console; managed ingestion is handled by ClickPipes; ML, BI, and notebook workflows are typically handled in external systems that read from ClickHouse.

| BigQuery | ClickHouse | Notes |
|---|---|---|
| BigQuery ML | External training and serving (notebooks, Spark, Vertex AI, feature stores) reading from ClickHouse; see [AI/ML in Cloud](/cloud/features/ai-ml) for managed-side features | ClickHouse has no in-database ML — the typical pattern is to use ClickHouse as the analytical store and run training elsewhere. |
| BI Engine | Direct querying — [ClickHouse](/concepts) is a column-oriented analytical engine | ClickHouse has no separate caching layer to provision for BI workloads; queries run against the storage engine directly. |
| OMNI / cross-cloud federated query | One ClickHouse service per [supported region](/cloud/reference/supported-regions) where the data lives, with cross-region replication as needed | Pattern is one service per cloud, not federated queries across clouds. |
| Data sources / file formats | [File-format and connector library](/integrations) | Managed connectors (ClickPipes) for sources like Kafka, Pub/Sub, MySQL, Postgres, and object storage; SQL table functions for ad-hoc reads of files in object storage. |
| Query jobs (ID, history, cancel) | [`system.query_log`](/operations/system-tables/query_log) and [`system.processes`](/operations/system-tables/processes) for inspection; [`KILL QUERY`](/sql-reference/statements/kill) to cancel | Same information, exposed through system tables instead of a job API. |
| `INFORMATION_SCHEMA` | Native [`system.*` tables](/operations/system-tables) for ClickHouse-specific detail, or the ANSI [`information_schema`](/operations/system-tables/information_schema) views for tool compatibility | Both surfaces available. |
| Data Transfer Service | [ClickPipes](/integrations/clickpipes) — scheduled and streaming ingestion from SaaS, storage, and OLTP sources | Covers the same scheduling and source-coverage role. |
| Audit logs | [Cloud audit log](/cloud/security/audit-logging) and system tables | Both systems log admin and query activity. |
| Change data capture ingestion | [ClickPipes for Postgres](/integrations/clickpipes/postgres), [MySQL](/integrations/clickpipes/mysql), or Kafka | Managed CDC from OLTP and streaming sources into ClickHouse tables. |
| BigQuery Studio notebooks / BigQuery DataFrames | Jupyter with `clickhouse-connect` or another [client library](/integrations/python) | No in-product notebook environment or pandas-compatible in-DB API; notebook-side libraries cover the same workflow. |
| Data Canvas / managed data preparations | [SQL Console](/integrations/sql-clients/sql-console) and [ClickPipes](/integrations/clickpipes); visual data-prep in an external orchestrator | SQL Console is the UI counterpart; ClickPipes covers managed ingestion. |
| Gemini in BigQuery (SQL generation, code completion) | Ask-AI button in docs and console | LLM assistance is surfaced through Ask-AI; ClickHouse has no in-query assistant. |
| Knowledge Catalog / data lineage / data quality | [`system.*`](/operations/system-tables) tables for metadata; external tools (dbt, DataHub) for lineage and quality | ClickHouse exposes metadata via system tables rather than a managed catalog product. |
| Cross-region replication / managed disaster recovery | Multi-AZ HA within a region (automatic); cross-region replication via [`Replicated*MergeTree`](/engines/table-engines/mergetree-family/replication) engines or the Enterprise tier's advanced DR features | Multi-AZ HA is on by default within a region. Cross-region replication is configurable; latency between regions affects write performance. |
