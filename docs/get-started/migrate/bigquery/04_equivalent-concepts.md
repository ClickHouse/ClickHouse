---
title: 'BigQuery and ClickHouse: equivalent concepts'
slug: /migrations/bigquery/equivalent-concepts
description: 'Table-format reference mapping each core BigQuery concept to its ClickHouse equivalent'
keywords: ['BigQuery', 'migration', 'concept mapping', 'equivalent concepts', 'comparison']
sidebar_label: 'Equivalent concepts'
doc_type: 'reference'
---

The tables below map each BigQuery concept to its ClickHouse equivalent. For the end-to-end migration walkthrough, see [Migrating from BigQuery to ClickHouse Cloud](./02_migrating-to-clickhouse-cloud.md).

## Resource hierarchy {#resource-hierarchy}

| BigQuery | ClickHouse | Notes |
|---|---|---|
| Organization | [Organization](/cloud/security/console-roles#organization-roles) | Root node of the hierarchy in both. |
| Project | Service (region-scoped); [warehouse](/cloud/reference/warehouses) for grouping services with shared storage and independent compute | A service is the data-container analogue; use a warehouse to group services that share storage but scale compute independently. Billing, quotas, and tier are set at the ClickHouse organization level, not per service. |
| Dataset | [Database](/sql-reference/statements/create/database) | Same role — logical container for tables and grant scope. |
| IAM permissions | [Console roles](/cloud/guides/sql-console/manage-sql-console-role-assignments) plus SQL [grants](/sql-reference/statements/grant) | Two-layer access: roles in `console.clickhouse.cloud` plus SQL grants in the database. Console users can also be granted DB roles for SQL Console use. |

## Compute and capacity {#compute-capacity}

| BigQuery | ClickHouse | Notes |
|---|---|---|
| Slot | Replica (whole node); queries can [parallelize](/optimize/query-parallelism) across replicas | A replica is a static compute node in a ClickHouse service. See the callout below. |
| Slot reservation | Vertical and horizontal [autoscaling](/cloud/features/autoscaling/vertical) bounds; [warehouses](/cloud/reference/warehouses) for workload isolation | Where BigQuery reservations guarantee slot capacity, ClickHouse sizes via min/max autoscaling bounds. Setting min = max effectively fixes the size. |
| Quotas | [Workload classes](/operations/workload-scheduling) plus per-query limits | Workload classes cover memory, CPU, concurrency, and I/O scheduling at runtime. Spend caps have no direct primitive; there's no setting that suspends a service on hitting a cost threshold. |

:::note[Slot vs replica]
Unlike a BigQuery slot, a ClickHouse replica is a static compute
node that serves many concurrent queries; it isn't allocated to a
query from a pool. Replicas are also much coarser-grained: a slot
is closer to a unit of CPU than to a whole node.
:::

## Billing and pricing model {#billing}

ClickHouse Cloud meters compute as per-minute [compute units (8 GiB RAM, 2 vCPU)](/cloud/manage/billing/overview#how-is-compute-metered) rather than per-TB scanned or flat-rate slot commitments, charges for storage as compressed bytes, and bills backups as a separate line item. There's no equivalent to BigQuery's logical-vs-physical storage choice. As in BigQuery, ClickHouse Cloud charges for public internet egress and cross-region data transfer and offers committed-spend discounts. Managed ingestion ([ClickPipes](/integrations/clickpipes)) is [metered separately](/cloud/reference/billing/clickpipes); other features (materialized view refresh, secondary index maintenance, dictionary loads) run on the service's compute. See [ClickHouse Cloud pricing](/cloud/manage/billing/overview) for current rates, tiers, and commitment options.

## Storage and tables {#storage-tables}

In ClickHouse, a table's behavior is set at creation time: the engine (MergeTree family) determines merge and storage semantics, and `ORDER BY` / `PARTITION BY` / `TTL` clauses configure physical layout and retention. Many BigQuery per-feature settings map to a clause in the ClickHouse `CREATE TABLE` statement. Physical schema design also differs between platforms; see the [migration guide](./02_migrating-to-clickhouse-cloud.md) for design tradeoffs.

| BigQuery | ClickHouse | Notes |
|---|---|---|
| Table | [MergeTree-family table](/engines/table-engines/mergetree-family/mergetree) | Engine choice determines storage and merge behavior; pick by access pattern ([`MergeTree`](/engines/table-engines/mergetree-family/mergetree) for append-mostly facts, [`ReplacingMergeTree`](/engines/table-engines/mergetree-family/replacingmergetree) for upserts, [`AggregatingMergeTree`](/engines/table-engines/mergetree-family/aggregatingmergetree) for pre-aggregations). |
| Column schema modes (`NULLABLE`, `REQUIRED`, `REPEATED`) | [`Nullable(T)`](/sql-reference/data-types/nullable) for optional; omit for required; [`Array(T)`](/sql-reference/data-types/array) for repeated; `Array(Tuple(...))` or [`Nested`](/sql-reference/data-types/nested-data-structures/nested) for repeated records | In ClickHouse, columns are non-nullable unless wrapped with `Nullable(T)`. Nullability has a small storage and query cost, so use it only when the column needs nulls. |
| Schema evolution (add / drop / modify columns) | [`ALTER TABLE ... ADD / DROP / MODIFY COLUMN`](/sql-reference/statements/alter/column) | Same DDL surface as BigQuery. Many column changes are metadata-only. |
| Partitioning | [`PARTITION BY`](/engines/table-engines/mergetree-family/custom-partitioning-key) clause on the table | Where BigQuery limits partitioning to time-unit, integer-range, or ingestion-time columns, ClickHouse takes an arbitrary expression. Use it for retention (drop a partition) and pruning. |
| Clustering | [`ORDER BY`](/guides/best-practices/sparse-primary-indexes) columns in the table definition | Where BigQuery clustering is best-effort and reorganized in the background, ClickHouse's `ORDER BY` is enforced at insert time and drives the sparse primary index. |
| External tables / BigLake | [`s3`](/sql-reference/table-functions/s3) / [`gcs`](/sql-reference/table-functions/gcs) / [`azureBlobStorage`](/sql-reference/table-functions/azureBlobStorage) table functions for direct file access; [Iceberg engine](/engines/table-engines/integrations/iceberg) for open catalogs | Object storage and open-table formats are accessed directly through these functions and engines. ClickHouse doesn't provide a unified-governance layer over external storage. |
| Object tables (SQL access to unstructured files) | [`s3`](/sql-reference/table-functions/s3) / [`gcs`](/sql-reference/table-functions/gcs) table functions over binary formats | Unstructured objects (images, PDFs, audio) are queried directly through these table functions, not via a dedicated table type. |
| In-SQL ML inference on objects (`ML.GENERATE_TEXT`, `ML.GENERATE_EMBEDDING`) | [`aiGenerate`](/sql-reference/functions/ai-functions#aiGenerate) / [`aiEmbed`](/sql-reference/functions/ai-functions#aiEmbed) for text | In-SQL LLM and embedding functions exist, but they operate on text columns, not directly on unstructured objects (images, PDFs, audio). For object content, convert to text first or call a multimodal model from the application layer. |
| Apache Iceberg | [Iceberg engine](/engines/table-engines/integrations/iceberg) | Wide open-table format support; see the [data lake support matrix](/use-cases/data-lake/support-matrix) for current capabilities. |
| Default table / partition / dataset expiration | [`TTL` clause](/sql-reference/statements/create/table#ttl-expression) on the table, column, or partition | `TTL` can be set at table creation or via [`ALTER TABLE ... MODIFY TTL`](/sql-reference/statements/alter/ttl), and applies at the table, column, or partition level rather than the dataset default. |
| Table snapshot | [`CLONE AS`](/sql-reference/statements/create/table#with-a-schema-and-data-cloned-from-another-table) for an instant copy, or a table-level [`BACKUP`](/operations/backup/overview#syntax) | ClickHouse has no read-only point-in-time snapshot type. `CLONE AS` gives an instant hardlinked copy; a table-level `BACKUP` covers point-in-time recovery. |
| Time travel | Point-in-time [backup](/cloud/manage/backups/overview) restore | No inline historical query; restore is the only way to reach prior state. See the callout below. |
| Authorized views | [View](/sql-reference/statements/create/view) with `SQL SECURITY DEFINER` (runs with the view-owner's privileges) | See [CREATE VIEW](/sql-reference/statements/create/view) for the syntax and the `INVOKER` / `DEFINER` / `NONE` modes. |
| Row-level security | [Row policy](/sql-reference/statements/create/row-policy) — a `WHERE`-style expression evaluated per user | Same role; the policy expression is attached per user (or role) rather than as a BigQuery RLS row access policy resource. |
| Wildcard tables (`_TABLE_SUFFIX`) | [`Merge`](/engines/table-engines/special/merge) table engine (persistent grouping) or [`merge()`](/sql-reference/table-functions/merge) function (inline) | Same idea, different syntax. `Merge` is a persistent table-of-tables; `merge()` is inline without creating one. |
| Table clone | [`CREATE TABLE ... CLONE AS`](/sql-reference/statements/create/table#with-a-schema-and-data-cloned-from-another-table) within a service, or [backup](/cloud/manage/backups/overview) restore into a new service | `CLONE AS` hardlinks the source table's parts (part-level copy-on-write), so no data is physically copied. Copying across services still reads the source fully. |

:::note[Backups]
ClickHouse Cloud backups via the UI are per-service, and create a new service on restore. Finer backup granularity is possible with [`BACKUP`/`RESTORE` SQL commands](/operations/backup/overview#syntax).
:::

:::note[Updates and deletes]
ClickHouse is append-optimized. There's no SQL `MERGE` statement
(unrelated to the `Merge` and `MergeTree` engines), and
[`ALTER TABLE … UPDATE`](/sql-reference/statements/alter/update) /
[`DELETE`](/sql-reference/statements/alter/delete) run as background mutations
rather than transactional row writes. BigQuery DML patterns (`MERGE`, `UPDATE`,
`DELETE`, dbt incremental updates) typically port to engine choice in ClickHouse:
[`ReplacingMergeTree`](/engines/table-engines/mergetree-family/replacingmergetree)
keeps the latest row by sort key, [`CollapsingMergeTree`](/engines/table-engines/mergetree-family/collapsingmergetree)
cancels rows by inserting a matching row with `Sign = -1`, and [`AggregatingMergeTree`](/engines/table-engines/mergetree-family/aggregatingmergetree)
maintains aggregated state. Engine choice is set at table creation and is
non-trivial to change later.
:::

## Query model and performance {#query-model}

Query acceleration in ClickHouse comes from three layers: primary-key ordering (a sparse index over the on-disk sort order), secondary indexes on non-key columns, and materialized views.

| BigQuery | ClickHouse | Notes |
|---|---|---|
| Primary key (advisory) | Primary key — drives the on-disk sort order and the [sparse primary index](/guides/best-practices/sparse-primary-indexes) | Where BigQuery's PK is advisory only, ClickHouse's PK is load-bearing — it determines physical layout and is used to prune granules, avoid re-sorts, and short-circuit `LIMIT`. Neither system enforces uniqueness. |
| Foreign key (advisory) | Wide tables or [dictionaries](/dictionary) for lookups | ClickHouse has no notion of foreign keys. This doesn't prohibit joins but means referential integrity is left to you to manage at the application level. |
| Search index | [Full-text index](/engines/table-engines/mergetree-family/textindexes) | Token index over string columns. Operator surface differs: BigQuery's `SEARCH()` maps to the `hasToken` / `hasAllTokens` / `hasAnyTokens` / `hasPhrase` family in ClickHouse; `LIKE` and `match` can also use the index when the pattern is tokenizable (see the index doc for restrictions). |
| Vector index | [`Array(Float32)`](/sql-reference/data-types/array) or [`Array(BFloat16)`](/sql-reference/data-types/float#bfloat16) with a [vector ANN index](/engines/table-engines/mergetree-family/annindexes); or [`QBit`](/sql-reference/data-types/qbit) for tunable-precision search | Embeddings store as `Array(Float32)`, or `Array(BFloat16)` to halve storage, with an ANN index accelerating approximate nearest-neighbor lookups. `QBit` keeps full precision while letting you trade bits for speed at query time. |
| Materialized view | [Incremental MV](/materialized-view/incremental-materialized-view) — updates on each insert into a base table | Cost is paid at insert time. BigQuery's auto-refreshed MVs map to this incremental model, not to [refreshable MVs](/materialized-view/refreshable-materialized-view). |
| Scheduled query | [Refreshable MV](/materialized-view/refreshable-materialized-view) for query-driven scheduled work; external orchestrator ([dbt](/integrations/dbt), Airflow) for procedural pipelines | Refreshable MVs run on a cron-style schedule and maintain a result table. |
| Streaming inserts (`tabledata.insertAll`, Storage Write API) | [Asynchronous inserts](/best-practices/selecting-an-insert-strategy#asynchronous-inserts) for server-side batching of small row-by-row writes; [`Buffer` engine](/engines/table-engines/special/buffer) for in-memory accumulation; [ClickPipes](/integrations/clickpipes) for managed streaming pipelines | The raw API surface is `INSERT` over HTTP or the native protocol, which accepts the [Arrow / ArrowStream](/interfaces/formats/ArrowStream) columnar format used by the Storage Write API. For the row-by-row semantics of BigQuery streaming inserts, async inserts are the closer fit, batching on the server. For deduplication, ClickHouse exposes block-level dedup on Replicated tables (`insert_deduplicate`, default-on, recent-blocks window) and user-supplied per-insert dedup via `insert_deduplication_token`; for row-level dedup, use `ReplacingMergeTree`. None of these reproduces the Storage Write API's full exactly-once-by-stream-offset model. |
| Continuous queries | Streaming [table engine](/engines/table-engines/integrations/kafka) (Kafka, NATS, RabbitMQ) feeding a materialized view that writes to a destination table; [ClickPipes](/integrations/clickpipes) for managed streaming from Pub/Sub or Kinesis; an [incremental MV](/materialized-view/incremental-materialized-view) on a regular table for inserts already landing in ClickHouse | "Always-on transform" maps to engine + MV rather than a single declarative resource. Pick by source: streaming queue → table engine or ClickPipes; ongoing inserts into an existing table → MV. |
| Dry run | [`EXPLAIN ESTIMATE`](/sql-reference/statements/explain) — reports rows, parts, and marks the query would read; other [`EXPLAIN`](/sql-reference/statements/explain) variants (`PLAN`, `PIPELINE`, `SYNTAX`) cover deeper plan inspection | Covers the plan-inspection role of BigQuery's dry run, not the cost-estimation role. ClickHouse billing isn't per-query, so there's no "bytes that will be billed" answer to return. |
| Federated queries (Spanner, Cloud SQL, AlloyDB, Bigtable) | External OLTP attached via [database engine](/engines/database-engines) (PostgreSQL, MySQL, MongoDB, SQLite) | Distinct from external tables in object storage; these attach a live source so its tables are queryable directly. Bigtable has no ClickHouse database-engine equivalent. |
| Cached results | [Query cache](/operations/query-cache) | ClickHouse's query cache lives in each replica's memory and is per-user by default; identical queries to different replicas don't share results. Not transactionally consistent. |
| Sessions / multi-statement queries | [`SET`](/sql-reference/statements/set) for session-scoped settings and [query parameters](/sql-reference/statements/set#setting-query-parameters) | `SET name = value` applies a setting for the session's lifetime, and `SET param_name = value` defines query parameters referenced as `{name:Type}`. Multi-statement scripting (`DECLARE`, procedural control flow) has no equivalent; keep that logic in the client or an orchestrator. |

## Security and governance {#security-governance}

Authorized views and row-level security are listed under [Storage and tables](#storage-tables).

| BigQuery | ClickHouse | Notes |
|---|---|---|
| Policy tags / column-level access control | Column-level [grants](/sql-reference/statements/grant) on specific columns of a table | Grants apply at the column level. BigQuery's centralized taxonomy/policy-tag governance has no direct equivalent. |
| Data masking | [`CREATE MASKING POLICY`](/sql-reference/statements/create/masking-policy) (ClickHouse Cloud); or [views](/sql-reference/statements/create/view) and [row policies](/sql-reference/statements/create/row-policy). See [data masking patterns](/cloud/guides/data-masking) | `CREATE MASKING POLICY` is a direct equivalent: function-based column masking applied at query time per role, without changing stored data. |
| Customer-managed encryption keys (CMEK) | [CMEK](/cloud/security/cmek) on the service | Supports key rotation and revocation. See the CMEK page for the current list of supported cloud providers. |
| AEAD / SQL-level encryption functions | [Encryption functions](/sql-reference/functions/encryption-functions) (`encrypt` / `decrypt`) | Covers AES-128/256-CBC/GCM and AEAD modes. |
| Differential privacy | — | Not a managed feature. Applying noise in a [UDF](/sql-reference/functions/udf) doesn't reproduce BigQuery's epsilon/delta-controlled privacy guarantees; for true DP, use an external library. |
| VPC Service Controls | [Private connectivity](/cloud/security/connectivity/private-networking) — Private Service Connect (GCP), PrivateLink (AWS, Azure), and IP allowlists for ingress restriction | Restricts ingress; doesn't replicate VPC SC's data-exfiltration boundary. |

## Data sharing {#data-sharing}

| BigQuery | ClickHouse | Notes |
|---|---|---|
| Analytics Hub / data exchanges / listings | Read access to a shared database, or a dedicated service with consumer-specific [row policies](/sql-reference/statements/create/row-policy) | ClickHouse has no in-product data marketplace; sharing uses standard access primitives. |
| Data clean rooms | [Row policies](/sql-reference/statements/create/row-policy) and [authorized views](/sql-reference/statements/create/view) | No managed clean-room product. |

## Operations and ecosystem {#operations}

ClickHouse surfaces operational state through `system.*` tables (queries, sessions, replication, parts, metrics) and the cloud console; managed ingestion is handled by ClickPipes; BI and notebook workflows are typically handled in external systems that read from ClickHouse.

| BigQuery | ClickHouse | Notes |
|---|---|---|
| BigQuery ML | External training and serving (notebooks, Spark, Vertex AI, feature stores) reading from ClickHouse; see [AI/ML in Cloud](/cloud/features/ai-ml) for managed-side features | ClickHouse has no in-database model training; use it as the analytical store and train externally, with [chDB](/chdb) embedding the engine in-process (native Pandas/Arrow) to feed pipelines. For in-SQL LLM inference, see [`aiGenerate`](/sql-reference/functions/ai-functions#aiGenerate) / [`aiEmbed`](/sql-reference/functions/ai-functions#aiEmbed). |
| BI Engine | Direct querying — no separate acceleration tier to provision | Sub-second BI latency comes from the storage engine itself; there's no in-memory cache layer to size or pay for separately. |
| OMNI / cross-cloud federated query | — | ClickHouse doesn't query in place across clouds. The closest pattern is one service per [supported region](/cloud/reference/supported-regions) with data staged into the target service before being queried. |
| Data sources / file formats | [File-format and connector library](/integrations) | Managed connectors (ClickPipes) for sources like Kafka, Pub/Sub, MySQL, Postgres, and object storage; SQL table functions for ad-hoc reads of files in object storage. |
| Query jobs (ID, history, cancel) | [`system.query_log`](/operations/system-tables/query_log) and [`system.processes`](/operations/system-tables/processes) for inspection; [`KILL QUERY`](/sql-reference/statements/kill) to cancel | Same information, exposed through system tables instead of a job API. |
| `INFORMATION_SCHEMA` | Native [`system.*` tables](/operations/system-tables) for ClickHouse-specific detail, or the ANSI [`information_schema`](/operations/system-tables/information_schema) views for tool compatibility | Both surfaces available. |
| Data Transfer Service | [ClickPipes](/integrations/clickpipes) — scheduled and streaming ingestion from SaaS, storage, and OLTP sources | ClickPipes is ClickHouse Cloud's managed connector platform; coverage spans streaming systems, OLTP sources, and object storage. |
| Audit logs (admin activity, data access, system events) | [Cloud audit log](/cloud/security/audit-logging) for org and service admin events; [`system.query_log`](/operations/system-tables/query_log) for data-access activity within the service | BigQuery's three audit streams collapse to two on ClickHouse Cloud: admin activity to the cloud audit log, query/data access to `system.query_log`. System-event telemetry is exposed through other `system.*` tables rather than a dedicated audit stream. |
| Change data capture ingestion | [ClickPipes for Postgres](/integrations/clickpipes/postgres), [MySQL](/integrations/clickpipes/mysql), or Kafka | Managed CDC from OLTP sources. |
| BigQuery Studio notebooks | [Hex](https://clickhouse.com/integrations/hex) or Jupyter with [`clickhouse-connect`](/integrations/python) | No in-product notebook surface. Hex is a first-class partner with a native ClickHouse connector; Jupyter covers the self-managed path through the Python client. |
| BigQuery DataFrames | [chDB](/chdb) for a pandas-compatible in-process API | chDB's DataStore is the analog to BigQuery DataFrames: it runs ClickHouse SQL in-process and returns results in Pandas/Arrow. |
| Data Canvas | — | No drag-and-drop NL canvas. [SQL Console](/integrations/sql-clients/sql-console) covers ad-hoc query authoring; visual data prep happens in an external orchestrator. |
| Gemini in BigQuery (SQL generation, code completion) | [ClickHouse Agents](/cloud/features/ai-ml/agents) in the cloud console; Ask-AI in the docs | Agents are conversational: natural-language queries against your data with tool calls and chat workflows. For in-SQL LLM calls analogous to `BQ.ML.GENERATE_TEXT`, see [`aiGenerate`](/sql-reference/functions/ai-functions#aiGenerate). Check the [Agents](/cloud/features/ai-ml/agents) page for the current capability surface. |
| Knowledge Catalog / data lineage / data quality | [`system.*`](/operations/system-tables) tables for metadata; external tools (dbt, DataHub) for lineage and quality | ClickHouse exposes metadata via system tables rather than a managed catalog product. |
| Cross-region replication / managed disaster recovery | Multi-AZ HA within a region (automatic) | Cross-region replication and failover work differently in ClickHouse Cloud. See [Data resiliency](/cloud/data-resiliency) for the current model. |

## Next steps {#next-steps}

- For the end-to-end migration walkthrough, see [Migrating from BigQuery to ClickHouse Cloud](./02_migrating-to-clickhouse-cloud.md).
- For the conceptual side-by-side, see [Comparing ClickHouse Cloud and BigQuery](./01_overview.md).
- For loading data, see [Loading data from BigQuery](./03_loading-data.md).
