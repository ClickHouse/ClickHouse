---
title: 'Snowflake and ClickHouse: equivalent concepts'
slug: /migrations/snowflake/equivalent-concepts
description: 'Table-format reference mapping each core Snowflake concept to its ClickHouse equivalent'
keywords: ['Snowflake', 'migration', 'concept mapping', 'equivalent concepts', 'comparison']
sidebar_label: 'Equivalent concepts'
doc_type: 'reference'
---

The tables below map each Snowflake concept to its ClickHouse equivalent — what to use instead, and where the model differs. For function-by-function SQL syntax mapping, see the [Snowflake → ClickHouse SQL translation reference](/migrations/snowflake-translation-reference). For the end-to-end migration walkthrough, see [Migrating from Snowflake to ClickHouse](./02_migration_guide.md).

## Resource hierarchy {#resource-hierarchy}

How the platform organizes accounts, logical containers for data, and where compute is provisioned.

| Snowflake | ClickHouse | Notes |
|---|---|---|
| Organization | [Organization](/cloud/security/console-roles#organization-roles) | Root node of the hierarchy in both. |
| Account | [Warehouse](/cloud/reference/warehouses) — one or more services sharing storage | Like a Snowflake account, a ClickHouse warehouse groups multiple compute units that share storage. Each service has its own compute pool but reads and writes from the warehouse's shared storage. Tier and billing are set at the organization level, not the warehouse. |
| Database | [Database](/sql-reference/statements/create/database) | Logical container for tables. Snowflake uses a Database → Schema → Table hierarchy; ClickHouse flattens this to Database → Table — see [Schemas](#schemas) below. |

## Schemas {#schemas}

A Snowflake schema serves multiple roles and has no single equivalent in ClickHouse. The table below maps each role to its ClickHouse counterpart.

| Snowflake | ClickHouse | Notes |
|---|---|---|
| Namespace partitioning — letting objects with the same name coexist (`analytics.users` vs `marketing.users`) | One [database](/sql-reference/statements/create/database) per Snowflake schema, or fold the schema name into the database (`analytics.public.events` → `analytics_public.events`) | Object references move from three-level (`DB.SCHEMA.TABLE`) to two-level (`DB.TABLE`). |
| Logical grouping by domain or processing stage (`analytics.raw`, `analytics.staging`, `analytics.marts`) | Separate databases or a consistent naming convention | — |
| Permission boundary (`GRANT SELECT ON ALL TABLES IN SCHEMA … TO role`) | [SQL grants](/sql-reference/statements/grant) at the database, table, or column level | Database-wide grants cover the schema-level grant footprint; per-table grants are also available for finer-grained control. |
| Future-grants anchor (`GRANT … ON FUTURE TABLES IN SCHEMA …`) | Database wildcards (`GRANT … ON db.* TO role`) apply to current and future tables | Future grants only apply to a whole database — you can't scope them to a subset of tables within it. |
| Schema `OWNERSHIP` and `MANAGED ACCESS` (by default the role with `OWNERSHIP` holds grant authority on its objects; `WITH MANAGED ACCESS` centralizes that authority on the schema owner instead) | — | No object ownership in ClickHouse, so grants are always explicit. Mirrors `MANAGED ACCESS` in Snowflake, but it's the only mode. |
| Cloning unit (`CREATE SCHEMA … CLONE …` for environment branching) | No copy-on-write at any granularity — see the [Storage and tables](#storage-tables) section for the zero-copy clone row | Every copy reads source data fully in ClickHouse. |
| Time Travel and replication boundary (per-schema retention windows, replication policies) | Handled at the table level ([TTL](/sql-reference/statements/create/table#ttl-expression)) or service level ([backups](/cloud/manage/backups/overview), [database replication](#operations)) | No intermediate per-schema boundary. |
| Tagging and classification scope (tags and policies applied at schema, inherited by contained objects) | Apply at the table or column level | No intermediate namespace inherits down. |

## Roles and access control {#roles-access}

ClickHouse Cloud has two access layers: [console roles](/cloud/guides/sql-console/manage-sql-console-role-assignments) at `console.clickhouse.cloud` for organization, billing, and service management; and SQL [roles and grants](/sql-reference/statements/grant) inside each service for database, table, and column access.

| Snowflake | ClickHouse | Notes |
|---|---|---|
| Account-level system roles (`ACCOUNTADMIN`, `SYSADMIN`, `SECURITYADMIN`, `USERADMIN`, `PUBLIC`) | [Console roles](/cloud/guides/sql-console/manage-sql-console-role-assignments) (Admin, Developer, Billing Admin, etc.) for organization-level access; SQL roles inside each service | The console layer covers org-level operations (billing, service creation, user management). SQL roles cover database, table, and column access within a service. |
| Custom account roles | [`CREATE ROLE`](/sql-reference/statements/create/role) in SQL | Same pattern: create a role, grant privileges to it, grant the role to users. |
| Database roles (a separate role identity scoped to one database) | — | ClickHouse has only one tier of SQL roles, all service-scoped. There's no equivalent to Snowflake's two-tier system of account roles vs database roles. |
| Role hierarchy (`GRANT ROLE … TO ROLE …`) | [`GRANT role1 TO role2`](/sql-reference/statements/grant) | — |
| Privilege grants on objects (`GRANT … ON … TO ROLE …`) | [`GRANT … ON db.table TO role`](/sql-reference/statements/grant) | — |
| Object ownership and ownership transfer | — | ClickHouse has no ownership concept. Access is controlled entirely through explicit grants. Snowflake patterns that rely on owners delegating access need to be rebuilt as explicit role-based grants. |
| `USE ROLE` to switch active role per session | Active roles are set per session via [`SET ROLE`](/sql-reference/statements/set-role) | — |

## Compute, capacity, pricing {#compute-capacity}

:::note Warehouse terminology
"Warehouse" means different things in the two systems. A
**Snowflake warehouse** is a compute cluster — what runs queries.
A **ClickHouse warehouse** is a grouping of services that share
storage and scale compute independently. Where this page says
"warehouse" unqualified, it refers to the Snowflake concept.
:::

How processing is allocated to a query, sized, and billed.

| Snowflake | ClickHouse | Notes |
|---|---|---|
| Virtual warehouse | Service compute pool — queries [parallelize](/optimize/query-parallelism) across all replicas | A ClickHouse service has a fixed pool of replicas; queries run across them. See the callout below for the sizing-model difference. |
| Warehouse size (XS through 6X-Large) | Vertical [autoscaling](/cloud/features/autoscaling/vertical) bounds — replica size in CPU and memory | Sizing is configured as min/max bounds rather than discrete t-shirt sizes. |
| Multi-cluster warehouse | Horizontal autoscaling — additional replicas added under load | Both add parallel compute to handle concurrency; ClickHouse scales replica count rather than cluster count. Snowflake's `Standard` vs `Economy` scaling policy has no direct equivalent — ClickHouse autoscaling is bounds-based, not policy-driven. |
| Auto-suspend / auto-resume | Service [idling](/cloud/features/autoscaling/idling) — pause when idle, resume on query | Same model: compute stops when there's no work, restarts on the next query. |
| Resource monitors (credit quotas) | [Workload classes](/operations/workload-scheduling) plus per-query limits | Covers memory, CPU, concurrency, and I/O scheduling. The two quota models don't map row-by-row, but concept-level coverage is similar. |
| Query Acceleration Service | No direct equivalent — service-level [autoscaling](/cloud/features/autoscaling/vertical) covers sustained load, not per-query straggler boosts | QAS adds serverless compute to outlier queries. ClickHouse has no per-query compute booster; scale the service if queries are consistently large. |
| Credits (compute) | Compute-time (replica-hours) — see [billing overview](/cloud/manage/billing/overview) | Both bill compute by time; ClickHouse meters replica-hours directly. |
| Storage billing | Compressed storage — see [billing overview](/cloud/manage/billing/overview) | Both bill compressed storage. The total billed volume on each platform depends on the retention model in use — for Snowflake, that includes data held for Time Travel and Fail-safe. |

:::note Warehouse size vs replica
A Snowflake warehouse size (XS, S, M, …) is a discrete t-shirt
that doubles compute at each step. A ClickHouse replica is sized
by autoscaling bounds in CPU and memory. Both are the unit of
compute allocated to a query — the sizing model differs.
:::

## Storage and tables {#storage-tables}

How tables are stored: engines, schema, partitioning, snapshots, and access primitives.

In ClickHouse, a table's behavior is set at creation time: the engine (MergeTree family) determines merge and storage semantics, and `ORDER BY` / `PARTITION BY` / `TTL` clauses configure physical layout and retention. Many Snowflake per-feature settings map to a clause in the ClickHouse `CREATE TABLE` statement.

| Snowflake | ClickHouse | Notes |
|---|---|---|
| Permanent table | [MergeTree-family table](/engines/table-engines/mergetree-family/mergetree) | Engine choice determines storage and merge behavior — pick by access pattern ([`MergeTree`](/engines/table-engines/mergetree-family/mergetree) for append-mostly facts, [`ReplacingMergeTree`](/engines/table-engines/mergetree-family/replacingmergetree) for upserts, [`AggregatingMergeTree`](/engines/table-engines/mergetree-family/aggregatingmergetree) for pre-aggregations). |
| Transient table (no Fail-safe) | MergeTree table | ClickHouse has no Fail-safe tier, so the permanent/transient distinction doesn't apply. |
| Temporary table (session-scoped) | [`CREATE TEMPORARY TABLE`](/sql-reference/statements/create/table#temporary-tables) | Session-scoped temporary tables exist in both; semantics are similar. |
| External table | [`s3`](/sql-reference/table-functions/s3) / [`gcs`](/sql-reference/table-functions/gcs) / [`azureBlobStorage`](/sql-reference/table-functions/azureBlobStorage) table functions for direct file access; [Iceberg engine](/engines/table-engines/integrations/iceberg) for open catalogs | Object storage and open-table formats are read directly through these functions and engines. |
| Stage (internal / external / user / table) | Object storage referenced directly via [`s3`](/sql-reference/table-functions/s3) / [`gcs`](/sql-reference/table-functions/gcs) / [`azureBlobStorage`](/sql-reference/table-functions/azureBlobStorage) table functions; [ClickPipes](/integrations/clickpipes) for managed staging on load | ClickHouse has no stage object: there's no managed internal storage layer for files awaiting load, and no `PUT` / `GET` equivalents for moving files in and out. Read from the bucket directly, or use ClickPipes to coordinate ingest. |
| Iceberg table (managed or unmanaged) | [Iceberg engine](/engines/table-engines/integrations/iceberg) (read-only) | Reads Iceberg tables stored in S3, Azure, HDFS, or local storage; writes aren't supported. See the engine page for the current list of supported features. |
| Snowflake Open Catalog (Polaris) | [Iceberg engine](/engines/table-engines/integrations/iceberg) with REST catalog support | Both expose Iceberg tables through a REST catalog; ClickHouse reads from the catalog, ClickHouse itself isn't a catalog server. |
| Hybrid table (Unistore) | — | ClickHouse is OLAP-only; OLTP-style point reads and writes aren't a supported workload pattern. |
| Dynamic table | [Refreshable MV](/materialized-view/refreshable-materialized-view) (scheduled) or [incremental MV](/materialized-view/incremental-materialized-view) (per-insert) | Dynamic tables maintain a query result on a target lag; ClickHouse MVs cover both the periodic-refresh and per-insert models — see callout under [Query model](#query-model). |
| Column data type modes (`NOT NULL` / nullable) | [`Nullable(T)`](/sql-reference/data-types/nullable) for optional; omit for required | In ClickHouse, columns are non-nullable unless wrapped with `Nullable(T)`. Nullability has a small storage and query cost, so use it only when the column actually needs nulls. |
| `VARIANT`, `OBJECT`, `ARRAY` (semi-structured) | [`JSON`](/sql-reference/data-types/newjson), [`Tuple`](/sql-reference/data-types/tuple), [`Nested`](/sql-reference/data-types/nested-data-structures/nested), [`Map`](/sql-reference/data-types/map), [`Array`](/sql-reference/data-types/array) | ClickHouse exposes typed alternatives instead of a single variant column — pick the type that matches the data's shape. The [`JSON`](/sql-reference/data-types/newjson) type covers schemaless cases; see the [SQL translation reference](/migrations/snowflake-translation-reference#semi-structured-data) for the full mapping. |
| Schema evolution (add / drop / modify columns) | [`ALTER TABLE ... ADD / DROP / MODIFY COLUMN`](/sql-reference/statements/alter/column) | Same DDL surface as Snowflake. Many column changes are metadata-only. |
| Micro-partitions | Data parts — created on insert and merged in the background | An implementation detail of how MergeTree organizes rows on disk. Not directly user-controlled. |
| Clustering key | [`ORDER BY`](/guides/best-practices/sparse-primary-indexes) columns in the table definition | Defined as part of the table; data is physically sorted on disk by the `ORDER BY` columns. ClickHouse sorts at insert time rather than reorganizing in the background. |
| Data retention (table / database default) | [`TTL` clause](/sql-reference/statements/create/table#ttl-expression) on the table, column, or partition | Both support automatic deletion of data older than a configured window. `TTL` can be set at table creation or via [`ALTER TABLE ... MODIFY TTL`](/sql-reference/statements/alter/ttl). |
| Time Travel | Point-in-time [backup](/cloud/manage/backups/overview) restore into a new service | See callout below — granularity differs significantly. |
| Fail-safe (7-day Snowflake-only recovery) | — | Recovery beyond the backup window goes through ClickHouse Cloud support, not a self-service tier. |
| Zero-copy clone | [`CREATE TABLE ... AS SELECT`](/sql-reference/statements/create/table) copy, or [backup](/cloud/manage/backups/overview) restore into a new service | ClickHouse has no copy-on-write primitive — every copy reads the source data fully. |
| Secure view | [View](/sql-reference/statements/create/view) with `SQL SECURITY DEFINER` (runs with the view-owner's privileges) | See [CREATE VIEW](/sql-reference/statements/create/view) for the syntax and the `INVOKER` / `DEFINER` / `NONE` modes. |
| Row access policy | [Row policy](/sql-reference/statements/create/row-policy) — a `WHERE`-style expression evaluated per user | Row policies apply transparently to every query against the table. |
| Sequence | No direct equivalent — use [`generateSnowflakeID`](/sql-reference/functions/uuid-functions#generateSnowflakeID), [`generateUUIDv7`](/sql-reference/functions/uuid-functions#generateUUIDv7), or an external generator | ClickHouse has no auto-incrementing sequence object; generated IDs are produced per row at insert time. |

:::note Time Travel and backups
Snowflake Time Travel is per-table and queryable inline
(`SELECT … AT (TIMESTAMP => …)`), with retention configurable
up to 90 days on Enterprise editions. ClickHouse Cloud backups
are per-service: restoring creates a new service, you can't
query historical state inline, and a single table can't be
restored back into the original service. The functional gap is
larger than the row above suggests — plan accordingly if your
workflow relies on per-table point-in-time queries.
:::

:::note Partitioning
Snowflake has no user-controlled partition primitive — micro-partitions
are managed automatically. ClickHouse exposes [`PARTITION BY`](/engines/table-engines/mergetree-family/custom-partitioning-key)
as an explicit clause, useful for retention (drop a partition) and
pruning. There's nothing on the Snowflake side that maps to this directly;
clustering keys are the closest user-controlled layout primitive.
:::

## Query model and performance {#query-model}

How queries run and are accelerated — indexes, materialized views, caches, and streaming inputs.

Query acceleration in ClickHouse comes from three layers: primary-key ordering (a sparse index over the on-disk sort order), secondary indexes on non-key columns, and materialized views — incremental or refreshable. The rows below map Snowflake's acceleration features onto these primitives.

| Snowflake | ClickHouse | Notes |
|---|---|---|
| Primary key (advisory) | Primary key — drives the on-disk sort order and the [sparse primary index](/guides/best-practices/sparse-primary-indexes) | Neither system enforces uniqueness; the ClickHouse optimizer uses the key to prune granules, avoid re-sorts, and short-circuit `LIMIT`. |
| Foreign key (advisory) | Wide tables or [dictionaries](/dictionary) for lookups | ClickHouse doesn't accept foreign-key declarations even as advisory hints. |
| Search optimization service | Secondary indexes — [bloom-filter](/engines/table-engines/mergetree-family/mergetree#bloom-filter), token-bloom, [minmax](/engines/table-engines/mergetree-family/mergetree#minmax) | Same role: accelerate filters on non-key columns. Snowflake's SOS is automatic and uniform across applied columns; ClickHouse asks you to pick the index type per column and tune its parameters. |
| Cortex Search / Snowflake Cortex Search | [Full-text index](/engines/table-engines/mergetree-family/textindexes) | Token index over string columns for in-database search. |
| `VECTOR` data type and vector search | [`Array(Float32)`](/sql-reference/data-types/array) plus a [vector ANN index](/engines/table-engines/mergetree-family/annindexes) | ClickHouse has no dedicated `VECTOR` type — embeddings are stored as `Array(Float32)` and accelerated with an ANN index for approximate nearest-neighbor lookups. |
| Materialized view | [Incremental MV](/materialized-view/incremental-materialized-view) — updates on each insert into a base table | The two systems define materialized views differently. Review Snowflake's source-shape requirements and ClickHouse's incremental-MV behavior before porting an existing MV — they aren't a one-to-one swap. Cost is paid at insert time in ClickHouse. |
| Dynamic table | [Refreshable MV](/materialized-view/refreshable-materialized-view) — runs the query on a schedule and maintains its result table | Dynamic tables target a lag SLA; refreshable MVs run on a cron-style schedule with the same end-state. |
| Result cache | [Query cache](/operations/query-cache) | Both transparently reuse results of recently executed queries. Snowflake's result cache is service-wide and persistent; ClickHouse's is per-replica and not transactionally consistent. |
| Task (scheduled SQL) | [Refreshable MV](/materialized-view/refreshable-materialized-view) for query-driven scheduled work; external orchestrator ([dbt](/integrations/dbt), Airflow) for procedural pipelines | Refreshable MVs replace the typical task-into-target-table pattern. Snowflake task DAGs (task graphs) have no direct equivalent — model dependencies in the orchestrator. |
| Stream (CDC over a table) | [Materialized view](/materialized-view) over base-table inserts, or [ClickPipes](/integrations/clickpipes) for source-side CDC | Conceptually related but not equivalent: a Snowflake stream tracks change offsets on a table and is consumed by a task or query. A ClickHouse MV reacts on each insert and writes to a destination table. The end-state pattern (react to changes → write) is similar; the offset/consume model isn't. |
| `EXPLAIN` / `EXPLAIN_JSON` | [`EXPLAIN`](/sql-reference/statements/explain) variants (`PLAN`, `PIPELINE`, `SYNTAX`, `ESTIMATE`) | `EXPLAIN ESTIMATE` reports rows, parts, and marks the query would read; other variants cover deeper plan inspection. |
| External functions (HTTPS endpoints via API integrations) | No direct equivalent — closest options are [executable UDFs](/sql-reference/functions/udf) (local script invocation) or a [database engine](/engines/database-engines) attaching a live source | Snowflake external functions invoke remote HTTPS endpoints from inside SQL. ClickHouse has no managed outbound HTTP call from SQL; the surrogates run locally or attach a database, not call an arbitrary service. |
| Sessions / session variables | Per-statement execution; multi-step state managed in the client or an orchestrator | ClickHouse has no per-session variables or shared state. |

### Secondary indexes {#secondary-indexes}

Indexes on non-primary-key columns, used when queries filter by columns outside the sort order:

- [Bloom-filter](/engines/table-engines/mergetree-family/mergetree#bloom-filter) — equality lookups (`=`, `IN`)
- Token-bloom — substring search on tokenized text
- [Minmax](/engines/table-engines/mergetree-family/mergetree#minmax) — range pruning by per-part min/max

:::note Materialized view update model
ClickHouse has two MV models:
**incremental** MVs update on every base-table insert (cost
proportional to the insert) and **refreshable** MVs run on a
schedule. Snowflake materialized views correspond to the
incremental model; Snowflake dynamic tables correspond to the
refreshable model. Use incremental for high-throughput
aggregations, refreshable for periodic snapshots. The source-shape
rules differ between platforms — consult Snowflake's MV documentation
and ClickHouse's [incremental MV guide](/materialized-view/incremental-materialized-view)
when porting.
:::

## SQL and functions {#sql-and-functions}

The query-language surface: SQL coverage, UDFs, and the built-in function library.

ClickHouse SQL covers the standard `SELECT` / `JOIN` / `GROUP BY` / window-function surface. Function-by-function mapping (date, JSON, string, regex, window) lives in the [Snowflake → ClickHouse SQL translation reference](/migrations/snowflake-translation-reference); the rows below are concept-level only.

| Snowflake | ClickHouse | Notes |
|---|---|---|
| Snowflake SQL | ClickHouse SQL — same `SELECT` / `JOIN` / `GROUP BY`, with [lambdas](/sql-reference/functions/overview#arrow-operator-and-lambda) and aggregate [combinators](/sql-reference/aggregate-functions/combinators) as additional language features | Compatible at the level of basic SQL. Lambdas and combinators are the two extensions worth getting familiar with. |
| Aggregate functions | [Aggregate functions](/sql-reference/aggregate-functions/reference) composable with [combinators](/sql-reference/aggregate-functions/combinators) (`-Array`, `-Map`, `-ForEach`, `-If`, …) | Combinators compose any aggregate with any input shape. |
| Array / object functions, `FLATTEN` (`LATERAL`) | [Array functions](/sql-reference/functions/array-functions) and lambdas; [`ARRAY JOIN`](/sql-reference/statements/select/array-join) for flattening | Common patterns: `arrayFilter`, `arrayMap`, `arrayZip`, `arrayReduce`. `ARRAY JOIN` replaces `LATERAL FLATTEN`. |
| Semi-structured access (`:` notation, `GET_PATH`) | [JSON functions](/sql-reference/functions/json-functions) and dot-notation on the [`JSON`](/sql-reference/data-types/newjson) type | Path access is built into the new `JSON` type; legacy `JSONExtract*` functions cover string-typed JSON. |
| SQL UDF | [`CREATE FUNCTION`](/sql-reference/statements/create/function) (SQL expression) | Same model — function from a SQL expression. |
| JavaScript / Python / Java UDF | [Executable UDF](/sql-reference/functions/udf) shelling out to a Python, shell, or other script | Different language and execution model, similar role. |
| Stored procedures (Snowflake Scripting, JavaScript, Python) | Client-side or orchestrator-side procedural logic ([dbt](/integrations/dbt), Airflow) | ClickHouse has no procedural SQL. |
| Transactions (multi-statement, `BEGIN` … `COMMIT`) | Per-insert and per-DDL atomic guarantees; application-layer grouping for multi-write batches | Multi-statement transactions are on the [roadmap](https://github.com/ClickHouse/ClickHouse/issues/58392). |
| Approximate aggregates (`HLL`, `APPROX_PERCENTILE`, `APPROX_TOP_K`) | [`uniqHLL12`](/sql-reference/aggregate-functions/reference/uniqhll12), [`quantileTDigest`](/sql-reference/aggregate-functions/reference/quantiletdigest), [`quantileDDSketch`](/sql-reference/aggregate-functions/reference/quantileddsketch), [`topK`](/sql-reference/aggregate-functions/reference/topk) — composable via `-State`/`-Merge` combinators | Approximate aggregates that serialize as state and merge across queries. |

## Security and governance {#security-governance}

Access control, encryption, masking, and network boundaries.

Secure views and row access policies are listed under [Storage and tables](#storage-tables). Roles and grants are covered in [Roles and access control](#roles-access).

| Snowflake | ClickHouse | Notes |
|---|---|---|
| Column masking policies (including tag-based) | Column-level [grants](/sql-reference/statements/grant) on specific columns of a table, or [data masking patterns](/cloud/guides/data-masking) | Grants apply at the column level. Snowflake's centralized tag/policy governance has no direct equivalent. |
| Dynamic data masking (function-based) | Views, [row policies](/sql-reference/statements/create/row-policy), or function-based transforms — see [data masking patterns](/cloud/guides/data-masking) | No column-mask primitive yet; patterns are SQL-level. |
| Network policies (IP allowlist) | IP allowlists and [PrivateLink](/manage/security/aws-privatelink) (AWS / Azure) for ingress restriction | Both restrict network ingress; ClickHouse adds PrivateLink for private connectivity. |
| Tri-Secret Secure (customer-managed keys) | [CMEK](/cloud/security/cmek) on the service | BYOK in AWS KMS, with rotation and revocation. |
| Object tagging (governance metadata) | — | ClickHouse exposes metadata via `system.*` tables rather than user-defined tags. |
| Data classification (sensitive-data detection) | No direct equivalent — external tools (e.g. DataHub) | Not a managed feature. |
| Encryption functions (`ENCRYPT` / `DECRYPT`) | [Encryption functions](/sql-reference/functions/encryption-functions) (`encrypt` / `decrypt`) | Covers AES-128/256-CBC/GCM and AEAD modes. |
| OAuth / SAML SSO | [SSO](/cloud/security/saml-sso-setup) (SAML, OIDC) | Same role; configured in the cloud console. |
| Audit logs (`ACCOUNT_USAGE.LOGIN_HISTORY`, `QUERY_HISTORY`) | [Cloud audit log](/cloud/security/audit-logging) and [`system.query_log`](/operations/system-tables/query_log) | Both systems log admin and query activity. |

## Data sharing {#data-sharing}

Cross-organization data exchange and clean-room patterns.

| Snowflake | ClickHouse | Notes |
|---|---|---|
| Secure Data Sharing | Read access to a shared database, or a dedicated service with consumer-specific [row policies](/sql-reference/statements/create/row-policy) | ClickHouse has no zero-copy cross-account share; sharing uses standard access primitives. |
| Snowflake Marketplace / Listings | — | ClickHouse has no in-product data marketplace. |
| Reader accounts (provider-managed consumer) | Dedicated service per consumer, or shared service with row policies | Same pattern: isolate consumer access at the service or row-policy level. |
| Data Clean Rooms | [Row policies](/sql-reference/statements/create/row-policy) and [secure views](/sql-reference/statements/create/view) — assembled per use case | No managed clean-room product. |

## Operations and ecosystem {#operations}

Day-2 concerns: ingestion, ML/BI integration, observability, metadata, and disaster recovery.

ClickHouse surfaces operational state through `system.*` tables (queries, sessions, replication, parts, metrics) and the cloud console; managed ingestion is handled by ClickPipes; ML, BI, and notebook workflows are typically handled in external systems that read from ClickHouse.

| Snowflake | ClickHouse | Notes |
|---|---|---|
| Snowpipe (continuous ingest from object storage) | [ClickPipes](/integrations/clickpipes) for S3, GCS, and Azure Blob Storage | Managed ingest from object storage. |
| Snowpipe Streaming | [ClickPipes](/integrations/clickpipes) for Kafka, Kinesis, Pub/Sub | Managed low-latency streaming ingest. |
| Openflow connectors / Snowflake Connectors | [ClickPipes](/integrations/clickpipes) and the broader [integrations library](/integrations) | Openflow is Snowflake's connector framework, built on Apache NiFi; ClickPipes is ClickHouse Cloud's managed connector platform. Both cover ingest from streaming systems, OLTP sources, and object storage; see each platform's documentation for the current source list. |
| Kafka connector | [ClickPipes for Kafka](/integrations/clickpipes/kafka), or the [Kafka table engine](/engines/table-engines/integrations/kafka) for self-managed pipelines | Same role; ClickPipes is the managed option. |
| Snowflake Connector for Postgres / MySQL | [ClickPipes for Postgres](/integrations/clickpipes/postgres), [MySQL](/integrations/clickpipes/mysql) | Managed CDC from OLTP sources. |
| Snowpark (Python / Java / Scala DataFrames) | External Python with [`clickhouse-connect`](/integrations/python) or another [client library](/integrations/python) | No in-database DataFrame runtime; notebook-side libraries cover the same workflow. |
| Snowpark ML (in-database training) | External training and serving (notebooks, Spark, Vertex AI, feature stores) reading from ClickHouse; see [AI/ML in Cloud](/cloud/features/ai-ml) for managed-side features | ClickHouse has no in-database ML — the typical pattern is to use ClickHouse as the analytical store and run training elsewhere. |
| Cortex LLM functions (`CORTEX.COMPLETE`, `CORTEX.SUMMARIZE`, etc.) | No in-SQL equivalent — call LLM providers from the application layer or an orchestrator and write results back to ClickHouse | Snowflake exposes hosted LLMs as SQL functions. ClickHouse has no in-query LLM functions; Ask-AI in the docs and console is a docs/console helper, not a SQL surface. |
| Cortex Analyst (natural-language to SQL over your data) | — | Snowflake offers an NL-to-SQL service grounded on your semantic model. ClickHouse has no in-product equivalent. |
| Snowsight (web UI: editor, dashboards, monitoring, admin) | ClickHouse Cloud console, which includes [SQL Console](/integrations/sql-clients/sql-console), service management, monitoring, and dashboards | The ClickHouse Cloud console is the equivalent web surface; SQL Console is one component of it, not the whole UI. |
| Streamlit in Snowflake / Native Apps / Snowpark Container Services | No direct equivalent — host Streamlit, container workloads, and packaged apps externally, then query ClickHouse over its native protocol or HTTP | ClickHouse has no in-product app-hosting, container, or app-distribution layer. |
| Notebooks in Snowflake | Jupyter with [`clickhouse-connect`](/integrations/python) or another [client library](/integrations/python) | No in-product notebook environment; notebook-side libraries cover the same workflow. |
| `INFORMATION_SCHEMA` | Native [`system.*` tables](/operations/system-tables) for ClickHouse-specific detail, or the ANSI [`information_schema`](/operations/system-tables/information_schema) views for tool compatibility | Both surfaces available. |
| `ACCOUNT_USAGE` / `READER_ACCOUNT_USAGE` views | Native [`system.*` tables](/operations/system-tables) — `system.query_log`, `system.metric_log`, `system.processes`, and others | Same kind of operational telemetry, exposed through system tables. |
| Query History (UI and view) | [`system.query_log`](/operations/system-tables/query_log) and [`system.processes`](/operations/system-tables/processes) for inspection; [`KILL QUERY`](/sql-reference/statements/kill) to cancel | Same information, exposed through system tables instead of a job view. |
| Data lineage / Snowflake Horizon Catalog | [`system.*`](/operations/system-tables) tables for metadata; external tools (dbt, DataHub) for lineage and quality | ClickHouse exposes metadata via system tables rather than a managed catalog product. |
| Database replication / Account replication / Failover Groups (Snowgrid) | Multi-AZ HA within a region (automatic); cross-region replication via [`Replicated*MergeTree`](/engines/table-engines/mergetree-family/replication) engines or the Enterprise tier's advanced DR features | Snowgrid is the underlying Snowflake fabric powering cross-region replication, global data sharing, and Failover Groups. On ClickHouse Cloud, Multi-AZ HA is on by default within a region, and cross-region replication is configured per service. Latency between regions affects write performance. |
