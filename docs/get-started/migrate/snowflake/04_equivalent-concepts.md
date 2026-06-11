---
title: 'Snowflake and ClickHouse: equivalent concepts'
slug: /migrations/snowflake/equivalent-concepts
description: 'Table-format reference mapping each core Snowflake concept to its ClickHouse equivalent'
keywords: ['Snowflake', 'migration', 'concept mapping', 'equivalent concepts', 'comparison']
sidebar_label: 'Equivalent concepts'
doc_type: 'reference'
---

The tables below map each Snowflake concept to its ClickHouse equivalent. For function-by-function SQL syntax mapping, see the [SQL translation reference](/migrations/snowflake-translation-reference). For the end-to-end migration walkthrough, see [Migrating from Snowflake to ClickHouse](./02_migration_guide.md).

## Resource hierarchy {#resource-hierarchy}

| Snowflake | ClickHouse | Notes |
|---|---|---|
| Account | [Organization](/cloud/security/console-roles#organization-roles) | Snowflake's identity, security, and billing boundary. A Snowflake org groups multiple accounts, with no equivalent tier above. |
| Virtual warehouse | Service | Compute (replicas) plus storage, equivalent to a Snowflake warehouse and the database it queries combined. |
| Database | [Database](/sql-reference/statements/create/database) | A namespace for tables and views. Snowflake's Database → Schema → Table flattens to Database → Table; see [Schemas](#schemas) below. |

:::note[Warehouse terminology]
A [**ClickHouse warehouse**](/cloud/reference/warehouses) is a grouping of services that share storage and scale compute independently, not a compute cluster as in Snowflake.
:::

## Schemas {#schemas}

A Snowflake schema serves multiple roles and has no single equivalent in ClickHouse.

| Snowflake | ClickHouse | Notes |
|---|---|---|
| Namespace partitioning — letting objects with the same name coexist (`analytics.users` vs `marketing.users`) | One [database](/sql-reference/statements/create/database) per Snowflake schema, or fold the schema name into the database (`analytics.public.events` → `analytics_public.events`) | Object references move from three-level (`DB.SCHEMA.TABLE`) to two-level (`DB.TABLE`). |
| Logical grouping by domain or processing stage (`analytics.raw`, `analytics.staging`, `analytics.marts`) | Separate databases or a consistent naming convention | — |
| Schema-level grants (`GRANT … ON SCHEMA …`) | [SQL grants](/sql-reference/statements/grant) at the database, table, or column level | A schema-level grant applies to the objects inside it; a database-wide grant (`GRANT … ON db.*`) covers the same footprint, and per-table or per-column grants give finer control. |
| Future grants | [Wildcard grants](/sql-reference/statements/grant#wildcard-grants): `GRANT … ON db.* TO role`, or a prefix like `db.prefix*` for a subset | A wildcard grant automatically covers tables created later. |
| Schema `OWNERSHIP` and `MANAGED ACCESS` | — | ClickHouse has no object-ownership model, so grants are always explicit. |
| Cloning unit (`CREATE SCHEMA … CLONE`) | Per-table [`CREATE TABLE ... CLONE AS`](/sql-reference/statements/create/table#with-a-schema-and-data-cloned-from-another-table), not per-schema; see the [Storage and tables](#storage-tables) section | No single-statement clone of a whole schema or database. Clone each table individually. |
| Time Travel and replication boundary | Service level — point-in-time recovery via [backups](/cloud/manage/backups/overview); replication via managed replicas, see [Operations and ecosystem](#operations) | No per-schema boundary: recovery and replication are scoped per service. TTL controls data expiry, not point-in-time recovery. See the [Data retention](#storage-tables) row. |
| Tagging and classification scope | Apply at the table or column level | No intermediate namespace inherits down. |

## Roles and access control {#roles-access}

ClickHouse Cloud's access layer splits into [console roles](/cloud/security/console-roles) at `console.clickhouse.cloud` (organization-, service-, and SQL-console-scoped) for org admin, billing, and service management; and SQL [roles and grants](/sql-reference/statements/grant) for database, table, and column access.

| Snowflake | ClickHouse | Notes |
|---|---|---|
| Account-level system roles (`ACCOUNTADMIN`, `SYSADMIN`, `SECURITYADMIN`, `USERADMIN`, `PUBLIC`) | [Organization roles](/cloud/security/console-roles#organization-roles) (Admin, Billing, Org API reader, Member) and [service roles](/cloud/security/console-roles#service-roles) (Service admin, Service reader, Service API admin/reader) in the console; SQL roles for database access | Org-scoped console roles cover billing, org admin, and user management; service-scoped roles cover service config, scaling, and backups. |
| Custom account roles | [Custom roles](/cloud/guides/security/manage-custom-roles) in the console | Like a Snowflake account role, a ClickHouse Cloud custom role is unified: one role can combine organization, service, and database permissions, scoped to all or a subset of services. |
| Database roles | — | No separate database-scoped role tier as in Snowflake's account/database split. A ClickHouse SQL role isn't confined to a single database, and a [custom role](/cloud/guides/security/manage-custom-roles) can combine organization, service, and database permissions in one role. |
| Role hierarchy (`GRANT ROLE … TO ROLE …`) | [`GRANT role1 TO role2`](/sql-reference/statements/grant) | — |
| Privilege grants on objects (`GRANT … ON … TO ROLE …`) | [`GRANT … ON db.table TO role`](/sql-reference/statements/grant), down to individual columns (`GRANT SELECT(col) ON db.table`) | ClickHouse grants can target the database, table, or column level. |
| Object ownership and ownership transfer | — | Access in ClickHouse is controlled entirely through explicit grants. Snowflake patterns that rely on owners delegating access need to be rebuilt as explicit role-based grants. |
| `USE ROLE` | [`SET ROLE`](/sql-reference/statements/set-role) | — |

:::note[Per-user SQL console access]
ClickHouse has no separate database-role tier, but ClickHouse Cloud can still grant per-user, scoped access to the SQL console. Create a role matching the `sql-console-role:<email>` naming convention and grant it the privileges that user should have; the console assigns it in place of the default `sql_console_admin` / `sql_console_read_only` roles. These are ordinary service-scoped SQL roles — the namespace is just a mapping convention. See [granular access control](/cloud/security/common-access-management-queries#granular-access-control).
:::

## Compute and capacity {#compute-capacity}

| Snowflake | ClickHouse | Notes |
|---|---|---|
| Virtual warehouse | Service (one or more replicas) | [Basic](/cloud/manage/cloud-tiers#basic) tier services are single-replica; [Scale and Enterprise](/cloud/manage/cloud-tiers#scale) support multi-replica (2+) deployments for higher SLAs. Queries [parallelize](/optimize/query-parallelism) across replicas. |
| Warehouse size (XS through 6X-Large) | Vertical [autoscaling](/cloud/features/autoscaling/vertical) bounds | Sizing is a min/max memory and CPU range the service autoscales within, not a discrete t-shirt size. Setting min equal to max pins the service to one fixed size, the closest equivalent to choosing a Snowflake size. |
| Multi-cluster warehouse | [Horizontal scaling](/cloud/features/autoscaling/horizontal) (replica count) | ClickHouse scales a service's replica count rather than adding clusters. The scaling model differs from Snowflake's auto-scaling policies (`Standard`/`Economy`); see [automatic scaling](/manage/scaling) for the options available today. |
| Auto-suspend / auto-resume | Service [idling](/cloud/features/autoscaling/idling) | Compute stops when there's no work, restarts on the next query. |
| Resource monitors (credit-quota spend caps) | [Workloads](/operations/workload-scheduling) for runtime scheduling; per-query limits (memory, threads, execution time) | ClickHouse workloads cover runtime resource scheduling but not spend caps; there's no primitive that suspends a service on hitting a credit threshold. |
| Query Acceleration Service | No direct equivalent | ClickHouse has no per-query compute booster; scale the service via [vertical autoscaling](/cloud/features/autoscaling/vertical) if queries are consistently large. |

## Billing and pricing model {#billing}

ClickHouse bills based on dedicated machines, metered in per-minute [compute units (8 GiB RAM, 2 vCPU)](https://github.com/cloud/manage/billing/overview#how-is-compute-metered). Storage pricing is based on compressed bytes, and backups are billed separately. [ClickPipes](/integrations/clickpipes) is [metered separately](/cloud/reference/billing/clickpipes).

Snowflake bills on credits scaled by warehouse size, charges for storage as compressed bytes without Time Travel or Fail-safe overhead, and bills backups as a separate line item rather than bundling them into retention windows. Most Snowflake "serverless compute" features (Snowpipe, Search Optimization, Auto-clustering, materialized view refresh, Cortex) are bundled into service compute on ClickHouse. 

Both charges for public internet egress and cross-region data transfer and offers committed-spend discounts. See [ClickHouse Cloud pricing](/cloud/manage/billing/overview) for current rates, tiers, and commitment options.

## Storage and tables {#storage-tables}

In ClickHouse, how a table stores and manages its data is set at creation time: the engine (MergeTree family) determines merge and storage semantics, and `ORDER BY` / `PARTITION BY` / `TTL` clauses configure physical layout and retention. Many Snowflake per-feature settings map to a clause in the ClickHouse `CREATE TABLE` statement. Physical schema design also differs between platforms; see the [migration guide](./02_migration_guide.md) for design tradeoffs.

| Snowflake | ClickHouse | Notes |
|---|---|---|
| Permanent table | [MergeTree-family table](/engines/table-engines/mergetree-family/mergetree) | Engine choice determines storage and merge behavior; pick by access pattern ([`MergeTree`](/engines/table-engines/mergetree-family/mergetree) for append-mostly facts, [`ReplacingMergeTree`](/engines/table-engines/mergetree-family/replacingmergetree) for upserts, [`AggregatingMergeTree`](/engines/table-engines/mergetree-family/aggregatingmergetree) for pre-aggregations). |
| Transient table | — | ClickHouse has no Fail-safe tier, so the permanent/transient distinction doesn't apply. |
| Temporary table (session-scoped) | [`CREATE TEMPORARY TABLE`](/sql-reference/statements/create/table#temporary-tables) | Session-scoped temporary tables exist in both; semantics are similar. |
| External table | [`s3`](/sql-reference/table-functions/s3) / [`gcs`](/sql-reference/table-functions/gcs) / [`azureBlobStorage`](/sql-reference/table-functions/azureBlobStorage) table functions for direct file access; [Iceberg engine](/engines/table-engines/integrations/iceberg) for open catalogs | Object storage and open-table formats are read directly through these functions and engines. |
| Stage (internal / external / user / table) | Object storage referenced directly via [`s3`](/sql-reference/table-functions/s3) / [`gcs`](/sql-reference/table-functions/gcs) / [`azureBlobStorage`](/sql-reference/table-functions/azureBlobStorage) table functions; [ClickPipes](/integrations/clickpipes) for managed staging on load | ClickHouse does not support an intermediary staging step. Read from the bucket directly, or use ClickPipes to coordinate ingest. |
| Iceberg table (managed or unmanaged) | [Iceberg engine](/engines/table-engines/integrations/iceberg) | See the [data lake support matrix](/use-cases/data-lake/support-matrix) for read, write, and storage-backend support. |
| Snowflake Open Catalog (Polaris) | [Iceberg engine](/engines/table-engines/integrations/iceberg) with REST catalog support | ClickHouse connects to an external REST catalog, which is managed outside ClickHouse. |
| Hybrid table (Unistore) | — | ClickHouse is OLAP-only; OLTP-style point reads and writes aren't a supported workload pattern. |
| Dynamic table | [Refreshable MV](/materialized-view/refreshable-materialized-view) | Maps to a scheduled Refreshable MV; see the [Query model](#query-model) section for the MV mapping. |
| Column data type modes (`NOT NULL` / nullable) | [`Nullable(T)`](/sql-reference/data-types/nullable) for optional; omit for required | In ClickHouse, columns are non-nullable unless wrapped with `Nullable(T)`. Nullability has a small storage and query cost, so use it only when the column needs nulls. |
| `VARIANT`, `OBJECT`, `ARRAY` (semi-structured) | [`JSON`](/sql-reference/data-types/newjson), [`Tuple`](/sql-reference/data-types/tuple), [`Nested`](/sql-reference/data-types/nested-data-structures/nested), [`Map`](/sql-reference/data-types/map), [`Array`](/sql-reference/data-types/array) | ClickHouse exposes typed alternatives instead of a single variant column. The [`JSON`](/sql-reference/data-types/newjson) type covers schemaless cases; see the [SQL translation reference](/migrations/snowflake-translation-reference#semi-structured-data) for the full mapping. |
| Schema evolution (add / drop / modify columns) | [`ALTER TABLE ... ADD / DROP / MODIFY COLUMN`](/sql-reference/statements/alter/column) | Same DDL surface as Snowflake. Many column changes are metadata-only. |
| Micro-partitions (auto-managed only) | Data parts (auto-managed) plus user-controlled [`PARTITION BY`](/engines/table-engines/mergetree-family/custom-partitioning-key) | Snowflake's micro-partitions are an internal storage detail with no user-facing knob. ClickHouse exposes `PARTITION BY` as an explicit clause, useful for retention (drop a partition) and pruning. |
| Clustering key | [`ORDER BY`](/guides/best-practices/sparse-primary-indexes) columns in the table definition | Where Snowflake's clustering key is advisory and reorganized in the background, ClickHouse's `ORDER BY` is enforced at insert time and drives the sparse primary index. |
| Data retention (table / database default) | [`TTL` clause](/sql-reference/statements/create/table#ttl-expression) on the table, column, or partition | `TTL` automatically deletes data older than a configured window. Set at table creation or via [`ALTER TABLE ... MODIFY TTL`](/sql-reference/statements/alter/ttl). |
| Time Travel | Point-in-time [backup](/cloud/manage/backups/overview) restore | ClickHouse has no inline query of historical state; recover a point in time via backups, at table or database granularity. |
| Fail-safe | — | Recovery beyond the backup window goes through ClickHouse Cloud support, not a self-service tier. |
| Zero-copy clone | [`CREATE TABLE ... CLONE AS`](/sql-reference/statements/create/table#with-a-schema-and-data-cloned-from-another-table) within a service, or [backup](/cloud/manage/backups/overview) restore into a new service | `CLONE AS` hardlinks the source table's parts (part-level copy-on-write), so no data is physically copied. Copying across services still reads the source fully. |
| Secure view | [View](/sql-reference/statements/create/view) with `SQL SECURITY DEFINER` | `SQL SECURITY DEFINER` delegates privileges (the view runs as its owner) but isn't a full Secure View: the definition stays readable via `SHOW CREATE` by anyone who can query it. See [CREATE VIEW](/sql-reference/statements/create/view) for the `DEFINER` / `INVOKER` / `NONE` modes. |
| Row access policy | [Row policy](/sql-reference/statements/create/row-policy) — a `WHERE`-style expression evaluated per user | Row policies apply transparently to every query against the table. |
| Sequence | [`generateSerialID`](/sql-reference/functions/other-functions#generateSerialID) for a Keeper-backed sequential counter; [`generateSnowflakeID`](/sql-reference/functions/uuid-functions#generateSnowflakeID) or [`generateUUIDv7`](/sql-reference/functions/uuid-functions#generateUUIDv7) for distributed unique IDs | `generateSerialID` is the closest match to an auto-incrementing sequence: a named, monotonic counter coordinated through ClickHouse Keeper. The UUID functions suit high-throughput unique IDs that don't need a shared counter. |

:::note[Updates and deletes]
ClickHouse is append-optimized. There's no SQL `MERGE` statement
(unrelated to the `Merge` and `MergeTree` engines), and
[`ALTER TABLE … UPDATE`](/sql-reference/statements/alter/update) /
[`DELETE`](/sql-reference/statements/alter/delete) run as background mutations
rather than transactional row writes. Update patterns from Snowflake (`MERGE`,
[dbt incremental](#transformation) updates) typically port to engine choice in ClickHouse:
[`ReplacingMergeTree`](/engines/table-engines/mergetree-family/replacingmergetree)
keeps the latest row by sort key, [`CollapsingMergeTree`](/engines/table-engines/mergetree-family/collapsingmergetree)
cancels rows by inserting a matching row with `Sign = -1`, and [`AggregatingMergeTree`](/engines/table-engines/mergetree-family/aggregatingmergetree)
maintains aggregated state. Engine choice is set at table creation and is
non-trivial to change later.
:::

## Query model and performance {#query-model}

Query acceleration in ClickHouse comes from three layers: primary-key ordering (a sparse index over the on-disk sort order), secondary indexes on non-key columns, and materialized views.

| Snowflake | ClickHouse | Notes |
|---|---|---|
| Primary key (advisory) | Primary key — drives the on-disk sort order and the [sparse primary index](/guides/best-practices/sparse-primary-indexes) | Where Snowflake's PK is advisory only, ClickHouse's PK is load-bearing: it sets physical layout and prunes granules at query time. Note that it is **not** a uniqueness constraint, unlike the usual SQL convention — ClickHouse neither requires nor enforces unique primary keys, so duplicate key values are allowed. |
| Foreign key constraint (advisory) | — | ClickHouse has no foreign-key constraint, and Snowflake's isn't enforced either. Joins work on any columns; model relationships with wide tables or [dictionaries](/dictionary). |
| Search Optimization Service | Secondary indexes — [bloom-filter](/engines/table-engines/mergetree-family/mergetree#bloom-filter), token-bloom, [minmax](/engines/table-engines/mergetree-family/mergetree#minmax) | ClickHouse asks you to pick the index type per column and tune its parameters; there's no automatic equivalent. |
| Cortex Search / Snowflake Cortex Search | [Full-text index](/engines/table-engines/mergetree-family/textindexes) | Token index over string columns for in-database search. |
| `VECTOR` data type and vector search | [`Array(Float32)`](/sql-reference/data-types/array) or [`Array(BFloat16)`](/sql-reference/data-types/float#bfloat16) with a [vector ANN index](/engines/table-engines/mergetree-family/annindexes); or [`QBit`](/sql-reference/data-types/qbit) for tunable-precision search | ClickHouse has no dedicated `VECTOR` type. Embeddings store as `Array(Float32)`, or `Array(BFloat16)` to halve storage, with an ANN index accelerating approximate nearest-neighbor lookups. `QBit` keeps full precision while letting you trade bits for speed at query time. |
| Materialized view | [Incremental MV](/materialized-view/incremental-materialized-view) — updates on each insert into a base table | Source-shape rules differ; review both before porting an existing MV. Cost is paid at insert time in ClickHouse. |
| Dynamic table | [Refreshable MV](/materialized-view/refreshable-materialized-view) | Refreshable MVs run on a cron-style schedule. |
| Result cache | [Query cache](/operations/query-cache) | ClickHouse's query cache lives in each replica's memory and is per-user by default; identical queries to different replicas don't share results. Not transactionally consistent. |
| Task (scheduled SQL) | [Refreshable MV](/materialized-view/refreshable-materialized-view) for query-driven scheduled work; external orchestrator ([dbt](/integrations/dbt), Airflow) for procedural pipelines | Task DAGs have no direct equivalent; model dependencies in the orchestrator. |
| Stream (CDC over a table) | [Materialized view](/materialized-views) over base-table inserts, or [ClickPipes](/integrations/clickpipes) for source-side CDC | ClickHouse MVs react on each insert; there's no offset/consume model. |
| `EXPLAIN` / `EXPLAIN_JSON` | [`EXPLAIN`](/sql-reference/statements/explain) variants (`PLAN`, `PIPELINE`, `SYNTAX`, `ESTIMATE`) | `EXPLAIN ESTIMATE` reports rows, parts, and marks the query would read; other variants cover deeper plan inspection. |
| External functions | [URL table engine](/engines/table-engines/special/url) or [`url`](/sql-reference/table-functions/url) for remote HTTP/HTTPS I/O, [`remote`](/sql-reference/table-functions/remote) for another ClickHouse server, [executable UDFs](/sql-reference/functions/udf) for local scripts, or a [database engine](/engines/database-engines) to attach a live source | ClickHouse can read from and write to HTTP endpoints from SQL via the URL engine, but has no per-row remote function call with managed batching and auth like a Snowflake External Function. |
| Sessions / session variables | [`SET`](/sql-reference/statements/set) for session-scoped settings and [query parameters](/sql-reference/statements/set#setting-query-parameters) | `SET name = value` applies a setting for the session's lifetime, and `SET param_name = value` defines query parameters referenced as `{name:Type}`. Free-form Snowflake-style variables (`$var`) and multi-step procedural state have no equivalent; keep those in the client or an orchestrator. |

## Transformation and modeling {#transformation}

| Snowflake | ClickHouse | Notes |
|---|---|---|
| dbt on Snowflake (`dbt-snowflake` adapter) | dbt on ClickHouse via the [`dbt-clickhouse` adapter](/integrations/dbt) | The adapter covers the standard dbt materializations (`view`, `table`, `incremental`, `materialized_view`, `ephemeral`) plus snapshots, seeds, sources, and tests. |
| dbt `incremental` (MERGE-based update strategy) | dbt `incremental` — supports `append`, `delete+insert`, `insert_overwrite`, and `microbatch` strategies (plus a legacy default) | ClickHouse incremental models don't issue SQL `MERGE`; the adapter rewrites the update pattern around append-optimized engines. See the [dbt materialization reference](/integrations/dbt/materializations) for strategy details. |
| dbt `materialized_view` (refresh-based) | dbt `materialized_view` — backed by ClickHouse [incremental MVs](/materialized-view/incremental-materialized-view); experimental in the adapter | ClickHouse MVs update on insert into the base table, not by re-running the model. Source-shape rules differ between platforms; see the [materialized_view materialization page](/integrations/dbt/materialization-materialized-view). |
| dbt Cloud | `dbt-clickhouse` isn't available in dbt Cloud today; dbt Core is the supported path | See the [`dbt-clickhouse` adapter page](/integrations/dbt) for current status. |
| Other transformation frameworks (Coalesce, SQLMesh, etc.) | Use the tool's ClickHouse adapter | Adapter coverage and maturity vary; verify supported features against the tool's own documentation. |

## Security and governance {#security-governance}

Secure views and row access policies are listed under [Storage and tables](#storage-tables). Roles and grants are covered in [Roles and access control](#roles-access).

| Snowflake | ClickHouse | Notes |
|---|---|---|
| Column masking policies (including tag-based) | [`CREATE MASKING POLICY`](/sql-reference/statements/create/masking-policy) (ClickHouse Cloud), or column-level [grants](/sql-reference/statements/grant). See [data masking patterns](/cloud/guides/data-masking) | Masking policies cover the column-masking part. ClickHouse targets roles, not tags, so Snowflake's centralized tag-based governance has no direct equivalent. |
| Dynamic data masking (function-based) | [`CREATE MASKING POLICY`](/sql-reference/statements/create/masking-policy) (ClickHouse Cloud); or [views](/sql-reference/statements/create/view) and [row policies](/sql-reference/statements/create/row-policy). See [data masking patterns](/cloud/guides/data-masking) | `CREATE MASKING POLICY` is a direct equivalent: function-based column masking applied at query time per role, without changing stored data. |
| Network policies (IP allowlist) | IP allowlists and [private connectivity](/cloud/security/connectivity/private-networking) — PrivateLink (AWS, Azure) and Private Service Connect (GCP) for ingress restriction | Private connectivity is available across the three major clouds. |
| Tri-Secret Secure (customer-managed keys) | [CMEK](/cloud/security/cmek) on the service | Supports key rotation and revocation. See the CMEK page for the current list of supported cloud providers. |
| Object tagging (governance metadata) | [Comments](/sql-reference/statements/alter/comment) on objects; Cloud service tags for cost | ClickHouse supports free-text comments on databases, tables, and columns, and ClickHouse Cloud tags services for usage and cost attribution. There's no tag-based data governance, though, such as tag-driven masking or classification. |
| Data classification (sensitive-data detection) | — | Not a managed feature; external tools (e.g. DataHub) cover this layer. |
| Encryption functions (`ENCRYPT` / `DECRYPT`) | [Encryption functions](/sql-reference/functions/encryption-functions) (`encrypt` / `decrypt`) | Covers AES-128/256-CBC/GCM and AEAD modes. |
| OAuth / SAML SSO | [SSO](/cloud/security/saml-setup) (SAML, OIDC) | Same role; configured in the cloud console. |
| Audit logs (`ACCOUNT_USAGE.LOGIN_HISTORY`, `QUERY_HISTORY`) | [Cloud audit log](/cloud/security/audit-logging) and [`system.query_log`](/operations/system-tables/query_log) | Admin events go to the audit log; query activity to `system.query_log`. |

## Data sharing {#data-sharing}

| Snowflake | ClickHouse | Notes |
|---|---|---|
| Secure Data Sharing | Read access to a shared database, or a dedicated service with consumer-specific [row policies](/sql-reference/statements/create/row-policy) | ClickHouse has no zero-copy cross-account share; sharing uses standard access primitives. |
| Snowflake Marketplace / Listings | — | ClickHouse has no in-product data marketplace. |
| Reader accounts (provider-managed consumer) | Dedicated service per consumer, or shared service with row policies | Consumers must have their own ClickHouse Cloud account; no equivalent for serving non-customers under the provider's billing. |
| Data Clean Rooms | [Row policies](/sql-reference/statements/create/row-policy), [views](/sql-reference/statements/create/view), and [masking policies](/sql-reference/statements/create/masking-policy) (ClickHouse Cloud) | No managed clean-room product; build access controls from row policies, views, and query-time column masking. |

## Operations and ecosystem {#operations}

ClickHouse surfaces operational state through `system.*` tables (queries, sessions, replication, parts, metrics) and the cloud console; managed ingestion is handled by ClickPipes; ML, BI, and notebook workflows are handled in external systems that read from ClickHouse.

| Snowflake | ClickHouse | Notes |
|---|---|---|
| Snowpipe (continuous ingest from object storage) | [ClickPipes](/integrations/clickpipes) for object storage (S3, GCS, Azure Blob Storage) | Managed ingest from object storage. See [supported data sources](/integrations/clickpipes#supported-data-sources) for the full list. |
| Snowpipe Streaming | [ClickPipes](/integrations/clickpipes) streaming sources (Kafka, Kinesis, Pub/Sub) | Managed low-latency streaming ingest. See [supported data sources](/integrations/clickpipes#supported-data-sources) for the full list. |
| Openflow connectors | [ClickPipes](/integrations/clickpipes) and the broader [integrations library](/integrations) | ClickPipes is ClickHouse Cloud's managed connector platform; coverage spans streaming systems, OLTP sources, and object storage. See the integrations library for the current source list. |
| Kafka connector | [ClickPipes for Kafka](/integrations/clickpipes/kafka), or the [Kafka table engine](/engines/table-engines/integrations/kafka) for self-managed pipelines | Same role; ClickPipes is the managed option. |
| Snowflake Connector for Postgres / MySQL | [ClickPipes for Postgres](/integrations/clickpipes/postgres), [MySQL](/integrations/clickpipes/mysql) | Managed CDC from OLTP sources. To host the source database in ClickHouse Cloud, [Managed Postgres](/cloud/managed-postgres) is an NVMe-backed Postgres service that replicates into ClickHouse via the same Postgres CDC connector. |
| Snowpark (Python / Java / Scala DataFrames) | External Python with [`clickhouse-connect`](/integrations/python) or another [client library](/integrations/python) | No in-database DataFrame runtime; notebook-side libraries cover the same workflow. |
| Snowflake ML (in-database training; formerly Snowpark ML) | External training and serving (notebooks, Spark, Vertex AI, feature stores) reading from ClickHouse; see [AI/ML in Cloud](/cloud/features/ai-ml) for managed-side features | ClickHouse has no in-database ML. Use it as the analytical store and run training elsewhere. |
| Cortex LLM functions (`CORTEX.COMPLETE`, `CORTEX.SUMMARIZE`, etc.) | — | No in-SQL LLM surface; call hosted models from the application layer or an orchestrator and write results back to ClickHouse. |
| Cortex Analyst | [ClickHouse Agents](/cloud/features/ai-ml/agents) in the cloud console | Agents are conversational: natural-language queries against your data with tool calls and chat workflows. Check the [Agents](/cloud/features/ai-ml/agents) page for the current capability surface. |
| Snowsight (web UI: editor, dashboards, monitoring, admin) | ClickHouse Cloud console, which includes [SQL Console](/integrations/sql-clients/sql-console), service management, monitoring, and dashboards | The ClickHouse Cloud console is the equivalent web surface; SQL Console is one component of it, not the whole UI. |
| Streamlit in Snowflake / Native Apps / Snowpark Container Services | — | ClickHouse has no in-product app-hosting, container, or app-distribution layer. Host Streamlit, container workloads, and packaged apps externally, then query ClickHouse over its native protocol or HTTP. |
| Notebooks in Snowflake | [Hex](https://clickhouse.com/integrations/hex), or Jupyter with [`clickhouse-connect`](/integrations/python) | No in-product notebook in ClickHouse Cloud. Hex is a first-class partner with a native ClickHouse connector; Jupyter covers the self-managed path through the Python client. |
| `INFORMATION_SCHEMA` | Native [`system.*` tables](/operations/system-tables) for ClickHouse-specific detail, or the ANSI [`information_schema`](/operations/system-tables/information_schema) views for tool compatibility | Both surfaces available. |
| `ACCOUNT_USAGE` / `READER_ACCOUNT_USAGE` views | Native [`system.*` tables](/operations/system-tables): `system.query_log`, `system.metric_log`, `system.processes`, and others | Same kind of operational telemetry, exposed through system tables. |
| Query History (UI and view) | [`system.query_log`](/operations/system-tables/query_log) and [`system.processes`](/operations/system-tables/processes) for inspection; [`KILL QUERY`](/sql-reference/statements/kill) to cancel | Same information, exposed through system tables instead of a job view. |
| Data lineage / Snowflake Horizon Catalog | [`system.*`](/operations/system-tables) tables for metadata; external tools (dbt, DataHub) for lineage and quality | ClickHouse exposes metadata via system tables rather than a managed catalog product. |
| Database replication / Account replication / Failover Groups (Snowgrid) | In-region high availability via multiple replicas (managed by Cloud) | Cross-region resiliency and failover work differently in ClickHouse Cloud. See [Disaster recovery](/cloud/data-resiliency) for the current model. |

## Next steps {#next-steps}

- For function-by-function SQL syntax mapping, see the [SQL translation reference](/migrations/snowflake-translation-reference).
- For the end-to-end migration walkthrough, see [Migrating from Snowflake to ClickHouse](./02_migration_guide.md).
