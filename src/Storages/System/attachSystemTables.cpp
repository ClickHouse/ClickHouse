#include <Storages/System/StorageSystemKeywords.h>
#include "config.h"

#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Databases/IDatabase.h>
#include <Storages/System/attachSystemTables.h>
#include <Storages/System/attachSystemTablesImpl.h>

#include <Storages/System/StorageSystemAggregateFunctionCombinators.h>
#include <Storages/System/StorageSystemAsynchronousMetrics.h>
#include <Storages/System/StorageSystemAsyncLoader.h>
#include <Storages/System/StorageSystemBackgroundSchedulePool.h>
#include <Storages/System/StorageSystemBackups.h>
#include <Storages/System/StorageSystemBuildOptions.h>
#include <Storages/System/StorageSystemHypotheticalIndexes.h>
#include <Storages/System/StorageSystemInstrumentation.h>
#include <Storages/System/StorageSystemCollations.h>
#include <Storages/System/StorageSystemClusters.h>
#include <Storages/System/StorageSystemColumns.h>
#include <Storages/System/StorageSystemCodecs.h>
#include <Storages/System/StorageSystemCompletions.h>
#include <Storages/System/StorageSystemDatabases.h>
#include <Storages/System/StorageSystemDataSkippingIndices.h>
#include <Storages/System/StorageSystemDataSkippingIndexTypes.h>
#include <Storages/System/StorageSystemDataTypeFamilies.h>
#include <Storages/System/StorageSystemDictionaryLayouts.h>
#include <Storages/System/StorageSystemDictionarySources.h>
#include <Storages/System/StorageSystemDocumentation.h>
#include <Storages/System/StorageSystemDetachedParts.h>
#include <Storages/System/StorageSystemDetachedTables.h>
#include <Storages/System/StorageSystemDictionaries.h>
#include <Storages/System/StorageSystemEvents.h>
#include <Storages/System/StorageSystemFailPoints.h>
#include <Storages/System/StorageSystemFormats.h>
#include <Storages/System/StorageSystemFunctions.h>
#include <Storages/System/StorageSystemUserDefinedFunctions.h>
#include <Storages/System/StorageSystemWorkloads.h>
#include <Storages/System/StorageSystemResources.h>
#include <Storages/System/StorageSystemGraphite.h>
#include <Storages/System/StorageSystemMacros.h>
#include <Storages/System/StorageSystemMerges.h>
#include <Storages/System/StorageSystemMoves.h>
#include <Storages/System/StorageSystemReplicatedFetches.h>
#include <Storages/System/StorageSystemMetrics.h>
#include <Storages/System/StorageSystemHistogramMetrics.h>
#include <Storages/System/StorageSystemDimensionalMetrics.h>
#include <Storages/System/StorageSystemMutations.h>
#include <Storages/System/StorageSystemNumbers.h>
#include <Storages/System/StorageSystemPrimes.h>
#include <Storages/System/StorageSystemOne.h>
#include <Storages/System/StorageSystemPartMovesBetweenShards.h>
#include <Storages/System/StorageSystemParts.h>
#include <Storages/System/StorageSystemProjectionParts.h>
#include <Storages/System/StorageSystemPartsColumns.h>
#include <Storages/System/StorageSystemProjectionPartsColumns.h>
#include <Storages/System/StorageSystemProcesses.h>
#include <Storages/System/StorageSystemUserProcesses.h>
#include <Storages/System/StorageSystemReplicas.h>
#include <Storages/System/StorageSystemDatabaseReplicas.h>
#include <Storages/System/StorageSystemReplicationQueue.h>
#include <Storages/System/StorageSystemDistributionQueue.h>
#include <Storages/System/StorageSystemServerSettings.h>
#include <Storages/System/StorageSystemSettings.h>
#include <Storages/System/StorageSystemSettingsChanges.h>
#include <Storages/System/StorageSystemMergeTreeSettings.h>
#include <Storages/System/StorageSystemDatabaseEngines.h>
#include <Storages/System/StorageSystemStatements.h>
#include <Storages/System/StorageSystemTableEngines.h>
#include <Storages/System/StorageSystemTableFunctions.h>
#include <Storages/System/StorageSystemTables.h>
#include <Storages/System/StorageSystemProjections.h>
#include <Storages/System/StorageSystemConstraints.h>
#include <Storages/System/StorageSystemZooKeeper.h>
#include <Storages/System/StorageSystemZooKeeperInfo.h>
#include <Storages/System/StorageSystemContributors.h>
#include <Storages/System/StorageSystemErrors.h>
#include <Storages/System/StorageSystemWarnings.h>
#include <Storages/System/StorageSystemDDLWorkerQueue.h>
#include <Storages/System/StorageSystemLicenses.h>
#include <Storages/System/StorageSystemTimeZones.h>
#include <Storages/System/StorageSystemDisks.h>
#include <Storages/System/StorageSystemDiskTypes.h>
#include <Storages/System/StorageSystemStoragePolicies.h>
#include <Storages/System/StorageSystemZeros.h>
#include <Storages/System/StorageSystemUsers.h>
#include <Storages/System/StorageSystemRoles.h>
#include <Storages/System/StorageSystemGrants.h>
#include <Storages/System/StorageSystemRoleGrants.h>
#include <Storages/System/StorageSystemCurrentRoles.h>
#include <Storages/System/StorageSystemEnabledRoles.h>
#include <Storages/System/StorageSystemSettingsProfiles.h>
#include <Storages/System/StorageSystemSettingsProfileElements.h>
#include <Storages/System/StorageSystemRowPolicies.h>
#include <Storages/System/StorageSystemMaskingPolicies.h>
#include <Storages/System/StorageSystemQuotas.h>
#include <Storages/System/StorageSystemQuotaLimits.h>
#include <Storages/System/StorageSystemQuotaUsage.h>
#include <Storages/System/StorageSystemQuotasUsage.h>
#include <Storages/System/StorageSystemUserDirectories.h>
#include <Storages/System/StorageSystemPrivileges.h>
#include <Storages/System/StorageSystemAsynchronousInserts.h>
#include <Storages/System/StorageSystemTransactions.h>
#include <Storages/System/StorageSystemFilesystemCache.h>
#include <Storages/System/StorageSystemFilesystemCacheSettings.h>
#include <Storages/System/StorageSystemQueryConditionCache.h>
#include <Storages/System/StorageSystemQueryResultCache.h>
#include <Storages/System/StorageSystemUserQueryLog.h>
#include <Storages/System/StorageSystemNamedCollections.h>
#include <Storages/System/StorageSystemHandlers.h>
#include <Storages/System/StorageSystemRemoteDataPaths.h>
#include <Storages/System/StorageSystemCertificates.h>
#include <Storages/System/StorageSystemTokenizers.h>
#include <Storages/System/StorageSystemStemmers.h>
#include <Storages/System/StorageSystemSchemaInferenceCache.h>
#include <Storages/System/StorageSystemDroppedTables.h>
#include <Storages/System/StorageSystemDroppedTablesParts.h>
#include <Storages/System/StorageSystemZooKeeperConnection.h>
#include <Storages/System/StorageSystemZooKeeperWatches.h>
#if USE_NURAFT
#include <Storages/System/StorageSystemKeeperChangelogs.h>
#include <Storages/System/StorageSystemKeeperSnapshots.h>
#include <Storages/System/StorageSystemKeeperStorage.h>
#endif
#include <Storages/System/StorageSystemJemalloc.h>
#include <Storages/System/StorageSystemJemallocProfileText.h>
#include <Storages/System/StorageSystemJemallocStats.h>
#if USE_NURAFT
#include <Storages/System/StorageSystemKeeperCluster.h>
#endif
#include <Storages/System/StorageSystemScheduler.h>
#include <Storages/System/StorageSystemObjectStorageQueueMetadata.h>
#include <Storages/System/StorageSystemObjectStorageQueueMetadataCache.h>
#include <Storages/System/StorageSystemObjectStorageQueueSettings.h>
#include <Storages/System/StorageSystemDashboards.h>
#include <Storages/System/StorageSystemViewRefreshes.h>
#include <Storages/System/StorageSystemDNSCache.h>
#include <Storages/System/StorageSystemIcebergFiles.h>
#if ENABLE_DISTRIBUTED_CACHE
#include <DistributedCache/Utils.h>
#endif
#include <Storages/System/StorageSystemIcebergHistory.h>
#if USE_ICU
#   include <Storages/System/StorageSystemUnicode.h>
#endif
#include <Storages/System/StorageSystemWasmModules.h>

#include <Interpreters/Context.h>

#include <Poco/Util/LayeredConfiguration.h>

#if (defined(__ELF__) && !defined(OS_FREEBSD)) || defined(OS_DARWIN)
#include <Storages/System/StorageSystemSymbols.h>
#endif

#if USE_RDKAFKA
#include <Storages/System/StorageSystemKafkaConsumers.h>
#endif

#if defined(OS_LINUX) || defined(OS_DARWIN)
#include <Storages/System/StorageSystemStackTrace.h>
#endif

#if USE_ROCKSDB
#include <Storages/RocksDB/StorageSystemRocksDB.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int TABLE_ALREADY_EXISTS;
}

void attachSystemTablesServer(ContextPtr context, IDatabase & system_database, bool has_zookeeper, [[maybe_unused]] bool has_keeper_server)
{
    auto component_guard = Coordination::setCurrentComponent("attachSystemTablesServer");
    attachNoDescription<StorageSystemOne>(context, system_database, "one", R"DOCS_MD(
.description
This table contains a single row with a single `dummy` UInt8 column containing the value 0.

This table is used if a `SELECT` query does not specify the `FROM` clause.

This is similar to the `DUAL` table found in other DBMSs.

.examples
```sql
SELECT * FROM system.one LIMIT 10;
```

```response
┌─dummy─┐
│     0 │
└───────┘

1 rows in set. Elapsed: 0.001 sec.
```
)DOCS_MD");
    attachNoDescription<StorageSystemNumbers>(context, system_database, "numbers", R"DOCS_MD(
.description
This table contains a single UInt64 column named `number` that contains almost all the natural numbers starting from zero.

You can use this table for tests, or if you need to do a brute force search.

Reads from this table are not parallelized.

.examples
```sql
SELECT * FROM system.numbers LIMIT 10;
```

```response
┌─number─┐
│      0 │
│      1 │
│      2 │
│      3 │
│      4 │
│      5 │
│      6 │
│      7 │
│      8 │
│      9 │
└────────┘

10 rows in set. Elapsed: 0.001 sec.
```

You can also limit the output by predicates.

```sql
SELECT * FROM system.numbers WHERE number < 10;
```

```response
┌─number─┐
│      0 │
│      1 │
│      2 │
│      3 │
│      4 │
│      5 │
│      6 │
│      7 │
│      8 │
│      9 │
└────────┘

10 rows in set. Elapsed: 0.001 sec.
```
)DOCS_MD", false, "number");
    attachNoDescription<StorageSystemNumbers>(context, system_database, "numbers_mt", R"DOCS_MD(
.description
The same as [`system.numbers`](/reference/system-tables/numbers) but reads are parallelized. The numbers can be returned in any order.

Used for tests.

.examples
```sql
SELECT * FROM system.numbers_mt LIMIT 10;
```

```response
┌─number─┐
│      0 │
│      1 │
│      2 │
│      3 │
│      4 │
│      5 │
│      6 │
│      7 │
│      8 │
│      9 │
└────────┘

10 rows in set. Elapsed: 0.001 sec.
```
)DOCS_MD", true, "number");
    attachNoDescription<StorageSystemPrimes>(context, system_database, "primes", R"DOCS_MD(
.description
This table contains a single UInt64 column named `prime` that contains prime numbers in ascending order, starting from 2.

You can use this table for tests, or if you need to do a brute force search over prime numbers.

Reads from this table are not parallelized.

This is similar to the [`primes`](/reference/functions/table-functions/primes) table function.

You can also limit the output by predicates.

.examples
The first 10 primes.
```sql
SELECT * FROM system.primes LIMIT 10;
```

```response
  ┌─prime─┐
  │     2 │
  │     3 │
  │     5 │
  │     7 │
  │    11 │
  │    13 │
  │    17 │
  │    19 │
  │    23 │
  │    29 │
  └───────┘
```

The first prime greater than 1e15.
```sql
SELECT prime FROM system.primes WHERE prime > 1e15 LIMIT 1;
```

```response
  ┌────────────prime─┐
  │ 1000000000000037 │ -- 1.00 quadrillion
  └──────────────────┘
```
)DOCS_MD", "prime");
    attachNoDescription<StorageSystemZeros>(context, system_database, "zeros", R"DOCS_MD(
.description
Produces unlimited number of non-materialized zeros.
)DOCS_MD", false);
    attachNoDescription<StorageSystemZeros>(context, system_database, "zeros_mt", R"DOCS_MD(
.description
Multithreaded version of system.zeros.
)DOCS_MD", true);
    attach<StorageSystemDatabases>(context, system_database, "databases", R"DOCS_MD(
.description
Contains information about the databases that are available to the current user.

.columns_notes
The `name` column from this system table is used for implementing the `SHOW DATABASES` query.

.examples
Create a database.

```sql title="Query"
CREATE DATABASE test;
```

Check all of the available databases to the user.

```sql title="Query"
SELECT * FROM system.databases;
```

```text title="Response"
┌─name────────────────┬─engine─────┬─data_path────────────────────┬─metadata_path─────────────────────────────────────────────────────────┬─uuid─────────────────────────────────┬─engine_full────────────────────────────────────────────┬─comment─┐
│ INFORMATION_SCHEMA  │ Memory     │ /data/clickhouse_data/       │                                                                       │ 00000000-0000-0000-0000-000000000000 │ Memory                                                 │         │
│ default             │ Atomic     │ /data/clickhouse_data/store/ │ /data/clickhouse_data/store/f97/f97a3ceb-2e8a-4912-a043-c536e826a4d4/ │ f97a3ceb-2e8a-4912-a043-c536e826a4d4 │ Atomic                                                 │         │
│ information_schema  │ Memory     │ /data/clickhouse_data/       │                                                                       │ 00000000-0000-0000-0000-000000000000 │ Memory                                                 │         │
│ replicated_database │ Replicated │ /data/clickhouse_data/store/ │ /data/clickhouse_data/store/da8/da85bb71-102b-4f69-9aad-f8d6c403905e/ │ da85bb71-102b-4f69-9aad-f8d6c403905e │ Replicated('some/path/database', 'shard1', 'replica1') │         │
│ system              │ Atomic     │ /data/clickhouse_data/store/ │ /data/clickhouse_data/store/b57/b5770419-ac7a-4b67-8229-524122024076/ │ b5770419-ac7a-4b67-8229-524122024076 │ Atomic                                                 │         │
│ test                │ Atomic     │ /data/clickhouse_data/store/ │ /data/clickhouse_data/store/2a1/2a1b3c4d-5e6f-7890-abcd-ef1234567890/ │ 2a1b3c4d-5e6f-7890-abcd-ef1234567890 │ Atomic                                                 │         │
└─────────────────────┴────────────┴──────────────────────────────┴───────────────────────────────────────────────────────────────────────┴──────────────────────────────────────┴────────────────────────────────────────────────────────┴─────────┘
```
)DOCS_MD");
    attachNoDescription<StorageSystemTables>(context, system_database, "tables", R"DOCS_MD(
.description
Contains metadata of each table that the server knows about.

[Detached](/reference/statements/detach) tables are not shown in `system.tables`.

[Temporary tables](/reference/statements/create/table/temporary-table) are visible in the `system.tables` only in those session where they have been created. They are shown with the empty `database` field and with the `is_temporary` flag switched on.

.examples
```sql
SELECT * FROM system.tables LIMIT 2 FORMAT Vertical;
```

```text
Row 1:
──────
database:                   base
name:                       t1
uuid:                       81b1c20a-b7c6-4116-a2ce-7583fb6b6736
engine:                     MergeTree
is_temporary:               0
data_paths:                 ['/var/lib/clickhouse/store/81b/81b1c20a-b7c6-4116-a2ce-7583fb6b6736/']
metadata_path:              /var/lib/clickhouse/store/461/461cf698-fd0b-406d-8c01-5d8fd5748a91/t1.sql
metadata_modification_time: 2021-01-25 19:14:32
dependencies_database:      []
dependencies_table:         []
create_table_query:         CREATE TABLE base.t1 (`n` UInt64) ENGINE = MergeTree ORDER BY n
engine_full:                MergeTree ORDER BY n
as_select:                  SELECT database AS table_catalog
partition_key:
sorting_key:                n
primary_key:                n
sampling_key:
skipping_indices_types:     []
storage_policy:             default
total_rows:                 1
total_bytes:                99
lifetime_rows:              ᴺᵁᴸᴸ
lifetime_bytes:             ᴺᵁᴸᴸ
comment:
has_own_data:               0
loading_dependencies_database: []
loading_dependencies_table:    []
loading_dependent_database:    []
loading_dependent_table:       []

Row 2:
──────
database:                   default
name:                       53r93yleapyears
uuid:                       00000000-0000-0000-0000-000000000000
engine:                     MergeTree
is_temporary:               0
data_paths:                 ['/var/lib/clickhouse/data/default/53r93yleapyears/']
metadata_path:              /var/lib/clickhouse/metadata/default/53r93yleapyears.sql
metadata_modification_time: 2020-09-23 09:05:36
dependencies_database:      []
dependencies_table:         []
create_table_query:         CREATE TABLE default.`53r93yleapyears` (`id` Int8, `febdays` Int8) ENGINE = MergeTree ORDER BY id
engine_full:                MergeTree ORDER BY id
as_select:                  SELECT name AS catalog_name
partition_key:
sorting_key:                id
primary_key:                id
sampling_key:
skipping_indices_types:     []
storage_policy:             default
total_rows:                 2
total_bytes:                155
lifetime_rows:              ᴺᵁᴸᴸ
lifetime_bytes:             ᴺᵁᴸᴸ
comment:
has_own_data:               0
loading_dependencies_database: []
loading_dependencies_table:    []
loading_dependent_database:    []
loading_dependent_table:       []
```
)DOCS_MD");
    attachNoDescription<StorageSystemDetachedTables>(context, system_database, "detached_tables", R"DOCS_MD(
.description
Contains information about each detached table.

.examples
```sql
SELECT * FROM system.detached_tables FORMAT Vertical;
```

```text
Row 1:
──────
database:                   base
table:                      t1
uuid:                       81b1c20a-b7c6-4116-a2ce-7583fb6b6736
metadata_path:              /var/lib/clickhouse/store/461/461cf698-fd0b-406d-8c01-5d8fd5748a91/t1.sql
is_permanently:             1
```
)DOCS_MD");
    attachNoDescription<StorageSystemColumns>(context, system_database, "columns", R"DOCS_MD(
.description
Contains information about columns in all tables.

You can use this table to get information similar to the [DESCRIBE TABLE](/reference/statements/describe-table) query, but for multiple tables at once.

Columns from [temporary tables](/reference/statements/create/table/temporary-table) are visible in the `system.columns` only in those session where they have been created. They are shown with the empty `database` field.

.examples
```sql
SELECT * FROM system.columns LIMIT 2 FORMAT Vertical;
```

```text
Row 1:
──────
database:                INFORMATION_SCHEMA
table:                   COLUMNS
name:                    table_catalog
type:                    String
position:                1
default_kind:
default_expression:
data_compressed_bytes:   0
data_uncompressed_bytes: 0
marks_bytes:             0
comment:
is_in_partition_key:     0
is_in_sorting_key:       0
is_in_primary_key:       0
is_in_sampling_key:      0
compression_codec:
character_octet_length:  ᴺᵁᴸᴸ
numeric_precision:       ᴺᵁᴸᴸ
numeric_precision_radix: ᴺᵁᴸᴸ
numeric_scale:           ᴺᵁᴸᴸ
datetime_precision:      ᴺᵁᴸᴸ

Row 2:
──────
database:                INFORMATION_SCHEMA
table:                   COLUMNS
name:                    table_schema
type:                    String
position:                2
default_kind:
default_expression:
data_compressed_bytes:   0
data_uncompressed_bytes: 0
marks_bytes:             0
comment:
is_in_partition_key:     0
is_in_sorting_key:       0
is_in_primary_key:       0
is_in_sampling_key:      0
compression_codec:
character_octet_length:  ᴺᵁᴸᴸ
numeric_precision:       ᴺᵁᴸᴸ
numeric_precision_radix: ᴺᵁᴸᴸ
numeric_scale:           ᴺᵁᴸᴸ
datetime_precision:      ᴺᵁᴸᴸ
```
)DOCS_MD");
    attach<StorageSystemFunctions>(context, system_database, "functions", R"DOCS_MD(
.description
Contains information about normal and aggregate functions.

.examples
```sql title="Query"
 SELECT name, is_aggregate, deterministic, case_insensitive, alias_to FROM system.functions LIMIT 5;
```

```text title="Response"
┌─name─────────────────────┬─is_aggregate─┬─deterministic─┬─case_insensitive─┬─alias_to─┐
│ BLAKE3                   │            0 │                1 │                0 │          │
│ sipHash128Reference      │            0 │                1 │                0 │          │
│ mapExtractKeyLike        │            0 │                1 │                0 │          │
│ sipHash128ReferenceKeyed │            0 │                1 │                0 │          │
│ mapPartialSort           │            0 │                1 │                0 │          │
└──────────────────────────┴──────────────┴──────────────────┴──────────────────┴──────────┘

5 rows in set. Elapsed: 0.002 sec.
```
)DOCS_MD");
    attach<StorageSystemUserDefinedFunctions>(context, system_database, "user_defined_functions", R"DOCS_MD(
.description
Contains loading status, error information, and configuration metadata for [User-Defined Functions (UDFs)](/reference/functions/regular-functions/udf).

.examples
View all UDFs and their loading status:

```sql
SELECT
    name,
    load_status,
    type,
    command,
    return_type,
    argument_types
FROM system.user_defined_functions
FORMAT Vertical;
```

```response
Row 1:
──────
name:           my_sum_udf
load_status:    Success
type:           executable
command:        /var/lib/clickhouse/user_scripts/sum.py
return_type:    UInt64
argument_types: ['UInt64','UInt64']
```

Find failed UDFs:

```sql
SELECT
    name,
    loading_error_message
FROM system.user_defined_functions
WHERE load_status = 'Failed';
```

.see_also
-   [User-Defined Functions](/reference/functions/regular-functions/udf) — How to create and configure UDFs.
)DOCS_MD");
    attach<StorageSystemEvents>(context, system_database, "events", R"DOCS_MD(
.description
Contains information about the number of events that have occurred in the system. For example, in the table, you can find how many `SELECT` queries were processed since the ClickHouse server started.

## Event descriptions {#event-descriptions}

{/*AUTOGENERATED_DESCRIPTIONS_START*/}
{{PROFILE_EVENTS}}
{/*AUTOGENERATED_DESCRIPTIONS_END*/}

.examples
```sql
SELECT * FROM system.events LIMIT 5
```

```text
┌─event─────────────────────────────────┬─value─┬─description────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Query                                 │    12 │ Number of queries to be interpreted and potentially executed. Does not include queries that failed to parse or were rejected due to AST size limits, quota limits or limits on the number of simultaneously running queries. May include internal queries initiated by ClickHouse itself. Does not count subqueries.                  │
│ SelectQuery                           │     8 │ Same as Query, but only for SELECT queries.                                                                                                                                                                                                                │
│ FileOpen                              │    73 │ Number of files opened.                                                                                                                                                                                                                                    │
│ ReadBufferFromFileDescriptorRead      │   155 │ Number of reads (read/pread) from a file descriptor. Does not include sockets.                                                                                                                                                                             │
│ ReadBufferFromFileDescriptorReadBytes │  9931 │ Number of bytes read from file descriptors. If the file is compressed, this will show the compressed data size.                                                                                                                                              │
└───────────────────────────────────────┴───────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

.see_also
- [system.asynchronous_metrics](/reference/system-tables/asynchronous_metrics) — Contains periodically calculated metrics.
- [system.metrics](/reference/system-tables/metrics) — Contains instantly calculated metrics.
- [system.metric_log](/reference/system-tables/metric_log) — Contains a history of metrics values from tables `system.metrics` and `system.events`.
- [Monitoring](/guides/oss/deployment-and-scaling/monitoring/monitoring) — Base concepts of ClickHouse monitoring.
)DOCS_MD");
    attach<StorageSystemSettings>(context, system_database, "settings", R"DOCS_MD(
.description
Contains information about session settings for current user.

.examples
The following example shows how to get information about settings which name contains `min_i`.

```sql
SELECT *
FROM system.settings
WHERE name LIKE '%min_insert_block_size_%'
FORMAT Vertical
```

```text
Row 1:
──────
name:        min_insert_block_size_rows
value:       1048449
changed:     0
description: Sets the minimum number of rows in the block that can be inserted into a table by an `INSERT` query. Smaller-sized blocks are squashed into bigger ones.

Possible values:

- Positive integer.
- 0 — Squashing disabled.
min:         ᴺᵁᴸᴸ
max:         ᴺᵁᴸᴸ
readonly:    0
type:        UInt64
default:     1048449
alias_for:
is_obsolete: 0
tier:        Production

Row 2:
──────
name:        min_insert_block_size_bytes
value:       268402944
changed:     0
description: Sets the minimum number of bytes in the block which can be inserted into a table by an `INSERT` query. Smaller-sized blocks are squashed into bigger ones.

Possible values:

- Positive integer.
- 0 — Squashing disabled.
min:         ᴺᵁᴸᴸ
max:         ᴺᵁᴸᴸ
readonly:    0
type:        UInt64
default:     268402944
alias_for:
is_obsolete: 0
tier:        Production

Row 3:
──────
name:        min_insert_block_size_rows_for_materialized_views
value:       0
changed:     0
description: Sets the minimum number of rows in the block which can be inserted into a table by an `INSERT` query. Smaller-sized blocks are squashed into bigger ones. This setting is applied only for blocks inserted into [materialized view](/reference/statements/create/view). By adjusting this setting, you control blocks squashing while pushing to materialized view and avoid excessive memory usage.

Possible values:

- Any positive integer.
- 0 — Squashing disabled.

## See Also {#see-also}

- [min_insert_block_size_rows](/reference/settings/session-settings/min-insert#min_insert_block_size_rows)
min:         ᴺᵁᴸᴸ
max:         ᴺᵁᴸᴸ
readonly:    0
type:        UInt64
default:     0
alias_for:
is_obsolete: 0
tier:        Production

Row 4:
──────
name:        min_insert_block_size_bytes_for_materialized_views
value:       0
changed:     0
description: Sets the minimum number of bytes in the block which can be inserted into a table by an `INSERT` query. Smaller-sized blocks are squashed into bigger ones. This setting is applied only for blocks inserted into [materialized view](/reference/statements/create/view). By adjusting this setting, you control blocks squashing while pushing to materialized view and avoid excessive memory usage.

Possible values:

- Any positive integer.
- 0 — Squashing disabled.

## See Also {#see-also}

- [min_insert_block_size_bytes](/reference/settings/session-settings/min-insert#min_insert_block_size_bytes)
min:         ᴺᵁᴸᴸ
max:         ᴺᵁᴸᴸ
readonly:    0
type:        UInt64
default:     0
alias_for:
is_obsolete: 0
tier:        Production
 ```

Using of `WHERE changed` can be useful, for example, when you want to check:

- Whether settings in configuration files are loaded correctly and are in use.
- Settings that changed in the current session.

```sql
SELECT * FROM system.settings WHERE changed AND name='load_balancing'
```

.see_also
- [Settings](/reference/system-tables/overview#system-tables-introduction)
- [Permissions for Queries](/concepts/features/configuration/settings/permissions-for-queries)
- [Constraints on Settings](/concepts/features/configuration/settings/constraints-on-settings)
- [SHOW SETTINGS](/reference/statements/show#show-settings) statement
)DOCS_MD");
    attach<StorageSystemServerSettings>(context, system_database, "server_settings", R"DOCS_MD(
.description
Contains information about global settings for the server, which are specified in `config.xml`.
The table also includes supported nested settings with a fixed structure; dynamic sections such as lists are not included.

.examples
The following example shows how to get information about server settings which name contains `thread_pool`.

```sql
SELECT *
FROM system.server_settings
WHERE name LIKE '%thread_pool%'
```

```text
┌─name──────────────────────────────────────────┬─value─┬─default─┬─changed─┬─description─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─type───┬─changeable_without_restart─┬─is_obsolete─┐
│ max_thread_pool_size                          │ 10000 │ 10000   │       0 │ The maximum number of threads that could be allocated from the OS and used for query execution and background operations.                           │ UInt64 │                         No │           0 │
│ max_thread_pool_free_size                     │ 1000  │ 1000    │       0 │ The maximum number of threads that will always stay in a global thread pool once allocated and remain idle in case of insufficient number of tasks. │ UInt64 │                         No │           0 │
│ thread_pool_queue_size                        │ 10000 │ 10000   │       0 │ The maximum number of tasks that will be placed in a queue and wait for execution.                                                                  │ UInt64 │                         No │           0 │
│ max_io_thread_pool_size                       │ 100   │ 100     │       0 │ The maximum number of threads that would be used for IO operations                                                                                  │ UInt64 │                         No │           0 │
│ max_io_thread_pool_free_size                  │ 0     │ 0       │       0 │ Max free size for IO thread pool.                                                                                                                   │ UInt64 │                         No │           0 │
│ io_thread_pool_queue_size                     │ 10000 │ 10000   │       0 │ Queue size for IO thread pool.                                                                                                                      │ UInt64 │                         No │           0 │
│ max_active_parts_loading_thread_pool_size     │ 64    │ 64      │       0 │ The number of threads to load active set of data parts (Active ones) at startup.                                                                    │ UInt64 │                         No │           0 │
│ max_outdated_parts_loading_thread_pool_size   │ 32    │ 32      │       0 │ The number of threads to load inactive set of data parts (Outdated ones) at startup.                                                                │ UInt64 │                         No │           0 │
│ max_unexpected_parts_loading_thread_pool_size │ 32    │ 32      │       0 │ The number of threads to load inactive set of data parts (Unexpected ones) at startup.                                                              │ UInt64 │                         No │           0 │
│ max_parts_cleaning_thread_pool_size           │ 128   │ 128     │       0 │ The number of threads for concurrent removal of inactive data parts.                                                                                │ UInt64 │                         No │           0 │
│ max_backups_io_thread_pool_size               │ 1000  │ 1000    │       0 │ The maximum number of threads that would be used for IO operations for BACKUP queries                                                               │ UInt64 │                         No │           0 │
│ max_backups_io_thread_pool_free_size          │ 0     │ 0       │       0 │ Max free size for backups IO thread pool.                                                                                                           │ UInt64 │                         No │           0 │
│ backups_io_thread_pool_queue_size             │ 0     │ 0       │       0 │ Queue size for backups IO thread pool.                                                                                                              │ UInt64 │                         No │           0 │
└───────────────────────────────────────────────┴───────┴─────────┴─────────┴─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴────────┴────────────────────────────┴─────────────┘

```

Using of `WHERE changed` can be useful, for example, when you want to check
whether settings in configuration files are loaded correctly and are in use.

{/* */}

```sql
SELECT * FROM system.server_settings WHERE changed AND name='max_thread_pool_size'
```

.see_also
- [Settings](/reference/system-tables/settings)
- [Configuration Files](/concepts/features/configuration/server-config/configuration-files)
- [Server Settings](/reference/settings/server-settings/settings)
)DOCS_MD");
    attach<StorageSystemSettingsChanges>(context, system_database, "settings_changes", R"DOCS_MD(
.description
Contains information about setting changes in previous ClickHouse versions.

.examples
```sql
SELECT *
FROM system.settings_changes
WHERE version = '23.5'
FORMAT Vertical
```

```text
Row 1:
──────
type:    Core
version: 23.5
changes: [('input_format_parquet_preserve_order','1','0','Allow Parquet reader to reorder rows for better parallelism.'),('parallelize_output_from_storages','0','1','Allow parallelism when executing queries that read from file/url/s3/etc. This may reorder rows.'),('use_with_fill_by_sorting_prefix','0','1','Columns preceding WITH FILL columns in ORDER BY clause form sorting prefix. Rows with different values in sorting prefix are filled independently'),('output_format_parquet_compliant_nested_types','0','1','Change an internal field name in output Parquet file schema.')]
```

.see_also
- [Settings](/reference/system-tables/overview#system-tables-introduction)
- [system.settings](/reference/system-tables/settings)
)DOCS_MD");
    attach<SystemMergeTreeSettings<false>>(context, system_database, "merge_tree_settings", R"DOCS_MD(
.description
Contains information about settings for `MergeTree` tables.

.examples
```sql
SELECT * FROM system.merge_tree_settings LIMIT 3 FORMAT Vertical;
```

```response
SELECT *
FROM system.merge_tree_settings
LIMIT 3
FORMAT Vertical

Query id: 2580779c-776e-465f-a90c-4b7630d0bb70

Row 1:
──────
name:        min_compress_block_size
value:       0
default:     0
changed:     0
description: When granule is written, compress the data in buffer if the size of pending uncompressed data is larger or equal than the specified threshold. If this setting is not set, the corresponding global setting is used.
min:         ᴺᵁᴸᴸ
max:         ᴺᵁᴸᴸ
readonly:    0
type:        UInt64
is_obsolete: 0
tier:        Production

Row 2:
──────
name:        max_compress_block_size
value:       0
default:     0
changed:     0
description: Compress the pending uncompressed data in buffer if its size is larger or equal than the specified threshold. Block of data will be compressed even if the current granule is not finished. If this setting is not set, the corresponding global setting is used.
min:         ᴺᵁᴸᴸ
max:         ᴺᵁᴸᴸ
readonly:    0
type:        UInt64
is_obsolete: 0
tier:        Production

Row 3:
──────
name:        index_granularity
value:       8192
default:     8192
changed:     0
description: How many rows correspond to one primary key value.
min:         ᴺᵁᴸᴸ
max:         ᴺᵁᴸᴸ
readonly:    0
type:        UInt64
is_obsolete: 0
tier:        Production

3 rows in set. Elapsed: 0.001 sec.
```
)DOCS_MD");
    attach<SystemMergeTreeSettings<true>>(context, system_database, "replicated_merge_tree_settings", R"DOCS_MD(
.description
Contains a list of all ReplicatedMergeTree engine specific settings, their current and default values along with descriptions. You may change any of them in SETTINGS section in CREATE query.
)DOCS_MD");
    attach<StorageSystemBuildOptions>(context, system_database, "build_options", R"DOCS_MD(
.description
Contains information about the ClickHouse server's build options.

.examples
```sql
SELECT * FROM system.build_options LIMIT 5
```

```text
┌─name─────────────┬─value─┐
│ USE_BROTLI       │ 1     │
│ USE_BZIP2        │ 1     │
│ USE_CAPNP        │ 1     │
│ USE_CASSANDRA    │ 1     │
│ USE_DATASKETCHES │ 1     │
└──────────────────┴───────┘
```
)DOCS_MD");
    attach<StorageSystemHypotheticalIndexes>(context, system_database, "hypothetical_indexes", R"DOCS_MD(
.description
Lists every hypothetical (what-if) skip index defined in the current session. See [`CREATE HYPOTHETICAL INDEX`](/reference/statements/hypothetical-index#create-hypothetical-index) and [`EXPLAIN WHATIF`](/reference/statements/explain#explain-whatif).

The contents are session-scoped: each connection sees only its own hypothetical indexes, and the table is empty when no indexes have been created in the current session.

The current `(database, table)` are resolved by UUID at query time, so they reflect `RENAME TABLE` and entries for dropped tables are hidden automatically.

.examples
```sql
CREATE HYPOTHETICAL INDEX i1 ON t (b) TYPE bloom_filter(0.01)  GRANULARITY 1;
CREATE HYPOTHETICAL INDEX i2 ON t (b) TYPE bloom_filter(0.001) GRANULARITY 1;

SELECT database, table, name, type, type_full, expression, granularity
FROM system.hypothetical_indexes;
```

```text
┌─database─┬─table─┬─name─┬─type─────────┬─type_full───────────┬─expression─┬─granularity─┐
│ default  │ t     │ i1   │ bloom_filter │ bloom_filter(0.01)  │ b          │           1 │
│ default  │ t     │ i2   │ bloom_filter │ bloom_filter(0.001) │ b          │           1 │
└──────────┴───────┴──────┴──────────────┴─────────────────────┴────────────┴─────────────┘
```

`type` is the base type name and `type_full` includes the arguments, so users can distinguish between parametrized variants like `bloom_filter(0.01)` and `bloom_filter(0.001)`.

.see_also
- [`CREATE HYPOTHETICAL INDEX`](/reference/statements/hypothetical-index#create-hypothetical-index)
- [`EXPLAIN WHATIF`](/reference/statements/explain#explain-whatif)
)DOCS_MD");
#if USE_XRAY
    attach<StorageSystemInstrumentation>(context, system_database, "instrumentation", R"DOCS_MD(
.description
Contains the instrumentation points using LLVM's XRay feature.

<Info>
**Availability**

`system.instrumentation` is present only in ClickHouse builds compiled with LLVM XRay support (`USE_XRAY`). On builds without it, the table does not exist and queries against it will fail with `UNKNOWN_TABLE`. You can check whether your build has it enabled with:

```sql
SELECT value FROM system.build_options WHERE name = 'USE_XRAY';
```
</Info>

.examples
```sql
SELECT * FROM system.instrumentation FORMAT Vertical;
```

```text
Row 1:
──────
id:            0
function_id:   231280
function_name: QueryMetricLog::startQuery
handler:       log
entry_type:    Entry
symbol:        DB::QueryMetricLog::startQuery(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&, std::__1::chrono::time_point<std::__1::chrono::system_clock, std::__1::chrono::duration<long long, std::__1::ratio<1l, 1000000l>>>, unsigned long)
arguments:     ['test']

Row 2:
──────
id:            1
function_id:   231280
function_name: QueryMetricLog::startQuery
handler:       profile
entry_type:    EntryAndExit
symbol:        DB::QueryMetricLog::startQuery(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&, std::__1::chrono::time_point<std::__1::chrono::system_clock, std::__1::chrono::duration<long long, std::__1::ratio<1l, 1000000l>>>, unsigned long)
arguments:     []

Row 3:
──────
id:            2
function_id:   231280
function_name: QueryMetricLog::startQuery
handler:       sleep
entry_type:    Exit
symbol:        DB::QueryMetricLog::startQuery(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&, std::__1::chrono::time_point<std::__1::chrono::system_clock, std::__1::chrono::duration<long long, std::__1::ratio<1l, 1000000l>>>, unsigned long)
arguments:     [0.3]

3 rows in set. Elapsed: 0.302 sec.
```

.see_also
- [SYSTEM INSTRUMENT](/reference/statements/system#instrument) — Add or remove instrumentation points.
- [system.trace_log](/reference/system-tables/trace_log) — Inspect profiling log.
- [system.symbols](/reference/system-tables/symbols) — Inspect symbols to add instrumentation points.
)DOCS_MD");
#endif
    attach<StorageSystemFormats>(context, system_database, "formats", R"DOCS_MD(
.description
Contains a list of all the formats along with flags whether a format is suitable for input/output or whether it supports parallelization.
)DOCS_MD");
    attach<StorageSystemTableFunctions>(context, system_database, "table_functions", R"DOCS_MD(
.description
Contains a list of all available table functions with their descriptions.
)DOCS_MD");
    attach<StorageSystemAggregateFunctionCombinators>(context, system_database, "aggregate_function_combinators", R"DOCS_MD(
.description
Contains a list of all available aggregate function combinators, which could be applied to aggregate functions and change the way they work.
)DOCS_MD");
    attach<StorageSystemDataTypeFamilies>(context, system_database, "data_type_families", R"DOCS_MD(
.description
Contains information about supported [data types](/reference/data-types/index).

.examples
```sql
SELECT name, case_insensitive, alias_to FROM system.data_type_families WHERE alias_to = 'String'
```

```text
┌─name───────┬─case_insensitive─┬─alias_to─┐
│ LONGBLOB   │                1 │ String   │
│ LONGTEXT   │                1 │ String   │
│ TINYTEXT   │                1 │ String   │
│ TEXT       │                1 │ String   │
│ VARCHAR    │                1 │ String   │
│ MEDIUMBLOB │                1 │ String   │
│ BLOB       │                1 │ String   │
│ TINYBLOB   │                1 │ String   │
│ CHAR       │                1 │ String   │
│ MEDIUMTEXT │                1 │ String   │
└────────────┴──────────────────┴──────────┘
```

.see_also
- [Syntax](/reference/syntax) — Information about supported syntax.
)DOCS_MD");
    attach<StorageSystemDictionaryLayouts>(context, system_database, "dictionary_layouts", R"DOCS_MD(
.description
Contains the list of dictionary layouts supported by the server, along with embedded documentation for each layout. A dictionary layout determines how a dictionary is stored in memory (or on disk) and how it is looked up; it is specified in the `LAYOUT` clause of a `CREATE DICTIONARY` query.

.examples
```sql title="Query"
SELECT name, is_complex, syntax
FROM system.dictionary_layouts
WHERE name IN ('flat', 'hashed', 'complex_key_hashed')
ORDER BY name
```

```text title="Response"
┌─name───────────────┬─is_complex─┬─syntax───────────────────────────────────────────────────┐
│ complex_key_hashed │          1 │ LAYOUT(COMPLEX_KEY_HASHED())                             │
│ flat               │          0 │ LAYOUT(FLAT([INITIAL_ARRAY_SIZE n] [MAX_ARRAY_SIZE n]))  │
│ hashed             │          0 │ LAYOUT(HASHED())                                         │
└────────────────────┴────────────┴──────────────────────────────────────────────────────────┘
```

.see_also
- [Dictionary layouts](/reference/statements/create/dictionary/layouts/overview) — Information about dictionaries and their layouts.
)DOCS_MD");
    attach<StorageSystemDiskTypes>(context, system_database, "disk_types", R"DOCS_MD(
.description
Contains the list of disk types supported by the server, along with embedded documentation for each type. A disk type is specified in the `type` of a disk configuration and determines where and how a disk stores its data (local filesystem, object storage, a cache over another disk, and so on).

Note that this table lists the available disk *types*, whereas [`system.disks`](/reference/system-tables/disks) lists the disk instances configured on the server.

## Configuration examples {#configuration-examples}

A disk can be configured in two ways: **statically**, in the server configuration files (XML or YAML), or **dynamically**, in the settings of a `CREATE`/`ATTACH` query using the `disk` function. The same disk type and parameters are accepted in both cases.

### Static configuration {#static-configuration}

Disks are defined under `storage_configuration` in the server configuration. The following example defines an `s3` disk and a storage policy that uses it.

```xml title="config.xml"
<clickhouse>
    <storage_configuration>
        <disks>
            <s3_disk>
                <type>s3</type>
                <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
                <use_environment_credentials>1</use_environment_credentials>
            </s3_disk>
        </disks>
        <policies>
            <s3_policy>
                <volumes>
                    <main>
                        <disk>s3_disk</disk>
                    </main>
                </volumes>
            </s3_policy>
        </policies>
    </storage_configuration>
</clickhouse>
```

The same configuration in YAML:

```yaml title="config.yaml"
storage_configuration:
  disks:
    s3_disk:
      type: s3
      endpoint: https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/
      use_environment_credentials: 1
  policies:
    s3_policy:
      volumes:
        main:
          disk: s3_disk
```

A table can then use the disk through its storage policy:

```sql title="Query"
CREATE TABLE test (a Int32, b String)
ENGINE = MergeTree() ORDER BY a
SETTINGS storage_policy = 's3_policy';
```

### Dynamic configuration {#dynamic-configuration}

A disk can also be defined directly in the settings of a `CREATE`/`ATTACH` query, without a predefined disk in the configuration files, using the `disk` function:

```sql title="Query"
CREATE TABLE test (a Int32, b String)
ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(
    type = s3,
    endpoint = 'https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/',
    use_environment_credentials = 1
);
```

See [Configuring external storage](/concepts/features/configuration/server-config/storing-data) for the full list of parameters of each disk type.

.examples
```sql title="Query"
SELECT name, description
FROM system.disk_types
WHERE name IN ('local', 'object_storage')
ORDER BY name
```

.see_also
- [`system.disks`](/reference/system-tables/disks) — The disk instances configured on the server.
- [`system.storage_policies`](/reference/system-tables/storage_policies) — Storage policies and volumes.
)DOCS_MD");
    attach<StorageSystemDictionarySources>(context, system_database, "dictionary_sources", R"DOCS_MD(
.description
Contains the list of dictionary sources supported by the server, along with embedded documentation for each source. A dictionary source determines where the dictionary data is loaded from; it is specified in the `SOURCE` clause of a `CREATE DICTIONARY` query.

.examples
```sql title="Query"
SELECT name, syntax
FROM system.dictionary_sources
WHERE name IN ('clickhouse', 'file')
ORDER BY name
```

```text title="Response"
┌─name───────┬─syntax─────────────────────────────────────────────────────────────────────────┐
│ clickhouse │ SOURCE(CLICKHOUSE(host 'host' port 9000 user 'default' password '' db 'db' table 'table')) │
│ file       │ SOURCE(FILE(path '/path/to/file' format 'CSV'))                                  │
└────────────┴────────────────────────────────────────────────────────────────────────────────┘
```

.see_also
- [Dictionary sources](/reference/statements/create/dictionary/sources/overview) — Information about dictionaries and their sources.
)DOCS_MD");
    attach<StorageSystemDataSkippingIndexTypes>(context, system_database, "data_skipping_index_types", R"DOCS_MD(
.description
Contains the list of data skipping index types supported by the server, along with embedded documentation for each type. A data skipping index type is specified in the `TYPE` of an `INDEX` declaration in a `CREATE TABLE` query and lets ClickHouse skip granules that cannot match a query's condition.

Note that this table lists the available index *types*, whereas [`system.data_skipping_indices`](/reference/system-tables/data_skipping_indices) lists the index instances defined on existing tables.

.examples
```sql title="Query"
SELECT name, syntax
FROM system.data_skipping_index_types
WHERE name IN ('minmax', 'set')
ORDER BY name
```

```text title="Response"
┌─name───┬─syntax───────────────────────────────────────┐
│ minmax │ INDEX name expr TYPE minmax GRANULARITY n     │
│ set    │ INDEX name expr TYPE set(max_rows) GRANULARITY n │
└────────┴──────────────────────────────────────────────┘
```

.see_also
- [Data skipping indices](/concepts/features/performance/skip-indexes/skipping-indexes) — Information about data skipping indexes.
- [`system.data_skipping_indices`](/reference/system-tables/data_skipping_indices) — The index instances defined on existing tables.
)DOCS_MD");
    attach<StorageSystemDocumentation>(context, system_database, "documentation", R"DOCS_MD(
.description
Collects the embedded documentation of the uniform components of the system into a single table. Every row corresponds to one entity (a function, a table engine, a data type, and so on) and contains its embedded reference documentation rendered as Markdown. This content backs generated website documentation and the per-kind `system.*` tables; website pages may add MDX-only preambles or guidance outside their generated bodies.

The `description` is assembled from the structured parts of the embedded documentation (`description`, `syntax`, arguments, examples, and so on), so a single column holds the complete embedded documentation of an entity. When the embedded documentation carries the entire reference page in its `description` (as is the case for the components whose website pages are autogenerated from it: table engines, database engines, data types, formats, table functions), the page is published as-is, without appending sections composed from the structured metadata fields — the page body already covers that material. Aliases are rendered as a short reference to the canonical entity, e.g. ``Alias of `trunc`.``

This table, in a certain way, collects the information available in the per-kind documentation tables ([`system.functions`](/reference/system-tables/functions), [`system.table_engines`](/reference/system-tables/table_engines), [`system.data_type_families`](/reference/system-tables/data_type_families), and others). It is meant, in particular, to back an interactive `help` command in the client, but is useful on its own.

The following kinds of entities are collected (the value of the `type` column is shown in parentheses):

- Functions (`Function`)
- Aggregate functions (`Aggregate Function`)
- Table functions (`Table Function`)
- Table engines (`Table Engine`)
- Database engines (`Database Engine`)
- Data types (`Data Type`)
- Dictionary layouts (`Dictionary Layout`)
- Dictionary sources (`Dictionary Source`)
- Aggregate function combinators (`Aggregate Function Combinator`)
- Data skipping index types (`Data Skipping Index`)
- Disk types (`Disk Type`)
- Settings (`Setting`)
- MergeTree settings (`MergeTree Setting`)
- Server settings (`Server Setting`)
- Formats (`Format`)
- Compression codecs (`Compression Codec`)
- Profile events (`Profile Event`)
- Current metrics (`Current Metric`)
- Asynchronous metrics (`Asynchronous Metric`)
- System tables (`System Table`)
- SQL statements (`Statement`)

For settings (of any kind), the documentation is the setting's description, together with its type and default value; obsolete settings are not exposed. It also carries the history of the changes of the setting's default value across ClickHouse versions: the version in which the setting was introduced and every later change of its default, with the previous value, the new value and the reason for the change. This is the same data that backs the `compatibility` setting and [`system.settings_changes`](/reference/system-tables/settings_changes), so it covers the changes recorded since that mechanism was introduced: an older setting whose default never changed has no history, and neither do server settings, which `compatibility` does not cover. A change recorded under an alias of a setting belongs to the history of that setting, the same way `compatibility` applies it, so the history of a setting that was renamed is not cut at the rename; the exception is a record written under an alias for the sole purpose of registering that alias, which is the history of the alias alone. An alias carries the history of its own name: every record written under it, plus the record that registered it as an alias, which the history file sometimes writes under another name of the same setting.

For system tables, the description, examples, and related material are stored in the table metadata comment using lightweight section markers. The complete generated page body is assembled from that comment and the live column schema. Event and metric catalogs are rendered from their registries, so generated details stay synchronized with the running binary.

For SQL statements, the documentation is the same as the one exposed by `system.statements`: it also names the enclosing statement, if any, e.g. the `WHERE` clause is a part of `SELECT`.

The `source` column holds a source path for the entity, relative to the repository root. For most entities it is captured automatically at the place where the documentation object is constructed (the registration site of the component); for system tables it is the table storage implementation, and for kinds documented in a single source file each (such as settings, profile events and current metrics), it is that file.

.examples
Read the documentation of a particular entity:

```sql title="Query"
SELECT description
FROM system.documentation
WHERE type = 'Table Engine' AND name = 'MergeTree'
FORMAT TSVRaw;
```

The same name can refer to several kinds of entities (for example, there is both a `file` table function and a `file` dictionary source), so it is convenient to look a name up across all kinds:

```sql title="Query"
SELECT type, name
FROM system.documentation
WHERE name = 'file'
ORDER BY type;
```

Count the documented entities of each kind:

```sql title="Query"
SELECT type, count()
FROM system.documentation
GROUP BY type
ORDER BY count() DESC;
```

Find out in which version a setting was introduced and how its default value changed since:

```sql title="Query"
SELECT description
FROM system.documentation
WHERE type = 'Setting' AND name = 'async_insert_max_data_size'
FORMAT TSVRaw;
```

.see_also
- [`system.functions`](/reference/system-tables/functions) — Regular and aggregate functions.
- [`system.table_functions`](/reference/system-tables/table_functions) — Table functions.
- [`system.table_engines`](/reference/system-tables/table_engines) — Table engines.
- [`system.database_engines`](/reference/system-tables/database_engines) — Database engines.
- [`system.data_type_families`](/reference/system-tables/data_type_families) — Data types.
- [`system.disk_types`](/reference/system-tables/disk_types) — Disk types.
- [`system.settings`](/reference/system-tables/settings) — Settings.
- [`system.merge_tree_settings`](/reference/system-tables/merge_tree_settings) — MergeTree settings.
- [`system.server_settings`](/reference/system-tables/server_settings) — Server settings.
- [`system.settings_changes`](/reference/system-tables/settings_changes) — The history of the changes of the default values of settings.
- [`system.formats`](/reference/system-tables/formats) — Formats.
)DOCS_MD");
    attach<StorageSystemCollations>(context, system_database, "collations", R"DOCS_MD(
.description
Contains a list of all available collations for alphabetical comparison of strings.
)DOCS_MD");
    attach<StorageSystemDatabaseEngines>(context, system_database, "database_engines", R"DOCS_MD(
.description
Contains the list of database engines supported by the server.

.examples
```sql title="Query"
SELECT name
FROM system.database_engines
WHERE name IN ('Atomic', 'Ordinary')
ORDER BY name
```

```text title="Response"
┌─name─────┐
│ Atomic   │
│ Ordinary │
└──────────┘
```
)DOCS_MD");
    attach<StorageSystemStatements>(context, system_database, "statements", R"DOCS_MD(
.description
Contains a list of all SQL statements of ClickHouse.
)DOCS_MD");
    attach<StorageSystemTableEngines>(context, system_database, "table_engines", R"DOCS_MD(
.description
Contains description of table engines supported by server and their feature support information.

This table contains the following columns (the column type is shown in brackets):

.examples
```sql title="Query"
SELECT
    name,
    supports_settings,
    supports_skipping_indices,
    supports_sort_order,
    supports_ttl,
    supports_replication,
    supports_deduplication,
    supports_parallel_insert
FROM system.table_engines
WHERE name IN ('Kafka', 'MergeTree', 'ReplicatedCollapsingMergeTree')
```

```text title="Response"
┌─name──────────────────────────┬─supports_settings─┬─supports_skipping_indices─┬─supports_sort_order─┬─supports_ttl─┬─supports_replication─┬─supports_deduplication─┬─supports_parallel_insert─┐
│ MergeTree                     │                 1 │                         1 │                   1 │            1 │                    0 │                      0 │                        1 │
│ Kafka                         │                 1 │                         0 │                   0 │            0 │                    0 │                      0 │                        0 │
│ ReplicatedCollapsingMergeTree │                 1 │                         1 │                   1 │            1 │                    1 │                      1 │                        1 │
└───────────────────────────────┴───────────────────┴───────────────────────────┴─────────────────────┴──────────────┴──────────────────────┴────────────────────────┴──────────────────────────┘
```

.see_also
- MergeTree family [query clauses](/reference/engines/table-engines/mergetree-family/mergetree#mergetree-query-clauses)
- Kafka [settings](/reference/engines/table-engines/integrations/kafka#creating-a-table)
- Join [settings](/reference/engines/table-engines/special/join#join-limitations-and-settings)
)DOCS_MD");
    attach<StorageSystemContributors>(context, system_database, "contributors", R"DOCS_MD(
.description
Contains information about contributors. The order is random at query execution time.

.examples
```sql
SELECT * FROM system.contributors LIMIT 10
```

```text
┌─name─────────────┐
│ Olga Khvostikova │
│ Max Vetrov       │
│ LiuYangkuan      │
│ svladykin        │
│ zamulla          │
│ Šimon Podlipský  │
│ BayoNet          │
│ Ilya Khomutov    │
│ Amy Krishnevsky  │
│ Loud_Scream      │
└──────────────────┘
```

To find out yourself in the table, use a query:

```sql
SELECT * FROM system.contributors WHERE name = 'Olga Khvostikova'
```

```text
┌─name─────────────┐
│ Olga Khvostikova │
└──────────────────┘
```
)DOCS_MD");
    attach<StorageSystemUsers>(context, system_database, "users", R"DOCS_MD(
.description
Contains a list of [user accounts](/concepts/features/security/access-rights#user-account-management) configured on the server.

.see_also
- [SHOW USERS](/reference/statements/show#show-users)
)DOCS_MD");
    attach<StorageSystemRoles>(context, system_database, "roles", R"DOCS_MD(
.description
Contains information about configured [roles](/concepts/features/security/access-rights#role-management).

.see_also
- [SHOW ROLES](/reference/statements/show#show-roles)
)DOCS_MD");
    attach<StorageSystemGrants>(context, system_database, "grants", R"DOCS_MD(
.description
Privileges granted to ClickHouse user accounts.
)DOCS_MD");
    attach<StorageSystemRoleGrants>(context, system_database, "role_grants", R"DOCS_MD(
.description
Contains the role grants for users and roles. To add entries to this table, use `GRANT role TO user`.
)DOCS_MD");
    attach<StorageSystemCurrentRoles>(context, system_database, "current_roles", R"DOCS_MD(
.description
Contains active roles of a current user. `SET ROLE` changes the contents of this table.
)DOCS_MD");
    attach<StorageSystemEnabledRoles>(context, system_database, "enabled_roles", R"DOCS_MD(
.description
Contains all active roles at the moment, including the current role of the current user and granted roles for the current role.
)DOCS_MD");
    attach<StorageSystemSettingsProfiles>(context, system_database, "settings_profiles", R"DOCS_MD(
.description
Contains properties of configured setting profiles.

.see_also
- [SHOW PROFILES](/reference/statements/show#show-profiles)
)DOCS_MD");
    attach<StorageSystemSettingsProfileElements>(context, system_database, "settings_profile_elements", R"DOCS_MD(
.description
Describes the content of the settings profile:

- Constraints.
- Roles and users that the setting applies to.
- Parent settings profiles.
)DOCS_MD");
    attach<StorageSystemRowPolicies>(context, system_database, "row_policies", R"DOCS_MD(
.description
Contains filters for one particular table, as well as a list of roles and/or users which should use this row policy.

.see_also
- [SHOW POLICIES](/reference/statements/show#show-policies)
)DOCS_MD");
    attach<StorageSystemMaskingPolicies>(context, system_database, "masking_policies", R"DOCS_MD(
.description
Contains information about all masking policies defined in the system.

Masking policies can only be created and applied in ClickHouse Cloud. In open-source builds the `system.masking_policies` table is always empty, but it is still present so that introspection queries such as `SHOW MASKING POLICIES` work and return an empty result instead of throwing.
)DOCS_MD");
    attach<StorageSystemQuotas>(context, system_database, "quotas", R"DOCS_MD(
.description
Contains information about [quotas](/reference/system-tables/quotas).

.see_also
- [SHOW QUOTAS](/reference/statements/show#show-quotas)
)DOCS_MD");
    attach<StorageSystemQuotaLimits>(context, system_database, "quota_limits", R"DOCS_MD(
.description
Contains information about maximums for all intervals of all quotas. Any number of rows or zero can correspond to one quota.
)DOCS_MD");
    attach<StorageSystemQuotaUsage>(context, system_database, "quota_usage", R"DOCS_MD(
.description
Quota usage by the current user: how much is used and how much is left.

.see_also
- [SHOW QUOTA](/reference/statements/show#show-quota))
)DOCS_MD");
    attach<StorageSystemQuotasUsage>(context, system_database, "quotas_usage", R"DOCS_MD(
.description
Quota usage by all users.

.see_also
- [SHOW QUOTA](/reference/statements/show#show-quota))
)DOCS_MD");
    attach<StorageSystemUserDirectories>(context, system_database, "user_directories", R"DOCS_MD(
.description
Contains the information about configured user directories - directories on the file system from which ClickHouse server is allowed to read user provided data.
)DOCS_MD");
    attach<StorageSystemPrivileges>(context, system_database, "privileges", R"DOCS_MD(
.description
Contains a list of all available privileges that could be granted to a user or role.
)DOCS_MD");
    attach<StorageSystemErrors>(context, system_database, "errors", R"DOCS_MD(
.description
Contains error codes with the number of times they have been triggered.

To show all possible error codes, including ones which were not triggered, set setting [system_events_show_zero_values](/reference/settings/session-settings/system#system_events_show_zero_values) to 1.

.columns_notes
<Note>
Counters for some errors may increase during successful query execution. It's not recommended to use this table for server monitoring purposes unless you are sure that corresponding error can not be a false positive.
</Note>

.examples
```sql title="Query"
SELECT name, code, value
FROM system.errors
WHERE value > 0
ORDER BY code ASC
LIMIT 1

┌─name─────────────┬─code─┬─value─┐
│ CANNOT_OPEN_FILE │   76 │     1 │
└──────────────────┴──────┴───────┘
```

```sql title="Response"
WITH arrayMap(x -> demangle(addressToSymbol(x)), last_error_trace) AS all
SELECT name, arrayStringConcat(all, '\n') AS res
FROM system.errors
LIMIT 1
SETTINGS allow_introspection_functions=1\G
```
)DOCS_MD");
    attach<StorageSystemWarnings>(context, system_database, "warnings", R"DOCS_MD(
.description
This table shows warnings about the ClickHouse server.
Warnings of the same type are combined into a single warning.
For example, if the number N of attached databases exceeds a configurable threshold T, a single entry containing the current value N is shown instead of N separate entries.
If current value drops below the threshold, the entry is removed from the table.

The table can be configured with these settings:

- [max_table_num_to_warn](/reference/settings/server-settings/settings/max-table#max_table_num_to_warn)
- [max_database_num_to_warn](/reference/settings/server-settings/settings/max-database#max_database_num_to_warn)
- [max_dictionary_num_to_warn](/reference/settings/server-settings/settings/max-dictionary#max_dictionary_num_to_warn)
- [max_view_num_to_warn](/reference/settings/server-settings/settings/max-view#max_view_num_to_warn)
- [max_part_num_to_warn](/reference/settings/server-settings/settings/max#max_part_num_to_warn)
- [max_pending_mutations_to_warn](/reference/settings/server-settings/settings/max-pending#max_pending_mutations_to_warn)
- [max_pending_mutations_execution_time_to_warn](/reference/settings/server-settings/settings/max-pending#max_pending_mutations_execution_time_to_warn)
- [max_named_collection_num_to_warn](/reference/settings/server-settings/settings/max-named#max_named_collection_num_to_warn)
- [resource_overload_warnings](/concepts/features/configuration/settings/server-overload#resource-overload-warnings)

.examples
```sql title="Query"
 SELECT * FROM system.warnings LIMIT 2 \G;
```

```text title="Response"
Row 1:
──────
message:               The number of active parts is more than 10.
message_format_string: The number of active parts is more than {}.

Row 2:
──────
message:               The number of attached databases is more than 2.
message_format_string: The number of attached databases is more than {}.
```
)DOCS_MD");
    attachNoDescription<StorageSystemDataSkippingIndices>(context, system_database, "data_skipping_indices", R"DOCS_MD(
.description
Contains information about existing data skipping indices in all the tables.

.examples
```sql
SELECT * FROM system.data_skipping_indices LIMIT 2 FORMAT Vertical;
```

```text
Row 1:
──────
database:                default
table:                   user_actions
name:                    clicks_idx
type:                    minmax
type_full:               minmax
expr:                    clicks
creation:                Explicit
granularity:             1
data_compressed_bytes:   58
data_uncompressed_bytes: 6
marks_bytes:             48

Row 2:
──────
database:                default
table:                   users
name:                    contacts_null_idx
type:                    minmax
type_full:               minmax
expr:                    assumeNotNull(contacts_null)
creation:                Explicit
granularity:             1
data_compressed_bytes:   58
data_uncompressed_bytes: 6
marks_bytes:             48
```
)DOCS_MD");
    attachNoDescription<StorageSystemProjections>(context, system_database, "projections", R"DOCS_MD(
.description
Contains information about existing projections in all tables.

.examples
```sql
SELECT * FROM system.projections LIMIT 2 FORMAT Vertical;
```

```text
Row 1:
──────
database:    default
table:       landing
name:        improved_sorting_key
type:        Normal
sorting_key: ['user_id','date']
query:       SELECT * ORDER BY user_id, date
settings:     {}

Row 2:
──────
database:    default
table:       landing
name:        agg_no_key
type:        Aggregate
sorting_key: []
query:       SELECT count()
settings:     {}
```
)DOCS_MD");
    attachNoDescription<StorageSystemConstraints>(context, system_database, "constraints", R"DOCS_MD(
.description
Contains information about existing constraints in all tables.

Constraints defined on [temporary tables](/reference/statements/create/table/temporary-table) are visible in `system.constraints` only in the session where they were created. They are shown with an empty `database` field.

.examples
```sql
SELECT * FROM system.constraints LIMIT 2 FORMAT Vertical;
```

```text
Row 1:
──────
database:    default
table:       hits
name:        check_hits
type:        CHECK
expression:  CounterID > 0

Row 2:
──────
database:    default
table:       hits
name:        assume_positive
type:        ASSUME
expression:  WatchID > 0
```
)DOCS_MD");
    attach<StorageSystemLicenses>(context, system_database, "licenses", R"DOCS_MD(
.description
Contains licenses of third-party libraries that are located in the [contrib](https://github.com/ClickHouse/ClickHouse/tree/master/contrib) directory of ClickHouse sources.

.examples
```sql
SELECT library_name, license_type, license_path FROM system.licenses LIMIT 15
```

```text
┌─library_name───────┬─license_type─┬─license_path────────────────────────┐
│ aws-c-common       │ Apache       │ /contrib/aws-c-common/LICENSE       │
│ boost              │ Boost        │ /contrib/boost/LICENSE_1_0.txt      │
│ brotli             │ MIT          │ /contrib/brotli/LICENSE             │
│ [...]              │ [...]        │ [...]                               │
└────────────────────┴──────────────┴─────────────────────────────────────┘
```
)DOCS_MD");
    attach<StorageSystemTimeZones>(context, system_database, "time_zones", R"DOCS_MD(
.description
Contains a list of time zones that are supported by the ClickHouse server. This list of timezones might vary depending on the version of ClickHouse.

.examples
```sql
SELECT * FROM system.time_zones LIMIT 10
```

```text
┌─time_zone──────────┐
│ Africa/Abidjan     │
│ Africa/Accra       │
│ Africa/Addis_Ababa │
│ Africa/Algiers     │
│ Africa/Asmara      │
│ Africa/Asmera      │
│ Africa/Bamako      │
│ Africa/Bangui      │
│ Africa/Banjul      │
│ Africa/Bissau      │
└────────────────────┘
```
)DOCS_MD");
    attach<StorageSystemBackups>(context, system_database, "backups", R"DOCS_MD(
.description
Contains a list of all `BACKUP` or `RESTORE` operations with their current states and other properties. Note, that table is not persistent and it shows only operations executed after the last server restart.

## Restore atomicity {#restore-atomicity}

`RESTORE` is not transactional and does not roll back on failure. For each table, all selected parts are copied before any are attached, but the attach phase itself is not transactional — parts are made visible one at a time. Tables are processed independently.

**Tables are independent.** A table whose restore completes stays in place even if another table in the same command later fails:

```sql
RESTORE TABLE db.t0, TABLE db.t1
FROM S3('<endpoint>', '<access_key>', '<secret_key>')
SETTINGS
    allow_non_empty_tables = true;
```

If this command fails after `db.t0` has been fully restored but `db.t1` has not finished, `db.t0` remains restored.

**The `PARTITIONS` clause is not a commit boundary.** It only selects which parts of a table are restored:

```sql
RESTORE TABLE db.t0 PARTITIONS '2026-06-01', '2026-06-02', '2026-06-03'
FROM S3('<endpoint>', '<access_key>', '<secret_key>')
SETTINGS
    allow_non_empty_tables = true;
```

All selected parts of the table are copied first and attached only once every one of them is ready. So if this command fails during the copy phase — e.g. after partition `2026-06-01` has been fully copied but `2026-06-02` and `2026-06-03` have not finished — then `2026-06-01` is **not** committed and the table is left with no restored data from this command. Once the copy phase completes and the attach step begins, parts are committed one at a time, so a failure during attach can leave the table partially restored, without rollback.

To commit partitions independently (so a completed partition survives a later failure and can be retried in isolation), run a separate `RESTORE` per partition, using `SETTINGS allow_non_empty_tables = true` after the first.
)DOCS_MD");
    attach<StorageSystemSchemaInferenceCache>(context, system_database, "schema_inference_cache", R"DOCS_MD(
.description
Contains information about all cached file schemas.

.examples
Let's say we have a file `data.jsonl` with this content:
```json
{"id" :  1, "age" :  25, "name" :  "Josh", "hobbies" :  ["football", "cooking", "music"]}
{"id" :  2, "age" :  19, "name" :  "Alan", "hobbies" :  ["tennis", "art"]}
{"id" :  3, "age" :  32, "name" :  "Lana", "hobbies" :  ["fitness", "reading", "shopping"]}
{"id" :  4, "age" :  47, "name" :  "Brayan", "hobbies" :  ["movies", "skydiving"]}
```

<Tip>
Place `data.jsonl` in the `user_files_path` directory.  You can find this by looking
in your ClickHouse configuration files. The default is:
```sql
<user_files_path>/var/lib/clickhouse/user_files/</user_files_path>
```
</Tip>

Open `clickhouse-client` and run the `DESCRIBE` query:

```sql
DESCRIBE file('data.jsonl') SETTINGS input_format_try_infer_integers=0;
```

```response
┌─name────┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ id      │ Nullable(Float64)       │              │                    │         │                  │                │
│ age     │ Nullable(Float64)       │              │                    │         │                  │                │
│ name    │ Nullable(String)        │              │                    │         │                  │                │
│ hobbies │ Array(Nullable(String)) │              │                    │         │                  │                │
└─────────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Let's see the content of the `system.schema_inference_cache` table:

```sql
SELECT *
FROM system.schema_inference_cache
FORMAT Vertical
```
```response
Row 1:
──────
storage:                File
source:                 /home/droscigno/user_files/data.jsonl
format:                 JSONEachRow
additional_format_info: schema_inference_hints=, max_rows_to_read_for_schema_inference=25000, schema_inference_make_columns_nullable=true, try_infer_integers=false, try_infer_dates=true, try_infer_datetimes=true, try_infer_numbers_from_strings=true, read_bools_as_numbers=true, try_infer_objects=false
registration_time:      2022-12-29 17:49:52
schema:                 id Nullable(Float64), age Nullable(Float64), name Nullable(String), hobbies Array(Nullable(String))
```

.see_also
- [Automatic schema inference from input data](/concepts/features/interfaces/schema-inference)
)DOCS_MD");
    attach<StorageSystemDroppedTables>(context, system_database, "dropped_tables", R"DOCS_MD(
.description
Contains information about tables that drop table has been executed on but for which data cleanup has not yet been performed.

.columns_notes
This table lists only tables dropped from `Atomic` databases. For these tables, `table_dropped_time` is normally based on [`database_atomic_delay_before_drop_table_sec`](/reference/settings/server-settings/settings/other#database_atomic_delay_before_drop_table_sec). For a `Shared` database in ClickHouse Cloud, the recovery period is instead controlled by [`database_shared_drop_table_delay_seconds`](/reference/settings/session-settings/database#database_shared_drop_table_delay_seconds), which defaults to 8 hours; dropped tables from `Shared` databases don't appear in `system.dropped_tables`.

.examples
The following example shows how to get information about `dropped_tables`.

```sql
SELECT *
FROM system.dropped_tables\G
```

```text
Row 1:
──────
index:                 0
database:              default
table:                 test
uuid:                  03141bb2-e97a-4d7c-a172-95cc066bb3bd
engine:                MergeTree
metadata_dropped_path: /data/ClickHouse/build/programs/data/metadata_dropped/default.test.03141bb2-e97a-4d7c-a172-95cc066bb3bd.sql
table_dropped_time:    2023-03-16 23:43:31
```
)DOCS_MD");
    attachNoDescription<StorageSystemDroppedTablesParts>(context, system_database, "dropped_tables_parts", R"DOCS_MD(
.description
Contains information about parts of [MergeTree](/reference/engines/table-engines/mergetree-family/mergetree) dropped tables from [system.dropped_tables](/reference/system-tables/dropped_tables)

The schema of this table is the same as [system.parts](/reference/system-tables/parts)

.see_also
- [MergeTree family](/reference/engines/table-engines/mergetree-family/mergetree)
- [system.parts](/reference/system-tables/parts)
- [system.dropped_tables](/reference/system-tables/dropped_tables)
)DOCS_MD");
    attach<StorageSystemScheduler>(context, system_database, "scheduler", R"DOCS_MD(
.description
Contains information about and status of [scheduling nodes](/concepts/features/configuration/server-config/workload-scheduling#hierarchy) residing on the local server.
This table can be used for monitoring. The table contains a row for every scheduling node.

.examples
```sql
SELECT *
FROM system.scheduler
WHERE resource = 'network_read' AND path = '/prio/fair/prod'
FORMAT Vertical
```

```text
Row 1:
──────
resource:          network_read
path:              /prio/fair/prod
type:              fifo
weight:            5
priority:          0
is_active:         0
active_children:   0
dequeued_requests: 67
canceled_requests: 0
dequeued_cost:     4692272
canceled_cost:     0
busy_periods:      63
vruntime:          938454.1999999989
system_vruntime:   ᴺᵁᴸᴸ
queue_length:      0
queue_cost:        0
budget:            -60524
is_satisfied:      ᴺᵁᴸᴸ
inflight_requests: ᴺᵁᴸᴸ
inflight_cost:     ᴺᵁᴸᴸ
max_requests:      ᴺᵁᴸᴸ
max_cost:          ᴺᵁᴸᴸ
max_speed:         ᴺᵁᴸᴸ
max_burst:         ᴺᵁᴸᴸ
throttling_us:     ᴺᵁᴸᴸ
tokens:            ᴺᵁᴸᴸ
```
)DOCS_MD");
    attach<StorageSystemDNSCache>(context, system_database, "dns_cache", R"DOCS_MD(
.description
Contains information about cached DNS records.

.examples
```sql title="Query"
SELECT * FROM system.dns_cache;
```

| hostname | ip\_address | ip\_family | cached\_at |
| :--- | :--- | :--- | :--- |
| localhost | ::1 | IPv6 | 2024-02-11 17:04:40 |
| localhost | 127.0.0.1 | IPv4 | 2024-02-11 17:04:40 |

.see_also
- [disable_internal_dns_cache setting](/reference/settings/server-settings/settings/disable#disable_internal_dns_cache)
- [dns_cache_max_entries setting](/reference/settings/server-settings/settings/dns-cache#dns_cache_max_entries)
- [dns_cache_update_period setting](/reference/settings/server-settings/settings/dns-cache#dns_cache_update_period)
- [dns_max_consecutive_failures setting](/reference/settings/server-settings/settings/other#dns_max_consecutive_failures)
)DOCS_MD");
#if (defined(__ELF__) && !defined(OS_FREEBSD)) || defined(OS_DARWIN)
    attachNoDescription<StorageSystemSymbols>(context, system_database, "symbols", R"DOCS_MD(
.description
Contains information for introspection of `clickhouse` binary. It requires the introspection privilege to access.
This table is only useful for C++ experts and ClickHouse engineers.

.examples
```sql
SELECT
    symbol,
    demangle(symbol) AS symbol_demangled,
    address_begin,
    address_end
FROM system.symbols
LIMIT 5
SETTINGS allow_introspection_functions = 1;
```

```text
Row 1:
──────
symbol:           _Z15isClickHouseAppNSt3__117basic_string_viewIcNS_11char_traitsIcEEEERNS_6vectorIPcNS_9allocatorIS5_EEEE
symbol_demangled: isClickHouseApp(std::__1::basic_string_view<char, std::__1::char_traits<char>>, std::__1::vector<char*, std::__1::allocator<char*>>&)
address_begin:    219229312 -- 219.23 million
address_end:      219231408 -- 219.23 million

Row 2:
──────
symbol:           main
symbol_demangled: main
address_begin:    219231872 -- 219.23 million
address_end:      219233485 -- 219.23 million

Row 3:
──────
symbol:           _ZN12_GLOBAL__N_19printHelpEiPPc
symbol_demangled: (anonymous namespace)::printHelp(int, char**)
address_begin:    219233536 -- 219.23 million
address_end:      219233902 -- 219.23 million

Row 4:
──────
symbol:           _ZNSt3__110filesystem4pathC2B8se210105IPcvEERKT_NS1_6formatE
symbol_demangled: std::__1::filesystem::path::path[abi:se210105]<char*, void>(char* const&, std::__1::filesystem::path::format)
address_begin:    219234496 -- 219.23 million
address_end:      219234620 -- 219.23 million

Row 5:
──────
symbol:           _ZNSt3__113unordered_setINS_17basic_string_viewIcNS_11char_traitsIcEEEENS_4hashIS4_EENS_8equal_toIS4_EENS_9allocatorIS4_EEEC2ESt16initializer_listIS4_E
symbol_demangled: std::__1::unordered_set<std::__1::basic_string_view<char, std::__1::char_traits<char>>, std::__1::hash<std::__1::basic_string_view<char, std::__1::char_traits<char>>>, std::__1::equal_to<std::__1::basic_string_view<char, std::__1::char_traits<char>>>, std::__1::allocator<std::__1::basic_string_view<char, std::__1::char_traits<char>>>>::unordered_set(std::initializer_list<std::__1::basic_string_view<char, std::__1::char_traits<char>>>)
address_begin:    219235584 -- 219.24 million
address_end:      219235708 -- 219.24 million
```
)DOCS_MD");
#endif
#if USE_RDKAFKA
    attach<StorageSystemKafkaConsumers>(context, system_database, "kafka_consumers", R"DOCS_MD(
.description
Contains information about Kafka consumers.
Applicable for [Kafka table engine](/reference/engines/table-engines/integrations/kafka) (native ClickHouse integration).

<Info>
**Availability**

`system.kafka_consumers` is present only in ClickHouse builds compiled with Kafka support (`USE_RDKAFKA`). On builds without it, the table does not exist and queries against it will fail with `UNKNOWN_TABLE`. You can check whether your build has it enabled with:

```sql
SELECT value FROM system.build_options WHERE name = 'USE_RDKAFKA';
```
</Info>

.examples
```sql
SELECT *
FROM system.kafka_consumers
FORMAT Vertical
```

```text
Row 1:
──────
database:                      test
table:                         kafka
consumer_id:                   ClickHouse-instance-test-kafka-1caddc7f-f917-4bb1-ac55-e28bd103a4a0
assignments.topic:             ['system_kafka_cons']
assignments.partition_id:      [0]
assignments.current_offset:    [18446744073709550615]
exceptions.time:               []
exceptions.text:               []
last_poll_time:                2006-11-09 18:47:47
num_messages_read:             4
last_commit_time:              2006-11-10 04:39:40
num_commits:                   1
last_rebalance_time:           1970-01-01 00:00:00
num_rebalance_revocations:     0
num_rebalance_assignments:     1
is_currently_used:             1
rdkafka_stat:                  {...}
dependencies:                  [['test.mv2','test.target2'],['test.mv1','test.target1']]
missing_dependencies:          []
```
)DOCS_MD");
#endif
#if defined(OS_LINUX) || defined(OS_DARWIN)
    attachNoDescription<StorageSystemStackTrace>(context, system_database, "stack_trace", R"DOCS_MD(
.description
Contains stack traces of all server threads. Allows developers to introspect the server state.

To analyze stack frames, use the `addressToLine`, `addressToLineWithInlines`, `addressToSymbol` and `demangle` [introspection functions](/reference/functions/regular-functions/introspection).

.examples
Enabling introspection functions:

```sql
SET allow_introspection_functions = 1;
```

Getting symbols from ClickHouse object files:

```sql
WITH arrayMap(x -> demangle(addressToSymbol(x)), trace) AS all SELECT thread_name, thread_id, query_id, arrayStringConcat(all, '\n') AS res FROM system.stack_trace LIMIT 1;
```

```text
Row 1:
──────
thread_name: QueryPipelineEx
thread_id:   743490
query_id:    dc55a564-febb-4e37-95bb-090ef182c6f1
res:         memcpy
large_ralloc
arena_ralloc
do_rallocx
Allocator<true, true>::realloc(void*, unsigned long, unsigned long, unsigned long)
HashTable<unsigned long, HashMapCell<unsigned long, char*, HashCRC32<unsigned long>, HashTableNoState, PairNoInit<unsigned long, char*>>, HashCRC32<unsigned long>, HashTableGrowerWithPrecalculation<8ul>, Allocator<true, true>>::resize(unsigned long, unsigned long)
void DB::Aggregator::executeImplBatch<false, false, true, DB::AggregationMethodOneNumber<unsigned long, HashMapTable<unsigned long, HashMapCell<unsigned long, char*, HashCRC32<unsigned long>, HashTableNoState, PairNoInit<unsigned long, char*>>, HashCRC32<unsigned long>, HashTableGrowerWithPrecalculation<8ul>, Allocator<true, true>>, true, false>>(DB::AggregationMethodOneNumber<unsigned long, HashMapTable<unsigned long, HashMapCell<unsigned long, char*, HashCRC32<unsigned long>, HashTableNoState, PairNoInit<unsigned long, char*>>, HashCRC32<unsigned long>, HashTableGrowerWithPrecalculation<8ul>, Allocator<true, true>>, true, false>&, DB::AggregationMethodOneNumber<unsigned long, HashMapTable<unsigned long, HashMapCell<unsigned long, char*, HashCRC32<unsigned long>, HashTableNoState, PairNoInit<unsigned long, char*>>, HashCRC32<unsigned long>, HashTableGrowerWithPrecalculation<8ul>, Allocator<true, true>>, true, false>::State&, DB::Arena*, unsigned long, unsigned long, DB::Aggregator::AggregateFunctionInstruction*, bool, char*) const
DB::Aggregator::executeImpl(DB::AggregatedDataVariants&, unsigned long, unsigned long, std::__1::vector<DB::IColumn const*, std::__1::allocator<DB::IColumn const*>>&, DB::Aggregator::AggregateFunctionInstruction*, bool, bool, char*) const
DB::Aggregator::executeOnBlock(std::__1::vector<COW<DB::IColumn>::immutable_ptr<DB::IColumn>, std::__1::allocator<COW<DB::IColumn>::immutable_ptr<DB::IColumn>>>, unsigned long, unsigned long, DB::AggregatedDataVariants&, std::__1::vector<DB::IColumn const*, std::__1::allocator<DB::IColumn const*>>&, std::__1::vector<std::__1::vector<DB::IColumn const*, std::__1::allocator<DB::IColumn const*>>, std::__1::allocator<std::__1::vector<DB::IColumn const*, std::__1::allocator<DB::IColumn const*>>>>&, bool&) const
DB::AggregatingTransform::work()
DB::ExecutionThreadContext::executeTask()
DB::PipelineExecutor::executeStepImpl(unsigned long, std::__1::atomic<bool>*)
void std::__1::__function::__policy_invoker<void ()>::__call_impl<std::__1::__function::__default_alloc_func<DB::PipelineExecutor::spawnThreads()::$_0, void ()>>(std::__1::__function::__policy_storage const*)
ThreadPoolImpl<ThreadFromGlobalPoolImpl<false>>::worker(std::__1::__list_iterator<ThreadFromGlobalPoolImpl<false>, void*>)
void std::__1::__function::__policy_invoker<void ()>::__call_impl<std::__1::__function::__default_alloc_func<ThreadFromGlobalPoolImpl<false>::ThreadFromGlobalPoolImpl<void ThreadPoolImpl<ThreadFromGlobalPoolImpl<false>>::scheduleImpl<void>(std::__1::function<void ()>, Priority, std::__1::optional<unsigned long>, bool)::'lambda0'()>(void&&)::'lambda'(), void ()>>(std::__1::__function::__policy_storage const*)
void* std::__1::__thread_proxy[abi:v15000]<std::__1::tuple<std::__1::unique_ptr<std::__1::__thread_struct, std::__1::default_delete<std::__1::__thread_struct>>, void ThreadPoolImpl<std::__1::thread>::scheduleImpl<void>(std::__1::function<void ()>, Priority, std::__1::optional<unsigned long>, bool)::'lambda0'()>>(void*)
```

Getting filenames and line numbers in ClickHouse source code:

```sql
WITH arrayMap(x -> addressToLine(x), trace) AS all, arrayFilter(x -> x LIKE '%/dbms/%', all) AS dbms SELECT thread_name, thread_id, query_id, arrayStringConcat(notEmpty(dbms) ? dbms : all, '\n') AS res FROM system.stack_trace LIMIT 1;
```

```text
Row 1:
──────
thread_name: clickhouse-serv

thread_id: 686
query_id:  cad353e7-1c29-4b2e-949f-93e597ab7a54
res:       /lib/x86_64-linux-gnu/libc-2.27.so
/build/obj-x86_64-linux-gnu/../src/Storages/System/StorageSystemStackTrace.cpp:182
/build/obj-x86_64-linux-gnu/../contrib/libcxx/include/vector:656
/build/obj-x86_64-linux-gnu/../src/Interpreters/InterpreterSelectQuery.cpp:1338
/build/obj-x86_64-linux-gnu/../src/Interpreters/InterpreterSelectQuery.cpp:751
/build/obj-x86_64-linux-gnu/../contrib/libcxx/include/optional:224
/build/obj-x86_64-linux-gnu/../src/Interpreters/InterpreterSelectWithUnionQuery.cpp:192
/build/obj-x86_64-linux-gnu/../src/Interpreters/executeQuery.cpp:384
/build/obj-x86_64-linux-gnu/../src/Interpreters/executeQuery.cpp:643
/build/obj-x86_64-linux-gnu/../src/Server/TCPHandler.cpp:251
/build/obj-x86_64-linux-gnu/../src/Server/TCPHandler.cpp:1197
/build/obj-x86_64-linux-gnu/../contrib/poco/Net/src/TCPServerConnection.cpp:57
/build/obj-x86_64-linux-gnu/../contrib/libcxx/include/atomic:856
/build/obj-x86_64-linux-gnu/../contrib/poco/Foundation/include/Poco/Mutex_POSIX.h:59
/build/obj-x86_64-linux-gnu/../contrib/poco/Foundation/include/Poco/AutoPtr.h:223
/lib/x86_64-linux-gnu/libpthread-2.27.so
/lib/x86_64-linux-gnu/libc-2.27.so
```

.see_also
- [Introspection Functions](/reference/functions/regular-functions/introspection) — Which introspection functions are available and how to use them.
- [system.trace_log](/reference/system-tables/trace_log) — Contains stack traces collected by the sampling query profiler.
- [arrayMap](/reference/functions/regular-functions/array-functions#arrayMap)) — Description and usage example of the `arrayMap` function.
- [arrayFilter](/reference/functions/regular-functions/array-functions#arrayFilter) — Description and usage example of the `arrayFilter` function.
)DOCS_MD");
#endif
#if USE_ROCKSDB
    attach<StorageSystemRocksDB>(context, system_database, "rocksdb", R"DOCS_MD(
.description
Contains a list of metrics exposed from embedded RocksDB.
)DOCS_MD");
#endif

    attach<StorageSystemKeywords>(context, system_database, "keywords", R"DOCS_MD(
.description
Contains a list of all keywords used in ClickHouse parser.
)DOCS_MD");
    attachNoDescription<StorageSystemParts>(context, system_database, "parts", R"DOCS_MD(
.description
Contains information about parts of [MergeTree](/reference/engines/table-engines/mergetree-family/mergetree) tables.

Each row describes one data part.

.examples
```sql
SELECT * FROM system.parts LIMIT 1 FORMAT Vertical;
```

```text
Row 1:
──────
partition:                             tuple()
name:                                  all_1_4_1_6
part_type:                             Wide
part_storage_type:                     Full
active:                                1
marks:                                 2
rows:                                  6
bytes_on_disk:                         310
data_compressed_bytes:                 157
data_uncompressed_bytes:               91
secondary_indices_compressed_bytes:    58
secondary_indices_uncompressed_bytes:  6
secondary_indices_marks_bytes:         48
marks_bytes:                           144
modification_time:                     2020-06-18 13:01:49
remove_time:                           1970-01-01 00:00:00
refcount:                              1
min_date:                              1970-01-01
max_date:                              1970-01-01
min_time:                              1970-01-01 00:00:00
max_time:                              1970-01-01 00:00:00
partition_id:                          all
min_block_number:                      1
max_block_number:                      4
level:                                 1
data_version:                          6
primary_key_bytes_in_memory:           8
primary_key_bytes_in_memory_allocated: 64
is_frozen:                             0
database:                              default
table:                                 months
engine:                                MergeTree
disk_name:                             default
path:                                  /var/lib/clickhouse/data/default/months/all_1_4_1_6/
hash_of_all_files:                     2d0657a16d9430824d35e327fcbd87bf
hash_of_uncompressed_files:            84950cc30ba867c77a408ae21332ba29
uncompressed_hash_of_compressed_files: 1ad78f1c6843bbfb99a2c931abe7df7d
delete_ttl_info_min:                   1970-01-01 00:00:00
delete_ttl_info_max:                   1970-01-01 00:00:00
move_ttl_info.expression:              []
move_ttl_info.min:                     []
move_ttl_info.max:                     []
```

.see_also
- [MergeTree family](/reference/engines/table-engines/mergetree-family/mergetree)
- [TTL for Columns and Tables](/reference/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-ttl)
)DOCS_MD");
    attachNoDescription<StorageSystemProjectionParts>(context, system_database, "projection_parts", R"DOCS_MD(
.description
This table contains information about projection parts for tables of the MergeTree family.
)DOCS_MD");
    attachNoDescription<StorageSystemDetachedParts>(context, system_database, "detached_parts", R"DOCS_MD(
.description
Contains information about detached parts of [MergeTree](/reference/engines/table-engines/mergetree-family/mergetree) tables. The `reason` column specifies why the part was detached.

For user-detached parts, the reason is empty. Such parts can be attached with [ALTER TABLE ATTACH PARTITION\|PART](/reference/statements/alter/partition#attach-partitionpart) command.

For the description of other columns, see [system.parts](/reference/system-tables/parts).

If part name is invalid, values of some columns may be `NULL`. Such parts can be deleted with [ALTER TABLE DROP DETACHED PART](/reference/statements/alter/partition#drop-detached-partitionpart).
)DOCS_MD");
    attachNoDescription<StorageSystemPartsColumns>(context, system_database, "parts_columns", R"DOCS_MD(
.description
Contains information about parts and columns of [MergeTree](/reference/engines/table-engines/mergetree-family/mergetree) tables.
Each row describes one data part.

.examples
```sql
SELECT * FROM system.parts_columns LIMIT 1 FORMAT Vertical;
```

```text
Row 1:
──────
partition:                             tuple()
name:                                  all_1_2_1
part_type:                             Wide
active:                                1
marks:                                 2
rows:                                  2
bytes_on_disk:                         155
data_compressed_bytes:                 56
data_uncompressed_bytes:               4
marks_bytes:                           96
modification_time:                     2020-09-23 10:13:36
remove_time:                           2106-02-07 06:28:15
refcount:                              1
min_date:                              1970-01-01
max_date:                              1970-01-01
partition_id:                          all
min_block_number:                      1
max_block_number:                      2
level:                                 1
data_version:                          1
primary_key_bytes_in_memory:           2
primary_key_bytes_in_memory_allocated: 64
database:                              default
table:                                 53r93yleapyears
engine:                                MergeTree
disk_name:                             default
path:                                  /var/lib/clickhouse/data/default/53r93yleapyears/all_1_2_1/
column:                                id
type:                                  Int8
column_position:                       1
default_kind:
default_expression:
column_bytes_on_disk:                  76
column_data_compressed_bytes:          28
column_data_uncompressed_bytes:        2
column_marks_bytes:                    48
```

.see_also
- [MergeTree family](/reference/engines/table-engines/mergetree-family/mergetree)
- [Calculating the number and size of compact and wide parts](/resources/support-center/knowledge-base/troubleshooting/count-parts-by-type)
)DOCS_MD");
    attachNoDescription<StorageSystemProjectionPartsColumns>(context, system_database, "projection_parts_columns", R"DOCS_MD(
.description
This table contains information about columns in projection parts for tables of the MergeTree family.
)DOCS_MD");
    attachNoDescription<StorageSystemDisks>(context, system_database, "disks", R"DOCS_MD(
.description
Contains information about disks defined in the [server configuration](/reference/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-multiple-volumes_configure).

.examples
```sql
SELECT * FROM system.disks;
```

```response
┌─name────┬─path─────────────────┬───free_space─┬──total_space─┬─keep_free_space─┐
│ default │ /var/lib/clickhouse/ │ 276392587264 │ 490652508160 │               0 │
└─────────┴──────────────────────┴──────────────┴──────────────┴─────────────────┘

1 rows in set. Elapsed: 0.001 sec.
```
)DOCS_MD");
    attachNoDescription<StorageSystemStoragePolicies>(context, system_database, "storage_policies", R"DOCS_MD(
.description
Contains information about storage policies and volumes which are defined in [server configuration](/reference/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-multiple-volumes_configure).

## Volume selection on `INSERT` {#volume-selection-on-insert}

When `INSERT` creates a new data part, ClickHouse picks a destination disk
by trying the rules below in order. The first rule that matches **and can
reserve space for the part** wins; otherwise (rule does not apply, no free
space, or `max_data_part_size` exceeded) evaluation continues with the next
rule.

1. **TTL move rule** — if a `TTL <expr> TO VOLUME 'X'` (or `TO DISK 'X'`)
   clause is already in the past for the rows being inserted, **and**
   `perform_ttl_move_on_insert = 1` (default) on the **TTL destination
   volume** (for `TO DISK 'X'`, the volume containing disk `X`), the part
   is written directly to that destination. If reservation there fails, the
   insert falls back to steps 2–4; a warning is logged but the `INSERT`
   does not fail for this reason alone.
2. **`max_data_part_size`** — a volume rejects parts larger than its
   `max_data_part_size`. This is checked per volume; it does not gate a
   step-1 `TTL ... TO DISK 'X'` reservation, which targets the disk
   directly.
3. **`volume_priority`** — among the remaining volumes, the one with the
   lowest `volume_priority` value is chosen. Volumes without an explicit
   `<volume_priority>` are ordered by their position in the configuration.
4. **`load_balancing`** — once a volume is chosen, the disk inside that
   volume is selected according to its `load_balancing` policy
   (`round_robin` or `least_used`).

<Info>
**Override**

If `min_free_disk_bytes_to_perform_insert` or
`min_free_disk_ratio_to_perform_insert` is non-zero, the precedence above
is bypassed. `INSERT` tries only the volume with the lowest
`volume_priority` and throws `NOT_ENOUGH_SPACE` if no disk in that volume
meets the threshold. Inserts into the `system` database are exempt.
</Info>

<Note>
`perform_ttl_move_on_insert` is read from the **TTL destination** volume,
not from the source volume. For a `TO DISK 'X'` rule, the flag is read
from the volume that contains disk `X`. Setting it on any other volume of
the policy has no effect on the insert path.
</Note>

To force inserts to honour `volume_priority` even when an "already
expired" TTL move rule applies, set `perform_ttl_move_on_insert = 0` on
the TTL destination volume (for `TO DISK 'X'`, on the volume that contains
disk `X`). The part is then written to the priority-N volume first and
moved to the TTL destination by a background move task (observable via
`system.moves`). See the
[`perform_ttl_move_on_insert` setting on the MergeTree engine](/reference/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-multiple-volumes_configure).
)DOCS_MD");
    attach<StorageSystemProcesses>(context, system_database, "processes", R"DOCS_MD(
.description
This system table is used for implementing the `SHOW PROCESSLIST` query.

.examples
```sql
SELECT * FROM system.processes LIMIT 10 FORMAT Vertical;
```

```response
Row 1:
──────
is_initial_query:     1
user:                 default
query_id:             35a360fa-3743-441d-8e1f-228c938268da
address:              ::ffff:172.23.0.1
port:                 47588
initial_user:         default
initial_query_id:     35a360fa-3743-441d-8e1f-228c938268da
initial_address:      ::ffff:172.23.0.1
initial_port:         47588
interface:            1
os_user:              bharatnc
client_hostname:      tower
client_name:          ClickHouse
client_revision:      54437
client_version_major: 20
client_version_minor: 7
client_version_patch: 2
http_method:          0
http_user_agent:
quota_key:
elapsed:              0.000582537
is_cancelled:         0
is_all_data_sent:     0
read_rows:            0
read_bytes:           0
total_rows_approx:    0
written_rows:         0
written_bytes:        0
memory_usage:         0
peak_memory_usage:    0
query:                SELECT * from system.processes LIMIT 10 FORMAT Vertical;
thread_ids:           [67]
ProfileEvents:        {'Query':1,'SelectQuery':1,'ReadCompressedBytes':36,'CompressedReadBufferBlocks':1,'CompressedReadBufferBytes':10,'IOBufferAllocs':1,'IOBufferAllocBytes':89,'ContextLock':15,'RWLockAcquiredReadLocks':1}
Settings:             {'background_pool_size':'32','load_balancing':'random','allow_suspicious_low_cardinality_types':'1','distributed_aggregation_memory_efficient':'1','skip_unavailable_shards':'1','log_queries':'1','max_bytes_before_external_group_by':'20000000000','max_bytes_before_external_sort':'20000000000','allow_introspection_functions':'1'}

1 rows in set. Elapsed: 0.002 sec.
```
)DOCS_MD");
    attach<StorageSystemMetrics>(context, system_database, "metrics", R"DOCS_MD(
.description
Contains metrics which can be calculated instantly, or have a current value. For example, the number of simultaneously processed queries or the current replica delay. This table is always up to date.

## Metric descriptions {#metric-descriptions}

{/*AUTOGENERATED_DESCRIPTIONS_START*/}
{{CURRENT_METRICS}}
{/*AUTOGENERATED_DESCRIPTIONS_END*/}

.columns_notes
You can find all supported metrics in source file [src/Common/CurrentMetrics.cpp](https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/CurrentMetrics.cpp).

.examples
```sql
SELECT * FROM system.metrics LIMIT 10
```

```text
┌─metric───────────────────────────────┬─value─┬─description────────────────────────────────────────────────────────────┐
│ Query                                │     1 │ Number of executing queries                                            │
│ Merge                                │     0 │ Number of executing background merges                                  │
│ PartMutation                         │     0 │ Number of mutations (ALTER DELETE/UPDATE)                              │
│ ReplicatedFetch                      │     0 │ Number of data parts being fetched from replicas                       │
│ ReplicatedSend                       │     0 │ Number of data parts being sent to replicas                            │
│ ReplicatedChecks                     │     0 │ Number of data parts checking for consistency                          │
│ BackgroundMergesAndMutationsPoolTask │     0 │ Number of active merges and mutations in an associated background pool │
│ BackgroundFetchesPoolTask            │     0 │ Number of active fetches in an associated background pool              │
│ BackgroundCommonPoolTask             │     0 │ Number of active tasks in an associated background pool                │
│ BackgroundMovePoolTask               │     0 │ Number of active tasks in BackgroundProcessingPool for moves           │
└──────────────────────────────────────┴───────┴────────────────────────────────────────────────────────────────────────┘
```
)DOCS_MD");
    attach<StorageSystemHistogramMetrics>(context, system_database, "histogram_metrics", R"DOCS_MD(
.description
This table contains histogram metrics that can be calculated instantly and exported in the Prometheus format. It is always up to date. Replaces the deprecated `system.latency_log`.

## Metric descriptions {#metric_descriptions}

| Metric | Description |
|---|---|
| `keeper_response_time_ms_bucket` | The response time of Keeper, in milliseconds. |
| `keeper_client_queue_duration_milliseconds_bucket` | Time requests spend waiting to be enqueued and waiting in the queue before being processed by the Keeper client, in milliseconds. |
| `keeper_receive_request_time_milliseconds_bucket` | Time to receive and parse a request from the client in the Keeper TCP handler, in milliseconds. |
| `keeper_dispatcher_requests_queue_time_milliseconds_bucket` | Time a request spends in the Keeper dispatcher requests queue, in milliseconds. |
| `keeper_write_pre_commit_time_milliseconds_bucket` | Time to preprocess a write request before Raft commit, in milliseconds. |
| `keeper_write_commit_time_milliseconds_bucket` | Time to process a write request after Raft commit, in milliseconds. |
| `keeper_dispatcher_responses_queue_time_milliseconds_bucket` | Time a response spends in the Keeper dispatcher responses queue, in milliseconds. |
| `keeper_send_response_time_milliseconds_bucket` | Time to send a response to the client in the Keeper TCP handler (includes queueing and writing to socket), in milliseconds. |
| `keeper_read_wait_for_write_time_milliseconds_bucket` | Time a read request waits for the write request it depends on to complete, in milliseconds. |
| `keeper_read_process_time_milliseconds_bucket` | Time to process a read request in Keeper, in milliseconds. |
| `keeper_batch_size_elements_bucket` | Batch size sent to Raft, in elements. |
| `keeper_batch_size_bytes_bucket` | Batch size sent to Raft, in bytes. |
| `filesystem_cache_evicted_segment_hits_bucket` | Distribution of cache-hit counts on file segments at the moment of their eviction, labelled by cache name. |
| `filesystem_cache_evicted_segment_size_bytes_bucket` | Distribution of byte sizes of evicted file segments, labelled by cache name. |
| `filesystem_cache_evicted_segment_hits_by_user_bucket` | Distribution of cache-hit counts on evicted file segments, labelled by cache name and user id. |
| `filesystem_cache_evicted_segment_size_bytes_by_user_bucket` | Distribution of byte sizes of evicted file segments, labelled by cache name and user id. |

.examples
You can use a query like this to export all the histogram metrics in the Prometheus format.
```sql
SELECT
  metric AS name,
  toFloat64(value) AS value,
  description AS help,
  labels,
  'histogram' AS type
FROM system.histogram_metrics
FORMAT Prometheus
```

.see_also
- [system.asynchronous_metrics](/reference/system-tables/asynchronous_metrics) — Contains periodically calculated metrics.
- [system.events](/reference/system-tables/events) — Contains a number of events that occurred.
- [system.metric_log](/reference/system-tables/metric_log) — Contains a history of metrics values from tables `system.metrics` and `system.events`.
- [Monitoring](/guides/oss/deployment-and-scaling/monitoring/monitoring) — Base concepts of ClickHouse monitoring.
)DOCS_MD");
    attach<StorageSystemDimensionalMetrics>(context, system_database, "dimensional_metrics", R"DOCS_MD(
.description
This table contains dimensional metrics that can be calculated instantly and exported in the Prometheus format. It is always up to date.

## Metric descriptions {#metric_descriptions}

### merge_failures {#merge_failures}
Number of all failed merges since startup.

### startup_scripts_failure_reason {#startup_scripts_failure_reason}
Indicates startup scripts failures by error type. Set to 1 when a startup script fails, labelled with the error name.

### merge_tree_parts {#merge_tree_parts}
Number of merge tree data parts, labelled by part state, part type, and whether it is a projection part.

### `filesystem_cache_evictions_total` {#filesystem-cache-evictions-total}
Number of file segments evicted from a filesystem cache, labelled by cache name. Disabled by default; enable with `expose_prometheus_eviction_metrics`.

### `filesystem_cache_evicted_bytes_total` {#filesystem-cache-evicted-bytes-total}
Total bytes of file segments evicted from a filesystem cache, labelled by cache name. Disabled by default; enable with `expose_prometheus_eviction_metrics`.

### `filesystem_cache_evictions_by_user_total` {#filesystem-cache-evictions-by-user-total}
Number of file segments evicted from a filesystem cache, labelled by cache name and user id. Disabled by default; enable with `expose_prometheus_eviction_metrics` and `expose_prometheus_eviction_metrics_per_user`.

### `filesystem_cache_evicted_bytes_by_user_total` {#filesystem-cache-evicted-bytes-by-user-total}
Total bytes of file segments evicted from a filesystem cache, labelled by cache name and user id. Disabled by default; enable with `expose_prometheus_eviction_metrics` and `expose_prometheus_eviction_metrics_per_user`.

### `object_storage_queue_failures_total` {#object-storage-queue-failures-total}
Number of `ObjectStorageQueue` (`S3Queue`/`AzureQueue`) failures, labelled by database, table, processing stage (`read`, `set_processing`, `insert`, `commit`) and error code.

### `object_storage_queue_permanently_failed_files_total` {#object-storage-queue-permanently-failed-files-total}
Number of `ObjectStorageQueue` (`S3Queue`/`AzureQueue`) files given up on for good after exhausting retries (or with retries disabled), labelled by database and table. Each of these represents a file whose data will never be processed.

### `object_storage_queue_newest_seen_object_timestamp_seconds` {#object-storage-queue-newest-seen-object-timestamp-seconds}
Unix timestamp of the last-modified time of the newest object seen so far by an `ObjectStorageQueue` (`S3Queue`/`AzureQueue`) table, labelled by database and table.

### `object_storage_queue_newest_committed_object_timestamp_seconds` {#object-storage-queue-newest-committed-object-timestamp-seconds}
Unix timestamp of the last-modified time of the newest object fully processed so far by an `ObjectStorageQueue` (`S3Queue`/`AzureQueue`) table, labelled by database and table.

.examples
You can use a query like this to export all the dimensional metrics in the Prometheus format.
```sql
SELECT
  metric AS name,
  toFloat64(value) AS value,
  description AS help,
  labels,
  'gauge' AS type
FROM system.dimensional_metrics
FORMAT Prometheus
```

.see_also
- [system.asynchronous_metrics](/reference/system-tables/asynchronous_metrics) — Contains periodically calculated metrics.
- [system.events](/reference/system-tables/events) — Contains a number of events that occurred.
- [system.metric_log](/reference/system-tables/metric_log) — Contains a history of metrics values from tables `system.metrics` and `system.events`.
- [Monitoring](/guides/oss/deployment-and-scaling/monitoring/monitoring) — Base concepts of ClickHouse monitoring.
)DOCS_MD");
    attach<StorageSystemMerges>(context, system_database, "merges", R"DOCS_MD(
.description
Contains information about merges and part mutations currently in process for tables in the MergeTree family.
)DOCS_MD");
    attach<StorageSystemMoves>(context, system_database, "moves", R"DOCS_MD(
.description
The table contains information about in-progress [data part moves](/reference/statements/alter/partition#move-partitionpart) of [MergeTree](/reference/engines/table-engines/mergetree-family/mergetree) tables. Each data part movement is represented by a single row.

.examples
```sql
SELECT * FROM system.moves
```

```response
┌─database─┬─table─┬─────elapsed─┬─target_disk_name─┬─target_disk_path─┬─part_name─┬─part_size─┬─thread_id─┐
│ default  │ test2 │ 1.668056039 │ s3               │ ./disks/s3/      │ all_3_3_0 │       136 │    296146 │
└──────────┴───────┴─────────────┴──────────────────┴──────────────────┴───────────┴───────────┴───────────┘
```

.see_also
- [MergeTree](/reference/engines/table-engines/mergetree-family/mergetree) table engine
- [Using Multiple Block Devices for Data Storage](/reference/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-multiple-volumes)
- [ALTER TABLE ... MOVE PART](/reference/statements/alter/partition#move-partitionpart) command
)DOCS_MD");
    attach<StorageSystemMutations>(context, system_database, "mutations", R"DOCS_MD(
.description
The table contains information about [mutations](/reference/statements/alter/index#mutations) of [MergeTree](/reference/engines/table-engines/mergetree-family/mergetree) tables and their progress. Each mutation command is represented by a single row.

## Monitoring Mutations {#monitoring-mutations}

To track the progress on the `system.mutations` table, use the following query:

```sql
SELECT * FROM clusterAllReplicas('cluster_name', 'system', 'mutations')
WHERE is_done = 0 AND table = 'tmp';

-- or

SELECT * FROM clusterAllReplicas('cluster_name', 'system.mutations')
WHERE is_done = 0 AND table = 'tmp';
```

Note: this requires read permissions on the `system.*` tables.

<Tip>
**Cloud usage**

In ClickHouse Cloud the `system.mutations` table on each node has all the mutations in the cluster, and there is no need for `clusterAllReplicas`.
</Tip>

.columns_notes
<Note>
- If a part name is not in `parts_postpone_reasons` and has not yet been mutated, it means the part is yet not scheduled for mutation.
- The part name `all_parts` represents all parts that have not yet been mutated.
</Note>

- `is_killed` ([UInt8](/reference/data-types/int-uint)) — Indicates whether a mutation has been killed. **Only available in ClickHouse Cloud.**

<Note>
`is_killed=1` does not necessarily mean the mutation is completely finalized. It is possible for a mutation to remain in a state where `is_killed=1` and `is_done=0` for an extended period. This can happen if another long-running mutation is blocking the killed mutation. This is a normal situation.
</Note>

- `is_done` ([UInt8](/reference/data-types/int-uint)) — The flag whether the mutation is done or not. Possible values:
  - `1` if the mutation is completed,
  - `0` if the mutation is still in process.

<Note>
Even if `parts_to_do = 0` it is possible that a mutation of a replicated table is not completed yet because of a long-running `INSERT` query, that will create a new data part needed to be mutated.
</Note>

If there were problems with mutating some data parts, the following columns contain additional information:

- `latest_failed_part` ([String](/reference/data-types/string)) — The name of the most recent part that could not be mutated.
- `latest_fail_time` ([DateTime](/reference/data-types/datetime)) — The date and time of the most recent part mutation failure.
- `latest_fail_reason` ([String](/reference/data-types/string)) — The exception message that caused the most recent part mutation failure.

.see_also
- [Mutations](/reference/statements/alter/index#mutations)
- [MergeTree](/reference/engines/table-engines/mergetree-family/mergetree) table engine
- [ReplicatedMergeTree](/reference/engines/table-engines/mergetree-family/replication) family
)DOCS_MD");
    attachNoDescription<StorageSystemReplicas>(context, system_database, "replicas", R"DOCS_MD(
.description
Contains information and status for replicated tables residing on the local server.
This table can be used for monitoring. The table contains a row for every Replicated\* table.

.examples
```sql
SELECT *
FROM system.replicas
WHERE table = 'test_table'
FORMAT Vertical
```

```text
Query id: dc6dcbcb-dc28-4df9-ae27-4354f5b3b13e

Row 1:
───────
database:                    db
table:                       test_table
engine:                      ReplicatedMergeTree
is_leader:                   1
can_become_leader:           1
is_readonly:                 0
is_session_expired:          0
future_parts:                0
parts_to_check:              0
zookeeper_path:              /test/test_table
replica_name:                r1
replica_path:                /test/test_table/replicas/r1
columns_version:             -1
queue_size:                  27
inserts_in_queue:            27
merges_in_queue:             0
part_mutations_in_queue:     0
queue_oldest_time:           2021-10-12 14:48:48
inserts_oldest_time:         2021-10-12 14:48:48
merges_oldest_time:          1970-01-01 03:00:00
part_mutations_oldest_time:  1970-01-01 03:00:00
oldest_part_to_get:          1_17_17_0
oldest_part_to_merge_to:
oldest_part_to_mutate_to:
log_max_index:               206
log_pointer:                 207
last_queue_update:           2021-10-12 14:50:08
absolute_delay:              99
total_replicas:              5
active_replicas:             5
lost_part_count:             0
last_queue_update_exception:
zookeeper_exception:
replica_is_active:           {'r1':1,'r2':1}
```
)DOCS_MD");
    attachNoDescription<StorageSystemDatabaseReplicas>(context, system_database, "database_replicas", R"DOCS_MD(
.description
Contains information of each Replicated database replicas.

.examples
```sql
SELECT * FROM system.database_replicas FORMAT Vertical;
```

```text
Row 1:
──────
database:            db_2
is_readonly:         0
max_log_ptr:         2
replica_name:        replica1
replica_path:        /test/db_2/replicas/shard1|replica1
zookeeper_path:      /test/db_2
shard_name:          shard1
log_ptr:             2
total_replicas:      1
zookeeper_exception:
is_session_expired:  0
```
)DOCS_MD");
    attach<StorageSystemReplicationQueue>(context, system_database, "replication_queue", R"DOCS_MD(
.description
Contains information about tasks from replication queues stored in ClickHouse Keeper, or ZooKeeper, for tables in the `ReplicatedMergeTree` family.

.examples
```sql
SELECT * FROM system.replication_queue LIMIT 1 FORMAT Vertical;
```

```text
Row 1:
──────
database:               merge
table:                  visits_v2
replica_name:           mtgiga001-1t
position:               15
node_name:              queue-0009325559
type:                   MERGE_PARTS
create_time:            2020-12-07 14:04:21
required_quorum:        0
source_replica:         mtgiga001-1t
new_part_name:          20201130_121373_121384_2
parts_to_merge:         ['20201130_121373_121378_1','20201130_121379_121379_0','20201130_121380_121380_0','20201130_121381_121381_0','20201130_121382_121382_0','20201130_121383_121383_0','20201130_121384_121384_0']
is_detach:              0
is_currently_executing: 0
num_tries:              36
last_exception:         Code: 226, e.displayText() = DB::Exception: Marks file '/opt/clickhouse/data/merge/visits_v2/tmp_fetch_20201130_121373_121384_2/CounterID.mrk' does not exist (version 20.8.7.15 (official build))
last_attempt_time:      2020-12-08 17:35:54
num_postponed:          0
postpone_reason:
last_postpone_time:     1970-01-01 03:00:00
```

.see_also
- [Managing ReplicatedMergeTree Tables](/reference/statements/system#managing-replicatedmergetree-tables)
)DOCS_MD");
    attach<StorageSystemDDLWorkerQueue>(context, system_database, "distributed_ddl_queue", R"DOCS_MD(
.description
Contains information about [distributed ddl queries (ON CLUSTER clause)](/reference/statements/distributed-ddl) that were executed on a cluster.

.examples
```sql
SELECT *
FROM system.distributed_ddl_queue
WHERE cluster = 'test_cluster'
LIMIT 2
FORMAT Vertical

Query id: f544e72a-6641-43f1-836b-24baa1c9632a

Row 1:
──────
entry:             query-0000000000
entry_version:     5
initiator_host:    clickhouse01
initiator_port:    9000
cluster:           test_cluster
query:             CREATE DATABASE test_db UUID '4a82697e-c85e-4e5b-a01e-a36f2a758456' ON CLUSTER test_cluster
settings:          {'max_threads':'16','use_uncompressed_cache':'0'}
query_create_time: 2023-09-01 16:15:14
host:              clickhouse-01
port:              9000
status:            Finished
exception_code:    0
exception_text:
query_finish_time: 2023-09-01 16:15:14
query_duration_ms: 154

Row 2:
──────
entry:             query-0000000001
entry_version:     5
initiator_host:    clickhouse01
initiator_port:    9000
cluster:           test_cluster
query:             CREATE DATABASE test_db UUID '4a82697e-c85e-4e5b-a01e-a36f2a758456' ON CLUSTER test_cluster
settings:          {'max_threads':'16','use_uncompressed_cache':'0'}
query_create_time: 2023-09-01 16:15:14
host:              clickhouse-01
port:              9000
status:            Finished
exception_code:    630
exception_text:    Code: 630. DB::Exception: Cannot drop or rename test_db, because some tables depend on it:
query_finish_time: 2023-09-01 16:15:14
query_duration_ms: 154

2 rows in set. Elapsed: 0.025 sec.
```
)DOCS_MD");
    attach<StorageSystemDistributionQueue>(context, system_database, "distribution_queue", R"DOCS_MD(
.description
Contains information about local files that are in the queue to be sent to the shards. These local files contain new parts that are created by inserting new data into the Distributed table in asynchronous mode.

.examples
```sql
SELECT * FROM system.distribution_queue LIMIT 1 FORMAT Vertical;
```

```text
Row 1:
──────
database:              default
table:                 dist
data_path:             ./store/268/268bc070-3aad-4b1a-9cf2-4987580161af/default@127%2E0%2E0%2E2:9000/
is_blocked:            1
error_count:           0
data_files:            1
data_compressed_bytes: 499
last_exception:
```

.see_also
- [Distributed table engine](/reference/engines/table-engines/special/distributed)
)DOCS_MD");
    attach<StorageSystemDictionaries>(context, system_database, "dictionaries", R"DOCS_MD(
.description
Contains information about [dictionaries](/reference/statements/create/dictionary).

.examples
Configure the dictionary:

```sql
CREATE DICTIONARY dictionary_with_comment
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'source_table'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000)
COMMENT 'The temporary dictionary';
```

Make sure that the dictionary is loaded.

```sql
SELECT * FROM system.dictionaries LIMIT 1 FORMAT Vertical;
```

```text
Row 1:
──────
database:                    default
name:                        dictionary_with_comment
uuid:                        4654d460-0d03-433a-8654-d4600d03d33a
status:                      NOT_LOADED
origin:                      4654d460-0d03-433a-8654-d4600d03d33a
type:
key.names:                   ['id']
key.types:                   ['UInt64']
attribute.names:             ['value']
attribute.types:             ['String']
bytes_allocated:             0
query_count:                 0
hit_rate:                    0
found_rate:                  0
element_count:               0
load_factor:                 0
source:
lifetime_min:                0
lifetime_max:                0
loading_start_time:          1970-01-01 00:00:00
last_successful_update_time: 1970-01-01 00:00:00
loading_duration:            0
last_exception:
comment:                     The temporary dictionary
```
)DOCS_MD");
    attach<StorageSystemClusters>(context, system_database, "clusters", R"DOCS_MD(
.description
Contains information about clusters available in the config file and the servers in them.

.examples
```sql title="Query"
SELECT * FROM system.clusters LIMIT 2 FORMAT Vertical;
```

```text title="Response"
Row 1:
──────
cluster:                 test_cluster_two_shards
shard_num:               1
shard_name:              shard_01
shard_weight:            1
replica_num:             1
host_name:               127.0.0.1
host_address:            127.0.0.1
port:                    9000
is_local:                1
user:                    default
default_database:
errors_count:            0
slowdowns_count:         0
estimated_recovery_time: 0
database_shard_name:
database_replica_name:
is_active:               NULL

Row 2:
──────
cluster:                 test_cluster_two_shards
shard_num:               2
shard_name:              shard_02
shard_weight:            1
replica_num:             1
host_name:               127.0.0.2
host_address:            127.0.0.2
port:                    9000
is_local:                0
user:                    default
default_database:
errors_count:            0
slowdowns_count:         0
estimated_recovery_time: 0
database_shard_name:
database_replica_name:
is_active:               NULL
```

.see_also
- [Table engine Distributed](/reference/engines/table-engines/special/distributed)
- [distributed_replica_error_cap setting](/reference/settings/session-settings/distributed-replica#distributed_replica_error_cap)
- [distributed_replica_error_half_life setting](/reference/settings/session-settings/distributed-replica#distributed_replica_error_half_life)
)DOCS_MD");
    attach<StorageSystemGraphite>(context, system_database, "graphite_retentions", R"DOCS_MD(
.description
Contains information about parameters [graphite_rollup](/reference/settings/server-settings/settings/graphite#graphite_rollup) which are used in tables with [\*GraphiteMergeTree](/reference/engines/table-engines/mergetree-family/graphitemergetree) engines.
)DOCS_MD");
    attach<StorageSystemMacros>(context, system_database, "macros", R"DOCS_MD(
.description
Contains a list of all macros defined in server configuration.
)DOCS_MD");
    attach<StorageSystemReplicatedFetches>(context, system_database, "replicated_fetches", R"DOCS_MD(
.description
Contains information about currently running background fetches.

.examples
```sql
SELECT * FROM system.replicated_fetches LIMIT 1 FORMAT Vertical;
```

```text
Row 1:
──────
database:                    default
table:                       t
elapsed:                     7.243039876
progress:                    0.41832135995612835
result_part_name:            all_0_0_0
result_part_path:            /var/lib/clickhouse/store/700/70080a04-b2de-4adf-9fa5-9ea210e81766/all_0_0_0/
partition_id:                all
total_size_bytes_compressed: 1052783726
bytes_read_compressed:       440401920
source_replica_path:         /clickhouse/test/t/replicas/1
source_replica_hostname:     node1
source_replica_port:         9009
interserver_scheme:          http
URI:                         http://node1:9009/?endpoint=DataPartsExchange%3A%2Fclickhouse%2Ftest%2Ft%2Freplicas%2F1&part=all_0_0_0&client_protocol_version=4&compress=false
to_detached:                 0
thread_id:                   54
```

.see_also
- [Managing ReplicatedMergeTree Tables](/reference/statements/system#managing-replicatedmergetree-tables)
)DOCS_MD");
    attach<StorageSystemPartMovesBetweenShards>(context, system_database, "part_moves_between_shards", R"DOCS_MD(
.description
Contains information about parts which are currently in a process of moving between shards and their progress.
)DOCS_MD");
    attach<StorageSystemAsynchronousInserts>(context, system_database, "asynchronous_inserts", R"DOCS_MD(
.description
Contains information about pending asynchronous inserts in queue.

.examples
```sql title="Query"
SELECT * FROM system.asynchronous_inserts LIMIT 1 \G;
```

```text title="Response"
Row 1:
──────
query:            INSERT INTO public.data_guess (user_id, datasource_id, timestamp, path, type, num, str) FORMAT CSV
database:         public
table:            data_guess
format:           CSV
first_update:     2023-06-08 10:08:54.199606
total_bytes:      133223
entries.query_id: ['b46cd4c4-0269-4d0b-99f5-d27668c6102e']
entries.bytes:    [133223]
```

.see_also
- [system.query_log](/reference/system-tables/query_log) — Description of the `query_log` system table which contains common information about queries execution.
- [system.asynchronous_insert_log](/reference/system-tables/asynchronous_insert_log) — This table contains information about async inserts performed.
)DOCS_MD");
    attachNoDescription<StorageSystemFilesystemCache>(context, system_database, "filesystem_cache", R"DOCS_MD(
.description
Contains information about all entries inside filesystem cache for remote objects.
)DOCS_MD");
    attachNoDescription<StorageSystemFilesystemCacheSettings>(context, system_database, "filesystem_cache_settings", R"DOCS_MD(
.description
Contains information about all filesystem cache settings
)DOCS_MD");
    attachNoDescription<StorageSystemQueryConditionCache>(context, system_database, "query_condition_cache", R"DOCS_MD(
.description
Shows the content of the [query condition cache](/concepts/features/performance/caches/query-condition-cache).

.examples
```sql title="Query"
SELECT * FROM system.query_condition_cache FORMAT Vertical;
```

```text title="Response"
Row 1:
──────
table_uuid:     28270a24-ea27-49f6-99cd-97b9bee976ac
part_name:      all_1_1_0
condition:      or(equals(b, 10000_UInt16), equals(c, 10000_UInt16))
condition_hash: 5456494897146899690 -- 5.46 quintillion
entry_size:     40
matching_marks: 111111110000000000000000000000000000000000000000000000000111111110000000000000000

1 row in set. Elapsed: 0.004 sec.
```
)DOCS_MD");
#if ENABLE_DISTRIBUTED_CACHE
    DistributedCache::attachSystemTablesDistributedCache(context, system_database);
#endif
    attachNoDescription<StorageSystemQueryResultCache>(context, system_database, "query_cache", R"DOCS_MD(
.description
Shows the content of the [query cache](/concepts/features/performance/caches/query-cache).

.examples
```sql
SELECT * FROM system.query_cache FORMAT Vertical;
```

```text
Row 1:
──────
query:       SELECT 1 SETTINGS use_query_cache = 1
query_id:    7c28bbbb-753b-4eba-98b1-efcbe2b9bdf6
result_size: 128
tag:
stale:       0
shared:      0
compressed:  1
expires_at:  2023-10-13 13:35:45
key_hash:    12188185624808016954

1 row in set. Elapsed: 0.004 sec.
```
)DOCS_MD");
    attachNoDescription<StorageSystemRemoteDataPaths>(context, system_database, "remote_data_paths", R"DOCS_MD(
.description
Contains information about data files stored on remote disks (e.g. S3, Azure Blob Storage), including the mapping between local metadata paths and remote blob paths.

Each row represents one remote blob object associated with a data file.

.examples
```sql
SELECT * FROM system.remote_data_paths LIMIT 1 FORMAT Vertical;
```

```text
Row 1:
──────
disk_name:              s3
path:                   /var/lib/clickhouse/disks/s3/
cache_base_path:        /var/lib/clickhouse/disks/s3_cache/
local_path:             store/123/1234abcd-1234-1234-1234-1234abcd1234/all_0_0_0/data.bin
remote_path:            abc123/all_0_0_0/data.bin
size:                   1048576
common_prefix_for_blobs:
cache_paths:            ['/var/lib/clickhouse/disks/s3_cache/a1/b2/c3d4e5f6']
```

.see_also
- [Using external storage for data storage](/reference/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-s3)
- [Configuring external storage](/concepts/features/configuration/server-config/storing-data#configuring-external-storage)
- [system.disks](/reference/system-tables/disks)
)DOCS_MD");
    attachNoDescription<StorageSystemTokenizers>(context, system_database, "tokenizers", R"DOCS_MD(
.description
Shows all available tokenizers.
These can be used in functions [tokens](/reference/functions/regular-functions/splitting-merging-functions#tokens), [hasAllTokens](/reference/functions/regular-functions/string-search-functions#hasAllTokens), [hasAnyTokens](/reference/functions/regular-functions/string-search-functions#hasAnyTokens), and the [text index](/reference/engines/table-engines/mergetree-family/textindexes).

.examples
```sql
SELECT * FROM system.tokenizers;
```

```text
┌─name────────────┐
│ ngrams          │
│ splitByNonAlpha │
│ sparseGrams     │
│ tokenbf_v1      │
│ ngrambf_v1      │
│ array           │
│ splitByString   │
│ sparse_grams    │
│ asciiCJK        │
│ icu             │
│ japanese        │
└─────────────────┘
```
)DOCS_MD");
#if USE_LIBSTEMMER
    attachNoDescription<StorageSystemStemmers>(context, system_database, "stemmers", R"DOCS_MD(
.description
Shows all available stemmers.
These can be used in the function [stem](/reference/functions/regular-functions/nlp-functions).

<Info>
**Availability**

`system.stemmers` is present only in ClickHouse builds compiled with the `libstemmer` dependency (`USE_LIBSTEMMER`). On builds without it, the table does not exist and queries against it will fail with `UNKNOWN_TABLE`. You can check whether your build has it enabled with:

```sql
SELECT value FROM system.build_options WHERE name = 'USE_LIBSTEMMER';
```
</Info>

.examples
```sql
SELECT * FROM system.stemmers;
```

```text
 ┌─name───────┐
 │ arabic     │
 │ armenian   │
 │ basque     │
 │ catalan    │
 │ danish     │
 │ dutch      │
 │ english    │
 │ [...]      │
 └────────────┘
```
)DOCS_MD");
#endif
    attach<StorageSystemCertificates>(context, system_database, "certificates", R"DOCS_MD(
.description
Contains information about available certificates and their sources.
)DOCS_MD");
    attachNoDescription<StorageSystemNamedCollections>(context, system_database, "named_collections", R"DOCS_MD(
.description
Contains a list of all named collections which were created via SQL query or parsed from configuration file.
)DOCS_MD");
    attachNoDescription<StorageSystemHandlers>(context, system_database, "handlers", R"DOCS_MD(
.description
Contains a list of all SQL-defined HTTP handlers created via CREATE HANDLER.
)DOCS_MD");
    attach<StorageSystemAsyncLoader>(context, system_database, "asynchronous_loader", R"DOCS_MD(
.description
Contains information and status for recent asynchronous jobs (e.g. for tables loading). The table contains a row for every job. There is a tool for visualizing information from this table `utils/async_loader_graph`.

.columns_notes
A pending job might be in one of the following states:
- `is_executing` (`UInt8`) - The job is currently being executed by a worker.
- `is_blocked` (`UInt8`) - The job waits for its dependencies to be done.
- `is_ready` (`UInt8`) - The job is ready to be executed and waits for a worker.
- `elapsed` (`Float64`) - Seconds elapsed since start of execution. Zero if job is not started. Total execution time if job finished.

Every job has a pool associated with it and is started in this pool. Each pool has a constant priority and a mutable maximum number of workers. Higher priority (lower `priority` value) jobs are run first. No job with lower priority is started while there is at least one higher priority job ready or executing. Job priority can be elevated (but cannot be lowered) by prioritizing it. For example jobs for a table loading and startup will be prioritized if incoming query required this table. It is possible prioritize a job during its execution, but job is not moved from its `execution_pool` to newly assigned `pool`. The job uses `pool` for creating new jobs to avoid priority inversion. Already started jobs are not preempted by higher priority jobs and always run to completion after start.
- `pool_id` (`UInt64`) - ID of a pool currently assigned to the job.
- `pool` (`String`) - Name of `pool_id` pool.
- `priority` (`Int64`) - Priority of `pool_id` pool.
- `execution_pool_id` (`UInt64`) - ID of a pool the job is executed in. Equals initially assigned pool before execution starts.
- `execution_pool` (`String`) - Name of `execution_pool_id` pool.
- `execution_priority` (`Int64`) - Priority of `execution_pool_id` pool.

- `ready_seqno` (`Nullable(UInt64)`) - Not null for ready jobs. Worker pulls the next job to be executed from a ready queue of its pool. If there are multiple ready jobs, then job with the lowest value of `ready_seqno` is picked.
- `waiters` (`UInt64`) - The number of threads waiting on this job.
- `exception` (`Nullable(String)`) - Not null for failed and canceled jobs. Holds error message raised during query execution or error leading to cancelling of this job along with dependency failure chain of job names.

Time instants during job lifetime:
- `schedule_time` (`DateTime64`) - Time when job was created and scheduled to be executed (usually with all its dependencies).
- `enqueue_time` (`Nullable(DateTime64)`) - Time when job became ready and was enqueued into a ready queue of its pool. Null if the job is not ready yet.
- `start_time` (`Nullable(DateTime64)`) - Time when worker dequeues the job from ready queue and start its execution. Null if the job is not started yet.
- `finish_time` (`Nullable(DateTime64)`) - Time when job execution is finished. Null if the job is not finished yet.

.examples
```sql
SELECT *
FROM system.asynchronous_loader
LIMIT 1
FORMAT Vertical
```
)DOCS_MD");
    attach<StorageSystemBackgroundSchedulePool>(context, system_database, "background_schedule_pool", R"DOCS_MD(
.description
Contains information about tasks in background schedule pools. Background schedule pools are used for executing periodic tasks such as distributed sends, buffer flushes, message broker operations, streaming queries background jobs, and Iceberg table metadata refresh.

.examples
```sql title="Query"
SELECT * FROM system.background_schedule_pool LIMIT 5 FORMAT Vertical;
```

```text title="Response"
Row 1:
──────
pool:        distributed
database:    default
table:       data
table_uuid:  00000000-0000-0000-0000-000000000000
query_id:
elapsed_ms:  0
log_name:    BackgroundJobsAssignee:DataProcessing
deactivated: 0
scheduled:   1
delayed:     0
executing:   0
```

.see_also
- [system.background_schedule_pool_log](/reference/system-tables/background_schedule_pool_log) — Contains history of background schedule pool task executions.
)DOCS_MD");
    attach<StorageSystemUserProcesses>(context, system_database, "user_processes", R"DOCS_MD(
.description
This system table can be used to get overview of memory usage and ProfileEvents of users.

.examples
```sql
SELECT * FROM system.user_processes LIMIT 10 FORMAT Vertical;
```

```response
Row 1:
──────
user:              default
memory_usage:      9832
peak_memory_usage: 9832
ProfileEvents:     {'Query':5,'SelectQuery':5,'QueriesWithSubqueries':38,'SelectQueriesWithSubqueries':38,'QueryTimeMicroseconds':842048,'SelectQueryTimeMicroseconds':842048,'ReadBufferFromFileDescriptorRead':6,'ReadBufferFromFileDescriptorReadBytes':234,'IOBufferAllocs':3,'IOBufferAllocBytes':98493,'ArenaAllocChunks':283,'ArenaAllocBytes':1482752,'FunctionExecute':670,'TableFunctionExecute':16,'DiskReadElapsedMicroseconds':19,'NetworkSendElapsedMicroseconds':684,'NetworkSendBytes':139498,'SelectedRows':6076,'SelectedBytes':685802,'ContextLock':1140,'RWLockAcquiredReadLocks':193,'RWLockReadersWaitMilliseconds':4,'RealTimeMicroseconds':1585163,'UserTimeMicroseconds':889767,'SystemTimeMicroseconds':13630,'SoftPageFaults':1947,'OSCPUWaitMicroseconds':6,'OSCPUVirtualTimeMicroseconds':903251,'OSReadChars':28631,'OSWriteChars':28888,'QueryProfilerRuns':3,'LogTrace':79,'LogDebug':24}

1 row in set. Elapsed: 0.010 sec.
```
)DOCS_MD");
    attachNoDescription<StorageSystemJemallocBins>(context, system_database, "jemalloc_bins", R"DOCS_MD(
.description
Contains information about memory allocations done via jemalloc allocator in different size classes (bins) aggregated from all arenas.
These statistics might not be absolutely accurate because of thread local caching in jemalloc.

.examples
Find the sizes of allocations that contributed the most to the current overall memory usage.

```sql
SELECT
    *,
    allocations - deallocations AS active_allocations,
    size * active_allocations AS allocated_bytes
FROM system.jemalloc_bins
WHERE allocated_bytes > 0
ORDER BY allocated_bytes DESC
LIMIT 10
```

```text
┌─index─┬─large─┬─────size─┬─allocactions─┬─deallocations─┬─active_allocations─┬─allocated_bytes─┐
│    82 │     1 │ 50331648 │            1 │             0 │                  1 │        50331648 │
│    10 │     0 │      192 │       512336 │        370710 │             141626 │        27192192 │
│    69 │     1 │  5242880 │            6 │             2 │                  4 │        20971520 │
│     3 │     0 │       48 │     16938224 │      16559484 │             378740 │        18179520 │
│    28 │     0 │     4096 │       122924 │        119142 │               3782 │        15491072 │
│    61 │     1 │  1310720 │        44569 │         44558 │                 11 │        14417920 │
│    39 │     1 │    28672 │         1285 │           913 │                372 │        10665984 │
│     4 │     0 │       64 │      2837225 │       2680568 │             156657 │        10026048 │
│     6 │     0 │       96 │      2617803 │       2531435 │              86368 │         8291328 │
│    36 │     1 │    16384 │        22431 │         21970 │                461 │         7553024 │
└───────┴───────┴──────────┴──────────────┴───────────────┴────────────────────┴─────────────────┘
```
)DOCS_MD");
    attachNoDescription<StorageSystemJemallocProfileText>(context, system_database, "jemalloc_profile_text", R"DOCS_MD(
.description
Displays the symbolized jemalloc heap profile. Run 'SYSTEM JEMALLOC FLUSH PROFILE' to generate a profile first.
)DOCS_MD");
    attach<StorageSystemJemallocStats>(context, system_database, "jemalloc_stats", R"DOCS_MD(
.description
Returns jemalloc statistics in a single row with a single column. Equivalent to SYSTEM JEMALLOC STATS command.
)DOCS_MD");
    attachNoDescription<StorageSystemObjectStorageQueueMetadataCache<ObjectStorageType::S3>>(context, system_database, "s3queue_metadata_cache", R"DOCS_MD(
.description
Contains in-memory state of S3Queue metadata and currently processed rows per file.
)DOCS_MD");
    attachNoDescription<StorageSystemObjectStorageQueueMetadataCache<ObjectStorageType::Azure>>(context, system_database, "azure_queue_metadata_cache", R"DOCS_MD(
.description
Contains in-memory state of AzureQueue metadata and currently processed rows per file.
)DOCS_MD");
    attachNoDescription<StorageSystemObjectStorageQueueMetadata<ObjectStorageType::S3>>(context, system_database, "s3_queue_metadata", R"DOCS_MD(
.description
Contains the current number of processed, processing and failed nodes in keeper for each S3Queue metadata object and, on demand, their contents. Unlike system.s3queue_metadata_cache, which shows the in-memory cache, this table reads the state directly from keeper.
)DOCS_MD");
    attachNoDescription<StorageSystemObjectStorageQueueMetadata<ObjectStorageType::Azure>>(context, system_database, "azure_queue_metadata", R"DOCS_MD(
.description
Contains the current number of processed, processing and failed nodes in keeper for each AzureQueue metadata object and, on demand, their contents. Unlike system.azure_queue_metadata_cache, which shows the in-memory cache, this table reads the state directly from keeper.
)DOCS_MD");
    attach<StorageSystemObjectStorageQueueSettings<ObjectStorageType::S3>>(context, system_database, "s3_queue_settings", R"DOCS_MD(
.description
Contains information about the settings of [S3Queue](/reference/engines/table-engines/integrations/s3queue) tables. Available from server version `24.10`.
)DOCS_MD");
    attach<StorageSystemObjectStorageQueueSettings<ObjectStorageType::Azure>>(context, system_database, "azure_queue_settings", R"DOCS_MD(
.description
Contains information about settings of [AzureQueue](/reference/engines/table-engines/integrations/azure-queue) tables.
Available from `24.10` server version.
)DOCS_MD");
    attach<StorageSystemDashboards>(context, system_database, "dashboards", R"DOCS_MD(
.description
Contains queries used by `/dashboard` page accessible though [HTTP interface](/concepts/features/interfaces/http).
This table can be useful for monitoring and troubleshooting. The table contains a row for every chart in a dashboard.

<Note>
`/dashboard` page can render queries not only from `system.dashboards`, but from any table with the same schema.
This can be useful to create custom dashboards.
</Note>

.examples
```sql
SELECT *
FROM system.dashboards
WHERE title ILIKE '%CPU%'
```

```text
Row 1:
──────
dashboard: overview
title:     CPU Usage (cores)
query:     SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(ProfileEvent_OSCPUVirtualTimeMicroseconds) / 1000000
FROM system.metric_log
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}

Row 2:
──────
dashboard: overview
title:     CPU Wait
query:     SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(ProfileEvent_OSCPUWaitMicroseconds) / 1000000
FROM system.metric_log
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}

Row 3:
──────
dashboard: overview
title:     OS CPU Usage (Userspace)
query:     SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(value)
FROM system.asynchronous_metric_log
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} AND metric = 'OSUserTimeNormalized'
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}

Row 4:
──────
dashboard: overview
title:     OS CPU Usage (Kernel)
query:     SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(value)
FROM system.asynchronous_metric_log
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} AND metric = 'OSSystemTimeNormalized'
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
```
)DOCS_MD");
    attach<StorageSystemViewRefreshes>(context, system_database, "view_refreshes", R"DOCS_MD(
.description
Information about [Refreshable Materialized Views](/reference/statements/create/view#refreshable-materialized-view). Contains all refreshable materialized views, regardless of whether there's a refresh in progress or not.

.examples
```sql
SELECT
    database,
    view,
    status,
    last_refresh_time,
    next_refresh_time
FROM system.view_refreshes

┌─database─┬─view───────────────────────┬─status────┬───last_refresh_time─┬───next_refresh_time─┐
│ default  │ hello_documentation_reader │ Scheduled │ 2023-12-01 01:24:00 │ 2023-12-01 01:25:00 │
└──────────┴────────────────────────────┴───────────┴─────────────────────┴─────────────────────┘
```
)DOCS_MD");
    attach<StorageSystemWorkloads>(context, system_database, "workloads", R"DOCS_MD(
.description
Contains information for [workloads](/concepts/features/configuration/server-config/workload-scheduling#workload_entity_storage) residing on the local server. The table contains a row for every workload.

.examples
```sql
SELECT *
FROM system.workloads
FORMAT Vertical
```

```text
Row 1:
──────
name:         production
parent:       all
create_query: CREATE WORKLOAD production IN `all` SETTINGS weight = 9

Row 2:
──────
name:         development
parent:       all
create_query: CREATE WORKLOAD development IN `all`

Row 3:
──────
name:         all
parent:
create_query: CREATE WORKLOAD `all`
```
)DOCS_MD");
    attach<StorageSystemResources>(context, system_database, "resources", R"DOCS_MD(
.description
Contains information about [resources](/concepts/features/configuration/server-config/workload-scheduling#workload_entity_storage) residing on the local server. The table contains a row for every resource.

.examples
```sql
SELECT *
FROM system.resources
FORMAT Vertical
```

```text
Row 1:
──────
name:         io_read
read_disks:   ['s3']
write_disks:  []
create_query: CREATE RESOURCE io_read (READ DISK s3)

Row 2:
──────
name:         io_write
read_disks:   []
write_disks:  ['s3']
create_query: CREATE RESOURCE io_write (WRITE DISK s3)
```
)DOCS_MD");
    attach<StorageSystemIcebergHistory>(context, system_database, "iceberg_history", R"DOCS_MD(
.description
This system table contains the snapshot history of Iceberg tables existing in ClickHouse. It will be empty if you don't have any Iceberg tables in ClickHouse.
)DOCS_MD");
    attachNoDescription<StorageSystemIcebergFiles>(context, system_database, "iceberg_files", R"DOCS_MD(
.description
This system table contains per-file metadata for Iceberg tables existing in ClickHouse, with one row per data or delete file referenced by the current snapshot of each table. It will be empty if you don't have any Iceberg tables in ClickHouse.
)DOCS_MD");
#if USE_ICU
    attach<StorageSystemUnicode>(context, system_database, "unicode", R"DOCS_MD(
.description
The `system.unicode` table is a virtual table that provides information about Unicode characters and their properties(https://unicode-org.github.io/icu/userguide/strings/properties.html). This table is generated on-the-fly.

The property names of Unicode code points from ICU are converted to `snake_case` for use as column names.

.examples
```sql
SELECT * FROM system.unicode WHERE code_point = 'a' LIMIT 1;
```

```text
Row 1:
──────
code_point:                      a
code_point_value:                97
notation:                        U+0061
alphabetic:                      1
ascii_hex_digit:                 1
bidi_control:                    0
bidi_mirrored:                   0
dash:                            0
default_ignorable_code_point:    0
deprecated:                      0
diacritic:                       0
extender:                        0
full_composition_exclusion:      0
grapheme_base:                   1
grapheme_extend:                 0
grapheme_link:                   0
hex_digit:                       1
hyphen:                          0
id_continue:                     1
id_start:                        1
ideographic:                     0
ids_binary_operator:             0
ids_trinary_operator:            0
join_control:                    0
logical_order_exception:         0
lowercase:                       1
math:                            0
noncharacter_code_point:         0
quotation_mark:                  0
radical:                         0
soft_dotted:                     0
terminal_punctuation:            0
unified_ideograph:               0
uppercase:                       0
white_space:                     0
xid_continue:                    1
xid_start:                       1
case_sensitive:                  1
sentence_terminal:               0
variation_selector:              0
nfd_inert:                       1
nfkd_inert:                      1
nfc_inert:                       0
nfkc_inert:                      0
segment_starter:                 1
pattern_syntax:                  0
pattern_white_space:             0
alnum:                           1
blank:                           0
graph:                           1
print:                           1
xdigit:                          1
cased:                           1
case_ignorable:                  0
changes_when_lowercased:         0
changes_when_uppercased:         1
changes_when_titlecased:         1
changes_when_casefolded:         0
changes_when_casemapped:         1
changes_when_nfkc_casefolded:    0
emoji:                           0
emoji_presentation:              0
emoji_modifier:                  0
emoji_modifier_base:             0
emoji_component:                 0
regional_indicator:              0
prepended_concatenation_mark:    0
extended_pictographic:           0
basic_emoji:                     0
emoji_keycap_sequence:           0
rgi_emoji_modifier_sequence:     0
rgi_emoji_flag_sequence:         0
rgi_emoji_tag_sequence:          0
rgi_emoji_zwj_sequence:          0
rgi_emoji:                       0
ids_unary_operator:              0
id_compat_math_start:            0
id_compat_math_continue:         0
bidi_class:                      0
block:                           1
canonical_combining_class:       0
decomposition_type:              0
east_asian_width:                4
general_category:                2
joining_group:                   0
joining_type:                    0
line_break:                      2
numeric_type:                    0
script:                          25
hangul_syllable_type:            0
nfd_quick_check:                 1
nfkd_quick_check:                1
nfc_quick_check:                 1
nfkc_quick_check:                1
lead_canonical_combining_class:  0
trail_canonical_combining_class: 0
grapheme_cluster_break:          0
sentence_break:                  4
word_break:                      1
bidi_paired_bracket_type:        0
indic_positional_category:       0
indic_syllabic_category:         0
vertical_orientation:            0
identifier_status:               1
general_category_mask:           4
numeric_value:                   0
age:                             1.1
bidi_mirroring_glyph:            a
case_folding:                    a
lowercase_mapping:               a
name:                            LATIN SMALL LETTER A
simple_case_folding:             a
simple_lowercase_mapping:        a
simple_titlecase_mapping:        A
simple_uppercase_mapping:        A
titlecase_mapping:               A
uppercase_mapping:               A
bidi_paired_bracket:             a
script_extensions:               ['Latin']
identifier_type:                 ['Recommended']

```

```sql
SELECT code_point, code_point_value, notation FROM system.unicode WHERE code_point = '😂';
```
```text
   ┌─code_point─┬─code_point_value─┬─notation─┐
1. │ 😂          │           128514 │ U+1F602  │
   └────────────┴──────────────────┴──────────┘
```
)DOCS_MD");
#endif

    if (has_zookeeper)
    {
        attachNoDescription<StorageSystemZooKeeper>(context, system_database, "zookeeper", R"DOCS_MD(
.description
The table does not exist unless ClickHouse Keeper or ZooKeeper is configured. The `system.zookeeper` table exposes data from the Keeper clusters defined in the config.
The query must either have a `path =`   condition or a `path IN`  condition set with the `WHERE` clause as shown below. This corresponds to the path of the children that you want to get data for.

The query `SELECT * FROM system.zookeeper WHERE path = '/clickhouse'` outputs data for all children on the `/clickhouse` node.
To output data for all root nodes, write path = '/'.
If the path specified in 'path' does not exist, an exception will be thrown.

The query `SELECT * FROM system.zookeeper WHERE path IN ('/', '/clickhouse')` outputs data for all children on the `/` and `/clickhouse` node.
If in the specified 'path' collection has does not exist path, an exception will be thrown.
It can be used to do a batch of Keeper path queries.

The query `SELECT * FROM system.zookeeper WHERE path = '/clickhouse' AND zookeeperName = 'auxiliary_cluster'` outputs data in `auxiliary_cluster` ZooKeeper cluster.
If the specified 'auxiliary_cluster' does not exists, an exception will be thrown.

.examples
```sql
SELECT *
FROM system.zookeeper
WHERE path = '/clickhouse/tables/01-08/visits/replicas'
FORMAT Vertical
```

```text
Row 1:
──────
name:           example01-08-1
value:
czxid:          932998691229
mzxid:          932998691229
ctime:          2015-03-27 16:49:51
mtime:          2015-03-27 16:49:51
version:        0
cversion:       47
aversion:       0
ephemeralOwner: 0
dataLength:     0
numChildren:    7
pzxid:          987021031383
path:           /clickhouse/tables/01-08/visits/replicas

Row 2:
──────
name:           example01-08-2
value:
czxid:          933002738135
mzxid:          933002738135
ctime:          2015-03-27 16:57:01
mtime:          2015-03-27 16:57:01
version:        0
cversion:       37
aversion:       0
ephemeralOwner: 0
dataLength:     0
numChildren:    7
pzxid:          987021252247
path:           /clickhouse/tables/01-08/visits/replicas
```
)DOCS_MD");
        attach<StorageSystemZooKeeperInfo>(context, system_database, "zookeeper_info", R"DOCS_MD(
.description
This table outputs combined introspection about zookeeper and the nodes are taken from config.

<Info>
**Availability**

`system.zookeeper_info` exists only when ClickHouse Keeper or ZooKeeper is configured. On servers without either configured, the table does not exist and queries against it will fail with `UNKNOWN_TABLE`.
</Info>
)DOCS_MD");
        attach<StorageSystemZooKeeperConnection>(context, system_database, "zookeeper_connection", R"DOCS_MD(
.description
This table does not exist if ZooKeeper is not configured. The 'system.zookeeper_connection' table shows current connections to ZooKeeper (including auxiliary ZooKeepers). Each row shows information about one connection.

.examples
```sql
SELECT
    name,
    host,
    port,
    index,
    connected_time,
    session_uptime_elapsed_seconds,
    is_expired,
    keeper_api_version,
    client_id,
    xid,
    enabled_feature_flags,
    availability_zone
FROM system.zookeeper_connection;
```

```text
┌─name────┬─host──────┬─port─┬─index─┬──────connected_time─┬─session_uptime_elapsed_seconds─┬─is_expired─┬─keeper_api_version─┬─client_id─┬─xid─┬─enabled_feature_flags────────────────────────────────────────────────────┬─availability_zone─┐
│ default │ 127.0.0.1 │ 2181 │     0 │ 2025-04-10 14:30:00 │                            943 │          0 │                  0 │       420 │  69 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS'] │ eu-west-1b        │
└─────────┴───────────┴──────┴───────┴─────────────────────┴────────────────────────────────┴────────────┴────────────────────┴───────────┴─────┴──────────────────────────────────────────────────────────────────────────┴───────────────────┘
```
)DOCS_MD");
        attach<StorageSystemZooKeeperWatches>(context, system_database, "zookeeper_watches", R"DOCS_MD(
.description
Shows currently active [watches](https://zookeeper.apache.org/doc/r3.3.3/zookeeperProgrammers.html#ch_zkWatches) registered by this ClickHouse server on ZooKeeper nodes (including auxiliary ZooKeepers). Each row represents one watch.

<Info>
**Availability**

`system.zookeeper_watches` exists only when ClickHouse Keeper or ZooKeeper is configured. On servers without either configured, the table does not exist and queries against it will fail with `UNKNOWN_TABLE`.
</Info>

.examples
```sql
SELECT * FROM system.zookeeper_watches FORMAT Vertical;
```

```text
Row 1:
──────
zookeeper_name:           default
create_time:              2026-03-16 12:00:00
create_time_microseconds: 2026-03-16 12:00:00.123456
path:                     /clickhouse/task_queue/ddl
session_id:               106662742089334927
request_xid:              10858
op_num:                   List
watch_type:               Children
```

.see_also
-   [ZooKeeper](/guides/oss/best-practices/tips#zookeeper)
-   [ZooKeeper guide](https://zookeeper.apache.org/doc/r3.3.3/zookeeperProgrammers.html)
)DOCS_MD");
    }

#if USE_NURAFT
    if (has_keeper_server)
    {
        attach<StorageSystemKeeperSnapshots>(context, system_database, "keeper_snapshots", R"DOCS_MD(
.description
This table does not exist if this node is not configured to run an in-process ClickHouse Keeper. It contains one row per Raft snapshot file tracked by the in-process Keeper state machine, including snapshots currently being received from the leader.

.examples
```sql
SELECT * FROM system.keeper_snapshots ORDER BY last_log_index;
```

```text
┌─last_log_index─┬─path──────────────────────────┬─disk_name─┬─size_bytes─┬────last_modified_at─┬─is_received─┬─exists_on_disk─┐
│           1000 │ snapshot_1000.bin.zstd        │ default   │      32468 │ 2026-05-22 14:00:00 │ false       │ true           │
│           2000 │ snapshot_2000.bin.zstd        │ default   │      48217 │ 2026-05-22 14:15:00 │ false       │ true           │
└────────────────┴───────────────────────────────┴───────────┴────────────┴─────────────────────┴─────────────┴────────────────┘
```
)DOCS_MD");
        attach<StorageSystemKeeperCluster>(context, system_database, "keeper_cluster", R"DOCS_MD(
.description
This table does not exist if this node is not configured to run an in-process ClickHouse Keeper. It contains one row per Raft cluster member, fusing static cluster topology (from the Raft configuration) with the local node's own log position.

Every node fills exactly one `last_log_index` value — the row matching its own `server_id`. Peer log positions are not surfaced here because they are tracked only on the leader and that view is not symmetric across the cluster.

.examples
```sql
SELECT * FROM system.keeper_cluster ORDER BY server_id;
```

```text
┌─server_id─┬─host──┬─endpoint───┬─is_observer─┬─priority─┬─is_leader─┬─is_self─┬─last_log_index─┐
│         1 │ node1 │ node1:9234 │ false       │        3 │ true      │ true    │             42 │
│         2 │ node2 │ node2:9234 │ false       │        2 │ false     │ false   │           ᴺᵁᴸᴸ │
│         3 │ node3 │ node3:9234 │ true        │        1 │ false     │ false   │           ᴺᵁᴸᴸ │
└───────────┴───────┴────────────┴─────────────┴──────────┴───────────┴─────────┴────────────────┘
```
)DOCS_MD");
        attach<StorageSystemKeeperChangelogs>(context, system_database, "keeper_changelogs", R"DOCS_MD(
.description
This table does not exist if this node is not configured to run an in-process ClickHouse Keeper. It contains one row per Raft changelog file (`changelog_<from>_<to>.bin[.zstd]`) tracked by the in-process Keeper log store, including the active file currently being appended to.

.examples
```sql
SELECT from_log_index, to_log_index, entries, path, active FROM system.keeper_changelogs ORDER BY from_log_index;
```

```text
┌─from_log_index─┬─to_log_index─┬─entries─┬─path───────────────────────────┬─active─┐
│              1 │         1000 │    1000 │ changelog_1_1000.bin.zstd      │ false  │
│           1001 │         2000 │     537 │ changelog_1001_2000.bin.zstd   │ true   │
└────────────────┴──────────────┴─────────┴────────────────────────────────┴────────┘
```
)DOCS_MD");
        attachNoDescription<StorageSystemKeeperStorage>(context, system_database, "keeper_storage", R"DOCS_MD(
.description
The table only exists for ClickHouse Keeper deployments that use the `clickhouse server` (and not `clickhouse keeper`) process. It contains one row per node of the data tree stored on the local Keeper node, including the `/keeper` system nodes.

Unlike `system.zookeeper`, this table does not send requests to a Keeper cluster. It reads the committed state of the local Keeper directly from a consistent lock-free view, without affecting request processing. Reading the table does not require a path condition and returns the whole tree, so it is suitable for queries that scan all nodes, such as finding the nodes with the most children or the largest data.

.examples
```sql
SELECT path, num_children, data_length
FROM system.keeper_storage
ORDER BY num_children DESC
LIMIT 3;
```

```text
┌─path──────────────┬─num_children─┬─data_length─┐
│ /                 │            3 │           0 │
│ /clickhouse/tasks │            2 │           0 │
│ /keeper           │            1 │           0 │
└───────────────────┴──────────────┴─────────────┘
```
)DOCS_MD");
    }
#endif

    if (context->getConfigRef().getInt("allow_experimental_transactions", 0))
    {
        attach<StorageSystemTransactions>(context, system_database, "transactions", R"DOCS_MD(
.description
Contains a list of transactions and their state.

<Info>
**Availability**

`system.transactions` is created only when the `allow_experimental_transactions` server configuration option is enabled. By default, the table does not exist and queries against it will fail with `UNKNOWN_TABLE`. Enable it in the server configuration with:

```xml
<clickhouse>
    <allow_experimental_transactions>1</allow_experimental_transactions>
</clickhouse>
```
</Info>
)DOCS_MD");
    }

    if (context->getConfigRef().getBool("query_log.enable_user_query_log", true))
    {
        /// The query log table is always created in the `system` database: `SystemLog::createSystemLog` coerces any
        /// other configured `query_log.database` back to `system`. So the collision with `system.user_query_log`
        /// happens for `query_log.table = user_query_log` regardless of the configured `query_log.database`.
        if (context->getConfigRef().getString("query_log.table", "query_log") == "user_query_log")
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "The `query_log.table` server setting cannot be set to `user_query_log`: "
                "the query log table is always created in the `system` database, where `system.user_query_log` "
                "shows the query log records of the current user. "
                "Rename the query log table or set `query_log.enable_user_query_log` to 0");

        /// A table with this name could have been created by a user before upgrading to a version with `system.user_query_log`.
        if (system_database.isTableExist("user_query_log", context))
            throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS,
                "Table `system.user_query_log` already exists, but this name is used for the query log records of the current user. "
                "Rename or drop the existing table, or set `query_log.enable_user_query_log` to 0");

        attach<StorageSystemUserQueryLog>(context, system_database, "user_query_log",
            R"DOCS_MD(
.description
Shows the current user their own query log records. It reads the query log table configured by the `query_log.database` and `query_log.table` server settings (`system.query_log` by default) and returns only the rows whose initiating user is equal to `currentUser()` (the initiating user is taken from `initial_user` when it is set, otherwise from `user`).

Unlike the query log table itself, `system.user_query_log` can be read without any grants, so users can inspect their own queries without being given access to the queries of others.

This is only supported when the query log is stored locally. If `query_log.engine` is configured as `Distributed` or any other engine that delegates reads to another server, `system.user_query_log` refuses to read from it and throws an exception, because the required access check cannot be enforced across a ClickHouse-protocol server boundary. In that case, disable the table with `query_log.enable_user_query_log = 0`.

<Info>
**Availability**

`system.user_query_log` is attached only when the `query_log.enable_user_query_log` server setting is enabled, which is the default. When the setting is `0`, the table does not exist and queries against it fail with `UNKNOWN_TABLE`.

When `query_log.enable_user_query_log` is enabled but the backing query log is not configured or its table has not been created yet, `system.user_query_log` exists but is empty.
</Info>

Conditions on the partition and key columns of the query log (`event_date`, `event_time`, `query_start_time`, `query_id`, `type`, and similar scalar columns) compared with constants are pushed down to the backing query log table, so ordinary lookups such as the example below keep partition pruning and do not scan the whole retained log.

<Warning>
If a table named `system.user_query_log` was created before upgrading to a ClickHouse version that provides this table, the server will not start until the existing table is renamed or dropped, or `query_log.enable_user_query_log` is set to `0`.
</Warning>

.examples
```sql
SELECT
    query_start_time,
    query_duration_ms,
    query
FROM system.user_query_log
ORDER BY query_start_time DESC
LIMIT 10;
```
)DOCS_MD");
    }

    attach<StorageSystemCodecs>(context, system_database, "codecs", R"DOCS_MD(
.description
Contains information about compression and encryption codecs.

You can use this table to get information about the available compression and encryption codecs

.examples
```sql title="Query"
SELECT * FROM system.codecs WHERE name='LZ4'
```

```text title="Response"
Row 1:
──────
name:                   LZ4
method_byte:            130
is_compression:         1
is_generic_compression: 1
is_encryption:          0
is_timeseries_codec:    0
is_experimental:        0
description:            Extremely fast; good compression; balanced speed and efficiency.
```
)DOCS_MD");
    attach<StorageSystemCompletions>(context, system_database, "completions", R"DOCS_MD(
.description
Contains a list of completion tokens.
)DOCS_MD");

    attach<StorageSystemFailPoints>(context, system_database, "fail_points", R"DOCS_MD(
.description
Contains a list of all available failpoints registered in the server, along with their type and whether they are currently enabled.

In builds with failpoint support (`USE_LIBFIU=1`), failpoints can be enabled and disabled at runtime using the `SYSTEM ENABLE FAILPOINT` and `SYSTEM DISABLE FAILPOINT` statements. These statements are not available in builds without failpoint support, although the table remains available.

.examples
This example requires a build with failpoint support (`USE_LIBFIU=1`).

```sql title="Query"
SYSTEM ENABLE FAILPOINT replicated_merge_tree_insert_retry_pause;
SELECT * FROM system.fail_points WHERE enabled = 1
```

```text title="Response"
┌─name──────────────────────────────────────┬─type────────────┬─enabled─┐
│ replicated_merge_tree_insert_retry_pause  │ pauseable_once  │       1 │
└───────────────────────────────────────────┴─────────────────┴─────────┘
```
)DOCS_MD");

    if (context->hasWasmModuleManager())
    {
        attach<StorageSystemWasmModules>(context, system_database, "webassembly_modules", "Allows to load Webassembly modules into ClickHouse to create User Defined Functions from them.",
            context->getWasmModuleManager());
    }
}

void attachSystemTablesAsync(ContextPtr context, IDatabase & system_database, AsynchronousMetrics & async_metrics)
{
    attachNoDescription<StorageSystemAsynchronousMetrics>(context, system_database, "asynchronous_metrics", R"DOCS_MD(
.description
Contains metrics that are calculated periodically in the background. For example, the amount of RAM in use.

A metric is either scalar (a single number in the `value` column) or key-value: broken down per entity, such as per CPU core, block device, network interface, or disk. Key-value metrics are represented as a single row with a [Map](/reference/data-types/map) in the `key_values` column (for example, `CPUFrequencyMHz` maps every CPU core number to its frequency), and their `value` column is `NaN`.

## Metric descriptions {#metric-descriptions}

The descriptions below are generated from the C++ source by `utils/generate-async-metrics-docs`. The single source of truth is the string literal next to each metric registration in `src/Common/AsynchronousMetrics.cpp`, `src/Interpreters/ServerAsynchronousMetrics.cpp`, and `src/Coordination/KeeperAsynchronousMetrics.cpp`. Metric names that include a variable part (currently only the HTTP connection pool group) are shown with a `*name*` placeholder; the running server reports them with the concrete name substituted in.

{/*AUTOGENERATED_METRICS_START*/}
{{ASYNCHRONOUS_METRICS}}
{/*AUTOGENERATED_METRICS_END*/}

.examples
```sql
SELECT metric, value, key_values
FROM system.asynchronous_metrics
WHERE metric IN ('MemoryResident', 'OSUserTime', 'DiskTotal', 'BlockReadBytes', 'NetworkReceiveBytes')
ORDER BY metric
```

```text
┌─metric──────────────┬─────────────value─┬─key_values──────────────────────────────────────┐
│ BlockReadBytes      │               nan │ {'nvme0n1':67420160}                            │
│ DiskTotal           │               nan │ {'backups':34359738368,'default':5199475388416} │
│ MemoryResident      │         970743808 │ {}                                              │
│ NetworkReceiveBytes │               nan │ {'docker0':0,'ens66':334554363}                 │
│ OSUserTime          │ 32.11807291562506 │ {}                                              │
└─────────────────────┴───────────────────┴─────────────────────────────────────────────────┘
```

A single entity of a key-value metric can be extracted with the map subscript:

```sql
SELECT key_values['default'] AS default_disk_total
FROM system.asynchronous_metrics
WHERE metric = 'DiskTotal'
```

.see_also
- [Monitoring](/guides/oss/deployment-and-scaling/monitoring/monitoring) — Base concepts of ClickHouse monitoring.
- [system.metrics](/reference/system-tables/metrics) — Contains instantly calculated metrics.
- [system.events](/reference/system-tables/events) — Contains a number of events that have occurred.
- [system.metric_log](/reference/system-tables/metric_log) — Contains a history of metrics values from tables `system.metrics` and `system.events`.
)DOCS_MD", async_metrics);
}

}
