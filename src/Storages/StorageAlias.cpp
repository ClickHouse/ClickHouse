#include <Storages/StorageAlias.h>
#include <Storages/StorageFactory.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/BlockIO.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Core/Settings.h>
#include <Access/Common/AccessFlags.h>


namespace DB
{

namespace Setting
{
    extern const SettingsSeconds lock_acquire_timeout;
}

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
}

StorageAlias::StorageAlias(
    const StorageID & table_id_,
    ContextPtr context_,
    const String & target_database_,
    const String & target_table_)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , target_database(target_database_)
    , target_table(target_table_)
{
    StorageID target_id(target_database, target_table);
    if (table_id_ == target_id)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Alias table cannot refer to itself");

    // Disallow target is also an alias
    auto target_storage = DatabaseCatalog::instance().tryGetTable(target_id, context_);
    if (target_storage && target_storage->getName() == "Alias")
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Alias table cannot refer to another Alias table");
}

StoragePtr StorageAlias::getTargetTable(std::optional<TargetAccess> access_check) const
{
    if (access_check)
    {
        if (access_check->column_names.empty())
            access_check->context->checkAccess(access_check->access_type, target_database, target_table);
        else
            access_check->context->checkAccess(access_check->access_type, target_database, target_table, access_check->column_names);
    }

    return DatabaseCatalog::instance().getTable(StorageID(target_database, target_table), getContext());
}

/// AliasSink: Writes data to the target table using full INSERT pipeline
/// which triggers materialized views on the target table.
class AliasSink final : public SinkToStorage, WithContext
{
public:
    AliasSink(
        StorageAlias & storage_,
        ContextPtr context_,
        const StorageMetadataPtr & metadata_snapshot_)
        : SinkToStorage(std::make_shared<const Block>(metadata_snapshot_->getSampleBlock()))
        , WithContext(context_)
        , storage(storage_)
        , non_materialized_header(metadata_snapshot_->getSampleBlockNonMaterialized())
    {
    }

    String getName() const override { return "AliasSink"; }

    void onStart() override
    {
        StoragePtr target = storage.getTargetTable();

        std::unique_ptr<ASTInsertQuery> insert = std::make_unique<ASTInsertQuery>();
        insert->table_id = target->getStorageID();
        ASTPtr query_ptr(insert.release());

        auto insert_context = Context::createCopy(getContext());
        insert_context->makeQueryContext();
        addInterpreterContext(insert_context);

        InterpreterInsertQuery interpreter(
            query_ptr,
            insert_context,
            /* allow_materialized */ false,
            /* no_squash */ false,
            /* no_destination */ false,
            /* async_insert */ false);

        block_io = interpreter.execute();
        executor = std::make_unique<PushingPipelineExecutor>(block_io.pipeline);
        executor->start();
    }

    void consume(Chunk & chunk) override
    {
        if (chunk.getNumRows() == 0)
            return;

        auto block = getHeader().cloneWithColumns(chunk.getColumns());

        Block non_materialized_block;
        for (const auto & col : non_materialized_header)
            non_materialized_block.insert(block.getByName(col.name));

        Chunk non_materialized_chunk(non_materialized_block.getColumns(), non_materialized_block.rows());
        non_materialized_chunk.setChunkInfos(chunk.getChunkInfos().clone());
        executor->push(std::move(non_materialized_chunk));
    }

    void onFinish() override
    {
        executor->finish();
        executor.reset();

        block_io.onFinish();
        block_io = {};
    }

    void onException(std::exception_ptr) override
    {
        if (executor)
            executor->cancel();
        executor.reset();
        block_io.onException();
        block_io = {};
    }

private:
    StorageAlias & storage;
    Block non_materialized_header;
    BlockIO block_io;
    std::unique_ptr<PushingPipelineExecutor> executor;
};

void StorageAlias::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & /*storage_snapshot*/,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    auto target_storage = getTargetTable(TargetAccess{local_context, AccessType::SELECT, column_names});
    auto lock = target_storage->lockForShare(
        local_context->getCurrentQueryId(),
        local_context->getSettingsRef()[Setting::lock_acquire_timeout]);

    auto target_metadata = target_storage->getInMemoryMetadataPtr(local_context, false);
    auto target_snapshot = target_storage->getStorageSnapshot(target_metadata, local_context);

    target_storage->read(
        query_plan,
        column_names,
        target_snapshot,
        query_info,
        local_context,
        processed_stage,
        max_block_size,
        num_streams);

    query_plan.addStorageHolder(target_storage);
    query_plan.addTableLock(std::move(lock));
}

SinkToStoragePtr StorageAlias::write(
    const ASTPtr & /*query*/,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    ContextPtr local_context,
    bool /*async_insert*/)
{
    auto target_storage = getTargetTable(TargetAccess{local_context, AccessType::INSERT});
    auto target_metadata = target_storage->getInMemoryMetadataPtr(local_context, false);

    /// Use AliasSink which executes full INSERT pipeline on target
    /// Therefore it will trigger the MV on the target
    return std::make_shared<AliasSink>(*this, local_context, target_metadata);
}

void StorageAlias::alter(
    const AlterCommands & params,
    ContextPtr local_context,
    AlterLockHolder & table_lock_holder)
{
    auto target_storage = getTargetTable(TargetAccess{local_context, AccessType::ALTER});

    /// ALTER through alias on a table in a Replicated database is not supported
    /// when the alias and target are in different databases. This is because the
    /// DDL worker path is bypassed and metadata changes won't be replicated to
    /// other replicas in ZooKeeper. If both are in the same Replicated database,
    /// the DDL worker handles the ALTER correctly.
    auto target_storage_id = target_storage->getStorageID();
    if (getStorageID().database_name != target_storage_id.database_name)
    {
        auto target_db = DatabaseCatalog::instance().tryGetDatabase(target_storage_id.database_name);
        if (target_db && target_db->getEngineName() == "Replicated")
        {
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "ALTER through alias is not supported when the target table is in a different Replicated database. "
                "Execute the ALTER directly on the target table: {}",
                target_storage_id.getNameForLogs());
        }
    }

    target_storage->alter(params, local_context, table_lock_holder);
}

void StorageAlias::truncate(
    const ASTPtr & query,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    ContextPtr local_context,
    TableExclusiveLockHolder & table_lock_holder)
{
    auto target_storage = getTargetTable(TargetAccess{local_context, AccessType::TRUNCATE});
    auto target_metadata = target_storage->getInMemoryMetadataPtr(local_context, false);
    target_storage->truncate(query, target_metadata, local_context, table_lock_holder);
}

bool StorageAlias::optimize(
    const ASTPtr & query,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    const ASTPtr & partition,
    bool final,
    bool deduplicate,
    const Names & deduplicate_by_columns,
    bool cleanup,
    ContextPtr local_context)
{
    auto target_storage = getTargetTable(TargetAccess{local_context, AccessType::OPTIMIZE});
    auto target_metadata = target_storage->getInMemoryMetadataPtr(local_context, false);
    return target_storage->optimize(query, target_metadata, partition, final, deduplicate,
                                    deduplicate_by_columns, cleanup, local_context);
}

Pipe StorageAlias::alterPartition(
    const StorageMetadataPtr &,
    const PartitionCommands & commands,
    ContextPtr local_context)
{
    auto target_storage = getTargetTable(TargetAccess{local_context, AccessType::ALTER});
    auto target_metadata = target_storage->getInMemoryMetadataPtr(local_context, false);
    return target_storage->alterPartition(target_metadata, commands, local_context);
}

void StorageAlias::checkAlterPartitionIsPossible(
    const PartitionCommands & commands,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    const Settings & settings,
    ContextPtr local_context) const
{
    auto target_storage = getTargetTable();
    auto target_metadata = target_storage->getInMemoryMetadataPtr(local_context, false);
    target_storage->checkAlterPartitionIsPossible(commands, target_metadata, settings, local_context);
}

void StorageAlias::mutate(const MutationCommands & commands, ContextPtr local_context)
{
    auto target_storage = getTargetTable(TargetAccess{local_context, AccessType::ALTER});
    target_storage->mutate(commands, local_context);
}

QueryPipeline StorageAlias::updateLightweight(const MutationCommands & commands, ContextPtr local_context)
{
    auto target_storage = getTargetTable(TargetAccess{local_context, AccessType::ALTER});
    return target_storage->updateLightweight(commands, local_context);
}

CancellationCode StorageAlias::killMutation(const String & mutation_id)
{
    return getTargetTable()->killMutation(mutation_id);
}

void StorageAlias::waitForMutation(const String & mutation_id, bool wait_for_another_mutation)
{
    getTargetTable()->waitForMutation(mutation_id, wait_for_another_mutation);
}

void StorageAlias::setMutationCSN(const String & mutation_id, UInt64 csn)
{
    getTargetTable()->setMutationCSN(mutation_id, csn);
}

CancellationCode StorageAlias::killPartMoveToShard(const UUID & task_uuid)
{
    return getTargetTable()->killPartMoveToShard(task_uuid);
}

void StorageAlias::updateExternalDynamicMetadataIfExists(ContextPtr local_context)
{
    getTargetTable()->updateExternalDynamicMetadataIfExists(local_context);
}

std::optional<QueryPipeline> StorageAlias::distributedWrite(const ASTInsertQuery & query, ContextPtr local_context)
{
    return getTargetTable(TargetAccess{local_context, AccessType::INSERT})->distributedWrite(query, local_context);
}

StorageSnapshotPtr StorageAlias::getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const
{
    return getTargetTable()->getStorageSnapshot(metadata_snapshot, query_context);
}

StorageSnapshotPtr StorageAlias::getStorageSnapshotWithoutData(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const
{
    return getTargetTable()->getStorageSnapshotWithoutData(metadata_snapshot, query_context);
}

void StorageAlias::rename(const String & /* new_path_to_table_data */, const StorageID & new_table_id)
{
    // Only rename the alias itself, not the target table
    renameInMemory(new_table_id);
}

QueryProcessingStage::Enum StorageAlias::getQueryProcessingStage(
    ContextPtr local_context,
    QueryProcessingStage::Enum to_stage,
    const StorageSnapshotPtr & /* storage_snapshot */,
    SelectQueryInfo & query_info) const
{
    auto target_storage = getTargetTable();
    auto target_metadata = target_storage->getInMemoryMetadataPtr(local_context, false);
    auto target_snapshot = target_storage->getStorageSnapshot(target_metadata, local_context);
    return target_storage->getQueryProcessingStage(local_context, to_stage, target_snapshot, query_info);
}

void registerStorageAlias(StorageFactory & factory)
{
    factory.registerStorage("Alias", [](const StorageFactory::Arguments & args)
    {
        // Supported syntaxes:
        //  CREATE TABLE t2 ENGINE = Alias(t)
        //  CREATE TABLE t2 ENGINE = Alias(db, t)
        //  CREATE TABLE t2 ENGINE = Alias('t')
        //  CREATE TABLE t2 ENGINE = Alias('db', 't')

        auto local_context = args.getLocalContext();

        String target_database;
        String target_table;

        if (args.engine_args.empty())
        {
            // Note: CREATE TABLE ... AS ... syntax is not supported
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Storage Alias requires target table name in ENGINE arguments. "
                "Use: ENGINE = Alias('table_name') or ENGINE = Alias('database', 'table_name').");
        }
        else if (args.engine_args.size() == 1)
        {
            // Syntax: ENGINE = Alias(table_name) or ENGINE = Alias(db.table_name)
            args.engine_args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(args.engine_args[0], local_context);
            String table_arg = checkAndGetLiteralArgument<String>(args.engine_args[0], "table_name");

            auto dot_pos = table_arg.find('.');
            if (dot_pos != String::npos)
            {
                target_database = table_arg.substr(0, dot_pos);
                target_table = table_arg.substr(dot_pos + 1);
            }
            else
            {
                target_table = table_arg;
                target_database = args.table_id.database_name;
            }
        }
        else if (args.engine_args.size() == 2)
        {
            // Syntax: ENGINE = Alias(database_name, table_name)
            args.engine_args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(args.engine_args[0], local_context);
            args.engine_args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(args.engine_args[1], local_context);
            target_database = checkAndGetLiteralArgument<String>(args.engine_args[0], "database_name");
            target_table = checkAndGetLiteralArgument<String>(args.engine_args[1], "table_name");
        }
        else
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Storage Alias requires at most 2 arguments: database name and table name");
        }

        // Storage Alias does not support explicit column definitions
        // Columns are always dynamically fetched from the target table
        // Only check for CREATE, not for ATTACH/RESTORE
        if (!args.columns.empty() && args.mode == LoadingStrictnessLevel::CREATE)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Storage Alias does not support explicit column definitions");
        }

        return std::make_shared<StorageAlias>(
            args.table_id,
            local_context,
            target_database,
            target_table);
    },
    {
        .supports_schema_inference = true
    },
    Documentation{
        .description = R"DOCS_MD(
# Alias table engine

The `Alias` engine creates a proxy to another table. All read and write operations are forwarded to the target table, while the alias itself stores no data and only maintains a reference to the target table.

## Creating a Table {#creating-a-table}

```sql
CREATE TABLE [db_name.]alias_name
ENGINE = Alias(target_table)
```

Or with explicit database name:

```sql
CREATE TABLE [db_name.]alias_name
ENGINE = Alias(target_db, target_table)
```

:::note
The `Alias` table does not support explicit column definitions. Columns are automatically inherited from the target table. This ensures that the alias always matches the target table's schema.
:::

## Engine Parameters {#engine-parameters}

- **`target_db (optional)`** — Name of the database containing the target table.
- **`target_table`** — Name of the target table.

:::note
When `target_db` is omitted and `target_table` is not fully qualified (e.g., `Alias('my_table')`), the target is resolved to the same database as the alias itself, not the session's current database.
:::

## Supported Operations {#supported-operations}

The `Alias` table engine supports all major operations. 
### Operations on Target Table {#operations-on-target}

These operations are proxied to the target table:

| Operation | Support | Description |
|-----------|---------|-------------|
| `SELECT` | ✅ | Read data from target table |
| `INSERT` | ✅ | Write data to target table |
| `INSERT SELECT` | ✅ | Batch insert into target table |
| `ALTER TABLE ADD COLUMN` | ✅ | Add columns to target table |
| `ALTER TABLE MODIFY SETTING` | ✅ | Modify target table settings |
| `ALTER TABLE PARTITION` | ✅ | Partition operations (DETACH/ATTACH/DROP) on target |
| `ALTER TABLE UPDATE` | ✅ | Update rows in target table (mutation) |
| `ALTER TABLE DELETE` | ✅ | Delete rows from target table (mutation) |
| `OPTIMIZE TABLE` | ✅ | Optimize target table (merge parts) |
| `TRUNCATE TABLE` | ✅ | Truncate target table |

### Operations on Alias Itself {#operations-on-alias}

These operations only affect the alias, **not** the target table:

| Operation | Support | Description |
|-----------|---------|-------------|
| `DROP TABLE` | ✅ | Drop the alias only, target table remains unchanged |
| `RENAME TABLE` | ✅ | Rename the alias only, target table remains unchanged |

## Usage Examples {#usage-examples}

### Basic Alias Creation {#basic-alias-creation}

Create a simple alias in the same database:

```sql
-- Create source table
CREATE TABLE source_data (
    id UInt32,
    name String,
    value Float64
) ENGINE = MergeTree
ORDER BY id;

-- Insert some data
INSERT INTO source_data VALUES (1, 'one', 10.1), (2, 'two', 20.2);

-- Create alias
CREATE TABLE data_alias ENGINE = Alias('source_data');

-- Query through alias
SELECT * FROM data_alias;
```

```text
┌─id─┬─name─┬─value─┐
│  1 │ one  │  10.1 │
│  2 │ two  │  20.2 │
└────┴──────┴───────┘
```

### Cross-Database Alias {#cross-database-alias}

Create an alias pointing to a table in a different database:

```sql
-- Create databases
CREATE DATABASE db1;
CREATE DATABASE db2;

-- Create source table in db1
CREATE TABLE db1.events (
    timestamp DateTime,
    event_type String,
    user_id UInt32
) ENGINE = MergeTree
ORDER BY timestamp;

-- Create alias in db2 pointing to db1.events
CREATE TABLE db2.events_alias ENGINE = Alias('db1', 'events');

-- Or using database.table format
CREATE TABLE db2.events_alias2 ENGINE = Alias('db1.events');

-- Both aliases work identically
INSERT INTO db2.events_alias VALUES (now(), 'click', 100);
SELECT * FROM db2.events_alias2;
```

### Write Operations Through Alias {#write-operations}

All write operations are forwarded to the target table:

```sql
CREATE TABLE metrics (
    ts DateTime,
    metric_name String,
    value Float64
) ENGINE = MergeTree
ORDER BY ts;

CREATE TABLE metrics_alias ENGINE = Alias('metrics');

-- Insert through alias
INSERT INTO metrics_alias VALUES 
    (now(), 'cpu_usage', 45.2),
    (now(), 'memory_usage', 78.5);

-- Insert with SELECT
INSERT INTO metrics_alias 
SELECT now(), 'disk_usage', number * 10 
FROM system.numbers 
LIMIT 5;

-- Verify data is in the target table
SELECT count() FROM metrics;  -- Returns 7
SELECT count() FROM metrics_alias;  -- Returns 7
```

### Schema Modification {#schema-modification}

Alter operations modify the target table schema:

```sql
CREATE TABLE users (
    id UInt32,
    name String
) ENGINE = MergeTree
ORDER BY id;

CREATE TABLE users_alias ENGINE = Alias('users');

-- Add column through alias
ALTER TABLE users_alias ADD COLUMN email String DEFAULT '';

-- Column is added to target table
DESCRIBE users;
```

```text
┌─name──┬─type───┬─default_type─┬─default_expression─┐
│ id    │ UInt32 │              │                    │
│ name  │ String │              │                    │
│ email │ String │ DEFAULT      │ ''                 │
└───────┴────────┴──────────────┴────────────────────┘
```

### Data Mutations {#data-mutations}

UPDATE and DELETE operations are supported:

```sql
CREATE TABLE products (
    id UInt32,
    name String,
    price Float64,
    status String DEFAULT 'active'
) ENGINE = MergeTree
ORDER BY id;

CREATE TABLE products_alias ENGINE = Alias('products');

INSERT INTO products_alias VALUES 
    (1, 'item_one', 100.0, 'active'),
    (2, 'item_two', 200.0, 'active'),
    (3, 'item_three', 300.0, 'inactive');

-- Update through alias
ALTER TABLE products_alias UPDATE price = price * 1.1 WHERE status = 'active';

-- Delete through alias
ALTER TABLE products_alias DELETE WHERE status = 'inactive';

-- Changes are applied to target table
SELECT * FROM products ORDER BY id;
```

```text
┌─id─┬─name─────┬─price─┬─status─┐
│  1 │ item_one │ 110.0 │ active │
│  2 │ item_two │ 220.0 │ active │
└────┴──────────┴───────┴────────┘
```

### Partition Operations {#partition-operations}

For partitioned tables, partition operations are forwarded:

```sql
CREATE TABLE logs (
    date Date,
    level String,
    message String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY date;

CREATE TABLE logs_alias ENGINE = Alias('logs');

INSERT INTO logs_alias VALUES 
    ('2024-01-15', 'INFO', 'message1'),
    ('2024-02-15', 'ERROR', 'message2'),
    ('2024-03-15', 'INFO', 'message3');

-- Detach partition through alias
ALTER TABLE logs_alias DETACH PARTITION '202402';

SELECT count() FROM logs_alias;  -- Returns 2 (partition 202402 detached)

-- Attach partition back
ALTER TABLE logs_alias ATTACH PARTITION '202402';

SELECT count() FROM logs_alias;  -- Returns 3
```

### Table Optimization {#table-optimization}

Optimize operations merge parts in the target table:

```sql
CREATE TABLE events (
    id UInt32,
    data String
) ENGINE = MergeTree
ORDER BY id;

CREATE TABLE events_alias ENGINE = Alias('events');

-- Multiple inserts create multiple parts
INSERT INTO events_alias VALUES (1, 'data1');
INSERT INTO events_alias VALUES (2, 'data2');
INSERT INTO events_alias VALUES (3, 'data3');

-- Check parts count
SELECT count() FROM system.parts 
WHERE database = currentDatabase() 
  AND table = 'events' 
  AND active;

-- Optimize through alias
OPTIMIZE TABLE events_alias FINAL;

-- Parts are merged in target table
SELECT count() FROM system.parts 
WHERE database = currentDatabase() 
  AND table = 'events' 
  AND active;  -- Returns 1
```

### Alias Management {#alias-management}

Aliases can be renamed or dropped independently:

```sql
CREATE TABLE important_data (
    id UInt32,
    value String
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO important_data VALUES (1, 'critical'), (2, 'important');

CREATE TABLE old_alias ENGINE = Alias('important_data');

-- Rename alias (target table unchanged)
RENAME TABLE old_alias TO new_alias;

-- Create another alias to same table
CREATE TABLE another_alias ENGINE = Alias('important_data');

-- Drop one alias (target table and other aliases unchanged)
DROP TABLE new_alias;

SELECT * FROM another_alias;  -- Still works
SELECT count() FROM important_data;  -- Data intact, returns 2
```
)DOCS_MD",
        .syntax = "ENGINE = Alias([target_db, ]target_table)"});
}

}
