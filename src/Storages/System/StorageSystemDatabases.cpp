#include <Access/ContextAccess.h>
#include <Common/SystemTableDocumentation.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/formatWithPossiblyHidingSecrets.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/System/StorageSystemDatabases.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_DATABASE;
}


ColumnsDescription StorageSystemDatabases::getColumnsDescription()
{
    auto description = ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Database name."},
        {"engine", std::make_shared<DataTypeString>(), "Database engine."},
        {"data_path", std::make_shared<DataTypeString>(), "Data path."},
        {"metadata_path", std::make_shared<DataTypeString>(), "Metadata path."},
        {"uuid", std::make_shared<DataTypeUUID>(), "Database UUID."},
        {"engine_full", std::make_shared<DataTypeString>(), "Parameters of the database engine."},
        {"comment", std::make_shared<DataTypeString>(), "Database comment."},
        {"is_external", std::make_shared<DataTypeUInt8>(), "Database is external (i.e. PostgreSQL/DataLakeCatalog)."},
    };

    description.setAliases({
        {"database", std::make_shared<DataTypeString>(), "name"}
    });

    return description;
}

static String getEngineFull(const ContextPtr & ctx, const DatabasePtr & database)
{
    DDLGuardPtr guard;
    while (true)
    {
        String name = database->getDatabaseName();
        guard = DatabaseCatalog::instance().getDDLGuard(name, "", nullptr);

        /// Ensure that the database was not renamed before we acquired the lock
        auto locked_database = DatabaseCatalog::instance().tryGetDatabase(name);

        if (locked_database.get() == database.get())
            break;

        /// Database was dropped
        if (name == database->getDatabaseName())
            return {};

        guard.reset();
        LOG_TRACE(getLogger("StorageSystemDatabases"), "Failed to lock database {} ({}), will retry", name, database->getUUID());
    }

    ASTPtr ast = database->getCreateDatabaseQuery();
    auto * ast_create = ast->as<ASTCreateQuery>();

    if (!ast_create || !ast_create->storage)
        return {};

    String engine_full = format({ctx, *ast_create->storage});
    static const char * const extra_head = " ENGINE = ";

    if (startsWith(engine_full, extra_head))
        engine_full = engine_full.substr(strlen(extra_head));

    return engine_full;
}

Block StorageSystemDatabases::getFilterSampleBlock() const
{
    /// Must list every column of the block passed to filterBlockWithPredicate in getFilteredDatabases.
    return {
        { {}, std::make_shared<DataTypeString>(), "name" },
        { {}, std::make_shared<DataTypeString>(), "engine" },
        { {}, std::make_shared<DataTypeUUID>(), "uuid" },
    };
}

static ColumnPtr getFilteredDatabases(const Databases & databases, const ActionsDAG::Node * predicate, ContextPtr context)
{
    MutableColumnPtr name_column = ColumnString::create();
    MutableColumnPtr engine_column = ColumnString::create();
    MutableColumnPtr uuid_column = ColumnUUID::create();

    for (const auto & [database_name, database] : databases)
    {
        if (database_name == DatabaseCatalog::TEMPORARY_DATABASE)
            continue; /// We don't want to show the internal database for temporary tables in system.tables

        name_column->insert(database_name);
        engine_column->insert(database->getEngineName());
        uuid_column->insert(database->getUUID());
    }

    Block block
    {
        ColumnWithTypeAndName(std::move(name_column), std::make_shared<DataTypeString>(), "name"),
        ColumnWithTypeAndName(std::move(engine_column), std::make_shared<DataTypeString>(), "engine"),
        ColumnWithTypeAndName(std::move(uuid_column), std::make_shared<DataTypeUUID>(), "uuid")
    };
    VirtualColumnUtils::filterBlockWithPredicate(predicate, block, context);
    return block.getByPosition(0).column;
}

void StorageSystemDatabases::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node * predicate, std::vector<UInt8> columns_mask) const
{
    const auto access = context->getAccess();
    const bool need_to_check_access_for_databases = !access->isGranted(AccessType::SHOW_DATABASES);
    /// Data lake catalogs and remote databases are always shown in `system.databases` regardless of system-table settings.
    /// Listing a database name is purely local metadata and never requires expensive calls to an external service.
    /// The settings only guard operations like `system.tables` / `system.columns` that enumerate a database's contents.
    const auto databases = DatabaseCatalog::instance().getDatabases(
        GetDatabasesOptions{.with_datalake_catalogs = true, .with_remote_databases = true});
    ColumnPtr filtered_databases_column = getFilteredDatabases(databases, predicate, context);

    for (size_t i = 0; i < filtered_databases_column->size(); ++i)
    {
        auto database_name = filtered_databases_column->getDataAt(i);

        if (need_to_check_access_for_databases && !access->isGranted(AccessType::SHOW_DATABASES, database_name))
            continue;

        if (database_name == DatabaseCatalog::TEMPORARY_DATABASE)
            continue; /// filter out the internal database for temporary tables in system.databases, asynchronous metric "NumberOfDatabases" behaves the same way

        auto database_it = databases.find(database_name);
        if (database_it == databases.end())
            throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database {} does not exist", database_name);
        const auto & database = database_it->second;

        size_t src_index = 0;
        size_t res_index = 0;
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(database_name);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(database->getEngineName());
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(context->getPath() + database->getDataPath());
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(database->getMetadataPath());
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(database->getUUID());
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(getEngineFull(context, database));
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(database->getDatabaseComment());
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(database->isExternal());
   }
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemDatabases) }

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "databases",
    .description = R"DOCS_MD(
Contains information about the databases that are available to the current user.
)DOCS_MD",
    .columns_notes = R"DOCS_MD(
The `name` column from this system table is used for implementing the `SHOW DATABASES` query.
)DOCS_MD",
    .examples = R"DOCS_MD(
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
)DOCS_MD")

}
