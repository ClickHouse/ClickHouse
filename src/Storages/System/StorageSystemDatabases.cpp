#include <Databases/IDatabase.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <Interpreters/Context.h>
#include <Interpreters/formatWithPossiblyHidingSecrets.h>
#include <Access/ContextAccess.h>
#include <Storages/System/StorageSystemDatabases.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/VirtualColumnUtils.h>
#include <Parsers/ASTCreateQuery.h>
#include <Common/logger_useful.h>


namespace DB
{

NamesAndTypesList StorageSystemDatabases::getNamesAndTypes()
{
    return {
        {"name", std::make_shared<DataTypeString>()},
        {"engine", std::make_shared<DataTypeString>()},
        {"data_path", std::make_shared<DataTypeString>()},
        {"metadata_path", std::make_shared<DataTypeString>()},
        {"uuid", std::make_shared<DataTypeUUID>()},
        {"engine_full", std::make_shared<DataTypeString>()},
        {"comment", std::make_shared<DataTypeString>()}
    };
}

NamesAndAliases StorageSystemDatabases::getNamesAndAliases()
{
    return {
        {"database", std::make_shared<DataTypeString>(), "name"}
    };
}

static String getEngineFull(const ContextPtr & ctx, const DatabasePtr & database)
{
    DDLGuardPtr guard;
    while (true)
    {
        String name = database->getDatabaseName();
        guard = DatabaseCatalog::instance().getDDLGuard(name, "");

        /// Ensure that the database was not renamed before we acquired the lock
        auto locked_database = DatabaseCatalog::instance().tryGetDatabase(name);

        if (locked_database.get() == database.get())
            break;

        /// Database was dropped
        if (name == database->getDatabaseName())
            return {};

        guard.reset();
        LOG_TRACE(&Poco::Logger::get("StorageSystemDatabases"), "Failed to lock database {} ({}), will retry", name, database->getUUID());
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

static ColumnPtr getFilteredDatabases(const Databases & databases, const SelectQueryInfo & query_info, ContextPtr context)
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
    VirtualColumnUtils::filterBlockWithQuery(query_info.query, block, context);
    return block.getByPosition(0).column;
}

void StorageSystemDatabases::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const
{
    const auto access = context->getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_DATABASES);

    const auto databases = DatabaseCatalog::instance().getDatabases();
    ColumnPtr filtered_databases_column = getFilteredDatabases(databases, query_info, context);

    for (size_t i = 0; i < filtered_databases_column->size(); ++i)
    {
        auto database_name = filtered_databases_column->getDataAt(i).toString();

        if (check_access_for_databases && !access->isGranted(AccessType::SHOW_DATABASES, database_name))
            continue;

        if (database_name == DatabaseCatalog::TEMPORARY_DATABASE)
            continue; /// filter out the internal database for temporary tables in system.databases, asynchronous metric "NumberOfDatabases" behaves the same way

        const auto & database = databases.at(database_name);

        size_t src_index = 0;
        size_t res_index = 0;
        const auto & columns_mask = query_info.columns_mask;
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
   }
}

}
