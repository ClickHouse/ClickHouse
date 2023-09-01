#include <Databases/IDatabase.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <Interpreters/Context.h>
#include <Access/ContextAccess.h>
#include <Storages/System/StorageSystemDatabases.h>
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

static String getEngineFull(const DatabasePtr & database)
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

    String engine_full = ast_create->storage->formatWithSecretsHidden();
    static const char * const extra_head = " ENGINE = ";

    if (startsWith(engine_full, extra_head))
        engine_full = engine_full.substr(strlen(extra_head));

    return engine_full;
}

void StorageSystemDatabases::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    const auto access = context->getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_DATABASES);

    const auto databases = DatabaseCatalog::instance().getDatabases();
    for (const auto & [database_name, database] : databases)
    {
        if (check_access_for_databases && !access->isGranted(AccessType::SHOW_DATABASES, database_name))
            continue;

        if (database_name == DatabaseCatalog::TEMPORARY_DATABASE)
            continue; /// filter out the internal database for temporary tables in system.databases, asynchronous metric "NumberOfDatabases" behaves the same way

        res_columns[0]->insert(database_name);
        res_columns[1]->insert(database->getEngineName());
        res_columns[2]->insert(context->getPath() + database->getDataPath());
        res_columns[3]->insert(database->getMetadataPath());
        res_columns[4]->insert(database->getUUID());
        res_columns[5]->insert(getEngineFull(database));
        res_columns[6]->insert(database->getDatabaseComment());
   }
}

}
