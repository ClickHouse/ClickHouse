#include <Interpreters/MongoUpdateAccess.h>

#if USE_RAPIDJSON

#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/Mongo/ParserMongoUpdateQuery.h>
#include <Storages/IStorage.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
}

std::vector<String> getMongoCollectionColumns(const String & database, const String & table, const ContextPtr & context)
{
    std::vector<String> column_names;
    /// The table is resolved as the interpreter of the update resolves it: an unqualified name is a
    /// temporary table of the session first, and a table of the current database otherwise.
    auto table_id = context->tryResolveStorageID(StorageID(database, table));
    if (!table_id)
        return column_names;
    auto storage = DatabaseCatalog::instance().tryGetTable(table_id, context);
    if (!storage)
        return column_names;
    auto metadata = storage->getInMemoryMetadataPtr(context, /* bypass_metadata_cache = */ false);
    for (const auto & column : metadata->getColumns())
        column_names.push_back(column.name);
    return column_names;
}

void expandMongoSubtreeAssignmentsCheckingAccess(ASTPtr & ast, const std::vector<String> & columns, const ContextPtr & context)
{
    const auto * alter = ast ? ast->as<ASTAlterQuery>() : nullptr;
    if (!alter || !alter->command_list)
        return;

    const String database = alter->getDatabase().empty() ? context->getCurrentDatabase() : alter->getDatabase();
    const String table = alter->getTable();
    const auto access = context->getAccess();

    /// The access is checked on the table the columns were read from, see
    /// `getMongoCollectionColumns`: an unqualified name of a temporary table of the session
    /// resolves to it, which is accessible to the session, rather than to a namesake in the current
    /// database. A name that does not resolve is checked as it is written, and the update fails
    /// with an unknown table anyway.
    String checked_database = database;
    String checked_table = table;
    if (auto table_id = context->tryResolveStorageID(StorageID(alter->getDatabase(), table)))
    {
        checked_database = table_id.database_name;
        checked_table = table_id.table_name;
    }

    Mongo::expandMongoSubtreeAssignments(
        ast,
        columns,
        [&](const std::vector<std::string> & written)
        {
            for (const auto & column : written)
            {
                if (access->isGranted(AccessType::ALTER_UPDATE, checked_database, checked_table, column)
                    || access->isGranted(AccessType::SHOW_COLUMNS, checked_database, checked_table, column))
                    continue;
                throw Exception(
                    ErrorCodes::ACCESS_DENIED,
                    "Not enough privileges: the update of '{}.{}' writes a field that the user is not allowed to update",
                    database,
                    table);
            }
        });
}

}

#endif
