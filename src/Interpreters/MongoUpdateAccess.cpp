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
    auto storage = DatabaseCatalog::instance().tryGetTable(StorageID(database, table), context);
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

    Mongo::expandMongoSubtreeAssignments(
        ast,
        columns,
        [&](const std::vector<std::string> & written)
        {
            for (const auto & column : written)
            {
                if (access->isGranted(AccessType::ALTER_UPDATE, database, table, column)
                    || access->isGranted(AccessType::SHOW_COLUMNS, database, table, column))
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
