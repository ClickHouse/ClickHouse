#include <Parsers/ASTRenameQuery.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Storages/IStorage.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/QueryLog.h>
#include <Access/AccessRightsElement.h>
#include <Common/typeid_cast.h>
#include <Databases/DatabaseReplicated.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

InterpreterRenameQuery::InterpreterRenameQuery(const ASTPtr & query_ptr_, Context & context_)
    : query_ptr(query_ptr_), context(context_)
{
}


BlockIO InterpreterRenameQuery::execute()
{
    const auto & rename = query_ptr->as<const ASTRenameQuery &>();

    if (!rename.cluster.empty())
        return executeDDLQueryOnCluster(query_ptr, context, getRequiredAccess());

    context.checkAccess(getRequiredAccess());

    String path = context.getPath();
    String current_database = context.getCurrentDatabase();

    /** In case of error while renaming, it is possible that only part of tables was renamed
      *  or we will be in inconsistent state. (It is worth to be fixed.)
      */

    RenameDescriptions descriptions;
    descriptions.reserve(rename.elements.size());

    /// Don't allow to drop tables (that we are renaming); don't allow to create tables in places where tables will be renamed.
    TableGuards table_guards;

    for (const auto & elem : rename.elements)
    {
        descriptions.emplace_back(elem, current_database);
        const auto & description = descriptions.back();

        UniqueTableName from(description.from_database_name, description.from_table_name);
        UniqueTableName to(description.to_database_name, description.to_table_name);

        table_guards[from];
        table_guards[to];
    }

    auto & database_catalog = DatabaseCatalog::instance();

    /// Must do it in consistent order.
    for (auto & table_guard : table_guards)
        table_guard.second = database_catalog.getDDLGuard(table_guard.first.database_name, table_guard.first.table_name);

    if (rename.database)
        return executeToDatabase(rename, descriptions);
    else
        return executeToTables(rename, descriptions, table_guards);
}

BlockIO InterpreterRenameQuery::executeToTables(const ASTRenameQuery & rename, const RenameDescriptions & descriptions, TableGuards & ddl_guards)
{
    auto & database_catalog = DatabaseCatalog::instance();

    for (const auto & elem : descriptions)
    {
        if (!rename.exchange)
            database_catalog.assertTableDoesntExist(StorageID(elem.to_database_name, elem.to_table_name), context);

        DatabasePtr database = database_catalog.getDatabase(elem.from_database_name);
        if (typeid_cast<DatabaseReplicated *>(database.get()) && context.getClientInfo().query_kind != ClientInfo::QueryKind::SECONDARY_QUERY)
        {
            if (1 < descriptions.size())
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Database {} is Replicated, "
                                "it does not support renaming of multiple tables in single query.", elem.from_database_name);

            UniqueTableName from(elem.from_database_name, elem.from_table_name);
            UniqueTableName to(elem.to_database_name, elem.to_table_name);
            ddl_guards[from]->releaseTableLock();
            ddl_guards[to]->releaseTableLock();
            return typeid_cast<DatabaseReplicated *>(database.get())->tryEnqueueReplicatedDDL(query_ptr, context);
        }
        else
        {
            database->renameTable(
                context,
                elem.from_table_name,
                *database_catalog.getDatabase(elem.to_database_name),
                elem.to_table_name,
                rename.exchange,
                rename.dictionary);
        }
    }

    return {};
}

BlockIO InterpreterRenameQuery::executeToDatabase(const ASTRenameQuery &, const RenameDescriptions & descriptions)
{
    assert(descriptions.size() == 1);
    assert(descriptions.front().from_table_name.empty());
    assert(descriptions.front().to_table_name.empty());

    const auto & old_name = descriptions.front().from_database_name;
    const auto & new_name = descriptions.back().to_database_name;
    auto & catalog = DatabaseCatalog::instance();

    auto db = catalog.getDatabase(old_name);
    catalog.assertDatabaseDoesntExist(new_name);
    db->renameDatabase(new_name);
    return {};
}

AccessRightsElements InterpreterRenameQuery::getRequiredAccess() const
{
    AccessRightsElements required_access;
    const auto & rename = query_ptr->as<const ASTRenameQuery &>();
    for (const auto & elem : rename.elements)
    {
        required_access.emplace_back(AccessType::SELECT | AccessType::DROP_TABLE, elem.from.database, elem.from.table);
        required_access.emplace_back(AccessType::CREATE_TABLE | AccessType::INSERT, elem.to.database, elem.to.table);
        if (rename.exchange)
        {
            required_access.emplace_back(AccessType::CREATE_TABLE | AccessType::INSERT, elem.from.database, elem.from.table);
            required_access.emplace_back(AccessType::SELECT | AccessType::DROP_TABLE, elem.to.database, elem.to.table);
        }
    }
    return required_access;
}

void InterpreterRenameQuery::extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr & ast, const Context &) const
{
    elem.query_kind = "Rename";
    const auto & rename = ast->as<const ASTRenameQuery &>();
    for (const auto & element : rename.elements)
    {
        {
            String database = backQuoteIfNeed(element.from.database.empty() ? context.getCurrentDatabase() : element.from.database);
            elem.query_databases.insert(database);
            elem.query_tables.insert(database + "." + backQuoteIfNeed(element.from.table));
        }
        {
            String database = backQuoteIfNeed(element.to.database.empty() ? context.getCurrentDatabase() : element.to.database);
            elem.query_databases.insert(database);
            elem.query_tables.insert(database + "." + backQuoteIfNeed(element.to.table));
        }
    }
}

}
