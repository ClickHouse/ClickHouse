#include <Parsers/ASTRenameQuery.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Storages/IStorage.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/QueryLog.h>
#include <Access/Common/AccessRightsElement.h>
#include <Common/typeid_cast.h>
#include <Core/Settings.h>
#include <Databases/DatabaseReplicated.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool check_table_dependencies;
    extern const SettingsBool check_referential_table_dependencies;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

InterpreterRenameQuery::InterpreterRenameQuery(const ASTPtr & query_ptr_, ContextPtr context_)
    : WithContext(context_), query_ptr(query_ptr_)
{
}


BlockIO InterpreterRenameQuery::execute()
{
    const auto & rename = query_ptr->as<const ASTRenameQuery &>();

    if (!rename.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        params.access_to_check = getRequiredAccess(rename.database ? RenameType::RenameDatabase : RenameType::RenameTable);
        return executeDDLQueryOnCluster(query_ptr, getContext(), params);
    }

    getContext()->checkAccess(getRequiredAccess(rename.database ? RenameType::RenameDatabase : RenameType::RenameTable));

    String current_database = getContext()->getCurrentDatabase();

    /** In case of error while renaming, it is possible that only part of tables was renamed
      *  or we will be in inconsistent state. (It is worth to be fixed.)
      */

    RenameDescriptions descriptions;
    descriptions.reserve(rename.getElements().size());

    /// Don't allow to drop tables (that we are renaming); don't allow to create tables in places where tables will be renamed.
    TableGuards table_guards;

    for (const auto & elem : rename.getElements())
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
    return executeToTables(rename, descriptions, table_guards);
}

BlockIO InterpreterRenameQuery::executeToTables(const ASTRenameQuery & rename, const RenameDescriptions & descriptions, TableGuards & ddl_guards)
{
    assert(!rename.rename_if_cannot_exchange || descriptions.size() == 1);
    assert(!(rename.rename_if_cannot_exchange && rename.exchange));
    auto & database_catalog = DatabaseCatalog::instance();

    for (const auto & elem : descriptions)
    {
        if (elem.if_exists)
        {
            assert(!rename.exchange);
            if (!database_catalog.isTableExist(StorageID(elem.from_database_name, elem.from_table_name), getContext()))
                continue;
        }

        bool exchange_tables;
        if (rename.exchange)
        {
            exchange_tables = true;
        }
        else if (rename.rename_if_cannot_exchange)
        {
            exchange_tables = database_catalog.isTableExist(StorageID(elem.to_database_name, elem.to_table_name), getContext());
            renamed_instead_of_exchange = !exchange_tables;
        }
        else
        {
            exchange_tables = false;
            database_catalog.assertTableDoesntExist(StorageID(elem.to_database_name, elem.to_table_name), getContext());
        }

        DatabasePtr database = database_catalog.getDatabase(elem.from_database_name);
        if (database->shouldReplicateQuery(getContext(), query_ptr))
        {
            if (1 < descriptions.size())
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Database {} is Replicated, "
                    "it does not support renaming of multiple tables in single query.",
                    elem.from_database_name);

            UniqueTableName from(elem.from_database_name, elem.from_table_name);
            UniqueTableName to(elem.to_database_name, elem.to_table_name);
            ddl_guards[from]->releaseTableLock();
            ddl_guards[to]->releaseTableLock();
            return database->tryEnqueueReplicatedDDL(query_ptr, getContext());
        }

        StorageID from_table_id{elem.from_database_name, elem.from_table_name};
        StorageID to_table_id{elem.to_database_name, elem.to_table_name};
        std::vector<StorageID> from_ref_dependencies;
        std::vector<StorageID> from_loading_dependencies;
        std::vector<StorageID> to_ref_dependencies;
        std::vector<StorageID> to_loading_dependencies;

        if (exchange_tables)
        {
            DatabaseCatalog::instance().checkTablesCanBeExchangedWithNoCyclicDependencies(from_table_id, to_table_id);
            std::tie(from_ref_dependencies, from_loading_dependencies) = database_catalog.removeDependencies(from_table_id, false, false);
            std::tie(to_ref_dependencies, to_loading_dependencies) = database_catalog.removeDependencies(to_table_id, false, false);
        }
        else
        {
            DatabaseCatalog::instance().checkTableCanBeRenamedWithNoCyclicDependencies(from_table_id, to_table_id);
            bool check_ref_deps = getContext()->getSettingsRef()[Setting::check_referential_table_dependencies];
            bool check_loading_deps = !check_ref_deps && getContext()->getSettingsRef()[Setting::check_table_dependencies];
            std::tie(from_ref_dependencies, from_loading_dependencies) = database_catalog.removeDependencies(from_table_id, check_ref_deps, check_loading_deps);
        }
        try
        {
            database->renameTable(
                getContext(),
                elem.from_table_name,
                *database_catalog.getDatabase(elem.to_database_name),
                elem.to_table_name,
                exchange_tables,
                rename.dictionary);

            DatabaseCatalog::instance().addDependencies(to_table_id, from_ref_dependencies, from_loading_dependencies);
            if (!to_ref_dependencies.empty() || !to_loading_dependencies.empty())
                DatabaseCatalog::instance().addDependencies(from_table_id, to_ref_dependencies, to_loading_dependencies);
        }
        catch (...)
        {
            /// Restore dependencies if RENAME fails
            DatabaseCatalog::instance().addDependencies(from_table_id, from_ref_dependencies, from_loading_dependencies);
            if (!to_ref_dependencies.empty() || !to_loading_dependencies.empty())
                DatabaseCatalog::instance().addDependencies(to_table_id, to_ref_dependencies, to_loading_dependencies);
            throw;
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

    auto db = descriptions.front().if_exists ? catalog.tryGetDatabase(old_name) : catalog.getDatabase(old_name);

    if (db)
    {
        catalog.assertDatabaseDoesntExist(new_name);
        db->renameDatabase(getContext(), new_name);
    }

    return {};
}

AccessRightsElements InterpreterRenameQuery::getRequiredAccess(InterpreterRenameQuery::RenameType type) const
{
    AccessRightsElements required_access;
    const auto & rename = query_ptr->as<const ASTRenameQuery &>();
    for (const auto & elem : rename.getElements())
    {
        if (type == RenameType::RenameTable)
        {
            required_access.emplace_back(AccessType::SELECT | AccessType::DROP_TABLE, elem.from.getDatabase(), elem.from.getTable());
            required_access.emplace_back(AccessType::CREATE_TABLE | AccessType::INSERT, elem.to.getDatabase(), elem.to.getTable());
            if (rename.exchange)
            {
                required_access.emplace_back(AccessType::CREATE_TABLE | AccessType::INSERT, elem.from.getDatabase(), elem.from.getTable());
                required_access.emplace_back(AccessType::SELECT | AccessType::DROP_TABLE, elem.to.getDatabase(), elem.to.getTable());
            }
        }
        else if (type == RenameType::RenameDatabase)
        {
            required_access.emplace_back(AccessType::SELECT | AccessType::DROP_DATABASE, elem.from.getDatabase());
            required_access.emplace_back(AccessType::CREATE_DATABASE | AccessType::INSERT, elem.to.getDatabase());
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown type of rename query");
        }
    }
    return required_access;
}

void InterpreterRenameQuery::extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr & ast, ContextPtr) const
{
    const auto & rename = ast->as<const ASTRenameQuery &>();
    for (const auto & element : rename.getElements())
    {
        {
            String database = backQuoteIfNeed(!element.from.database ? getContext()->getCurrentDatabase() : element.from.getDatabase());
            elem.query_databases.insert(database);
            elem.query_tables.insert(database + "." + backQuoteIfNeed(element.from.getTable()));
        }
        {
            String database = backQuoteIfNeed(!element.to.database ? getContext()->getCurrentDatabase() : element.to.getDatabase());
            elem.query_databases.insert(database);
            elem.query_tables.insert(database + "." + backQuoteIfNeed(element.to.getTable()));
        }
    }
}

void registerInterpreterRenameQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterRenameQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterRenameQuery", create_fn);
}

}
