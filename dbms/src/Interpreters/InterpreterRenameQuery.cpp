#include <Parsers/ASTRenameQuery.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Storages/IStorage.h>
#include <Interpreters/DDLWorker.h>
#include <Common/typeid_cast.h>


namespace DB
{


InterpreterRenameQuery::InterpreterRenameQuery(const ASTPtr & query_ptr_, Context & context_)
    : query_ptr(query_ptr_), context(context_)
{
}


struct RenameDescription
{
    RenameDescription(const ASTRenameQuery::Element & elem, const String & current_database) :
        from_database_name(elem.from.database.empty() ? current_database : elem.from.database),
        from_table_name(elem.from.table),
        to_database_name(elem.to.database.empty() ? current_database : elem.to.database),
        to_table_name(elem.to.table)
    {}

    String from_database_name;
    String from_table_name;

    String to_database_name;
    String to_table_name;
};


BlockIO InterpreterRenameQuery::execute()
{
    const auto & rename = query_ptr->as<ASTRenameQuery &>();

    if (!rename.cluster.empty())
    {
        NameSet databases;
        for (const auto & elem : rename.elements)
        {
            databases.emplace(elem.from.database);
            databases.emplace(elem.to.database);
        }

        return executeDDLQueryOnCluster(query_ptr, context, std::move(databases));
    }

    String path = context.getPath();
    String current_database = context.getCurrentDatabase();

    /** In case of error while renaming, it is possible that only part of tables was renamed
      *  or we will be in inconsistent state. (It is worth to be fixed.)
      */

    std::vector<RenameDescription> descriptions;
    descriptions.reserve(rename.elements.size());

    /// To avoid deadlocks, we must acquire locks for tables in same order in any different RENAMES.
    struct UniqueTableName
    {
        String database_name;
        String table_name;

        UniqueTableName(const String & database_name_, const String & table_name_)
            : database_name(database_name_), table_name(table_name_) {}

        bool operator< (const UniqueTableName & rhs) const
        {
            return std::tie(database_name, table_name) < std::tie(rhs.database_name, rhs.table_name);
        }
    };

    std::set<UniqueTableName> unique_tables_from;

    /// Don't allow to drop tables (that we are renaming); don't allow to create tables in places where tables will be renamed.
    std::map<UniqueTableName, std::unique_ptr<DDLGuard>> table_guards;

    for (const auto & elem : rename.elements)
    {
        descriptions.emplace_back(elem, current_database);

        UniqueTableName from(descriptions.back().from_database_name, descriptions.back().from_table_name);
        UniqueTableName to(descriptions.back().to_database_name, descriptions.back().to_table_name);

        unique_tables_from.emplace(from);

        if (!table_guards.count(from))
            table_guards.emplace(from, context.getDDLGuard(from.database_name, from.table_name));

        if (!table_guards.count(to))
            table_guards.emplace(to, context.getDDLGuard(to.database_name, to.table_name));
    }

    std::vector<TableStructureWriteLockHolder> locks;
    locks.reserve(unique_tables_from.size());

    for (const auto & names : unique_tables_from)
        if (auto table = context.tryGetTable(names.database_name, names.table_name))
            locks.emplace_back(table->lockExclusively(context.getCurrentQueryId()));

    /** All tables are locked. If there are more than one rename in chain,
      *  we need to hold global lock while doing all renames. Order matters to avoid deadlocks.
      * It provides atomicity of all RENAME chain as a whole, from the point of view of DBMS client,
      *  but only in cases when there was no exceptions during this process and server does not fall.
      */

    decltype(context.getLock()) lock;

    if (descriptions.size() > 1)
        lock = context.getLock();

    for (const auto & elem : descriptions)
    {
        context.assertTableDoesntExist(elem.to_database_name, elem.to_table_name);

        context.getDatabase(elem.from_database_name)->renameTable(
            context, elem.from_table_name, *context.getDatabase(elem.to_database_name), elem.to_table_name);
    }

    return {};
}


}
