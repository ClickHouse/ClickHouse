#include <Parsers/ASTMoveQuery.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterMoveQuery.h>
#include <Storages/IStorage.h>
#include <Interpreters/DDLWorker.h>
#include <Common/typeid_cast.h>


namespace DB
{


InterpreterMoveQuery::InterpreterMoveQuery(const ASTPtr & query_ptr_, const Context & context_)
        : query_ptr(query_ptr_), context(context_)
{
}


BlockIO InterpreterMoveQuery::execute()
{
    const auto & move = query_ptr->as<ASTMoveQuery &>();

    if (!move.cluster.empty())
    {
        ; ///@TODO_IGR ASK WHAT SHOULD I DO HERE
//            NameSet databases;
//            for (const auto & elem : rename.elements)
//            {
//                databases.emplace(elem.from.database);
//                databases.emplace(elem.to.database);
//            }
//
//            return executeDDLQueryOnCluster(query_ptr, context, std::move(databases));
    }

    String path = context.getPath();
    String current_database = context.getCurrentDatabase();

    auto table = context.tryGetTable(move.table.database, move.table.table);
//    if (!table)
//        ; ///@TODO_IGR

    TableStructureWriteLockHolder table_lock(table->lockExclusively(context.getCurrentQueryId()));

    decltype(context.getLock()) lock;

    lock = context.getLock();

    table->move(move.part_name, move.destination_disk_name);

    return {};
}


}
