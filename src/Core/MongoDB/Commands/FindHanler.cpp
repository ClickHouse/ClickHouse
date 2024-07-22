#include "FindHandler.h"
#include "QueryBuilder.h"

namespace DB
{
namespace MongoDB
{

FindCommand parseFindCommand(Command::Ptr command)
{
    FindCommand find_cmd = FindCommand();
    // TODO add other features
    find_cmd.db_name = command->getDBName();
    find_cmd.collection_name = command->getCollectionName();
    auto extra = command->getExtra();
    if (extra->exists("filter"))
        find_cmd.filter = extra->takeValue<BSON::Document::Ptr>("filter");
    if (extra->exists("projection"))
        find_cmd.projection = extra->takeValue<BSON::Document::Ptr>("projection");
    if (extra->exists("sort"))
        find_cmd.sort = extra->takeValue<BSON::Document::Ptr>("sort");
    if (extra->exists("limit"))
        find_cmd.limit = extra->takeValue<Int32>("limit");
    return find_cmd;
}

static std::string translateQuery(FindCommand && find_cmd, std::vector<std::string> && columns)
{
    auto builder = QueryBuilder(find_cmd.collection_name, std::move(columns));
    if (find_cmd.filter.has_value())
        builder.handleWhere(find_cmd.filter.value());
    if (find_cmd.sort.has_value())
        builder.handleSort(find_cmd.sort.value());
    if (find_cmd.projection.has_value())
        builder.handleProject(find_cmd.projection.value());
    if (find_cmd.limit.has_value())
        builder.setLimit(find_cmd.limit.value());
    return std::move(builder).buildQuery();
}


BSON::Document::Ptr handleFind(Command::Ptr command, ContextMutablePtr context)
{
    auto find_cmd = parseFindCommand(command);
    auto columns = getColumnsFromTable(context, find_cmd.collection_name);
    auto query = translateQuery(std::move(find_cmd), std::move(columns));

    ReadBufferFromString read_buf(query);
    auto writer = WriteBufferFromOwnString();

    CurrentThread::QueryScope query_scope{context};
    executeQuery(read_buf, writer, false, context, {});

    std::string ns = fmt::format("{}.{}", find_cmd.db_name, find_cmd.collection_name);
    return queryToDocument(std::move(writer.str()), std::move(ns));
}

}
}
