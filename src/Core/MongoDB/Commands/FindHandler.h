#pragma once
#include <stdexcept>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/executeQuery.h>
#include <Common/CurrentThread.h>
#include "../Binary.h"
#include "../Document.h"
#include "../Element.h"
#include "Commands.h"

namespace DB
{
namespace MongoDB
{

struct FindCommand
{
    using Ptr = Poco::SharedPtr<FindCommand>;

    FindCommand() = default;

    std::string db_name;
    std::string collection_name;

    BSON::Document::Ptr filter;
    BSON::Document::Ptr sort;
    BSON::Document::Ptr projection;
    std::optional<Int32> limit;
    // TODO support other options
};


FindCommand::Ptr parseFindCommand(Command::Ptr command)
{
    FindCommand::Ptr find_cmd = new FindCommand();
    // TODO add other features
    find_cmd->db_name = command->getDBName();
    find_cmd->collection_name = command->getCollectionName();
    auto extra = command->getExtra();
    if (extra->exists("filter"))
        find_cmd->filter = extra->takeValue<BSON::Document::Ptr>("filter");
    if (extra->exists("projection"))
        find_cmd->projection = extra->takeValue<BSON::Document::Ptr>("projection");
    if (extra->exists("sort"))
        find_cmd->sort = extra->takeValue<BSON::Document::Ptr>("sort");
    if (extra->exists("limit"))
        find_cmd->limit = extra->takeValue<Int32>("limit");
    return find_cmd;
}

std::string makeQueryConditionForColumn(const std::string & column_name, BSON::Element::Ptr payload)
{
    if (payload->getType() != BSON::ElementTraits<BSON::Document::Ptr>::TypeId)
    {
        // 'exact' filter
        return fmt::format("{} == {}", column_name, makeElementIntoQuery(payload));
    }
    BSON::Document::Ptr doc = payload.cast<BSON::ConcreteElement<BSON::Document::Ptr>>()->getValue();
    const auto & names = doc->elementNames();
    std::string query;
    static auto and_str = " AND ";
    for (const auto & name : names)
    {
        auto value = doc->get(name);
        if (name == "$gt")
        {
            query += fmt::format("{} > {}", column_name, makeElementIntoQuery(value));
        }
        else if (name == "$lt")
        {
            query += fmt::format("{} < {}", column_name, makeElementIntoQuery(value));
        }
        else
        {
            LOG_WARNING(getLogger("MongoDB::MakeQuery"), "Unsupported filter; column_name: {}", column_name);
            continue;
        }
        query += and_str;
    }
    if (query.ends_with(and_str))
        query.resize(query.size() - std::string(and_str).length());
    return query;
}

std::string makeQueryConditions(const BSON::Document::Ptr filter)
{
    if (!filter || filter->empty())
        return "";
    std::string query = "WHERE ";
    static auto and_str = " AND ";
    const auto & element_names = filter->elementNames();
    for (const auto & name : element_names)
    {
        query += makeQueryConditionForColumn(name, filter->get(name));
        query += and_str;
    }
    if (query.ends_with(and_str))
        query.resize(query.size() - std::string(and_str).length());
    return query;
}

std::string makeOrderBy(const BSON::Document::Ptr sort)
{
    if (!sort || sort->empty())
        return "";
    std::string query = "ORDER BY ";
    const auto & element_names = sort->elementNames();
    for (const auto & name : element_names)
    {
        Int32 order = sort->get<Int32>(name);
        std::string order_str = order > 0 ? "ASC" : "DESC";
        query += fmt::format("{} {}", name, order_str);
        query += ", ";
    }
    if (query.ends_with(", "))
        query.resize(query.size() - std::string(", ").length());
    return query;
}


BSON::Document::Ptr handleFind(Command::Ptr command, ContextMutablePtr context)
{
    // NOTE can improve output efficiency by making one document with row values
    // instead [{name: 'Alex', age: 20}, {name: 'Max', age: 35}]
    // better to use {name: ['Alex', 'Max'], age: [20, 35]}

    // TODO add other features
    auto find_cmd = parseFindCommand(command);
    std::string query = fmt::format("SELECT * FROM {} ", find_cmd->collection_name);
    query += makeQueryConditions(find_cmd->filter) + " ";
    query += makeOrderBy(find_cmd->sort) + " ";
    if (find_cmd->limit.has_value())
        query += fmt::format("LIMIT {}", find_cmd->limit.value()) + " ";
    query += "FORMAT TabSeparatedWithNamesAndTypes";
    ReadBufferFromString read_buf(query);
    auto writer = WriteBufferFromOwnString();

    CurrentThread::QueryScope query_scope{context};
    executeQuery(read_buf, writer, false, context, {});

    std::string ns = fmt::format("{}.{}", find_cmd->db_name, find_cmd->collection_name);
    ProjectionMap projection_map = ProjectionMap(find_cmd->projection);
    return queryToDocument(std::move(writer.str()), ns, std::move(projection_map));
}


}
} // namespace DB::MongoDB
