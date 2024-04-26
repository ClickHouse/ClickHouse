#pragma once

#include <unordered_map>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/executeQuery.h>
#include <Loggers/Loggers.h>
#include <Poco/StringTokenizer.h>
#include <Common/CurrentThread.h>
#include <Common/logger_useful.h>
#include "../Binary.h"
#include "../ObjectId.h"
#include "../OpMsgMessage.h"
#include "ProjectionMap.h"

namespace DB
{
namespace MongoDB
{

enum CommandTypes
{
    IsMaster,
    Hello,
    BuildInfo,
    GetParameter,
    Ping,
    GetLog,
    Aggregate,
    Find,
    Unknown
};

std::unordered_map<CommandTypes, std::string> command_types_names
    = {{IsMaster, "ismaster"},
       {Hello, "hello"},
       {GetParameter, "getParameter"},
       {BuildInfo, "buildInfo"},
       {Ping, "ping"},
       {GetLog, "getLog"},
       {Aggregate, "aggregate"},
       {Find, "find"}};

class Command
{
public:
    using Ptr = Poco::SharedPtr<Command>;

    Command() = default;

    CommandTypes getType() const;


    static Command::Ptr parseCommand(OpMsgMessage::Ptr message);

    const std::string & getName() const;

    const BSON::Document::Ptr getExtra() const;

    BSON::Document::Ptr getExtra();

    const std::string getDBName() const;

    const std::string getCollectionName() const;

private:
    std::string command_name;
    CommandTypes type;
    BSON::Element::Ptr payload;
    BSON::Element::Ptr lsid;
    std::string db_name;
    BSON::Document::Ptr extra;
    std::string collection_name;
};

inline CommandTypes Command::getType() const
{
    return type;
}

inline const std::string & Command::getName() const
{
    return command_name;
}

inline const BSON::Document::Ptr Command::getExtra() const
{
    return extra;
}

inline BSON::Document::Ptr Command::getExtra()
{
    return extra;
}

inline const std::string Command::getDBName() const
{
    return db_name;
}

inline const std::string Command::getCollectionName() const
{
    return collection_name;
}


CommandTypes getCommandType(const std::string & name)
{
    for (const auto & [type, c_name] : command_types_names)
        if (name == c_name)
            return type;
    return CommandTypes::Unknown;
}


Command::Ptr Command::parseCommand(OpMsgMessage::Ptr message)
{
    Command::Ptr command = new Command();
    auto element_names = message->getBody()->elementNames();
    command->extra = new BSON::Document();
    for (const auto & name : element_names)
    {
        if (name == "$db")
        {
            command->db_name = message->getBody()->get<std::string>(name);
            message->getBody()->remove(name);
            continue;
        }

        if (name == "lsid")
        {
            command->lsid = message->getBody()->take(name);
            continue;
        }

        if (auto type = getCommandType(name); type != CommandTypes::Unknown)
        {
            command->command_name = name;
            command->type = type;
            command->payload = message->getBody()->take(name);
            if (command->payload->getType() == BSON::ElementTraits<std::string>::TypeId)
            {
                command->collection_name = command->payload.cast<BSON::ConcreteElement<std::string>>()->getValue();

                command->payload.reset();
            }
            continue;
        }
    }
    command->extra = message->getBody();
    return command;
}

BSON::Array::Ptr parseQuery(std::string && query)
{
    BSON::Array::Ptr table = new BSON::Array();
    for (const auto & line : Poco::StringTokenizer(query, "\n", Poco::StringTokenizer::TOK_IGNORE_EMPTY))
    {
        BSON::Array::Ptr row = new BSON::Array();
        for (const auto & col : Poco::StringTokenizer(line, "\t", Poco::StringTokenizer::TOK_IGNORE_EMPTY))
        {
            row->add(col);
            // FIXME should all types be string???
        }
        LOG_DEBUG(getLogger("MongoDB::parseQuery"), "Iterating; row.size(): {}", row->size());
        table->add(row);
    }
    return table;
}

BSON::Document::Ptr queryToDocument(std::string && query, std::string ns, ProjectionMap && projection_map)
{
    LoggerPtr log = getLogger("Command::parseQuery");
    LOG_DEBUG(log, "query: {}, ns: {}", query, ns);
    BSON::Document::Ptr doc = new BSON::Document();
    BSON::Document::Ptr cursor = new BSON::Document();
    BSON::Array::Ptr results = parseQuery(std::move(query));
    assert(results->size() >= 2);

    BSON::Array::Ptr col_names = results->get<BSON::Array::Ptr>(0);
    std::vector<bool> include_column(col_names->size());
    // TODO change projection map after query to projection map before query
    for (size_t i = 0; i < col_names->size(); i++)
        include_column[i] = projection_map.include(col_names->get<std::string>(i));
    BSON::Array::Ptr types = results->get<BSON::Array::Ptr>(1);
    BSON::Array::Ptr first_batch = new BSON::Array();
    for (size_t i = 2; i < results->size(); i++)
    {
        BSON::Document::Ptr row = new BSON::Document();
        auto res_row = results->get<BSON::Array::Ptr>(i);
        LOG_INFO(getLogger("MongoDB::QueryToDocument"), "Iterating; i: {}, res_row.size(): {}", i, res_row->size());
        for (size_t j = 0; j < res_row->size(); j++)
        {
            if (!include_column[j])
                continue;
            LOG_INFO(getLogger("MongoDB::QueryToDocument"), "Iterating; j: {}, col_names.size(): {}", j, col_names->size());
            const auto & col_name = col_names->get<std::string>(j);
            const auto & type = types->get<std::string>(j);
            row->addElement(BSON::Element::createElementWithType(type, col_name, res_row->get<std::string>(j)));
        }
        first_batch->add(row);
    }
    (*cursor).add("firstBatch", first_batch).addElement(new BSON::ConcreteElement<Int64>("id", 0)).add("ns", ns);
    doc->add("cursor", cursor);
    doc->add("ok", 1.0);
    return doc;
}

std::string parseRegex(const std::string & str)
{
    std::string new_str;
    for (size_t i = 0; i < str.length(); i++)
        if (str[i] == '/')
            new_str += '%';
        else if (str[i] != '\\')
            new_str += str[i];
    return new_str;
}

std::string makeElementIntoQuery(BSON::Element::Ptr elem)
{
    switch (elem->getType())
    {
        case BSON::ElementTraits<double>::TypeId:
        case BSON::ElementTraits<Int32>::TypeId:
        case BSON::ElementTraits<Int64>::TypeId:
        case BSON::ElementTraits<Poco::Timestamp>::TypeId:
            return elem->toString();
        case BSON::ElementTraits<BSON::Document::Ptr>::TypeId:
        case BSON::ElementTraits<BSON::Binary::Ptr>::TypeId:
        case BSON::ElementTraits<BSON::Array::Ptr>::TypeId:
        case BSON::ElementTraits<BSON::ObjectId::Ptr>::TypeId:
            LOG_WARNING(getLogger("MongoDB::QueryParser"), "invalid data in query");
            throw std::logic_error("ObjectId is not supported in query");
        case BSON::ElementTraits<bool>::TypeId: {
            auto tmp = elem.cast<BSON::ConcreteElement<bool>>();
            return (tmp->getValue() ? "True" : "False");
        }
        case BSON::ElementTraits<std::string>::TypeId: {
            auto tmp = elem.cast<BSON::ConcreteElement<std::string>>();
            return fmt::format("\'{}\'", tmp->getValue());
        }
        default: {
            throw Poco::NotImplementedException("Unsupported element type, cannot make into query");
        }
    }
}

// TODO make Find use the same funcion
BSON::Document::Ptr launchQuery(std::string && query, ContextMutablePtr context, const std::string & db_name, const std::string & coll_name)
{
    ReadBufferFromString read_buf(query);
    auto writer = WriteBufferFromOwnString();
    CurrentThread::QueryScope query_scope{context};
    executeQuery(read_buf, writer, false, context, {});
    std::string ns = fmt::format("{}.{}", db_name, coll_name);
    ProjectionMap proj_map;
    LOG_INFO(getLogger("MongoDB::LaunchQuery"), "execution finished, proceeding to parsing result");
    return queryToDocument(std::move(writer.str()), ns, std::move(proj_map));
}

}
} // namespace DB::MongoDB
