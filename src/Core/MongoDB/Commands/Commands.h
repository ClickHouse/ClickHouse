#pragma once

#include <unordered_map>
#include <Loggers/Loggers.h>
#include <Poco/StringTokenizer.h>
#include <Common/logger_useful.h>
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
    /* if (name == "ismaster") {
        return CommandTypes::IsMaster;
    }
    if (name == "hello") {
        return CommandTypes::Hello;
    }
    if (name == "getParameter") {
        return CommandTypes::GetParameter;
    }
    if (body->exists("buildInfo")) {
        return CommandTypes::BuildInfo;
    }
    if (body->exists("ping")) {
        return CommandTypes::Ping;
    }
    if (body->exists("getLog")) {
        return CommandTypes::GetLog;
    }
    if (body->exists("aggregate")) {
        return CommandTypes::Aggregate;
    } */
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
    message->getBody();
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


BSON::Document::Ptr handleIsMaster()
{
    // TODO make all settings configable
    BSON::Document::Ptr response = new BSON::Document();
    Poco::Timestamp current_time;
    current_time.update();
    BSON::Document::Ptr topology_version = new BSON::Document();
    (*topology_version).add("processId", BSON::ObjectId::Ptr(new BSON::ObjectId("661d3910ecbeac371e0f1d23"))).add("counter", 0);
    (*response)
        .add("helloOk", true)
        .add("ismaster", true)
        .add("isWritablePrimary", true)
        .add("topologyVersion", topology_version)
        .add("maxBsonObjectSize", 16777216)
        .add("maxMessageSizeBytes", 48000000)
        .add("maxWriteBatchSize", 100000)
        .add("localTime", current_time)
        .add("logicalSessionTimeoutMinutes", 30)
        .add("connectionId", 228)
        .add("minWireVersion", 0)
        .add("maxWireVersion", 17)
        .add("readOnly", false)
        .add("ok", 1.0);
    return response;
}


BSON::Document::Ptr handleBuildInfo()
{
    // FIXME normal parameters
    BSON::Document::Ptr response = new BSON::Document();
    BSON::Array::Ptr empty_array = new BSON::Array();
    BSON::Array::Ptr version_array = new BSON::Array();
    (*version_array).add(6);
    (*version_array).add(0);
    (*version_array).add(10);
    (*version_array).add(0);
    BSON::Document::Ptr openssl = new BSON::Document();
    (*openssl).add("running", "Apple Secure Transport");
    BSON::Array::Ptr storage_engines = new BSON::Array();
    (*storage_engines).add("devnull");
    (*storage_engines).add("ephemeralForTest");
    (*storage_engines).add("wiredTiger");
    (*response)
        .add("version", "6.0.10")
        .add("gitVersion", "8e4b5670df9b9fe814e57cb5f3f8ee9407237b5a")
        .add("modules", empty_array)
        .add("allocator", "system")
        .add("javascriptEngine", "mozjs")
        .add("sysInfo", "deprecated")
        .add("versionArray", version_array)
        .add("openssl", openssl)
        .add("bits", 64)
        .add("debug", false)
        .add("maxBsonObjectSize", 16777216)
        .add("storageEngines", storage_engines)
        .add("ok", 1.0);
    return response;
}

BSON::Document::Ptr handleGetParameter(Command::Ptr command)
{
    auto element_names = command->getExtra()->elementNames();
    BSON::Document::Ptr doc = new BSON::Document();
    // FIXME make it map like, not if like
    for (const auto & parameter_name : element_names)
    {
        if (parameter_name == "featureCompatibilityVersion")
        {
            BSON::Document::Ptr version = new BSON::Document();
            version->add("version", "7.0");
            doc->add("featureCompatibilityVersion", version);
        }
    }
    doc->add("ok", 1.0);
    return doc;
}

BSON::Document::Ptr handlePing()
{
    BSON::Document::Ptr response = new BSON::Document();
    (*response).add("ok", 1.0);
    return response;
}

BSON::Document::Ptr handleGetLog(Command::Ptr command)
{
    LOG_DEBUG(getLogger("handleGetLog"), "Not yet implemented {}", command->getName());
    BSON::Document::Ptr response = new BSON::Document();
    (*response).add("totalLinesWritten", 0).add("log", BSON::Array::Ptr(new BSON::Array())).add("ok", 1.0);
    return response;
}

BSON::Document::Ptr handleAggregate(const Command::Ptr command)
{
    LOG_DEBUG(getLogger("handleGetLog"), "Not yet implemented {}", command->getName());
    BSON::Document::Ptr response = new BSON::Document();
    BSON::Document::Ptr cursor = new BSON::Document();
    (*cursor).add("firstBatch", BSON::Array::Ptr(new BSON::Array())).add("id", 0).add("ns", "admin.atlascli");
    (*response).add("cursor", cursor).add("ok", 1.0);
    return response;
}

BSON::Document::Ptr handleUnknownCommand(Command::Ptr command)
{
    BSON::Document::Ptr response = new BSON::Document();
    (*response)
        .add("ok", 0.0)
        .add("errmsg", fmt::format("no such command: '{}'", command->getName()))
        .add("code", 59)
        .add("codeName", "CommandNotFound");
    return response;
}


}
} // namespace DB::MongoDB
