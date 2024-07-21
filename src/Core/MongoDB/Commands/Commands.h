#pragma once

#include <unordered_map>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/executeQuery.h>
#include <Loggers/Loggers.h>
#include <Poco/StringTokenizer.h>
#include <Common/CurrentThread.h>
#include <Common/logger_useful.h>
#include "../BSON/Array.h"
#include "../BSON/Binary.h"
#include "../BSON/ObjectId.h"
#include "../Messages/OpMsgMessage.h"
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


CommandTypes getCommandType(const std::string & name);

using QueryResult = std::vector<std::vector<std::string>>;

QueryResult parseQuery(std::string && query_result);

BSON::Document::Ptr queryToDocument(std::string && query, std::string && ns);

std::pair<std::string, bool> parseRegex(const std::string & str);

std::string makeElementIntoQuery(BSON::Element::Ptr elem);


inline void removeDollarFromName(std::string & name)
{
    if (!name.empty() && name[0] == '$')
        name.erase(name.begin());
}


std::string makeExpression(BSON::Element::Ptr elem);

BSON::Document::Ptr
launchQuery(std::string && query, ContextMutablePtr context, const std::string & db_name, const std::string & coll_name);


std::vector<std::string> getColumnsFromTable(ContextMutablePtr context, const std::string & table_name);

}
}
