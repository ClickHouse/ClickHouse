#include "Commands.h"

namespace DB
{
namespace MongoDB
{

std::unordered_map<CommandTypes, std::string> command_types_names
    = {{IsMaster, "ismaster"},
       {Hello, "hello"},
       {GetParameter, "getParameter"},
       {BuildInfo, "buildInfo"},
       {Ping, "ping"},
       {GetLog, "getLog"},
       {Aggregate, "aggregate"},
       {Find, "find"}};


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

QueryResult parseQuery(std::string && query_result)
{
    LoggerPtr log = getLogger("Command::parseQuery");
    LOG_DEBUG(log, "query_result: {}", query_result);
    QueryResult result;
    for (auto & line : Poco::StringTokenizer(query_result, "\n", Poco::StringTokenizer::TOK_IGNORE_EMPTY))
    {
        std::vector<std::string> row;
        for (auto & col : Poco::StringTokenizer(line, "\t", Poco::StringTokenizer::TOK_IGNORE_EMPTY))
            row.emplace_back(std::move(col));
        result.emplace_back(std::move(row));
    }
    return result;
}

BSON::Document::Ptr queryToDocument(std::string && query, std::string && ns)
{
    // TODO maybe use column names more efficiently?
    BSON::Document::Ptr doc = new BSON::Document();
    BSON::Document::Ptr cursor = new BSON::Document();

    auto results = parseQuery(std::move(query));
    assert(results.size() >= 2);
    const auto & col_names = results[0];
    const auto & types = results[1];

    BSON::Array::Ptr first_batch = new BSON::Array();
    for (size_t i = 2; i < results.size(); i++)
    {
        BSON::Document::Ptr row = new BSON::Document();
        const auto & res_row = results[i]; // TODO make it movable
        for (size_t j = 0; j < res_row.size(); j++)
        {
            const auto & col_name = col_names[j];
            const auto & type = types[j];
            row->addElement(BSON::Element::createElementWithType(type, col_name, res_row[j]));
        }
        first_batch->add(row);
    }

    (*cursor).add("firstBatch", first_batch).addElement(new BSON::ConcreteElement<Int64>("id", 0)).add("ns", ns);
    doc->add("cursor", cursor);
    doc->add("ok", 1.0);
    return doc;
}

std::pair<std::string, bool> parseRegex(const std::string & str)
{
    bool status;
    std::string new_str;
    for (size_t i = 0; i < str.length(); i++)
        if (str[i] == '/')
        {
            new_str += '%';
            status = true;
        }
        else if (str[i] != '\\')
        {
            new_str += str[i];
        }
    return std::make_pair(std::move(new_str), status);
}

std::string makeElementIntoQuery(BSON::Element::Ptr elem)
{
    switch (elem->getType())
    {
        case BSON::ElementTraits<double>::TypeId:
        case BSON::ElementTraits<Int32>::TypeId:
        case BSON::ElementTraits<Int64>::TypeId:
            return elem->toString();
        case BSON::ElementTraits<Poco::Timestamp>::TypeId:
            return fmt::format("toDateTime64({}, 3)", elem->toString());
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


std::string makeExpression(BSON::Element::Ptr elem)
{
    if (elem->getType() == BSON::ElementTraits<Int32>::TypeId)
        return std::to_string(BSON::Element::cast<Int32>(elem)->getValue());
    if (elem->getType() == BSON::ElementTraits<std::string>::TypeId)
    {
        auto elem_cast = BSON::Element::cast<std::string>(elem);
        auto str = std::move(elem_cast->getValue());
        removeDollarFromName(str);
        return str;
    }
    {
        auto doc = BSON::Element::cast<BSON::Document::Ptr>(elem)->getValue();
        const auto & elem_names = doc->elementNames();
        assert(elem_names.size() > 0);
        elem = doc->take(elem_names[0]);
    }
    const auto & name = elem->getName();

    std::string_view result_template;
    if (name == "$strLenBytes")
        result_template = "length({})";
    else if (name == "$minute")
        result_template = "extract(minute FROM {})";
    else if (name == "$toLong")
        result_template = "CAST({} AS Int64)";
    else if (name == "$toDecimal")
        result_template = "CAST({} AS Float32)";
    else if (name == "$dateTrunc")
    {
        auto doc = BSON::Element::cast<BSON::Document::Ptr>(elem)->getValue();
        std::string date = makeExpression(doc->take("date"));
        std::string unit = doc->take("unit")->toString();
        return fmt::format("DATE_TRUNC({}, {})", unit, date);
    }
    auto elem_cast = BSON::Element::cast<std::string>(elem);
    removeDollarFromName(elem_cast->getValue());
    return fmt::vformat(result_template, fmt::make_format_args(std::move(elem_cast->getValue())));
}


static std::string getQueryResult(std::string && query, ContextMutablePtr context)
{
    ReadBufferFromString read_buf(query);
    auto writer = WriteBufferFromOwnString();
    CurrentThread::QueryScope query_scope{context};
    executeQuery(read_buf, writer, false, context, {});
    return writer.str();
}

// TODO make Find use the same function
BSON::Document::Ptr launchQuery(std::string && query, ContextMutablePtr context, const std::string & db_name, const std::string & coll_name)
{
    auto result = getQueryResult(std::move(query), context);
    std::string ns = fmt::format("{}.{}", db_name, coll_name);
    return queryToDocument(std::move(result), std::move(ns));
}


std::vector<std::string> getColumnsFromTable(ContextMutablePtr context, const std::string & table_name)
{
    std::string query = fmt::format("SELECT * FROM {} LIMIT 0 FORMAT TabSeparatedWithNamesAndTypes", table_name);
    std::string query_res = getQueryResult(std::move(query), context);
    return parseQuery(std::move(query_res))[0];
}

}
}
