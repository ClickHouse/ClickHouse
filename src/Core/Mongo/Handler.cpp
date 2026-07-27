#include <Core/Mongo/Handler.h>

#include <memory>
#include <Core/Mongo/Document.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>
#include <Core/Mongo/Handlers/IsMaster.h>
#include <Core/Mongo/MongoProtocol.h>
#include <Core/Mongo/Wire/OpMessage.h>
#include <Core/Mongo/Wire/OpQuery.h>
#include <bson/bson.h>
#include <IO/ReadBufferFromString.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/quoteString.h>

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace DB::MongoProtocol
{

std::vector<std::string> splitByNewline(const std::string & s)
{
    std::vector<std::string> result;
    std::stringstream ss(s);
    std::string line;

    while (std::getline(ss, line, '\n'))
        result.push_back(line);

    return result;
}

static void AddPrefixToKeys(
    rapidjson::Value & value, rapidjson::Document::AllocatorType & allocator, const String & current_path = "", bool in_projection = false)
{
    if (value.IsObject())
    {
        rapidjson::Value new_object(rapidjson::kObjectType);
        for (auto it = value.MemberBegin(); it != value.MemberEnd(); ++it)
        {
            if (!in_projection)
            {
                std::string key = it->name.GetString();
                if (key.empty() || key[0] == '$')
                {
                    new_object.AddMember(it->name, it->value, allocator);
                }
                else
                {
                    auto result_path = current_path.empty() ? key : current_path + "." + key;
                    rapidjson::Value new_key(result_path.c_str(), allocator);
                    new_object.AddMember(new_key, it->value, allocator);
                }
            }
            else if (it->value.IsString())
            {
                std::string str_value = it->value.GetString();
                if (str_value.empty() || str_value[0] == '$')
                {
                    new_object.AddMember(it->name, it->value, allocator);
                }
                else
                {
                    auto result_path = current_path.empty() ? str_value : current_path + "." + str_value;
                    rapidjson::Value new_value(result_path.c_str(), allocator);
                    new_object.AddMember(it->name, new_value, allocator);
                }
            }
            else
            {
                new_object.AddMember(it->name, it->value, allocator);
            }
        }
        value = std::move(new_object);
    }

    if (value.IsObject())
    {
        for (auto & member : value.GetObject())
        {
            String name = member.name.GetString();
            if (name == "$projection")
                AddPrefixToKeys(member.value, allocator, current_path, true);
            else if (!current_path.empty())
                AddPrefixToKeys(member.value, allocator, current_path + "." + name, in_projection);
            else
                AddPrefixToKeys(member.value, allocator, name, in_projection);
        }
    }
    else if (value.IsArray())
    {
        for (auto & element : value.GetArray())
        {
            AddPrefixToKeys(element, allocator);
        }
    }
}

String modifyFilter(const String & json)
{
    rapidjson::Document doc;
    doc.Parse(json.c_str());

    if (doc.HasParseError())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect json in filter");
    }

    AddPrefixToKeys(doc, doc.GetAllocator());

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);

    String result = buffer.GetString();
    return result;
}

String CollectionRef::getQualifiedName() const
{
    return backQuoteIfNeed(database) + "." + backQuoteIfNeed(collection);
}

CollectionRef getCollectionRef(const Document & command, const String & command_name)
{
    auto json = command.getRapidJsonRepresentation();

    auto collection_it = json.FindMember(command_name.c_str());
    if (collection_it == json.MemberEnd() || !collection_it->value.IsString())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Mongo command '{}' does not contain a collection name", command_name);

    /// Every command sent over `OP_MSG` carries the database it applies to in `$db`.
    auto database_it = json.FindMember("$db");
    if (database_it == json.MemberEnd() || !database_it->value.IsString())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Mongo command '{}' does not contain the '$db' database name", command_name);

    CollectionRef result{.database = database_it->value.GetString(), .collection = collection_it->value.GetString()};

    if (result.database.empty() || result.collection.empty())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Empty Mongo database or collection name in the command '{}': '{}.{}'",
            command_name,
            result.database,
            result.collection);

    /// Handlers build a query in the Mongo dialect, where a collection is addressed as
    /// `<database>.<collection>`. Restricting the names to word characters keeps that text
    /// unambiguous and makes it impossible for a name to change the meaning of the query.
    auto validate = [](const String & name)
    {
        for (char c : name)
            if (!isWordCharASCII(c) && c != '-')
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Mongo database and collection names must consist of letters, digits, '_' and '-', got '{}'",
                    name);
    };
    validate(result.database);
    validate(result.collection);

    return result;
}

bool objectExists(std::shared_ptr<QueryExecutor> executor, const String & object_kind, const String & name)
{
    /// `EXISTS TABLE` also answers `0` when the database itself is absent.
    auto output = executor->execute(fmt::format("EXISTS {} {}", object_kind, name));
    return !output.empty() && output[0] == '1';
}

Header makeResponseHeader(Header request_header, Int32 message_size, Int32 response_id)
{
    Header result;
    result.message_length = message_size;
    result.operation_code = static_cast<Int32>(OperationCode::OP_REPLY);
    result.response_to = request_header.request_id;
    result.request_id = response_id;
    return result;
}

std::vector<Document> runMessageRequest(const std::vector<OpMessageSection> & sections, std::shared_ptr<QueryExecutor> executor)
{
    if (sections.empty() || sections[0].kind != 0 || sections[0].documents.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Mongo message does not start with a command document");

    auto keys = sections[0].documents[0].getDocumentKeys();
    if (keys.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Mongo command document is empty");

    const auto & command = keys[0];
    auto handler = HandlerRegitstry().getHandler(command);
    if (!handler)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Command {} is not supported yet.", command);

    return handler->handle(sections, executor);
}

std::vector<Document> runQueryRequst(const std::vector<Document> &, std::shared_ptr<QueryExecutor> executor)
{
    auto handler = IsMasterHandler();
    return handler.handle({}, executor);
}


namespace
{

/// Turns the current exception into the `{"errmsg": ..., "ok": 0}` document Mongo clients expect.
std::vector<Document> makeErrorResponse()
{
    bson_t * bson_doc = bson_new();

    BSON_APPEND_UTF8(bson_doc, "errmsg", getCurrentExceptionMessage(false).c_str());
    BSON_APPEND_DOUBLE(bson_doc, "ok", 0.0);

    std::vector<Document> result;
    result.emplace_back(bson_doc);
    return result;
}

}

void handle(
    const Header & header, ReadBuffer & payload, std::shared_ptr<MessageTransport> transport, std::shared_ptr<QueryExecutor> executor)
{
    auto op_code = static_cast<OperationCode>(header.operation_code);
    switch (op_code)
    {
        case OperationCode::OP_MSG: {
            OpMessage request;
            request.deserialize(payload);

            std::vector<Document> response_doc;
            try
            {
                response_doc = runMessageRequest(request.sections, executor);
            }
            catch (...)
            {
                response_doc = makeErrorResponse();
            }
            auto response = OpMessage(request.flags, 0, response_doc);
            auto response_header = makeResponseHeader(header, response.size(), transport->getNextResponseId());
            response_header.operation_code = static_cast<Int32>(OperationCode::OP_MSG);
            response.header = response_header;

            transport->send(response, true);
            break;
        }
        case OperationCode::OP_QUERY: {
            OpQuery request;
            request.deserialize(payload);

            std::vector<Document> response_doc;
            try
            {
                response_doc = runQueryRequst({request.query}, executor);
            }
            catch (...)
            {
                response_doc = makeErrorResponse();
            }
            auto response = OpQuery(std::move(response_doc[0]));
            auto response_header = makeResponseHeader(header, response.size(), transport->getNextResponseId());
            response.header = response_header;

            transport->send(response, true);
            break;
        }
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not supported operation code {}", header.operation_code);
    }
}

}
