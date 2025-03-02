#include "Handler.h"

#include <memory>
#include <Core/Mongo/Document.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>
#include <Core/Mongo/Handlers/IsMaster.h>
#include <Core/Mongo/MongoProtocol.h>
#include <Core/Mongo/Wire/OpMessage.h>
#include <Core/Mongo/Wire/OpQuery.h>
#include <bson/bson.h>
#include <Common/Exception.h>

#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

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

void AddPrefixToKeys(
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

String clearQuery(const String & query)
{
    String cleared_query;
    for (auto elem : query)
        if (elem != '`' && elem != 0)
            cleared_query.push_back(elem);
    return cleared_query;
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
    auto command = sections[0].documents[0].getDocumentKeys()[0];
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


void handle(Header header, std::shared_ptr<MessageTransport> transport, std::shared_ptr<QueryExecutor> executor)
{
    auto op_code = static_cast<OperationCode>(header.operation_code);
    switch (op_code)
    {
        case OperationCode::OP_MSG: {
            auto request = transport->receive<OpMessage>();

            std::vector<Document> docs;
            std::vector<Document> response_doc;
            try
            {
                response_doc = runMessageRequest(request->sections, executor);
            }
            catch (const Exception & ex)
            {
                bson_t * bson_doc = bson_new();

                BSON_APPEND_UTF8(bson_doc, "errmsg", ex.what());
                BSON_APPEND_DOUBLE(bson_doc, "ok", 0.0);

                Document doc(bson_doc);
                response_doc = std::vector{doc};
            }
            auto response = OpMessage(request->flags, 0, response_doc);
            auto response_header = makeResponseHeader(header, response.size(), transport->getNextResponseId());
            response_header.operation_code = static_cast<Int32>(OperationCode::OP_MSG);
            response.header = response_header;

            transport->send(response, true);
            break;
        }
        case OperationCode::OP_QUERY: {
            auto request = transport->receive<OpQuery>();
            std::vector<Document> response_doc;
            try
            {
                response_doc = runQueryRequst({request->query}, executor);
            }
            catch (const Exception & ex)
            {
                bson_t * bson_doc = bson_new();

                BSON_APPEND_UTF8(bson_doc, "errmsg", ex.what());
                BSON_APPEND_DOUBLE(bson_doc, "ok", 0.0);

                Document doc(bson_doc);
                response_doc = std::vector{doc};
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
