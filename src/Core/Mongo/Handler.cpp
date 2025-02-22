#include "Handler.h"

#include <memory>
#include <Core/Mongo/Document.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>
#include <Core/Mongo/Handlers/IsMaster.h>
#include <Core/Mongo/MongoProtocol.h>
#include <Core/Mongo/Wire/OpMessage.h>
#include <Core/Mongo/Wire/OpQuery.h>
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

void AddPrefixToKeys(rapidjson::Value & value, rapidjson::Document::AllocatorType & allocator)
{
    if (value.IsObject())
    {
        rapidjson::Value newObject(rapidjson::kObjectType);
        for (auto it = value.MemberBegin(); it != value.MemberEnd(); ++it)
        {
            std::string key = it->name.GetString();
            if (key.empty() || key[0] == '$')
            {
                newObject.AddMember(it->name, it->value, allocator);
            }
            else
            {
                rapidjson::Value newKey(("json." + key).c_str(), allocator);
                newObject.AddMember(newKey, it->value, allocator);
            }
        }
        value = std::move(newObject);
    }

    if (value.IsObject())
    {
        for (auto & member : value.GetObject())
        {
            AddPrefixToKeys(member.value, allocator);
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

    return buffer.GetString();
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

std::vector<Document> runMessageRequest(const std::vector<OpMessageSection> & sections, std::unique_ptr<Session> & session)
{
    auto command = sections[0].documents[0].getDocumentKeys()[0];
    std::cerr << "Command " << command << '\n';
    auto handler = HandlerRegitstry().getHandler(command);
    if (!handler)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Command {} is not supported yet.", command);

    return handler->handle(sections, session);
}

std::vector<Document> runQueryRequst(const std::vector<Document> &, std::unique_ptr<Session> & session)
{
    auto handler = IsMasterHandler();
    return handler.handle({}, session);
    //throw Exception(ErrorCodes::BAD_ARGUMENTS, "Query requst should be only is master request");
}


void handle(Header header, std::shared_ptr<MessageTransport> transport, std::unique_ptr<Session> & session)
{
    auto op_code = static_cast<OperationCode>(header.operation_code);
    std::cerr << "op code " << header.operation_code << '\n';
    switch (op_code)
    {
        case OperationCode::OP_MSG: {
            auto request = transport->receive<OpMessage>();
            std::cerr << "request readed successfull\n";
            //if (request->sections.size() > 1)
            //    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multiple sections not supported yet.");

            std::cerr << "run message request\n";
            std::vector<Document> docs;
            auto response_doc = runMessageRequest(request->sections, session);

            auto response = OpMessage(request->flags, 0, response_doc);
            std::cerr << "response.size() " << response.sections.size() << ' ' << response.size() << ' ' << request->flags << '\n';
            auto response_header = makeResponseHeader(header, response.size(), transport->getNextResponseId());
            response_header.operation_code = static_cast<Int32>(OperationCode::OP_MSG);
            response.header = response_header;

            transport->send(response, true);
            break;
        }
        case OperationCode::OP_QUERY: {
            std::cerr << "OP QUERY\n";
            auto request = transport->receive<OpQuery>();
            std::cerr << "OP QUERY bp1\n";
            //auto doc = transport->receive<Document>();
            auto response_doc = runQueryRequst({request->query}, session);

            std::cerr << "OP QUERY bp2\n";
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
