#pragma once

#include <memory>
#include <Core/Mongo/Document.h>
#include <Core/Mongo/MongoProtocol.h>
#include <Core/Mongo/Wire/OpMessage.h>
#include <Core/Mongo/Wire/OpQuery.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Session.h>

namespace DB::MongoProtocol
{

std::vector<std::string> splitByNewline(const std::string & s);

String modifyFilter(const String & json);

struct OpMessageSection;

struct IHandler
{
    virtual std::vector<String> getIdentifiers() const = 0;
    virtual std::vector<Document> handle(const std::vector<OpMessageSection> & documents, std::unique_ptr<Session> & session) = 0;

    virtual ~IHandler() = default;
};
using HandlerPtr = std::shared_ptr<IHandler>;

Header makeResponseHeader(Header request_header, Int32 message_size, Int32 response_id);

std::vector<Document> runMessageRequest(const std::vector<OpMessageSection> & sections, std::unique_ptr<Session> & session);
std::vector<Document> runQueryRequst(const std::vector<Document> & documents, std::unique_ptr<Session> & session);

void handle(Header header, std::shared_ptr<MessageTransport> transport, std::unique_ptr<Session> & session);

}
