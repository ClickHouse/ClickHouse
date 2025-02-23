#include <cstdint>
#include <Core/Mongo/Document.h>
#include <Core/Mongo/MongoProtocol.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/executeQuery.h>
#include <Poco/Net/SocketAddress.h>
#include "Common/randomSeed.h"

namespace DB::MongoProtocol
{

Header::Header(const Header & other)
{
    message_length = other.message_length;
    request_id = other.request_id;
    response_to = other.response_to;
    operation_code = other.operation_code;
}

Header & Header::operator=(const Header & right)
{
    if (this == &right)
        return *this;

    message_length = right.message_length;
    request_id = right.request_id;
    response_to = right.response_to;
    operation_code = right.operation_code;
    return *this;
}

void Header::deserialize(ReadBuffer & in)
{
    readBinaryLittleEndian(message_length, in);
    readBinaryLittleEndian(request_id, in);
    readBinaryLittleEndian(response_to, in);
    readBinaryLittleEndian(operation_code, in);
}

void Header::serialize(WriteBuffer & out) const
{
    writeBinaryLittleEndian(message_length, out);
    writeBinaryLittleEndian(request_id, out);
    writeBinaryLittleEndian(response_to, out);
    writeBinaryLittleEndian(operation_code, out);
}

Int32 Header::size() const
{
    return 16;
}

QueryExecutor::QueryExecutor(std::unique_ptr<Session> & session_, const Poco::Net::SocketAddress & address_)
    : session(session_), address(address_), gen(randomSeed()), dis(0, INT32_MAX)
{
}

String QueryExecutor::execute(const String & query)
{
    auto query_context = session->makeQueryContext();
    auto secret_key = dis(gen);

    query_context->setCurrentQueryId(fmt::format("mongo:{:d}", secret_key));

    CurrentThread::QueryScope query_scope{query_context};
    ReadBufferFromString read_buf(query);

    WriteBufferFromOwnString out;
    executeQuery(read_buf, out, false, query_context, {});

    return out.str();
}

void QueryExecutor::authenticate(const String & username, const String & password)
{
    session->authenticate(username, password, address);
}

}
