#include <Storages/MergeTree/RemoteQueryExecutor.h>
#include <Interpreters/executeQuery.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Poco/Net/HTTPRequest.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
}

namespace RemoteQueryExecutor
{

namespace
{

std::string getEndpointId(const std::string & node_id)
{
    return "RemoteQueryExecutor:" + node_id;
}

}

Service::Service(Context & context_)
    : context{context_}
{
}

std::string Service::getId(const std::string & node_id) const
{
    return getEndpointId(node_id);
}

void Service::processQuery(const Poco::Net::HTMLForm & params, ReadBuffer & body, WriteBuffer & out, Poco::Net::HTTPServerResponse & response)
{
    if (is_cancelled)
        throw Exception{"RemoteQueryExecutor service terminated", ErrorCodes::ABORTED};

    std::string query = params.get("query");

    bool flag = true;

    try
    {
        (void) executeQuery(query, context, true);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        flag = false;
    }

    writeBinary(flag, out);
    out.next();
}

bool Client::executeQuery(const InterserverIOEndpointLocation & location, const std::string & query)
{
    Poco::URI uri;
    uri.setScheme("http");
    uri.setHost(location.host);
    uri.setPort(location.port);
    uri.setQueryParameters(
    {
        {"endpoint", getEndpointId(location.name)},
        {"compress", "false"},
        {"query", query}
    });

    ReadWriteBufferFromHTTP in{uri, Poco::Net::HTTPRequest::HTTP_POST};

    bool flag;
    readBinary(flag, in);
    assertEOF(in);

    return flag;
}

}

}
