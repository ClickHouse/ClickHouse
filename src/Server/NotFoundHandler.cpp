#include "NotFoundHandler.h"

#include <IO/HTTPCommon.h>

#include <Common/Exception.h>

#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>

namespace DB
{

void NotFoundHandler::handleRequest(
    Poco::Net::HTTPServerRequest & request,
    Poco::Net::HTTPServerResponse & response)
{
    try
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
        response.send() << "There is no handle " << request.getURI() << "\n\n"
                        << "Use / or /ping for health checks.\n"
                        << "Or /replicas_status for more sophisticated health checks.\n\n"
                        << "Send queries from your program with POST method or GET /?query=...\n\n"
                        << "Use clickhouse-client:\n\n"
                        << "For interactive data analysis:\n"
                        << "    clickhouse-client\n\n"
                        << "For batch query processing:\n"
                        << "    clickhouse-client --query='SELECT 1' > result\n"
                        << "    clickhouse-client < query > result\n";
    }
    catch (...)
    {
        tryLogCurrentException("NotFoundHandler");
    }
}

}
