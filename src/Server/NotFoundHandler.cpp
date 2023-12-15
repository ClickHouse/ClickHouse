#include <Server/NotFoundHandler.h>

#include <IO/HTTPCommon.h>
#include <Common/Exception.h>

namespace DB
{
void NotFoundHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & /*write_event*/)
{
    try
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
        auto out = response.send();
        out->write("There is no handle ");
        out->write(request.getURI());
        out->write(!hints.empty() ? fmt::format(". Maybe you meant {}.", hints.front()) : "");
        out->write("\n\n", 2);
        out->write("Use / or /ping for health checks.\n");
        out->write("Or /replicas_status for more sophisticated health checks.\n\n");
        out->write("Send queries from your program with POST method or GET /?query=...\n\n");
        out->write("Use clickhouse-client:\n\n");
        out->write("For interactive data analysis:\n");
        out->write("    clickhouse-client\n\n");
        out->write("For batch query processing:\n");
        out->write("    clickhouse-client --query='SELECT 1' > result\n");
        out->write("    clickhouse-client < query > result\n");
    }
    catch (...)
    {
        tryLogCurrentException("NotFoundHandler");
    }
}

}
