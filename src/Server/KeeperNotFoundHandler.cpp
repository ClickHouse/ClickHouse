#include <Server/KeeperNotFoundHandler.h>

#include <IO/HTTPCommon.h>
#include <IO/Operators.h>
#include <Common/Exception.h>

namespace DB
{
void KeeperNotFoundHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & /*write_event*/)
{
    try
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
        *response.send() << "There is no handle " << request.getURI()
                         << (!hints.empty() ? fmt::format(". Maybe you meant {}.", hints.front()) : "") << "\n\n"
                         << "Use /ready for health checks.\n"
                         << "Or /api/v1/commands for more sophisticated health checks and monitoring.\n\n"
                         << "Use Web UI monitoring panel at /dashboard.\n\n"
                         << "Access Keeper storage directly with /api/v1/storage POST or GET methods.\n";
    }
    catch (...)
    {
        tryLogCurrentException("KeeperNotFoundHandler");
    }
}

}
