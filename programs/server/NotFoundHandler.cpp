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

        std::stringstream output_description;
        output_description << "There is no handle " << request.getURI() << "\n\n";

        if (!no_handler_description.empty())
            output_description << no_handler_description << "\n";

        response.send() << output_description.str();
    }
    catch (...)
    {
        tryLogCurrentException("NotFoundHandler");
    }
}

}
