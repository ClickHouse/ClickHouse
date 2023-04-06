#include "FileRequestHandler.h"
#include "IServer.h"

#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Util/LayeredConfiguration.h>

#include <IO/HTTPCommon.h>
#include <Common/getResource.h>

#include <re2/re2.h>


namespace DB
{

FileRequestHandler::FileRequestHandler(IServer & server_, const std::string & base_directory_path_)
    : server(server_), base_directory_path(base_directory_path_)
{
}


void FileRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response)
{
    auto keep_alive_timeout = server.config().getUInt("keep_alive_timeout", 10);

    response.setContentType("text/html; charset=UTF-8");

    if (request.getVersion() == HTTPServerRequest::HTTP_1_1)
        response.setChunkedTransferEncoding(true);

    setResponseDefaultHeaders(response, keep_alive_timeout);


    response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_OK);
    *response.send() << getResource(base_directory_path + request.getURI());

}

}
