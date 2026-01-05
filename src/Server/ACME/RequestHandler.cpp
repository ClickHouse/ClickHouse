#include <Server/ACME/RequestHandler.h>

#if USE_SSL

#include <IO/HTTPCommon.h>
#include <IO/Operators.h>
#include <Server/ACME/Client.h>

#include <Poco/Net/HTTPServerResponse.h>


namespace DB
{

void ACMERequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponseBase & response)
{
    auto challenge = ACME::Client::instance().requestChallenge(request.getURI());

    if (challenge.empty())
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
        const char * msg = "Not found.\n";
        response.makeStream()->sendBufferAndFinalize(msg, strlen(msg));
        return;
    }

    response.setResponseDefaultHeaders();
    response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_OK);

    auto wb = response.makeStream();
    wb->write(challenge.data(), challenge.size());
    wb->finalize();
}


}

#endif
