#include <Server/ACME/RequestHandler.h>

#if USE_SSL

#include <IO/HTTPCommon.h>
#include <IO/Operators.h>
#include <Server/ACME/Client.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>

#include <Poco/Net/HTTPServerResponse.h>


namespace DB
{

void ACMERequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event &)
{
    auto challenge = ACME::Client::instance().requestChallenge(request.getURI());

    if (challenge.empty())
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
        *response.send() << "Not found.\n";
    }

    setResponseDefaultHeaders(response);
    response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_OK);

    auto wb = WriteBufferFromHTTPServerResponse(response, request.getMethod() == HTTPRequest::HTTP_HEAD);
    wb.write(challenge.data(), challenge.size());
    wb.finalize();
}


}

#endif
