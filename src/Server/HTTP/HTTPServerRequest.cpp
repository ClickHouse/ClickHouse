#include <Server/HTTP/HTTPServerRequest.h>

#include <Server/HTTP/HTTPServerResponse.h>

#include <Poco/Net/HTTPChunkedStream.h>
#include <Poco/Net/HTTPFixedLengthStream.h>
#include <Poco/Net/HTTPHeaderStream.h>
#include <Poco/Net/HTTPStream.h>

namespace DB
{

HTTPServerRequest::HTTPServerRequest(
    HTTPServerResponse & response_, Poco::Net::HTTPServerSession & session_, Poco::Net::HTTPServerParams * params_)
    : response(response_), session(session_), params(params_, true)
{
    response.attachRequest(this);

    Poco::Net::HTTPHeaderInputStream hs(session);
    read(hs);

    // Now that we know socket is still connected, obtain addresses
    client_address = session.clientAddress();
    server_address = session.serverAddress();

    if (getChunkedTransferEncoding())
        stream = std::make_unique<Poco::Net::HTTPChunkedInputStream>(session);
    else if (hasContentLength())
        stream = std::make_unique<Poco::Net::HTTPFixedLengthInputStream>(session, getContentLength64());
    else if (getMethod() == HTTPRequest::HTTP_GET || getMethod() == HTTPRequest::HTTP_HEAD || getMethod() == HTTPRequest::HTTP_DELETE)
        stream = std::make_unique<Poco::Net::HTTPFixedLengthInputStream>(session, 0);
    else
        stream = std::make_unique<Poco::Net::HTTPInputStream>(session);
}

}
