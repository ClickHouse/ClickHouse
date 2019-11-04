#pragma once

#include "IServer.h"

#include <Poco/Net/HTTPRequestHandler.h>

#include <Common/CurrentMetrics.h>
#include <Common/HTMLForm.h>

#include <Interpreters/CustomHTTP/HTTPOutputStreams.h>


namespace CurrentMetrics
{
    extern const Metric HTTPConnection;
}

namespace Poco { class Logger; }

namespace DB
{

class WriteBufferFromHTTPServerResponse;


class HTTPHandler : public Poco::Net::HTTPRequestHandler
{
public:
    explicit HTTPHandler(IServer & server_);

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override;

private:
    using HTTPRequest = Poco::Net::HTTPServerRequest;
    using HTTPResponse = Poco::Net::HTTPServerResponse;

    struct SessionContextHolder
    {
        ~SessionContextHolder();

        SessionContextHolder(IServer & accepted_server, HTMLForm & params);

        void authentication(HTTPServerRequest & request, HTMLForm & params);

        String session_id;
        std::unique_ptr<Context> context = nullptr;
        std::shared_ptr<Context> session_context = nullptr;
        std::chrono::steady_clock::duration session_timeout;
    };

    IServer & server;
    Poco::Logger * log;

    /// It is the name of the server that will be sent in an http-header X-ClickHouse-Server-Display-Name.
    String server_display_name;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::HTTPConnection};

    /// Also initializes 'used_output'.
    void processQuery(HTTPRequest & request, HTMLForm & params, HTTPResponse & response, SessionContextHolder & holder);

    void trySendExceptionToClient(
        const std::string & message, int exception_code, HTTPRequest & request, HTTPResponse & response, HTTPOutputStreams & used_output);

};

}
