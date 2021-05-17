#pragma once

#include <Core/Names.h>
#include <Server/HTTP/HTMLForm.h>
#include <Server/HTTP/HTTPRequestHandler.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>

#include <re2/re2.h>

namespace CurrentMetrics
{
    extern const Metric HTTPConnection;
}

namespace Poco { class Logger; }

namespace DB
{

class Credentials;
class IServer;
class WriteBufferFromHTTPServerResponse;

using CompiledRegexPtr = std::shared_ptr<const re2::RE2>;

class HTTPStreamHandler : public HTTPRequestHandler
{
public:
    HTTPStreamHandler(IServer & server_, const std::string & name, const std::string & param_name_);
    virtual ~HTTPStreamHandler() override;

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override;

    void customizeContext(HTTPServerRequest & /* request */, ContextPtr /* context */) {}

    bool customizeQueryParam(ContextPtr context, const std::string & key, const std::string & value);

    std::string getQuery(HTTPServerRequest & request, HTMLForm & params, ContextPtr context);

private:
    struct Output
    {
        /* Raw data
         * ↓
         * CascadeWriteBuffer out_maybe_delayed_and_compressed (optional)
         * ↓ (forwards data if an overflow is occur or explicitly via pushDelayedResults)
         * CompressedWriteBuffer out_maybe_compressed (optional)
         * ↓
         * WriteBufferFromHTTPServerResponse out
         */

        std::shared_ptr<WriteBufferFromHTTPServerResponse> out;
        /// Points to 'out' or to CompressedWriteBuffer(*out), depending on settings.
        std::shared_ptr<WriteBuffer> out_maybe_compressed;
        /// Points to 'out' or to CompressedWriteBuffer(*out) or to CascadeWriteBuffer.
        std::shared_ptr<WriteBuffer> out_maybe_delayed_and_compressed;

        inline bool hasDelayed() const
        {
            return out_maybe_delayed_and_compressed != out_maybe_compressed;
        }
    };

    IServer & server;
    Poco::Logger * log;
    std::string param_name;

    /// It is the name of the server that will be sent in an http-header X-ClickHouse-Server-Display-Name.
    String server_display_name;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::HTTPConnection};

    // The request_context and the request_credentials instances may outlive a single request/response loop.
    // This happens only when the authentication mechanism requires more than a single request/response exchange (e.g., SPNEGO).
    ContextPtr request_context;
    std::unique_ptr<Credentials> request_credentials;

    // Returns true when the user successfully authenticated,
    //  the request_context instance will be configured accordingly, and the request_credentials instance will be dropped.
    // Returns false when the user is not authenticated yet, and the 'Negotiate' response is sent,
    //  the request_context and request_credentials instances are preserved.
    // Throws an exception if authentication failed.
    bool authenticateUser(
        ContextPtr context,
        HTTPServerRequest & request,
        HTMLForm & params,
        HTTPServerResponse & response);

    /// Also initializes 'used_output'.
    void processQuery(
        ContextPtr context,
        HTTPServerRequest & request,
        HTMLForm & params,
        HTTPServerResponse & response,
        Output & used_output,
        std::optional<CurrentThread::QueryScope> & query_scope);

    void trySendExceptionToClient(
        const std::string & s,
        int exception_code,
        HTTPServerRequest & request,
        HTTPServerResponse & response,
        Output & used_output);

    static void pushDelayedResults(Output & used_output);

    void sendSummary(WriteBuffer & ostr, bool is_starting);
    // void sendHello();
    // void sendData(const Block & block);    /// Write a block to the network.
    // void sendLogData(const Block & block);
    // void sendTableColumns(const ColumnsDescription & columns);
    // void sendException(const Exception & e, bool with_stack_trace); //done
    // void sendProgress();
    // void sendLogs();
    // void sendEndOfStream();
    // void sendPartUUIDs();
    // void sendReadTaskRequestAssumeLocked();
    // void sendProfileInfo(const BlockStreamProfileInfo & info);
    // void sendTotals(const Block & totals);
    // void sendExtremes(const Block & extremes);
};

}
