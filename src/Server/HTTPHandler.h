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

class HTTPHandler : public HTTPRequestHandler
{
public:
    HTTPHandler(IServer & server_, const std::string & name);
    virtual ~HTTPHandler() override;

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override;

    /// This method is called right before the query execution.
    virtual void customizeContext(HTTPServerRequest & /* request */, ContextMutablePtr /* context */) {}

    virtual bool customizeQueryParam(ContextMutablePtr context, const std::string & key, const std::string & value) = 0;

    virtual std::string getQuery(HTTPServerRequest & request, HTMLForm & params, ContextMutablePtr context) = 0;

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

    /// It is the name of the server that will be sent in an http-header X-ClickHouse-Server-Display-Name.
    String server_display_name;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::HTTPConnection};

    // The request_context and the request_credentials instances may outlive a single request/response loop.
    // This happens only when the authentication mechanism requires more than a single request/response exchange (e.g., SPNEGO).
    ContextMutablePtr request_context;
    std::unique_ptr<Credentials> request_credentials;

    // Returns true when the user successfully authenticated,
    //  the request_context instance will be configured accordingly, and the request_credentials instance will be dropped.
    // Returns false when the user is not authenticated yet, and the 'Negotiate' response is sent,
    //  the request_context and request_credentials instances are preserved.
    // Throws an exception if authentication failed.
    bool authenticateUser(
        ContextMutablePtr context,
        HTTPServerRequest & request,
        HTMLForm & params,
        HTTPServerResponse & response);

    /// Also initializes 'used_output'.
    void processQuery(
        ContextMutablePtr context,
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
};

class DynamicQueryHandler : public HTTPHandler
{
private:
    std::string param_name;
public:
    explicit DynamicQueryHandler(IServer & server_, const std::string & param_name_ = "query");

    std::string getQuery(HTTPServerRequest & request, HTMLForm & params, ContextMutablePtr context) override;

    bool customizeQueryParam(ContextMutablePtr context, const std::string &key, const std::string &value) override;
};

class PredefinedQueryHandler : public HTTPHandler
{
private:
    NameSet receive_params;
    std::string predefined_query;
    CompiledRegexPtr url_regex;
    std::unordered_map<String, CompiledRegexPtr> header_name_with_capture_regex;
public:
    PredefinedQueryHandler(
        IServer & server_, const NameSet & receive_params_, const std::string & predefined_query_
        , const CompiledRegexPtr & url_regex_, const std::unordered_map<String, CompiledRegexPtr> & header_name_with_regex_);

    virtual void customizeContext(HTTPServerRequest & request, ContextMutablePtr context) override;

    std::string getQuery(HTTPServerRequest & request, HTMLForm & params, ContextMutablePtr context) override;

    bool customizeQueryParam(ContextMutablePtr context, const std::string & key, const std::string & value) override;
};

}
