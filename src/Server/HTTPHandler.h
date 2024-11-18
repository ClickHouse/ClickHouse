#pragma once

#include <optional>
#include <string>
#include <unordered_map>
#include <Core/Names.h>
#include <Server/HTTP/HTMLForm.h>
#include <Server/HTTP/HTTPRequestHandler.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <IO/CascadeWriteBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Common/re2.h>
#include <Access/Credentials.h>

#include "HTTPResponseHeaderWriter.h"

namespace CurrentMetrics
{
    extern const Metric HTTPConnection;
}

namespace Poco { class Logger; }

namespace DB
{

class Session;
class IServer;
struct Settings;
class WriteBufferFromHTTPServerResponse;

using CompiledRegexPtr = std::shared_ptr<const re2::RE2>;

struct HTTPHandlerConnectionConfig
{
    std::optional<BasicCredentials> credentials;

    /// TODO:
    /// String quota;
    /// String default_database;

    HTTPHandlerConnectionConfig() = default;
    HTTPHandlerConnectionConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix);
};

class HTTPHandler : public HTTPRequestHandler
{
public:
    HTTPHandler(IServer & server_, const HTTPHandlerConnectionConfig & connection_config_, const std::string & name, const HTTPResponseHeaderSetup & http_response_headers_override_);
    ~HTTPHandler() override;

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;

    /// This method is called right before the query execution.
    virtual void customizeContext(HTTPServerRequest & /* request */, ContextMutablePtr /* context */, ReadBuffer & /* body */) {}

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

        /// Holds original response buffer
        std::shared_ptr<WriteBufferFromHTTPServerResponse> out_holder;
        /// If HTTP compression is enabled holds compression wrapper over original response buffer
        std::shared_ptr<WriteBuffer> wrap_compressed_holder;
        /// Points either to out_holder or to wrap_compressed_holder
        std::shared_ptr<WriteBuffer> out;

        /// If internal compression is enabled holds compression wrapper over out buffer
        std::shared_ptr<CompressedWriteBuffer> out_compressed_holder;
        /// Points to 'out' or to CompressedWriteBuffer(*out)
        std::shared_ptr<WriteBuffer> out_maybe_compressed;

        /// If output should be delayed holds cascade buffer
        std::unique_ptr<CascadeWriteBuffer> out_delayed_and_compressed_holder;
        /// Points to out_maybe_compressed or to CascadeWriteBuffer.
        WriteBuffer * out_maybe_delayed_and_compressed = nullptr;

        bool finalized = false;
        bool canceled = false;

        bool exception_is_written = false;
        std::function<void(WriteBuffer &, const String &)> exception_writer;

        bool hasDelayed() const
        {
            return out_maybe_delayed_and_compressed != out_maybe_compressed.get();
        }

        void finalize()
        {
            if (finalized)
                return;
            finalized = true;

            if (out_compressed_holder)
                out_compressed_holder->finalize();
            if (out)
                out->finalize();
        }

        void cancel()
        {
            if (canceled)
                return;
            canceled = true;

            if (out_compressed_holder)
                out_compressed_holder->cancel();
            if (out)
                out->cancel();
        }


        bool isCanceled() const
        {
            return canceled;
        }

        bool isFinalized() const
        {
            return finalized;
        }
    };

    IServer & server;
    LoggerPtr log;

    /// It is the name of the server that will be sent in an http-header X-ClickHouse-Server-Display-Name.
    String server_display_name;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::HTTPConnection};

    /// Reference to the immutable settings in the global context.
    /// Those settings are used only to extract a http request's parameters.
    /// See settings http_max_fields, http_max_field_name_size, http_max_field_value_size in HTMLForm.
    const Settings & default_settings;

    /// Overrides for response headers.
    HTTPResponseHeaderSetup http_response_headers_override;

    // session is reset at the end of each request/response.
    std::unique_ptr<Session> session;

    // The request_credential instance may outlive a single request/response loop.
    // This happens only when the authentication mechanism requires more than a single request/response exchange (e.g., SPNEGO).
    std::unique_ptr<Credentials> request_credentials;
    HTTPHandlerConnectionConfig connection_config;

    /// Also initializes 'used_output'.
    void processQuery(
        HTTPServerRequest & request,
        HTMLForm & params,
        HTTPServerResponse & response,
        Output & used_output,
        std::optional<CurrentThread::QueryScope> & query_scope,
        const ProfileEvents::Event & write_event);

    void trySendExceptionToClient(
        const std::string & s,
        int exception_code,
        HTTPServerRequest & request,
        HTTPServerResponse & response,
        Output & used_output);

    static void pushDelayedResults(Output & used_output);

protected:
    // @see authenticateUserByHTTP()
    virtual bool authenticateUser(
        HTTPServerRequest & request,
        HTMLForm & params,
        HTTPServerResponse & response);
};

class DynamicQueryHandler : public HTTPHandler
{
private:
    std::string param_name;

public:
    explicit DynamicQueryHandler(
        IServer & server_,
        const HTTPHandlerConnectionConfig & connection_config,
        const std::string & param_name_ = "query",
        const HTTPResponseHeaderSetup & http_response_headers_override_ = std::nullopt);

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
        IServer & server_,
        const HTTPHandlerConnectionConfig & connection_config,
        const NameSet & receive_params_,
        const std::string & predefined_query_,
        const CompiledRegexPtr & url_regex_,
        const std::unordered_map<String, CompiledRegexPtr> & header_name_with_regex_,
        const HTTPResponseHeaderSetup & http_response_headers_override_ = std::nullopt);

    void customizeContext(HTTPServerRequest & request, ContextMutablePtr context, ReadBuffer & body) override;

    std::string getQuery(HTTPServerRequest & request, HTMLForm & params, ContextMutablePtr context) override;

    bool customizeQueryParam(ContextMutablePtr context, const std::string & key, const std::string & value) override;
};

}
