#pragma once

#include <optional>
#include <string>
#include <unordered_map>
#include <Core/Names.h>
#include <Server/HTTP/HTMLForm.h>
#include <Server/HTTP/HTTPRequestHandler.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <Server/HTTPPathHints.h>
#include <Common/CurrentMetrics.h>
#include <Common/QueryScope.h>
#include <IO/CascadeWriteBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Common/re2.h>
#include <Access/Credentials.h>

#include <Server/HTTPResponseHeaderWriter.h>

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
struct SQLDefinedHandler;

using CompiledRegexPtr = std::shared_ptr<const re2::RE2>;

struct HTTPHandlerConnectionConfig
{
    std::optional<AlwaysAllowCredentials> credentials;

    /// If set, overrides the `default_session_user` server setting for requests
    /// without credentials (composable protocols allow a per-endpoint default user).
    std::optional<String> default_session_user;

    /// TODO:
    /// String quota;
    /// String default_database;

    HTTPHandlerConnectionConfig() = default;
    HTTPHandlerConnectionConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix);
};

class HTTPHandler : public HTTPRequestHandler
{
public:
    HTTPHandler(IServer & server_, const HTTPHandlerConnectionConfig & connection_config_, const std::string & name, const HTTPResponseHeaderSetup & http_response_headers_override_, const std::string & url_prefix_ = "", HTTPPathHintsPtr path_hints_ = nullptr);
    ~HTTPHandler() override;

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;

    /// This method is called right before the query execution.
    virtual void customizeContext(HTTPServerRequest & /* request */, ContextMutablePtr /* context */, ReadBuffer & /* body */) {}

    virtual bool customizeQueryParam(NameToNameMap & query_parameters, const std::string & key, const std::string & value) = 0;

    /// Only the dynamic query handler interprets arbitrary request paths as query inputs. Configured
    /// and SQL-defined handlers own their matched path and must execute their stored query unchanged.
    virtual bool parsesHTTPPath() const { return false; }

    /// `body` is the request body wrapped in the transport decompression chain - the same object the query
    /// itself would read. Handlers must read the body only through it, never through `request.getStream()`
    /// directly: the wrapper snapshots the inner buffer state on construction, so bytes taken from the inner
    /// stream behind its back would be delivered again when the wrapper is read later (e.g. appended to the
    /// query text).
    virtual std::string getQuery(HTTPServerRequest & request, HTMLForm & params, ContextMutablePtr context, ReadBuffer & body) = 0;

protected:
    LoggerPtr log;

    /// Set by SQL-defined handlers so that `currentHandler()` and the query_log can report the handler name.
    void setIntrospectionHandlerName(const String & name_) { introspection_handler_name = name_; }

    /// Set by SQL-defined handlers, whose query is fully known in advance, so it is known whether it can consume
    /// the request body at all (see `SQLDefinedHandler`). The other handlers do not know it: for them the body may
    /// be the rest of the query text or the data of an `INSERT`, so they have to assume that it is consumed.
    void setConsumesRequestBody(bool value)
    {
        body_contract_known = true;
        consumes_request_body = value;
        feeds_request_body_to_query = value;
    }

private:
    String introspection_handler_name;

    /// Whether `consumes_request_body` carries a definitive answer. Only SQL-defined handlers set it: for them a
    /// `POST` request needs `Content-Length` up front only when the body is actually consumed. For the other
    /// handlers `POST` requires the length unconditionally, as it did before SQL-defined handlers existed.
    bool body_contract_known = false;

    /// Whether a body-carrying method must come with a length up front. Defaults to `false`: for the handlers that
    /// do not set it, only `POST` requires the length, as it did before SQL-defined handlers existed.
    bool consumes_request_body = false;

    /// Whether the request body is appended to the query text. Defaults to `true` - the historical behavior, where
    /// the body is the continuation of the `query` parameter or the data of an `INSERT`.
    bool feeds_request_body_to_query = true;

    struct Output
    {
        /* Raw data
         * ↓
         * CascadeWriteBuffer out_maybe_delayed_and_compressed (optional)
         * ↓ (forwards data if an overflow occurs or explicitly via pushDelayedResults)
         * CompressedWriteBuffer out_maybe_compressed (optional)
         * ↓
         * WriteBufferFromHTTPServerResponse out
         */

        /// Holds original response buffer
        std::shared_ptr<WriteBufferFromHTTPServerResponse> out_holder;
        /// If HTTP compression is enabled holds compression wrapper over original response buffer
        std::shared_ptr<WriteBuffer> wrap_compressed_holder;
        /// If `compression` setting (or URL path file extension) is set, holds the generic compression wrapper.
        /// Sits between the HTTP-encoding wrapper and the internal compression wrapper in the chain.
        std::shared_ptr<WriteBuffer> generic_compression_holder;
        /// Points either to out_holder or to wrap_compressed_holder
        std::shared_ptr<WriteBuffer> out;

        /// If internal compression is enabled holds compression wrapper over out buffer
        std::shared_ptr<CompressedWriteBuffer> out_compressed_holder;
        /// Points to 'out' or to CompressedWriteBuffer(*out)
        std::shared_ptr<WriteBuffer> out_maybe_compressed;

        /// If output should be delayed holds cascade buffer
        std::shared_ptr<CascadeWriteBuffer> out_delayed_and_compressed_holder;
        /// Points to out_maybe_compressed or to CascadeWriteBuffer.
        std::shared_ptr<WriteBuffer>  out_maybe_delayed_and_compressed;

        bool finalized = false;
        bool canceled = false;

        /// The response is a stream of packets produced by a framing format (see
        /// `framing_output_format`). Once `finalize` has started on such a response, nothing may
        /// be appended to it anymore (see `trySendExceptionToClient`).
        bool framed = false;

        bool exception_is_written = false;
        std::function<void(WriteBuffer &, int code, const String &)> exception_writer;

        bool hasDelayed() const
        {
            return out_maybe_delayed_and_compressed && out_maybe_delayed_and_compressed != out_maybe_compressed;
        }

        void pushDelayedResults() const;

        void finalize();

        void cancel();

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

    /// It is the name of the server that will be sent in an http-header X-ClickHouse-Server-Display-Name.
    String server_display_name;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::HTTPConnection};

    /// Reference to the immutable settings in the global context.
    /// Those settings are used only to extract a http request's parameters.
    /// See settings http_max_fields, http_max_field_name_size, http_max_field_value_size in HTMLForm.
    const Settings & default_settings;

    /// Overrides for response headers.
    HTTPResponseHeaderSetup http_response_headers_override;

    /// URL path prefix under which this handler is registered. When set, the prefix is stripped from
    /// `request.getURI()` before parsing the URL path for database/table/format/compression/filters.
    /// Empty by default (handler is at the URL root).
    std::string url_prefix;

    /// Optional registry of known HTTP handler paths. Used to enrich UNKNOWN_DATABASE / UNKNOWN_TABLE
    /// exceptions thrown during path resolution with a "Maybe you meant /dashboard?"-style hint,
    /// alongside the database/table name hint computed by the catalog.
    HTTPPathHintsPtr path_hints;

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
        QueryScope & query_scope,
        const ProfileEvents::Event & write_event);

    bool trySendExceptionToClient(
        int exception_code,
        const std::string & message,
        HTTPServerRequest & request,
        HTTPServerResponse & response,
        Output & used_output);

    void releaseOrCloseSession(const String & session_id, bool close_session);

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
        const HTTPResponseHeaderSetup & http_response_headers_override_ = std::nullopt,
        const std::string & url_prefix_ = "",
        HTTPPathHintsPtr path_hints_ = nullptr);

    std::string getQuery(HTTPServerRequest & request, HTMLForm & params, ContextMutablePtr context, ReadBuffer & body) override;

    bool customizeQueryParam(NameToNameMap & query_parameters, const std::string &key, const std::string &value) override;

    bool parsesHTTPPath() const override { return true; }
};

class PredefinedQueryHandler : public HTTPHandler
{
private:
    NameSet receive_params;
    std::string predefined_query;
    CompiledRegexPtr url_regexp;
    std::unordered_map<String, CompiledRegexPtr> header_name_with_capture_regexp;

public:
    PredefinedQueryHandler(
        IServer & server_,
        const HTTPHandlerConnectionConfig & connection_config,
        const NameSet & receive_params_,
        const std::string & predefined_query_,
        const CompiledRegexPtr & url_regexp_,
        const std::unordered_map<String, CompiledRegexPtr> & header_name_with_regexp_,
        const HTTPResponseHeaderSetup & http_response_headers_override_ = std::nullopt);

    void customizeContext(HTTPServerRequest & request, ContextMutablePtr context, ReadBuffer & body) override;

    std::string getQuery(HTTPServerRequest & request, HTMLForm & params, ContextMutablePtr context, ReadBuffer & body) override;

    bool customizeQueryParam(NameToNameMap & query_parameters, const std::string & key, const std::string & value) override;
};

/// A handler defined from SQL via CREATE HANDLER. It executes a stored query, exactly like
/// PredefinedQueryHandler, and additionally reports its handler name for introspection
/// (`currentHandler()` and the query_log `http_handler_name` column).
class SQLDefinedQueryHandler : public PredefinedQueryHandler
{
public:
    SQLDefinedQueryHandler(
        IServer & server_,
        const HTTPHandlerConnectionConfig & connection_config,
        const SQLDefinedHandler & handler);

    /// Append a newline after the stored query so that, for INSERT handlers, the request body
    /// (concatenated after the query) is correctly separated and parsed as the inserted data.
    std::string getQuery(HTTPServerRequest & request, HTMLForm & params, ContextMutablePtr context, ReadBuffer & body) override;
};

}
