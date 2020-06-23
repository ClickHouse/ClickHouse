#pragma once

#include "IServer.h"

#include <Poco/Net/HTTPRequestHandler.h>

#include <Common/CurrentMetrics.h>
#include <Common/HTMLForm.h>
#include <Core/Names.h>

#include <re2/re2.h>

namespace CurrentMetrics
{
    extern const Metric HTTPConnection;
}

namespace Poco { class Logger; }

namespace DB
{

class WriteBufferFromHTTPServerResponse;

using CompiledRegexPtr = std::shared_ptr<const re2::RE2>;

class HTTPHandler : public Poco::Net::HTTPRequestHandler
{
public:
    explicit HTTPHandler(IServer & server_, const std::string & name);

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override;

    /// This method is called right before the query execution.
    virtual void customizeContext(Poco::Net::HTTPServerRequest & /*request*/, Context & /* context */) {}

    virtual bool customizeQueryParam(Context & context, const std::string & key, const std::string & value) = 0;

    virtual std::string getQuery(Poco::Net::HTTPServerRequest & request, HTMLForm & params, Context & context) = 0;

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

    /// Also initializes 'used_output'.
    void processQuery(
        Context & context,
        Poco::Net::HTTPServerRequest & request,
        HTMLForm & params,
        Poco::Net::HTTPServerResponse & response,
        Output & used_output);

    void trySendExceptionToClient(
        const std::string & s,
        int exception_code,
        Poco::Net::HTTPServerRequest & request,
        Poco::Net::HTTPServerResponse & response,
        Output & used_output);

    static void pushDelayedResults(Output & used_output);
};

class DynamicQueryHandler : public HTTPHandler
{
private:
    std::string param_name;
public:
    explicit DynamicQueryHandler(IServer & server_, const std::string & param_name_ = "query");

    std::string getQuery(Poco::Net::HTTPServerRequest & request, HTMLForm & params, Context & context) override;

    bool customizeQueryParam(Context &context, const std::string &key, const std::string &value) override;
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

    virtual void customizeContext(Poco::Net::HTTPServerRequest & request, Context & context) override;

    std::string getQuery(Poco::Net::HTTPServerRequest & request, HTMLForm & params, Context & context) override;

    bool customizeQueryParam(Context & context, const std::string & key, const std::string & value) override;
};

}
