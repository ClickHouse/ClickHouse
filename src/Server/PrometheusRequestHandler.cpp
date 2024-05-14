#include <Server/PrometheusRequestHandler.h>

#include <Common/logger_useful.h>
#include <IO/HTTPCommon.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <Server/IServer.h>
#include <Server/PrometheusMetricsWriter.h>
#include "config.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
}

/// Base implementation of a prometheus protocol.
class PrometheusRequestHandler::Impl
{
public:
    explicit Impl(PrometheusRequestHandler & parent) : parent_ref(parent) {}
    virtual ~Impl() = default;
    virtual void beforeHandlingRequest(HTTPServerRequest & /* request */) {}
    virtual void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) = 0;
    virtual void onException() {}

protected:
    PrometheusRequestHandler & parent() { return parent_ref; }
    IServer & server() { return parent().server; }
    const PrometheusRequestHandlerConfig & config() { return parent().config; }
    PrometheusMetricsWriter & metrics_writer() { return *parent().metrics_writer; }
    LoggerPtr log() { return parent().log; }
    WriteBuffer & getOutputStream(HTTPServerResponse & response) { return parent().getOutputStream(response); }

private:
    PrometheusRequestHandler & parent_ref;
};


/// Implementation of the exposing metrics protocol.
class PrometheusRequestHandler::ExposeMetricsImpl : public Impl
{
public:
    explicit ExposeMetricsImpl(PrometheusRequestHandler & parent) : Impl(parent) {}

    void beforeHandlingRequest(HTTPServerRequest & request) override
    {
        LOG_INFO(log(), "Handling metrics request from {}", request.get("User-Agent"));
        chassert(config().type == PrometheusRequestHandlerConfig::Type::ExposeMetrics);
    }

    void handleRequest(HTTPServerRequest & /* request */, HTTPServerResponse & response) override
    {
        response.setContentType("text/plain; version=0.0.4; charset=UTF-8");
        auto & out = getOutputStream(response);

        if (config().expose_events)
            metrics_writer().writeEvents(out);

        if (config().expose_metrics)
            metrics_writer().writeMetrics(out);

        if (config().expose_asynchronous_metrics)
            metrics_writer().writeAsynchronousMetrics(out, parent().async_metrics);

        if (config().expose_errors)
            metrics_writer().writeErrors(out);
    }
};


PrometheusRequestHandler::PrometheusRequestHandler(
    IServer & server_,
    const PrometheusRequestHandlerConfig & config_,
    const AsynchronousMetrics & async_metrics_,
    std::shared_ptr<PrometheusMetricsWriter> metrics_writer_)
    : server(server_)
    , config(config_)
    , async_metrics(async_metrics_)
    , metrics_writer(metrics_writer_)
    , log(getLogger("PrometheusRequestHandler"))
{
    createImpl();
}

PrometheusRequestHandler::~PrometheusRequestHandler() = default;

void PrometheusRequestHandler::createImpl()
{
    switch (config.type)
    {
        case PrometheusRequestHandlerConfig::Type::ExposeMetrics:
        {
            impl = std::make_unique<ExposeMetricsImpl>(*this);
            return;
        }
    }
    UNREACHABLE();
}

void PrometheusRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event_)
{
    try
    {
        write_event = write_event_;
        http_method = request.getMethod();
        chassert(!write_buffer_from_response);

        /// Make keep-alive works.
        if (request.getVersion() == HTTPServerRequest::HTTP_1_1)
            response.setChunkedTransferEncoding(true);

        setResponseDefaultHeaders(response, config.keep_alive_timeout);

        impl->beforeHandlingRequest(request);
        impl->handleRequest(request, response);

        if (write_buffer_from_response)
        {
            write_buffer_from_response->finalize();
            write_buffer_from_response = nullptr;
        }
    }
    catch (...)
    {
        tryLogCurrentException(log);
        tryCallOnException();

        /// `write_buffer_from_response` must be finalized already or at least tried to finalize.
        write_buffer_from_response = nullptr;
    }
}

WriteBuffer & PrometheusRequestHandler::getOutputStream(HTTPServerResponse & response)
{
    if (write_buffer_from_response)
        return *write_buffer_from_response;
    write_buffer_from_response = std::make_unique<WriteBufferFromHTTPServerResponse>(
        response, http_method == HTTPRequest::HTTP_HEAD, config.keep_alive_timeout, write_event);
    return *write_buffer_from_response;
}

void PrometheusRequestHandler::tryCallOnException()
{
    try
    {
        if (impl)
            impl->onException();
    }
    catch (...)
    {
        tryLogCurrentException(log, "onException");
    }
}

}
