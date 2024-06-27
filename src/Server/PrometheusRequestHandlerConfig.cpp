#include <Server/PrometheusRequestHandlerConfig.h>

#include <Core/Defines.h>
#include <Server/HTTP/HTTPServerRequest.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
}


PrometheusRequestHandlerConfig::PrometheusRequestHandlerConfig(const Poco::Util::AbstractConfiguration & config_, const String & config_prefix, bool detect_handler_by_endpoint_)
{
    detect_handler_by_endpoint = detect_handler_by_endpoint_;

    if (!detect_handler_by_endpoint && config_.has(config_prefix + ".endpoint"))
        throw Exception(ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG, "URL path at {}.endpoint can't be used and must not be specified", config_prefix);

    metrics.emplace();
    metrics->endpoint = config_.getString(config_prefix + ".endpoint", "/metrics");
    metrics->send_metrics = config_.getBool(config_prefix + ".metrics", true);
    metrics->send_asynchronous_metrics = config_.getBool(config_prefix + ".asynchronous_metrics", true);
    metrics->send_events = config_.getBool(config_prefix + ".events", true);
    metrics->send_errors = config_.getBool(config_prefix + ".errors", true);

    keep_alive_timeout = config_.getUInt("keep_alive_timeout", DEFAULT_HTTP_KEEP_ALIVE_TIMEOUT);
}

bool PrometheusRequestHandlerConfig::filterRequest(const HTTPServerRequest & request) const
{
    const auto & path = request.getURI();
    const auto & method = request.getMethod();

    if (metrics && (!detect_handler_by_endpoint || (path == metrics->endpoint)))
        return (method == Poco::Net::HTTPRequest::HTTP_GET) || (method == Poco::Net::HTTPRequest::HTTP_HEAD);

    return false;
}

}
