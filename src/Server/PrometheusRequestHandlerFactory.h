#pragma once

#include <base/types.h>
#include <Core/Types_fwd.h>
#include <memory>
#include <optional>


namespace Poco::Util { class AbstractConfiguration; }

namespace DB
{

class IServer;
class HTTPRequestHandlerFactory;
using HTTPRequestHandlerFactoryPtr = std::shared_ptr<HTTPRequestHandlerFactory>;
class AsynchronousMetrics;

/// Makes a handler factory to handle prometheus protocols.
/// Expects a configuration like this:
///
/// <prometheus>
///     <port>1234</port>
///     <endpoint>/metric</endpoint>
///     <metrics>true</metrics>
///     <asynchronous_metrics>true</asynchronous_metrics>
///     <events>true</events>
///     <errors>true</errors>
/// </prometheus>
///
/// More prometheus protocols can be supported with using a different configuration <prometheus.handlers>
/// (which is similar to the <http_handlers> section):
///
/// <prometheus>
///     <port>1234</port>
///     <handlers>
///         <my_rule1>
///             <url>/metrics</url>
///             <handler>
///                 <type>metrics</type>
///                 <metrics>true</metrics>
///                 <asynchronous_metrics>true</asynchronous_metrics>
///                 <events>true</events>
///                 <errors>true</errors>
///             </handler>
///         </my_rule1>
///    </handlers>
/// </prometheus>
///
/// An alternative port to serve prometheus protocols can be specified in the <protocols> section:
///
/// <protocols>
///     <my_protocol_1>
///         <port>4321</port>
///         <type>prometheus</type>
///     </my_protocol_1>
/// </protocols>
/// @param default_session_user - overrides the `default_session_user` server setting for this listener
HTTPRequestHandlerFactoryPtr createPrometheusHandlerFactory(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const AsynchronousMetrics & asynchronous_metrics,
    const String & name,
    const std::optional<String> & default_session_user = {});

/// Makes a HTTP handler factory to handle requests for prometheus metrics for a HTTP rule in the <http_handlers> section.
/// Expects a configuration like this:
///
/// <http_port>8123</http_port>
/// <http_handlers>
///     <my_rule_1>
///         <url>/metrics</url>
///         <handler>
///             <type>prometheus_metrics</type>
///             <metrics>true</metrics>
///             <asynchronous_metrics>true</asynchronous_metrics>
///             <events>true</events>
///             <errors>true</errors>
///         </handler>
///     </my_rule_1>
///     <my_rule2>
///         <url_prefix>/prometheus/api/v1</url_prefix>
///         <handler>
///             <type>prometheus_api_v1</type>
///             <table>db.time_series_table_name</table>
///         </handler>
///     </my_rule2>
/// </http_handlers>
HTTPRequestHandlerFactoryPtr createPrometheusHandlerFactoryForHTTPRule(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix, /// path to "http_handlers.my_handler_1"
    const AsynchronousMetrics & asynchronous_metrics,
    std::unordered_map<String, String> & common_headers,
    const std::optional<String> & default_session_user = {});

/// Makes a HTTP Handler factory to handle requests for prometheus metrics as a part of the default HTTP rule in the <http_handlers> section.
/// Expects a configuration like this:
///
/// <http_port>8123</http_port>
/// <http_handlers>
///     <defaults/>
/// </http_handlers>
/// <prometheus>
///     <endpoint>/metric</endpoint>
///     <metrics>true</metrics>
///     <asynchronous_metrics>true</asynchronous_metrics>
///     <events>true</events>
///     <errors>true</errors>
/// </prometheus>
///
/// The "defaults" HTTP handler should serve the prometheus exposing metrics protocol on the http port
/// only if it isn't already served on its own port <prometheus.port>,
/// and also if there is no <prometheus.handlers> section in the configuration
/// (because if that section exists then it must be in charge of how prometheus protocols are handled).
HTTPRequestHandlerFactoryPtr createPrometheusHandlerFactoryForHTTPRuleDefaults(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const AsynchronousMetrics & asynchronous_metrics,
    const std::optional<String> & default_session_user = {});

/// Whether an HTTP listener serving the rules of the `<http_handlers>`-style section `http_handlers_key`
/// can expose the Prometheus metrics protocol: through a rule with a `prometheus` handler type, or
/// through the default `/metrics` route registered from the `prometheus` section.
bool httpHandlersCanExposePrometheusMetrics(
    const Poco::Util::AbstractConfiguration & config,
    const String & http_handlers_key);

/// Checks the constant labels of every Prometheus metrics endpoint of `config` against the labels that
/// endpoint would write itself, including the asynchronous metric key labels, which are only written
/// when `asynchronous_metrics_key_values_mode` publishes the key-value form. Throws
/// `INVALID_CONFIG_PARAMETER` on a collision, so that such a configuration can be rejected before it is
/// installed - the same check runs again for each endpoint when its handler factory is built.
/// @param http_handlers_keys - the `<http_handlers>`-style sections HTTP listeners of `config` serve, so
///        that a section no listener serves is not checked.
/// @param has_prometheus_listener - whether a listener of `config` serves the `prometheus` section on a
///        port of its own (the standalone `prometheus.port` one, or a composable `type = prometheus`
///        endpoint). Without one, that section is only read when it registers the default `/metrics`
///        route of an HTTP listener; an inert section that nothing serves is not read at a fresh start
///        either, and is therefore not checked here.
void validatePrometheusConstantLabels(
    const Poco::Util::AbstractConfiguration & config,
    const Strings & http_handlers_keys,
    bool has_prometheus_listener);

/// Makes a handler factory to handle prometheus protocols.
/// Supports the "metrics" protocol only.
/// @param default_session_user - overrides the `default_session_user` server setting for this listener
HTTPRequestHandlerFactoryPtr createKeeperPrometheusHandlerFactory(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const AsynchronousMetrics & asynchronous_metrics,
    const String & name,
    const std::optional<String> & default_session_user = {});

}
