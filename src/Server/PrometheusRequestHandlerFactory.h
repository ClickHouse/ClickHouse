#pragma once

#include <base/types.h>
#include <memory>


namespace Poco::Util { class AbstractConfiguration; }

namespace DB
{

class IServer;
class HTTPRequestHandlerFactory;
using HTTPRequestHandlerFactoryPtr = std::shared_ptr<HTTPRequestHandlerFactory>;
class HTTPRequestHandlerFactoryMain;
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
///                 <type>expose_metrics</type>
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
HTTPRequestHandlerFactoryPtr createPrometheusHandlerFactory(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const AsynchronousMetrics & asynchronous_metrics,
    const String & name);

/// Makes a HTTP handler factory to handle Prometheus protocol requests for a HTTP rule in the
/// @c http_handlers section. Two handler shapes are supported here:
///
///   - The legacy @c <type>prometheus</type> shape continues to work: it serves the same
///     expose-metrics handler the historical code did, with @c metrics / @c events / etc.
///     toggles read from the same handler block. This keeps existing user configurations
///     working unchanged. @c expose_metrics is accepted as an equivalent spelling.
///
///   - The new explicit handler types @c prometheus_remote_write / @c prometheus_remote_read /
///     @c prometheus_query_api enable dynamic-routing-only mode: the target TimeSeries table is
///     taken from @c database / @c table query parameters (or @c X-ClickHouse-Database /
///     @c X-ClickHouse-Table headers) after the user-configured @c url filter matches, so @c table
///     and @c database must NOT be specified on the handler (the factory will reject the rule
///     otherwise).
///
/// Example dynamic-routing rule:
///
/// @code{.xml}
/// <http_port>8123</http_port>
/// <http_handlers>
///     <my_rule2>
///         <url>regex:^/foo/write$</url>
///         <methods>POST</methods>
///         <handler>
///             <type>prometheus_remote_write</type>
///             <http_path_prefix>/foo</http_path_prefix>
///         </handler>
///     </my_rule2>
/// </http_handlers>
/// @endcode
HTTPRequestHandlerFactoryPtr createPrometheusHandlerFactoryForHTTPRule(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix, /// path to "http_handlers.my_handler_1"
    const AsynchronousMetrics & asynchronous_metrics,
    std::unordered_map<String, String> & common_headers);

/// Auto-mounts the three dynamic-routing Prometheus protocol rules (remote_write, remote_read,
/// query_api) on @p factory under the prefix configured by @c prometheus.http_path_prefix
/// (default @c /prometheus). No-op when @c prometheus is not configured or the prefix is empty.
///
/// @c expose_metrics (Prometheus text scrape) is intentionally NOT registered here. When
/// @c prometheus.port is non-zero, scrape is served only on that dedicated listener at
/// @c prometheus.endpoint . Optional registration of scrape on @c http_port (same endpoint path)
/// happens only via createPrometheusHandlerFactoryForHTTPRuleDefaults() when @c prometheus.port
/// is zero and there is no @c prometheus.handlers section.
void addPrometheusProtocolsToHTTPDefaults(
    HTTPRequestHandlerFactoryMain & factory,
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const AsynchronousMetrics & async_metrics);

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
/// Optional backward-compatible path: registers text scrape on @c http_port only when
/// @c prometheus.port is zero and there is no @c prometheus.handlers section (see implementation).
/// When a dedicated @c prometheus.port is configured, this returns nullptr so scrape is not
/// duplicated on @c http_port — use the dedicated listener for @c expose_metrics in that case.
HTTPRequestHandlerFactoryPtr createPrometheusHandlerFactoryForHTTPRuleDefaults(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const AsynchronousMetrics & asynchronous_metrics);

/// Makes a handler factory to handle prometheus protocols.
/// Supports the "expose_metrics" protocol only.
HTTPRequestHandlerFactoryPtr createKeeperPrometheusHandlerFactory(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const AsynchronousMetrics & asynchronous_metrics,
    const String & name);

}
