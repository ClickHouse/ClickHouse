#pragma once

#include <cstdint>

namespace Poco::Util
{
    class AbstractConfiguration;
}

namespace DB
{

/// In which form the key-value asynchronous metrics - those broken down per CPU core, block device,
/// network interface, disk, temperature sensor, memory controller or logging channel - are published to
/// `system.asynchronous_metrics`, `system.asynchronous_metric_log`, the Prometheus endpoint and Graphite.
/// Controlled by the `asynchronous_metrics_key_values_mode` server setting.
enum class AsynchronousMetricsKeyValuesMode : uint8_t
{
    /// A single key-value metric per family, with the key published as a map key, as the value of the `key`
    /// column, or as a Prometheus label. The default since version 26.8.
    KeyValues,
    /// A separate scalar metric per key, with the key mangled into the metric name (`BlockReadBytes_sda`).
    /// The only form before version 26.8.
    LegacyNames,
    /// Both of the above at the same time.
    Both,
};

/// Reads the `asynchronous_metrics_key_values_mode` server setting from a server configuration.
AsynchronousMetricsKeyValuesMode getAsynchronousMetricsKeyValuesMode(const Poco::Util::AbstractConfiguration & config);

}
