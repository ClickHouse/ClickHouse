#include <Server/StatelessWorker/StatelessWorkersProviderRegistry.h>
#include <Server/StatelessWorker/StatelessWorkersProviderFactory.h>
#include <Interpreters/Context.h>
#include <Common/Exception.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <fmt/ranges.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int LOGICAL_ERROR;
    extern const int INVALID_CONFIG_PARAMETER;
}

StatelessWorkersProviderRegistry::StatelessWorkersProviderRegistry()
    : log(getLogger("StatelessWorkersProviderRegistry"))
{
}

StatelessWorkersProviderRegistry::~StatelessWorkersProviderRegistry() = default;

namespace
{
    /// Exactly one worker source is required per cluster: zero cannot provide workers, and more than one
    /// is an ambiguous config that would silently pick a single source (wrong-pool risk).
    size_t countWorkerSources(const Poco::Util::AbstractConfiguration & config, const String & prefix)
    {
        return StatelessWorkersProviderFactory::instance().countWorkerSources(config, prefix);
    }
}

String StatelessWorkersProviderRegistry::clusterConfigPrefix(const String & canonical_cluster_name)
{
    if (canonical_cluster_name.empty())
        return "stateless_worker_client";
    return "stateless_worker_client.clusters." + canonical_cluster_name;
}

String StatelessWorkersProviderRegistry::canonicalizeClusterName(
    const Poco::Util::AbstractConfiguration & config, const String & worker_cluster)
{
    /// The default cluster is the flat `stateless_worker_client` block. It is selected by an empty
    /// value, or by the optional name it is given via `default_cluster_name`.
    const String default_cluster_name = config.getString("stateless_worker_client.default_cluster_name", "");
    /// A named default must not also exist under `clusters`: selecting that name would resolve to the
    /// flat default and the `clusters.<name>` entry would be unreachable, silently routing to the wrong
    /// pool. Reject the collision so the ambiguity surfaces as a loud configuration error.
    if (!default_cluster_name.empty() && config.has("stateless_worker_client.clusters." + default_cluster_name))
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
            "Stateless worker `default_cluster_name` '{}' also names a cluster under `clusters`; cluster names must be unique",
            default_cluster_name);
    if (worker_cluster.empty() || (!default_cluster_name.empty() && worker_cluster == default_cluster_name))
    {
        /// A default exists only when the flat block itself names a worker source. A symmetric config
        /// that keeps only shared settings there (every cluster under `clusters.<name>`) has none, so a
        /// query must name its cluster — reject an empty selection and point at the configured names.
        const size_t sources = countWorkerSources(config, "stateless_worker_client");
        if (sources == 0)
        {
            Poco::Util::AbstractConfiguration::Keys names;
            config.keys("stateless_worker_client.clusters", names);
            throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG,
                "No default stateless worker cluster is configured; set `distributed_plan_worker_cluster` to one of: {}",
                fmt::join(names, ", "));
        }
        if (sources > 1)
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                "The default stateless worker cluster configures multiple worker sources; set exactly one");
        return {};
    }

    /// A named cluster must exist under `clusters` and name a worker source of its own.
    const String prefix = "stateless_worker_client.clusters." + worker_cluster;
    if (!config.has(prefix))
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG,
            "Stateless worker cluster '{}' is not configured", worker_cluster);
    const size_t sources = countWorkerSources(config, prefix);
    if (sources == 0)
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG,
            "Stateless worker cluster '{}' configures no worker source",
            worker_cluster);
    if (sources > 1)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
            "Stateless worker cluster '{}' configures multiple worker sources; set exactly one",
            worker_cluster);
    return worker_cluster;
}

StatelessWorkersProviderPtr StatelessWorkersProviderRegistry::getProvider(const String & cluster_name)
{
    auto global_context = Context::getGlobalContextInstance();
    if (!global_context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No global context to resolve a stateless worker cluster");
    const auto & config = global_context->getConfigRef();
    /// Resolve the selection (default naming, no-default rejection, unknown/sourceless checks) so the
    /// default cluster is cached under one key whether it was selected by an empty value or by name.
    const String canonical = canonicalizeClusterName(config, cluster_name);

    std::lock_guard lock(mutex);
    if (auto it = providers.find(canonical); it != providers.end())
        return it->second.provider;

    /// Sign before building: a reload published in between then leaves the signature older than the
    /// provider, so the next reload rebuilds it at worst, instead of keeping a stale provider under a
    /// newer signature.
    String signature = computeConfigSignature(config, canonical);
    auto provider = buildProvider(canonical);
    providers.emplace(canonical, ProviderEntry{provider, std::move(signature)});
    return provider;
}

StatelessWorkersProviderPtr StatelessWorkersProviderRegistry::tryGetProvider(const String & cluster_name)
{
    auto global_context = Context::getGlobalContextInstance();
    if (!global_context)
        return nullptr;
    /// Introspection reports no provider for a selection it cannot resolve (e.g. the default cluster
    /// when none is configured) instead of propagating the validation error.
    String canonical;
    try
    {
        canonical = canonicalizeClusterName(global_context->getConfigRef(), cluster_name);
    }
    catch (const Exception &)
    {
        return nullptr;
    }

    std::lock_guard lock(mutex);
    if (auto it = providers.find(canonical); it != providers.end())
        return it->second.provider;
    return nullptr;
}

StatelessWorkersProviderPtr StatelessWorkersProviderRegistry::buildProvider(const String & cluster_name)
{
    auto global_context = Context::getGlobalContextInstance();
    if (!global_context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No global context to build a stateless worker provider");

    const auto & config = global_context->getConfigRef();

    if (!config.getBool("stateless_worker_client.enabled", false))
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG,
            "Stateless worker client is not enabled in configuration");

    /// Only the worker source is per-cluster; other settings are read from the top-level block and
    /// shared across clusters. `getProvider` has already validated the cluster name.
    const String config_prefix = clusterConfigPrefix(cluster_name);
    const auto & [kind_name, kind] = StatelessWorkersProviderFactory::instance().getKind(config, config_prefix);
    LOG_DEBUG(log, "Building {} stateless worker provider for cluster '{}'", kind_name, cluster_name);
    return kind.create(config, config_prefix);
}

String StatelessWorkersProviderRegistry::computeConfigSignature(const Poco::Util::AbstractConfiguration & config, const String & cluster_name)
{
    if (!config.getBool("stateless_worker_client.enabled", false))
        return {};

    String config_prefix = "stateless_worker_client";
    if (!cluster_name.empty())
    {
        config_prefix += ".clusters." + cluster_name;
        if (!config.has(config_prefix))
            return {};
    }

    /// A cluster that no longer names exactly one worker source cannot be rebuilt as it was.
    if (countWorkerSources(config, config_prefix) != 1)
        return {};
    const auto & [kind_name, kind] = StatelessWorkersProviderFactory::instance().getKind(config, config_prefix);
    return kind_name + "|" + kind.config_signature(config, config_prefix);
}

void StatelessWorkersProviderRegistry::reloadConfig(const Poco::Util::AbstractConfiguration & config)
{
    /// Never throws: a cluster whose new configuration cannot be applied is retired, so the error
    /// surfaces to the next query that selects it (when it is rebuilt) rather than to the reload.
    std::lock_guard lock(mutex);
    for (auto it = providers.begin(); it != providers.end();)
    {
        try
        {
            if (computeConfigSignature(config, it->first) == it->second.config_signature)
            {
                /// Unchanged: refresh in place (e.g. re-read a token file).
                it->second.provider->reloadConfig(config);
                ++it;
                continue;
            }
            /// The cluster was removed or reconfigured: stop selecting it. In-flight queries keep
            /// their provider alive through their allocation and drain naturally; a reconfigured
            /// cluster is rebuilt from the new configuration on its next use.
            LOG_INFO(log, "Retiring stateless worker provider for cluster '{}' after a configuration change", it->first);
        }
        catch (...)
        {
            tryLogCurrentException(log, fmt::format("Retiring stateless worker provider for cluster '{}': cannot apply its new configuration", it->first));
        }
        it = providers.erase(it);
    }
}

void StatelessWorkersProviderRegistry::shutdown()
{
    std::lock_guard lock(mutex);
    for (auto & [name, entry] : providers)
        entry.provider->shutdown();
    providers.clear();
}

}
