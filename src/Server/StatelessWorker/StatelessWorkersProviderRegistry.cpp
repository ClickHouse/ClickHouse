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
    extern const int SUPPORT_IS_DISABLED;
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
    Strings getWorkerSources(const Poco::Util::AbstractConfiguration & config, const String & prefix)
    {
        return StatelessWorkersProviderFactory::instance().getWorkerSources(config, prefix);
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
    /// A named default must not also exist under `clusters`, or that entry would be unreachable.
    if (!default_cluster_name.empty() && config.has("stateless_worker_client.clusters." + default_cluster_name))
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
            "Stateless worker `default_cluster_name` '{}' also names a cluster under `clusters`; cluster names must be unique",
            default_cluster_name);
    if (worker_cluster.empty() || (!default_cluster_name.empty() && worker_cluster == default_cluster_name))
    {
        /// A default exists only when the flat block names a worker source; otherwise a query must
        /// name its cluster.
        const Strings sources = getWorkerSources(config, "stateless_worker_client");
        if (sources.empty())
        {
            Poco::Util::AbstractConfiguration::Keys names;
            config.keys("stateless_worker_client.clusters", names);
            throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG,
                "No default stateless worker cluster is configured; set `distributed_plan_worker_cluster` to one of: {}",
                fmt::join(names, ", "));
        }
        if (sources.size() > 1)
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                "The default stateless worker cluster configures multiple worker sources ({}); set exactly one",
                fmt::join(sources, ", "));
        return {};
    }

    /// A named cluster must exist under `clusters` and name a worker source of its own.
    const String prefix = "stateless_worker_client.clusters." + worker_cluster;
    if (!config.has(prefix))
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG,
            "Stateless worker cluster '{}' is not configured", worker_cluster);
    const Strings sources = getWorkerSources(config, prefix);
    if (sources.empty())
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG,
            "Stateless worker cluster '{}' configures no worker source",
            worker_cluster);
    if (sources.size() > 1)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
            "Stateless worker cluster '{}' configures multiple worker sources ({}); set exactly one",
            worker_cluster, fmt::join(sources, ", "));
    return worker_cluster;
}

StatelessWorkersProviderPtr StatelessWorkersProviderRegistry::getProvider(const String & cluster_name)
{
    auto global_context = Context::getGlobalContextInstance();
    if (!global_context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No global context to resolve a stateless worker cluster");
    /// One configuration for the whole lookup, so the selection, the signature and the build agree.
    const auto & config = global_context->getConfigRef();
    if (!config.getBool("stateless_worker_client.enabled", false))
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Stateless worker client is not enabled in configuration");
    /// The default cluster is cached under one key whether it was selected by an empty value or by name.
    const String canonical = canonicalizeClusterName(config, cluster_name);

    /// Sign before building, so a reload published in between can only cause a rebuild, never keep a
    /// stale provider under a newer signature. Signing reads only the configuration, so it runs outside
    /// the lock and concurrent queries do not serialize on it.
    String signature = computeConfigSignatureForCluster(config, canonical);

    std::lock_guard lock(mutex);
    if (auto it = providers.find(canonical); it != providers.end())
    {
        /// The server publishes a new configuration before it reloads the registry; a cached provider
        /// built from the previous one is replaced here, so a query started after the change never
        /// resolves to it. Queries already holding it drain on it.
        if (it->second.config_signature == signature)
            return it->second.provider;
        providers.erase(it);
    }

    auto provider = buildProvider(config, canonical);
    providers.emplace(canonical, ProviderEntry{provider, std::move(signature)});
    return provider;
}

StatelessWorkersProviderPtr StatelessWorkersProviderRegistry::tryGetProvider(const String & cluster_name)
{
    auto global_context = Context::getGlobalContextInstance();
    if (!global_context)
        return nullptr;
    /// Introspection reports no provider for a selection it cannot resolve instead of throwing.
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

std::vector<std::pair<String, StatelessWorkersProviderPtr>> StatelessWorkersProviderRegistry::getBuiltProviders()
{
    auto global_context = Context::getGlobalContextInstance();
    const String default_cluster_name = global_context
        ? global_context->getConfigRef().getString("stateless_worker_client.default_cluster_name", "")
        : "";

    std::lock_guard lock(mutex);
    std::vector<std::pair<String, StatelessWorkersProviderPtr>> result;
    result.reserve(providers.size());
    for (const auto & [key, entry] : providers)
        result.emplace_back(key.empty() ? default_cluster_name : key, entry.provider);
    return result;
}

StatelessWorkersProviderPtr StatelessWorkersProviderRegistry::buildProvider(
    const Poco::Util::AbstractConfiguration & config, const String & cluster_name)
{
    const String config_prefix = clusterConfigPrefix(cluster_name);
    const Strings sources = getWorkerSources(config, config_prefix);
    if (sources.size() != 1)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
            "Stateless worker cluster '{}' must configure exactly one worker source, got {}", cluster_name, sources.size());
    LOG_DEBUG(log, "Building stateless worker provider for cluster '{}' from `{}`", cluster_name, sources.front());
    return StatelessWorkersProviderFactory::instance().get(sources.front()).create(config, config_prefix);
}

String StatelessWorkersProviderRegistry::computeConfigSignatureForCluster(const Poco::Util::AbstractConfiguration & config, const String & cluster_name)
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
    const Strings sources = getWorkerSources(config, config_prefix);
    if (sources.size() != 1)
        return {};
    return sources.front() + "|" + StatelessWorkersProviderFactory::instance().get(sources.front()).config_signature(config, config_prefix);
}

void StatelessWorkersProviderRegistry::reloadConfig(const Poco::Util::AbstractConfiguration & config)
{
    std::lock_guard lock(mutex);
    for (auto it = providers.begin(); it != providers.end();)
    {
        try
        {
            if (computeConfigSignatureForCluster(config, it->first) == it->second.config_signature)
            {
                /// Unchanged: refresh in place (e.g. re-read a token file).
                it->second.provider->reloadConfig(config, clusterConfigPrefix(it->first));
                ++it;
                continue;
            }
            /// Removed or reconfigured: queries holding the provider drain on it, and the cluster is
            /// rebuilt on its next use.
            /// A retired provider is not refreshed any more, so queries draining on it may eventually fail.
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
