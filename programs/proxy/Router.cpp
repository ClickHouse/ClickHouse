#include "config.h"

#if USE_SILK

#include <Router.h>

#include <Common/Exception.h>
#include <Common/ShellCommand.h>
#include <Common/logger_useful.h>

#include <silk/fibers/fiber.h>

#include <fmt/format.h>

#include <iterator>


namespace DB::Proxy
{

namespace
{

String shellQuote(const String & value)
{
    String res = "'";
    for (char c : value)
    {
        if (c == '\'')
            res += "'\\''";
        else
            res += c;
    }
    res += "'";
    return res;
}

}

Router::Router(const ProxyConfiguration & config, bool passive_marking_down_)
    : hooks(config.hooks)
    , stickiness(config.stickiness)
    , passive_marking_down(passive_marking_down_)
    , failures_to_mark_down(config.health_check.failures_to_mark_down)
    , table(std::make_unique<ConfigRoutingTable>(config.rules))
    , log(getLogger("ProxyRouter"))
{
    for (const auto & [name, pool_config] : config.pools)
        pools.emplace(name, std::make_shared<BackendPool>(pool_config, stickiness));
}

std::vector<BackendPoolPtr> Router::dynamicPoolsSnapshot() const
{
    std::vector<BackendPoolPtr> res;
    std::lock_guard lock(dynamic_mutex);
    res.reserve(dynamic_pools.size());
    for (const auto & [_, pool] : dynamic_pools)
        res.push_back(pool);
    return res;
}

bool Router::runHook(const String & command, const char * kind, const RouteAttributes & attributes)
{
    const String line = fmt::format("{} {} {} {} {} {}",
        command, kind, toString(attributes.protocol),
        shellQuote(attributes.host), shellQuote(attributes.user), shellQuote(attributes.database));

    LOG_INFO(log, "Running hook: {}", line);

    try
    {
        /// The hook is an external blocking process: leave the cooperative scheduler while waiting for it.
        silk::FiberScheduler::ThreadModeScope thread_mode;

        ShellCommand::Config config(line);
        /// Hooks do not consume their output. Let it inherit the proxy's standard streams
        /// instead of creating unread pipes that can block a verbose hook on write(2).
        config.pipe_stdin_only = true;
        auto process = ShellCommand::execute(config);
        int exit_code = process->tryWait();
        if (exit_code != 0)
        {
            LOG_WARNING(log, "Hook {} exited with code {}", kind, exit_code);
            return false;
        }
        return true;
    }
    catch (...)
    {
        LOG_ERROR(log, "Hook {} failed: {}", kind, getCurrentExceptionMessage(/*with_stacktrace=*/ false));
        return false;
    }
}

void Router::runFirstSeenHook(const String & command, const char * kind, const String & value,
    std::map<String, bool> & seen, const RouteAttributes & attributes)
{
    if (command.empty() || value.empty())
        return;

    {
        std::unique_lock lock(first_seen_mutex);
        auto [it, inserted] = seen.emplace(value, false);
        if (!inserted)
        {
            if (it->second)
                return;

            /// Another connection is running the hook right now: wait for it to finish.
            /// Waiting on a condition variable blocks the borrowed thread, so leave the scheduler.
            silk::FiberScheduler::ThreadModeScope thread_mode;
            first_seen_finished.wait(lock, [&] { return seen[value]; });
            return;
        }
    }

    runHook(command, kind, attributes);

    {
        std::lock_guard lock(first_seen_mutex);
        seen[value] = true;
    }
    first_seen_finished.notify_all();
}

BackendPoolPtr Router::poolForDynamicBackend(const BackendConfig & backend_config, const ListenerConfig & listener)
{
    /// Dynamic pools may only be shared by configurations with identical behavior.
    /// Length-prefix strings so that delimiters in names, hosts, or credentials cannot alias.
    String key;
    const auto append = [&key](const auto & value)
    {
        const String text = fmt::format("{}", value);
        fmt::format_to(std::back_inserter(key), "{}:{}", text.size(), text);
    };

    append(backend_config.name);
    append(backend_config.host);
    append(backend_config.tcp_port);
    append(backend_config.http_port);
    append(backend_config.mysql_port);
    append(backend_config.postgresql_port);
    append(backend_config.ssh_port);
    append(backend_config.raw_port);
    append(backend_config.secure);
    append(backend_config.weight);
    append(backend_config.monitor_user);
    append(backend_config.monitor_password);
    append(toString(listener.protocol));
    append(listener.port);
    append(backendPortFor(listener.protocol, backend_config, listener.port));

    std::lock_guard lock(dynamic_mutex);
    auto it = dynamic_pools.find(key);
    if (it == dynamic_pools.end())
    {
        LOG_INFO(log, "Creating a dynamic backend {}", backend_config.name);
        auto pool = std::make_shared<BackendPool>(
            "dynamic:" + backend_config.name, std::make_shared<Backend>(backend_config), stickiness);
        it = dynamic_pools.emplace(key, std::move(pool)).first;
    }
    return it->second;
}

BackendPoolPtr Router::resolvePool(const RouteAttributes & attributes, const ListenerConfig & listener)
{
    if (auto target = table->resolve(attributes))
    {
        if (target->backend)
            return poolForDynamicBackend(*target->backend, listener);

        auto it = pools.find(target->pool_name);
        chassert(it != pools.end());    /// Pool names are validated when the configuration is loaded.
        return it->second;
    }

    if (!listener.default_pool.empty())
        return pools.at(listener.default_pool);

    return nullptr;
}

Router::Decision Router::routeStatic(const RouteAttributes & attributes, const ListenerConfig & listener)
{
    BackendPoolPtr pool = resolvePool(attributes, listener);
    if (!pool)
        return {};
    return {pool, pool->choose(attributes)};
}

Router::Decision Router::route(const RouteAttributes & attributes, const ListenerConfig & listener)
{
    runFirstSeenHook(hooks.on_first_seen_user, "first_seen_user", attributes.user, seen_users, attributes);
    runFirstSeenHook(hooks.on_first_seen_database, "first_seen_database", attributes.database, seen_databases, attributes);

    BackendPoolPtr pool = resolvePool(attributes, listener);
    if (!pool && !hooks.on_unknown.empty() && runHook(hooks.on_unknown, "unknown", attributes))
        pool = resolvePool(attributes, listener);

    if (!pool)
        return {};

    BackendPtr backend = pool->choose(attributes);
    if (!backend && !hooks.on_no_backends.empty() && runHook(hooks.on_no_backends, "no_backends", attributes))
    {
        /// The hook reported success: wait for the health checker to see the backends up.
        static constexpr UInt64 retry_period_ms = 250;
        for (UInt64 waited_ms = 0; waited_ms < hooks.timeout_ms; waited_ms += retry_period_ms)
        {
            backend = pool->choose(attributes);
            if (backend)
                break;
            silk::FiberScheduler::sleep(retry_period_ms * 1'000'000);
        }
    }

    return {pool, backend};
}

}

#endif
