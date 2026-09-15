#include <HealthMonitor.h>

#if USE_SILK

#include <Router.h>
#include <SocketIO.h>

#include <Common/Base64.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <Poco/Net/SocketAddress.h>
#include <Poco/String.h>
#include <Poco/URI.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>

#include <chrono>
#include <deque>
#include <unordered_set>
#include <vector>


namespace DB::Proxy
{

namespace
{

/// Fiber entry point running a single backend check. The parameters live in the supervisor frame.
struct CheckTask
{
    HealthMonitor * monitor;
    Backend * backend;
    bool poll_resources;
};

int runCheck(CheckTask * task) noexcept
{
    try
    {
        task->monitor->checkBackend(*task->backend, task->poll_resources);
    }
    catch (...)  // NOLINT(bugprone-empty-catch)
    {
        /// checkBackend records failures itself; it is Ok to drop anything else, as an exception
        /// must not escape a fiber entry point.
    }
    return 0;
}

}

HealthMonitor::HealthMonitor(const ProxyConfiguration & config_, Router & router_)
    : config(config_)
    , router(router_)
    , log(getLogger("ProxyHealth"))
{
}

HealthMonitor::~HealthMonitor() = default;

void HealthMonitor::interruptibleSleep(UInt64 total_ms)
{
    /// Sleep in small steps so stop() is observed promptly at shutdown.
    static constexpr UInt64 step_ms = 200;
    for (UInt64 slept = 0; slept < total_ms && !stopped.load(std::memory_order_relaxed); slept += step_ms)
        silk::FiberScheduler::sleep(std::min(step_ms, total_ms - slept) * 1'000'000);
}

void HealthMonitor::join()
{
    if (supervisor_future)
        supervisor_future->wait();
}

std::vector<BackendPtr> HealthMonitor::collectBackends() const
{
    std::vector<BackendPtr> result;
    std::unordered_set<Backend *> seen;

    auto add = [&](const std::vector<BackendPtr> & backends)
    {
        for (const auto & backend : backends)
            if (seen.insert(backend.get()).second)
                result.push_back(backend);
    };

    for (const auto & [_, pool] : router.staticPools())
        add(pool->backends());
    for (const auto & pool : router.dynamicPoolsSnapshot())
        add(pool->backends());

    return result;
}

void HealthMonitor::checkBackend(Backend & backend, bool poll_resources)
{
    const UInt16 port = backend.config().tcp_port ? backend.config().tcp_port : 9000;
    const auto started = std::chrono::steady_clock::now();
    try
    {
        FiberSocket socket = FiberSocket::connect(
            Poco::Net::SocketAddress(backend.config().host, port), config.health_check.timeout_ms);
        const double latency_ms = std::chrono::duration<double, std::milli>(
            std::chrono::steady_clock::now() - started).count();
        socket.close();
        backend.reportCheckSuccess(latency_ms);
    }
    catch (...)
    {
        backend.reportCheckFailure(config.health_check.failures_to_mark_down);
        LOG_DEBUG(log, "Backend {} health check failed: {}", backend.name(),
            getCurrentExceptionMessage(/*with_stacktrace=*/ false));
        return;
    }

    if (poll_resources && !backend.config().monitor_user.empty())
        pollResources(backend);
}

void HealthMonitor::pollResources(Backend & backend)
{
    const UInt16 port = backend.config().http_port ? backend.config().http_port : 8123;
    try
    {
        FiberSocket socket = FiberSocket::connect(
            Poco::Net::SocketAddress(backend.config().host, port), config.health_check.timeout_ms);
        socket.setTimeouts(config.health_check.timeout_ms, config.health_check.timeout_ms);

        const String credentials = base64Encode(backend.config().monitor_user + ":" + backend.config().monitor_password);
        String query;
        Poco::URI::encode(config.health_check.resource_query, "", query);

        const String request =
            "GET /?query=" + query + " HTTP/1.0\r\n"
            "Host: " + backend.config().host + "\r\n"
            "Authorization: Basic " + credentials + "\r\n"
            "Connection: close\r\n\r\n";
        socket.sendAll(request.data(), request.size());

        RecordingReader reader(socket);
        String status_line;
        if (!reader.readLine(status_line, 8192) || !status_line.contains(" 200 "))
        {
            socket.close();
            LOG_DEBUG(log, "Resource poll of {} returned '{}'", backend.name(), status_line);
            return;
        }

        /// Skip headers.
        String header;
        while (reader.readLine(header, 8192) && !header.empty())
        {
        }

        String body;
        if (!reader.readLine(body, 8192))
        {
            socket.close();
            return;
        }
        socket.close();

        const size_t tab = body.find('\t');
        if (tab == String::npos)
            return;

        const double cpu = std::stod(Poco::trim(body.substr(0, tab)));
        const double memory = std::stod(Poco::trim(body.substr(tab + 1)));
        backend.setResourceUsage(cpu, memory);
        LOG_TRACE(log, "Backend {} resource usage: {} cores, {} bytes", backend.name(), cpu, memory);
    }
    catch (...)
    {
        LOG_DEBUG(log, "Resource poll of {} failed: {}", backend.name(),
            getCurrentExceptionMessage(/*with_stacktrace=*/ false));
    }
}

void HealthMonitor::superviseLoop()
{
    LOG_INFO(log, "Health monitoring started (interval {} ms)", config.health_check.interval_ms);

    while (!stopped.load(std::memory_order_relaxed))
    {
        std::vector<BackendPtr> backends = collectBackends();

        /// Liveness is probed every `interval_ms`, but the more expensive resource poll is throttled to
        /// `resource_poll_interval_ms`. The decision is made here, in the single supervisor fiber, so that
        /// `last_resource_poll` is never touched concurrently by the per-backend check fibers.
        const auto now = std::chrono::steady_clock::now();
        const auto resource_poll_interval = std::chrono::milliseconds(config.health_check.resource_poll_interval_ms);

        std::vector<CheckTask> tasks;
        tasks.reserve(backends.size());
        for (auto & backend : backends)
        {
            bool poll_resources = false;
            if (!backend->config().monitor_user.empty())
            {
                auto & last = last_resource_poll[backend.get()];
                if (last == std::chrono::steady_clock::time_point{} || now - last >= resource_poll_interval)
                {
                    poll_resources = true;
                    last = now;
                }
            }
            tasks.push_back(CheckTask{this, backend.get(), poll_resources});
        }

        /// FiberFuture is neither copyable nor movable (it holds an atomic), so it cannot live in a
        /// vector that may reallocate; a deque constructs its elements in place and never moves them.
        std::deque<silk::FiberFuture> futures(tasks.size());

        for (size_t i = 0; i < tasks.size(); ++i)
        {
            if (silk::FiberScheduler::run(runCheck, CheckTask(tasks[i]), &futures[i]) != 0)
            {
                /// Out of fibers: check inline instead. A failed run never attaches the future,
                /// so deliver the result manually, or the unconditional wait below would suspend forever.
                futures[i].set(runCheck(&tasks[i]));
            }
        }
        for (auto & future : futures)
            future.wait();

        interruptibleSleep(config.health_check.interval_ms);
    }
}

void HealthMonitor::start()
{
    if (!config.health_check.enabled)
    {
        LOG_INFO(log, "Health monitoring is disabled; all backends are assumed to be always available");
        return;
    }

    struct SelfParam { HealthMonitor * self; };
    auto supervisor = +[](SelfParam * p) noexcept -> int
    {
        p->self->superviseLoop();
        return 0;
    };
    supervisor_future = std::make_unique<silk::FiberFuture>();
    if (silk::FiberScheduler::run(supervisor, SelfParam{this}, supervisor_future.get()) != 0)
    {
        LOG_ERROR(log, "Cannot start the health monitoring fiber");
        supervisor_future = nullptr;
    }
}

}

#endif
