#include <GeneratedRunner.h>

#include <chrono>
#include <deque>
#include <iostream>

#include <Common/EventNotifier.h>
#include <Common/Exception.h>
#include <Common/InterruptListener.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Core/UUID.h>
#include <IO/WriteBufferFromString.h>
#include <Poco/Timestamp.h>

namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

GeneratedRunner::GeneratedRunner(
        std::optional<size_t> concurrency_,
        const std::string & config_path,
        const Strings & hosts_strings_,
        std::optional<double> max_time_,
        std::optional<double> delay_,
        std::optional<bool> continue_on_error_,
        std::optional<size_t> max_iterations_)
        : info(std::make_shared<Stats>())
{
    if (config_path.empty())
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS, "--config is required (pass --input-request-log to replay a request log instead)");

    DB::ConfigProcessor config_processor(config_path, true, false);
    config_ptr = config_processor.loadConfig().configuration;

    if (!config_ptr->has("generator"))
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS,
            "Config file must contain a `generator` section (pass --input-request-log to replay a request log instead)");

    std::cerr << "---- Run options ---- " << std::endl;

    concurrency = getOption<size_t>(concurrency_, config_ptr, "concurrency", 1);
    std::cerr << "Concurrency: " << concurrency << std::endl;

    pipeline_depth = config_ptr->getUInt64("pipeline_depth", 1);
    if (pipeline_depth == 0)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "pipeline_depth must be >= 1, got 0");
    if (pipeline_depth > 1)
        std::cerr << "Pipeline depth: " << pipeline_depth << std::endl;

    max_iterations = getOption<size_t>(max_iterations_, config_ptr, "iterations", 0);
    std::cerr << "Iterations: " << max_iterations << std::endl;

    delay = getOption<double>(delay_, config_ptr, "report_delay", 1.0);
    std::cerr << "Report delay: " << delay << std::endl;

    max_time = getOption<double>(max_time_, config_ptr, "timelimit", 0.0);
    std::cerr << "Time limit: " << max_time << std::endl;

    continue_on_error = getOption<bool>(continue_on_error_, config_ptr, "continue_on_error", false);
    std::cerr << "Continue on error: " << continue_on_error << std::endl;

    enable_tracing = config_ptr->getBool("enable_tracing", false);
    std::cerr << "Enable tracing: " << enable_tracing << std::endl;

    warmup_seconds = config_ptr->getDouble("warmup_seconds", 0);
    if (warmup_seconds > 0)
        std::cerr << "Warmup: " << warmup_seconds << " seconds" << std::endl;

    connection_factory.initialize(hosts_strings_, config_ptr.get(), enable_tracing);

    nodes_setup.initializeFromConfig(*config_ptr);
    output.initializeFromConfig(*config_ptr);

    std::cerr << "---- Run options ----\n" << std::endl;
}

void GeneratedRunner::printNumberOfRequestsExecuted(size_t num)
{
    std::cerr << "Requests executed: " << num << ".\n";
}

void GeneratedRunner::thread(std::vector<std::shared_ptr<Coordination::ZooKeeper>> zookeepers, ThreadState & thread_state)
{
    struct RequestResult
    {
        size_t response_bytes;
        uint64_t elapsed_microseconds;
    };

    struct InFlightRequest
    {
        std::future<RequestResult> future;
        Coordination::ZooKeeperRequestPtr request;
    };

    /// Copy the shared_ptr so callbacks can capture it and outlive this frame safely.
    auto shared_generator = generator;
    GenerateContext ctx{thread_state.rng, thread_state.thread_idx};

    /// Randomly choosing connection index
    pcg64 rng(randomSeed());
    std::uniform_int_distribution<size_t> distribution(0, zookeepers.size() - 1);

    /// SIGINT is blocked in all threads (inherited from the main thread, see
    /// mainEntryClickHouseKeeperBench) and handled by the main loop's InterruptListener.

    std::deque<InFlightRequest> in_flight;

    const auto handle_request_exception = [&](const Coordination::ZooKeeperRequestPtr & request)
    {
        std::cerr << DB::getCurrentExceptionMessage(true, true /*check embedded stack trace*/) << std::endl;
        if (request)
            std::cerr << "For request:\n" << request->toString() << std::endl;

        if (!continue_on_error)
        {
            shutdown = true;
            throw;
        }
        info->errors.fetch_add(1, std::memory_order_relaxed);

        bool got_expired = false;
        for (const auto & connection : zookeepers)
        {
            if (connection->isExpired())
            {
                got_expired = true;
                break;
            }
        }

        if (got_expired)
        {
            while (true)
            {
                try
                {
                    zookeepers = refreshConnections();
                    break;
                }
                catch (...)
                {
                    std::cerr << DB::getCurrentExceptionMessage(true, true /*check embedded stack trace*/) << std::endl;
                }
            }
        }
    };

    /// Collect the result of a completed in-flight request
    const auto collect_request = [&](InFlightRequest & slot)
    {
        try
        {
            auto result = slot.future.get();

            if (warmup_complete)
            {
                auto bytes = slot.request->bytesSize() + result.response_bytes;

                if (slot.request->isReadRequest())
                    thread_state.thread_info.addRead(result.elapsed_microseconds, 1, bytes);
                else
                    thread_state.thread_info.addWrite(result.elapsed_microseconds, 1, bytes);
            }
        }
        catch (...) // Ok: handle_request_exception logs and counts the error
        {
            handle_request_exception(slot.request);
        }
    };

    while (true)
    {
        if (shutdown)
        {
            /// Drain remaining in-flight requests
            for (auto & slot : in_flight)
                collect_request(slot);
            return;
        }
        size_t iteration_idx = requests_started.fetch_add(1);
        if (max_iterations && warmup_complete && iteration_idx >= max_iterations)
        {
            shutdown = true;
            continue;
        }

        /// Wait for the oldest request if the pipeline is full
        if (in_flight.size() >= pipeline_depth)
        {
            collect_request(in_flight.front());
            in_flight.pop_front();
        }

        ZooKeeperRequestWithCallbacks request_with_callbacks = shared_generator->generate(ctx);

        const auto connection_index = distribution(rng);
        auto & zk = zookeepers[connection_index];

        auto promise = std::make_shared<std::promise<RequestResult>>();
        auto future = promise->get_future();

        auto inner_callback = std::move(request_with_callbacks.callback);

        auto watch = std::make_shared<Stopwatch>();

        Coordination::ResponseCallback callback =
            [promise,
             inner_callback,
             watch,
             shared_generator](const Coordination::Response & response)
        {
            auto elapsed = watch->elapsedMicroseconds();
            if (inner_callback)
                inner_callback(&response);
            if (response.error == Coordination::Error::ZOK)
                promise->set_value(RequestResult{response.bytesSize(), elapsed});
            else
                promise->set_exception(std::make_exception_ptr(zkutil::KeeperException(response.error)));
        };

        auto & request = request_with_callbacks.request;

        if (enable_tracing)
        {
            request->tracing_context = std::make_shared<DB::OpenTelemetry::TracingContext>();
            request->tracing_context->trace_id = DB::UUIDHelpers::generateV4();
            request->tracing_context->span_id = 0;
            request->tracing_context->trace_flags = DB::OpenTelemetry::TRACE_FLAG_SAMPLED | DB::OpenTelemetry::TRACE_FLAG_KEEPER_SPANS;
        }

        InFlightRequest slot;
        slot.request = std::move(request);

        try
        {
            zk->executeGenericRequest(slot.request, callback, slot.request->watch_callback);
            slot.future = std::move(future);
            in_flight.push_back(std::move(slot));
        }
        catch (...) // Ok: handle_request_exception logs and counts the error
        {
            if (inner_callback)
                inner_callback(nullptr);
            handle_request_exception(slot.request);
        }
    }
}

void GeneratedRunner::runBenchmark()
{
    pool.emplace(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, concurrency);
    createConnections();

    std::cerr << "Preparing to run\n";
    nodes_setup.startup(*connections[0]);

    /// Initialize the generator up front, before any worker thread starts
    /// executing requests. Generator startup resolves `children_of` paths by
    /// listing them, which must not race with workers mutating the tree.
    const auto * tagged_paths = nodes_setup.getTaggedPaths().empty() ? nullptr : &nodes_setup.getTaggedPaths();
    auto list_children = [&zk = *connections[0]](const std::string & parent_path) -> std::vector<std::string>
    {
        auto list_promise = std::make_shared<std::promise<Coordination::ListResponse>>();
        auto list_future = list_promise->get_future();
        auto callback = [list_promise] (const Coordination::ListResponse & response)
        {
            if (response.error != Coordination::Error::ZOK)
                list_promise->set_exception(std::make_exception_ptr(zkutil::KeeperException(response.error)));
            else
                list_promise->set_value(response);
        };
        zk.list(parent_path, Coordination::ListRequestType::ALL, std::move(callback), {}, false, false);
        return list_future.get().names;
    };

    generator = std::make_shared<Generator>();
    generator->startup(*config_ptr, list_children, tagged_paths);
    generator->setWatchCallback(std::make_shared<Coordination::WatchCallback>(
        [stats = info](const Coordination::WatchResponse &)
        {
            stats->watches_fired.fetch_add(1, std::memory_order_relaxed);
        }));

    threads = std::vector<ThreadState>(concurrency);
    for (size_t i = 0; i < concurrency; ++i)
    {
        threads[i].thread_idx = i;
        threads[i].rng.seed(generator->getSeedFor(i));
    }
    std::cerr << "Prepared\n";

    warmup_complete = warmup_seconds <= 0;

    int64_t start_timestamp_ms = 0;

    try
    {
        for (size_t i = 0; i < concurrency; ++i)
        {
            auto thread_connections = connections;
            pool->scheduleOrThrowOnError([this, i, my_connections = std::move(thread_connections)]() mutable { thread(my_connections, threads.at(i)); });
        }
    }
    catch (...)
    {
        shutdown = true;
        pool->wait();
        throw;
    }

    DB::InterruptListener interrupt_listener;
    /// Reset regardless of warmup so setup time is excluded from throughput and time limit
    info->elapsed.restart();
    total_watch.restart();
    start_timestamp_ms = Poco::Timestamp().epochMicroseconds() / 1000;
    Stopwatch warmup_watch;
    delay_watch.restart();

    /// Accumulates stats across all periods for the final report.
    auto cumulative_info = std::make_shared<Stats>();
    cumulative_info->elapsed.restart();

    while (!shutdown)
    {
        if (max_time > 0 && total_watch.elapsedSeconds() >= max_time)
        {
            std::cerr << "Stopping launch of queries. Requested time limit is exhausted.\n";
            shutdown = true;
            break;
        }

        if (interrupt_listener.check())
        {
            std::cerr << "Stopping launch of queries. SIGINT received." << std::endl;
            shutdown = true;
            break;
        }

        if (delay > 0 && delay_watch.elapsedSeconds() > delay)
        {
            printNumberOfRequestsExecuted(requests_started);

            std::lock_guard lock(mutex);
            auto period_info = mergeThreadInfos();
            cumulative_info->merge(*period_info);
            period_info->report(*cumulative_info);
            delay_watch.restart();
        }

        if (!warmup_complete && warmup_watch.elapsedSeconds() >= warmup_seconds)
        {
            std::lock_guard lock(mutex);
            mergeThreadInfos(); /// discard warmup stats
            cumulative_info->clear();
            cumulative_info->elapsed.restart();
            requests_started = 0;
            warmup_complete = true;
            std::cerr << "Warmup complete, starting measurement" << std::endl;
            total_watch.restart();
            delay_watch.restart();
            start_timestamp_ms = Poco::Timestamp().epochMicroseconds() / 1000;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    pool->wait();
    total_watch.stop();

    printNumberOfRequestsExecuted(requests_started);

    std::lock_guard lock(mutex);
    auto remaining_info = mergeThreadInfos();
    cumulative_info->merge(*remaining_info);
    cumulative_info->report(*cumulative_info);

    DB::WriteBufferFromOwnString out;
    cumulative_info->writeJSON(out, start_timestamp_ms);
    output.write(out.str(), start_timestamp_ms);
}

void GeneratedRunner::createConnections()
{
    DB::EventNotifier::init();
    std::cerr << "---- Creating connections ---- " << std::endl;
    const auto & connection_infos = connection_factory.connectionInfos();
    for (size_t connection_info_idx = 0; connection_info_idx < connection_infos.size(); ++connection_info_idx)
    {
        const auto & connection_info = connection_infos[connection_info_idx];
        std::cerr << fmt::format("Creating {} session(s) for:\n"
                                 "- host: {}\n"
                                 "- secure: {}\n"
                                 "- session timeout: {}ms\n"
                                 "- operation timeout: {}ms\n"
                                 "- connection timeout: {}ms",
                                 connection_info.sessions,
                                 connection_info.host,
                                 connection_info.secure,
                                 connection_info.session_timeout_ms,
                                 connection_info.operation_timeout_ms,
                                 connection_info.connection_timeout_ms) << std::endl;

        for (size_t session = 0; session < connection_info.sessions; ++session)
        {
            connections.emplace_back(connection_factory.getConnection(connection_info, connection_info_idx));
            connections_to_info_map[connections.size() - 1] = connection_info_idx;
        }
    }
    std::cerr << "---- Done creating connections ----\n" << std::endl;
}

std::vector<std::shared_ptr<Coordination::ZooKeeper>> GeneratedRunner::refreshConnections()
{
    std::lock_guard lock(connection_mutex);
    const auto & connection_infos = connection_factory.connectionInfos();
    for (size_t connection_idx = 0; connection_idx < connections.size(); ++connection_idx)
    {
        auto & connection = connections[connection_idx];
        if (connection->isExpired())
        {
            const auto & connection_info = connection_infos[connections_to_info_map[connection_idx]];
            connection = connection_factory.getConnection(connection_info, connection_idx);
        }
    }
    return connections;
}

std::shared_ptr<Stats> GeneratedRunner::mergeThreadInfos()
{
    auto merged = std::make_shared<Stats>();
    merged->elapsed = info->elapsed;
    info->extractInto(*merged);
    info->elapsed.restart();
    for (auto & t : threads)
        t.thread_info.extractInto(*merged);
    return merged;
}

GeneratedRunner::~GeneratedRunner()
{
    shutdown = true;

    if (pool)
        pool->wait();

    if (!nodes_setup.hasNodes())
        return;

    try
    {
        auto connection = connection_factory.getConnection(connection_factory.connectionInfos()[0], 0);
        nodes_setup.cleanup(*connection);
    }
    catch (...)
    {
        DB::tryLogCurrentException("While trying to clean nodes");
    }
}
