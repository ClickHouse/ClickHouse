#include <StorageRunner.h>

#include <chrono>
#include <iomanip>
#include <iostream>
#include <sstream>

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/MemoryStatisticsOS.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Coordination/CoordinationSettings.h>
#include <IO/Operators.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/Logger.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{
    constexpr std::chrono::microseconds POLL_SLEEP{100};

    bool isListResponseOp(Coordination::OpNum op)
    {
        using enum Coordination::OpNum;
        return op == List || op == SimpleList || op == FilteredList || op == FilteredListWithStatsAndData
            || op == ListRecursive;
    }

    size_t countListEntries(const Coordination::ZooKeeperResponse & response, Coordination::OpNum op_num)
    {
        if (!isListResponseOp(op_num))
            return 0;
        if (const auto * list = dynamic_cast<const Coordination::ZooKeeperListResponse *>(&response))
            return list->names.size();
        return 0;
    }
}

StorageRunner::StorageRunner(
    const std::string & config_path_,
    std::optional<size_t> concurrency_,
    std::optional<double> max_time_,
    std::optional<double> report_delay_,
    std::optional<size_t> max_iterations_,
    std::optional<bool> continue_on_error_)
    : config_path(config_path_)
{
    if (config_path.empty())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "--config is required in --storage mode");

    auto log_channel = Poco::AutoPtr<Poco::ConsoleChannel>(new Poco::ConsoleChannel(std::cerr));
    Poco::Logger::root().setChannel(log_channel);
    Poco::Logger::root().setLevel("error");

    DB::ConfigProcessor config_processor(config_path, true, false);
    config_ptr = config_processor.loadConfig().configuration;

    parseConfig(*config_ptr);

    if (concurrency_)
        concurrency = *concurrency_;
    if (max_time_)
        max_time = *max_time_;
    if (report_delay_)
        report_delay = *report_delay_;
    if (max_iterations_)
        max_iterations = *max_iterations_;
    if (continue_on_error_)
        continue_on_error = *continue_on_error_;
}

StorageRunner::~StorageRunner()
{
    shutdown = true;
    for (auto & t : generator_threads)
        if (t.joinable())
            t.join();
    if (preprocess_thread_handle && preprocess_thread_handle->joinable())
        preprocess_thread_handle->join();
    if (commit_thread_handle && commit_thread_handle->joinable())
        commit_thread_handle->join();
}

void StorageRunner::parseConfig(const Poco::Util::AbstractConfiguration & config)
{
    concurrency = config.getUInt("concurrency", 4);
    max_time = config.getDouble("timelimit", 0.0);
    report_delay = config.getDouble("report_delay", 1.0);
    max_iterations = config.getUInt64("iterations", 0);
    continue_on_error = config.getBool("continue_on_error", false);

    writes_per_read_batch = config.getUInt64("storage.writes_per_read_batch", 10);
    snapshot_toggle_periods = config.getUInt64("storage.snapshot_toggle_periods", 0);
    preprocess_commit_queue_size = config.getUInt64("storage.preprocess_commit_queue_size", 32);
    input_queue_size = config.getUInt64("storage.input_queue_size", 2048);
    tick_time_ms = config.getInt64("storage.tick_time_ms", 500);

    if (writes_per_read_batch == 0)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "storage.writes_per_read_batch must be >= 1");
    if (preprocess_commit_queue_size < 2)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "storage.preprocess_commit_queue_size must be >= 2");
    if (input_queue_size < 2)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "storage.input_queue_size must be >= 2");

    benchmark_context.initializeFromConfig(config);
}

size_t StorageRunner::opIndex(Coordination::OpNum op_num)
{
    /// Dense mapping for the ops we expect in keeper-bench workloads.
    using enum Coordination::OpNum;
    switch (op_num)
    {
        case Create: return 1;
        case Remove: return 2;
        case Exists: return 3;
        case Get: return 4;
        case Set: return 5;
        case SimpleList: return 6;
        case List: return 7;
        case Check: return 8;
        case Multi: return 9;
        case MultiRead: return 10;
        case FilteredList: return 11;
        case CheckNotExists: return 12;
        case CreateIfNotExists: return 13;
        case RemoveRecursive: return 14;
        case CheckStat: return 15;
        case TryRemove: return 16;
        case FilteredListWithStatsAndData: return 17;
        case ListRecursive: return 18;
        case Sync: return 19;
        case Heartbeat: return 20;
        default: return 0;
    }
}

namespace
{
    std::string_view opSlotName(size_t slot)
    {
        using namespace std::string_view_literals;
        switch (slot)
        {
            case 1: return "create"sv;
            case 2: return "remove"sv;
            case 3: return "exists"sv;
            case 4: return "get"sv;
            case 5: return "set"sv;
            case 6: return "simplelist"sv;
            case 7: return "list"sv;
            case 8: return "check"sv;
            case 9: return "multi"sv;
            case 10: return "multiread"sv;
            case 11: return "filteredlist"sv;
            case 12: return "checknotexists"sv;
            case 13: return "createifnotexists"sv;
            case 14: return "removerecursive"sv;
            case 15: return "checkstat"sv;
            case 16: return "tryremove"sv;
            case 17: return "filteredliststatsdata"sv;
            case 18: return "listrecursive"sv;
            case 19: return "sync"sv;
            case 20: return "heartbeat"sv;
            default: return "other"sv;
        }
    }
}

void StorageRunner::setupStorage()
{
    auto settings = std::make_shared<DB::CoordinationSettings>();
    settings->loadFromConfig("storage.coordination_settings", *config_ptr);
    keeper_context = std::make_shared<DB::KeeperContext>(/*standalone_keeper=*/true, settings);
    keeper_context->setLocalLogsPreprocessed();
    keeper_context->setRocksDBOptions();
    keeper_context->setServerState(DB::KeeperContext::Phase::RUNNING);

    storage = std::make_unique<Storage>(tick_time_ms, /*superdigest=*/"", keeper_context);

    /// Allocate one session for setup and one per generator thread.
    /// All subsequent requests from a generator use its dedicated session.
    const int64_t setup_timeout_ms = std::max<int64_t>(60'000, tick_time_ms * 1000);
    setup_session_id = storage->getSessionID(setup_timeout_ms);
    for (size_t i = 0; i < concurrency; ++i)
        generator_session_ids.push_back(storage->getSessionID(setup_timeout_ms));

    /// Recursively create the setup tree. The real `BenchmarkContext::startup` uses a
    /// ZooKeeper client, so we walk the node tree here and issue preprocess+commit
    /// directly against the storage, populating `tagged_paths` as we go.
    std::function<void(const BenchmarkContext::Node &, const std::string &)> create_subtree;
    auto & tagged = benchmark_context.getTaggedPaths();
    const Coordination::ACLs & default_acls = benchmark_context.getDefaultAcls();

    create_subtree = [&](const BenchmarkContext::Node & node, const std::string & parent_path)
    {
        std::string path = parent_path == "/" ? "/" + node.name.getString() : parent_path + "/" + node.name.getString();

        auto request = std::make_shared<Coordination::ZooKeeperCreateRequest>();
        request->path = path;
        request->data = node.data ? node.data->getString() : "";
        request->acls = default_acls;

        int64_t zxid = next_zxid.fetch_add(1);
        try
        {
            storage->preprocessRequest(request, setup_session_id, 0, zxid);
            auto responses = storage->processRequest(request, setup_session_id, zxid);
            for (const auto & response : responses)
            {
                if (response.response->error != Coordination::Error::ZOK)
                    throw zkutil::KeeperException::fromPath(response.response->error, path);
            }
        }
        catch (...)
        {
            std::cerr << "Setup request failed for path " << path << ": "
                << DB::getCurrentExceptionMessage(false) << std::endl;
            throw;
        }

        if (node.tag)
            tagged[*node.tag].push_back(path);

        for (const auto & child : node.children)
            create_subtree(*child, path);
    };

    std::cerr << "---- Populating storage for benchmark ----" << std::endl;
    for (const auto & root_node : benchmark_context.getRootNodes())
        create_subtree(*root_node, "/");

    if (!benchmark_context.getTaggedPaths().empty())
    {
        std::cerr << "Tagged paths:" << std::endl;
        for (const auto & [tag_name, paths] : benchmark_context.getTaggedPaths())
            std::cerr << "  \"" << tag_name << "\": " << paths.size() << " paths" << std::endl;
    }
    std::cerr << "Populated " << storage->getNodesCount() << " znodes.\n" << std::endl;
}

void StorageRunner::startGenerators()
{
    const auto * tagged_paths = benchmark_context.getTaggedPaths().empty() ? nullptr : &benchmark_context.getTaggedPaths();

    auto list_children = [this](const std::string & parent_path) -> std::vector<std::string>
    {
        Coordination::KeeperRequestsForSessions requests;
        DB::KeeperRequestForSession rfs;
        rfs.session_id = setup_session_id;
        auto list_req = std::make_shared<Coordination::ZooKeeperListRequest>();
        list_req->path = parent_path;
        rfs.request = list_req;
        requests.push_back(std::move(rfs));

        auto responses = storage->processLocalRequests(requests, /*check_acl=*/false);
        if (responses.empty())
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Empty response to list of {}", parent_path);
        const auto & resp = *responses[0].response;
        if (resp.error != Coordination::Error::ZOK)
            throw zkutil::KeeperException::fromPath(resp.error, parent_path);
        const auto & list = dynamic_cast<const Coordination::ZooKeeperListResponse &>(resp);
        return list.names;
    };

    generators.resize(concurrency);
    for (size_t i = 0; i < concurrency; ++i)
    {
        generators[i] = std::make_shared<Generator>();
        generators[i]->startup(*config_ptr, list_children, i, tagged_paths);
        generators[i]->setWatchCallback(std::make_shared<Coordination::WatchCallback>(
            [](const Coordination::WatchResponse &) {}));
    }
}

template <typename QueueT>
void StorageRunner::pushBlocking(QueueT & queue, QueueItem && item)
{
    while (!shutdown.load(std::memory_order_relaxed))
    {
        if (queue.tryPush(std::move(item))) // NOLINT(bugprone-use-after-move)
            return;
        std::this_thread::sleep_for(POLL_SLEEP);
    }
}

void StorageRunner::generatorThread(size_t idx)
{
    auto & generator = *generators[idx];
    const int64_t session_id = generator_session_ids[idx];

    while (!shutdown.load(std::memory_order_relaxed))
    {
        if (max_iterations != 0)
        {
            size_t started = requests_started.fetch_add(1);
            if (started >= max_iterations)
            {
                shutdown = true;
                break;
            }
        }
        else
        {
            requests_started.fetch_add(1, std::memory_order_relaxed);
        }

        ZooKeeperRequestWithCallbacks request_with_callbacks;
        try
        {
            request_with_callbacks = generator.generate();
        }
        catch (...)
        {
            std::cerr << "Generator exception: " << DB::getCurrentExceptionMessage(false) << std::endl;
            if (!continue_on_error)
                shutdown = true;
            continue;
        }

        QueueItem item;
        item.request = std::move(request_with_callbacks.request);
        item.session_id = session_id;
        item.op_num = item.request->getOpNum();
        item.is_write = !item.request->isReadRequest();
        item.callback = std::move(request_with_callbacks.callback);

        if (item.is_write)
            pushBlocking(*preprocess_queue, std::move(item));
        else
            pushBlocking(*read_queue, std::move(item));
    }
}

void StorageRunner::preprocessThread()
{
    Stopwatch watch;
    QueueItem item;
    while (true)
    {
        if (!preprocess_queue->tryPop(item))
        {
            if (generators_done.load(std::memory_order_relaxed) && preprocess_queue->size() == 0)
                break;
            std::this_thread::sleep_for(POLL_SLEEP);
            continue;
        }

        item.zxid = next_zxid.fetch_add(1, std::memory_order_relaxed);

        watch.restart();
        try
        {
            std::shared_lock lock(state_machine_storage_mutex);
            storage->preprocessRequest(item.request, item.session_id, /*time=*/0, item.zxid);
        }
        catch (...)
        {
            std::cerr << "preprocessRequest exception: " << DB::getCurrentExceptionMessage(false) << std::endl;
            if (!continue_on_error)
                shutdown = true;
        }
        uint64_t elapsed = watch.elapsedNanoseconds();
        period_stats.preprocess_busy_ns.fetch_add(elapsed, std::memory_order_relaxed);
        period_stats.per_op[opIndex(item.op_num)].preprocess_ns.fetch_add(elapsed, std::memory_order_relaxed);

        pushBlocking(*commit_queue, std::move(item));
    }
}

void StorageRunner::commitThread()
{
    pcg64 rng{randomSeed()};
    std::uniform_int_distribution<size_t> read_trigger_dist(0, writes_per_read_batch - 1);

    Stopwatch watch;

    auto drain_reads = [&]()
    {
        Coordination::KeeperRequestsForSessions batch;
        std::vector<QueueItem> batch_items;

        QueueItem read_item;
        while (read_queue->tryPop(read_item))
        {
            DB::KeeperRequestForSession rfs;
            rfs.session_id = read_item.session_id;
            rfs.request = read_item.request;
            batch.push_back(std::move(rfs));
            batch_items.push_back(std::move(read_item));
        }
        if (batch.empty())
            return;

        watch.restart();
        Coordination::KeeperResponsesForSessions responses;
        try
        {
            std::shared_lock lock(state_machine_storage_mutex);
            responses = storage->processLocalRequests(batch, /*check_acl=*/false);
        }
        catch (...)
        {
            std::cerr << "processLocalRequests exception: " << DB::getCurrentExceptionMessage(false) << std::endl;
            if (!continue_on_error)
                shutdown = true;
            period_stats.commit_read_busy_ns.fetch_add(watch.elapsedNanoseconds(), std::memory_order_relaxed);
            return;
        }
        uint64_t total_ns = watch.elapsedNanoseconds();
        period_stats.commit_read_busy_ns.fetch_add(total_ns, std::memory_order_relaxed);

        /// Attribute the batch time uniformly to individual requests.
        uint64_t per_request_ns = batch_items.empty() ? 0 : total_ns / batch_items.size();

        for (size_t i = 0; i < batch_items.size(); ++i)
        {
            auto & resp_item = responses[i];
            auto slot = opIndex(batch_items[i].op_num);
            auto & s = period_stats.per_op[slot];
            s.count.fetch_add(1, std::memory_order_relaxed);
            s.process_ns.fetch_add(per_request_ns, std::memory_order_relaxed);
            if (isListResponseOp(batch_items[i].op_num) && resp_item.response)
                s.list_entries.fetch_add(countListEntries(*resp_item.response, batch_items[i].op_num), std::memory_order_relaxed);

            period_stats.reads_committed.fetch_add(1, std::memory_order_relaxed);
        }
    };

    QueueItem write_item;
    while (true)
    {
        if (!commit_queue->tryPop(write_item))
        {
            if (generators_done.load(std::memory_order_relaxed)
                && preprocess_queue->size() == 0 && commit_queue->size() == 0)
            {
                drain_reads();
                if (read_queue->size() == 0)
                    break;
                continue;
            }
            std::this_thread::sleep_for(POLL_SLEEP);
            continue;
        }

        watch.restart();
        Coordination::KeeperResponsesForSessions responses;
        try
        {
            std::shared_lock lock(state_machine_storage_mutex);
            responses = storage->processRequest(write_item.request, write_item.session_id, write_item.zxid);
        }
        catch (...)
        {
            std::cerr << "processRequest exception: " << DB::getCurrentExceptionMessage(false) << std::endl;
            if (!continue_on_error)
                shutdown = true;
        }
        uint64_t elapsed = watch.elapsedNanoseconds();
        period_stats.commit_write_busy_ns.fetch_add(elapsed, std::memory_order_relaxed);

        auto slot = opIndex(write_item.op_num);
        auto & s = period_stats.per_op[slot];
        s.count.fetch_add(1, std::memory_order_relaxed);
        s.process_ns.fetch_add(elapsed, std::memory_order_relaxed);

        period_stats.writes_committed.fetch_add(1, std::memory_order_relaxed);

        /// processRequest produces one response for the request + any number of WatchResponse-s
        /// for triggered watches.
        const Coordination::Response * main_response = nullptr;
        for (const auto & response : responses)
        {
            if (dynamic_cast<const Coordination::WatchResponse *>(response.response.get()))
                continue;
            chassert(!main_response);
            main_response = response.response.get();
        }
        chassert(main_response);

        if (write_item.callback)
            write_item.callback(main_response);

        /// With probability 1/writes_per_read_batch, drain and process reads.
        if (read_trigger_dist(rng) == 0)
            drain_reads();
    }
}

void StorageRunner::report(double period_seconds, bool snapshot_mode_during_period)
{
    /// Swap/reset atomic counters for this period.
    auto take = [](std::atomic<uint64_t> & v) { return v.exchange(0, std::memory_order_relaxed); };

    uint64_t writes = take(period_stats.writes_committed);
    uint64_t reads = take(period_stats.reads_committed);
    uint64_t pre_busy = take(period_stats.preprocess_busy_ns);
    uint64_t w_busy = take(period_stats.commit_write_busy_ns);
    uint64_t r_busy = take(period_stats.commit_read_busy_ns);

    struct Snap { uint64_t count; uint64_t process_ns; uint64_t preprocess_ns; uint64_t list_entries; };
    std::array<Snap, NUM_OP_SLOTS> snap{};
    for (size_t i = 0; i < NUM_OP_SLOTS; ++i)
    {
        snap[i].count = take(period_stats.per_op[i].count);
        snap[i].process_ns = take(period_stats.per_op[i].process_ns);
        snap[i].preprocess_ns = take(period_stats.per_op[i].preprocess_ns);
        snap[i].list_entries = take(period_stats.per_op[i].list_entries);
    }

    double period_ns = period_seconds * 1e9;
    auto util = [period_ns](uint64_t ns) { return period_ns > 0 ? 100.0 * static_cast<double>(ns) / period_ns : 0.0; };

    uint64_t rss_mb = 0;
#if defined(OS_LINUX) || defined(OS_FREEBSD)
    try
    {
        DB::MemoryStatisticsOS mem;
        rss_mb = mem.get().resident / (1024 * 1024);
    }
    catch (...) // NOLINT(bugprone-empty-catch): best-effort memory readout, 'Ok' to ignore if /proc/self/statm is unavailable
    {
    }
#endif

    uint64_t znode_count;
    {
        std::lock_guard lock(state_machine_storage_mutex);
        znode_count = storage ? storage->getNodesCount() : 0;
    }

    std::stringstream out; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    out << std::fixed << std::setprecision(1);
    out << "\n==== period " << period_seconds << "s ====\n";
    out << "writes/s: " << static_cast<double>(writes) / period_seconds
        << ", reads/s: " << static_cast<double>(reads) / period_seconds << "\n";
    out << "util: preprocess " << util(pre_busy)
        << "%, commit(writes) " << util(w_busy)
        << "%, commit(reads) " << util(r_busy) << "%\n";
    out << "znodes: " << znode_count
        << ", rss: " << rss_mb << " MiB"
        << ", snapshot_mode: " << (snapshot_mode_during_period ? "yes" : "no") << "\n";
    out << "per-op avg ns per request:\n";
    for (size_t i = 1; i < NUM_OP_SLOTS; ++i)
    {
        if (snap[i].count == 0)
            continue;
        uint64_t avg_process = snap[i].process_ns / snap[i].count;
        out << "  " << opSlotName(i) << ": count=" << snap[i].count
            << " process=" << avg_process << " ns";
        if (snap[i].preprocess_ns > 0)
        {
            uint64_t avg_preprocess = snap[i].preprocess_ns / snap[i].count;
            out << " preprocess=" << avg_preprocess << " ns";
        }
        if (snap[i].list_entries > 0)
        {
            uint64_t ns_per_entry = snap[i].process_ns / snap[i].list_entries;
            out << " (" << snap[i].list_entries << " names, " << ns_per_entry << " ns/name)";
        }
        out << "\n";
    }

    std::cerr << out.str() << std::flush;
}

void StorageRunner::runBenchmark()
{
    shared_context = DB::Context::createShared();
    global_context = DB::Context::createGlobal(shared_context.get());
    global_context->makeGlobalContext();
    global_context->setApplicationType(DB::Context::ApplicationType::KEEPER);

    setupStorage();
    startGenerators();

    preprocess_queue = std::make_unique<NonblockingBoundedQueue<QueueItem>>(input_queue_size);
    read_queue = std::make_unique<NonblockingBoundedQueue<QueueItem>>(input_queue_size);
    commit_queue = std::make_unique<NonblockingBoundedQueue<QueueItem>>(preprocess_commit_queue_size);

    total_watch.restart();

    preprocess_thread_handle = std::make_unique<std::thread>([this] { preprocessThread(); });
    commit_thread_handle = std::make_unique<std::thread>([this] { commitThread(); });
    for (size_t i = 0; i < concurrency; ++i)
        generator_threads.emplace_back([this, i] { generatorThread(i); });

    /// Main thread: periodic reporting and snapshot-mode toggles.
    Stopwatch period_watch;
    size_t period_idx = 0;
    bool period_had_snapshot = false;
    while (!shutdown.load(std::memory_order_relaxed))
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(std::max<int64_t>(10, static_cast<int64_t>(report_delay * 100))));

        if (max_time > 0 && total_watch.elapsedSeconds() >= max_time)
            shutdown = true;

        double elapsed_period = period_watch.elapsedSeconds();
        if (elapsed_period >= report_delay || shutdown.load())
        {
            period_idx++;

            /// Toggle snapshot mode at period boundaries.
            bool want_snapshot_now = snapshot_enabled.load(std::memory_order_relaxed);
            if (snapshot_toggle_periods > 0 && (period_idx % snapshot_toggle_periods == 0))
            {
                std::lock_guard lock(state_machine_storage_mutex);
                if (snapshot_enabled.load())
                {
                    storage->disableSnapshotMode();
                    storage->clearGarbageAfterSnapshot();
                    snapshot_enabled.store(false);
                }
                else
                {
                    auto version = storage->container.snapshotSizeWithVersion().second;
                    storage->enableSnapshotMode(version);
                    snapshot_enabled.store(true);
                }
            }

            report(elapsed_period, period_had_snapshot || want_snapshot_now);
            period_watch.restart();
            period_had_snapshot = snapshot_enabled.load();
        }
    }

    shutdown = true;
    for (auto & t : generator_threads)
        if (t.joinable())
            t.join();
    generators_done = true;
    if (preprocess_thread_handle->joinable())
        preprocess_thread_handle->join();
    if (commit_thread_handle->joinable())
        commit_thread_handle->join();

    /// Disable snapshot mode before the storage is destroyed: SnapshotableHashTable's
    /// destructor asserts !snapshot_mode via clearOutdatedNodes.
    if (snapshot_enabled.load())
    {
        storage->disableSnapshotMode();
        storage->clearGarbageAfterSnapshot();
        snapshot_enabled.store(false);
    }

    std::cerr << "Total elapsed: " << total_watch.elapsedSeconds() << "s\n";
}
