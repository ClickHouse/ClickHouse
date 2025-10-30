#include <unistd.h>
#include <cstdlib>
#include <csignal>
#include <iostream>
#include <mutex>
#include <optional>
#include <random>
#include <string_view>
#include <pcg_random.hpp>
#include <Poco/UUIDGenerator.h>
#include <Poco/Util/Application.h>
#include <Common/Config/parseConnectionCredentials.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>
#include <AggregateFunctions/ReservoirSampler.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <base/defines.h>
#include <boost/program_options.hpp>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/Exception.h>
#include <Common/randomSeed.h>
#include <Common/clearPasswordFromCommandLine.h>
#include <Core/Settings.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <IO/ConnectionTimeouts.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Interpreters/Context.h>
#include <Client/Connection.h>
#include <Common/InterruptListener.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/Config/getClientConfigPath.h>
#include <Common/TerminalSize.h>
#include <Common/StudentTTest.h>
#include <Common/CurrentMetrics.h>
#include <IO/WriteBuffer.h>
#include <Client/ClientApplicationBaseParser.h>

/** A tool for evaluating ClickHouse performance.
  * The tool emulates a case with fixed amount of simultaneously executing queries.
  */

namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}

namespace DB
{

using Ports = std::vector<UInt16>;
static constexpr std::string_view DEFAULT_CLIENT_NAME = "benchmark";

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_BLOCK_SIGNAL;
    extern const int EMPTY_DATA_PASSED;
    extern const int UNRECOGNIZED_ARGUMENTS;
}

class Benchmark : public Poco::Util::Application
{
public:
    Benchmark(
        const boost::program_options::variables_map & options,
        String && proto_send_chunked,
        String && proto_recv_chunked,
        bool print_stacktrace_,
        Settings && settings_)
        : connection_arguments(ConnectionArguments{
            .hosts = options.contains("host") ? std::make_optional(options["host"].as<Strings>()) : std::nullopt,
            .ports = options.contains("port") ? std::make_optional(options["port"].as<Ports>()) : std::nullopt,
            .user = options.contains("user") ? std::make_optional(options["user"].as<std::string>()) : std::nullopt,
            .password = options.contains("password") ? std::make_optional(options["password"].as<std::string>()) : std::nullopt,
            .database = options.contains("database") ? std::make_optional(options["database"].as<std::string>()) : std::nullopt,
            .secure = options.contains("secure") ? std::make_optional(true) : std::nullopt,
            .accept_invalid_certificate = options.contains("accept-invalid-certificate") ? std::make_optional(true) : std::nullopt,
            .quota_key = options.contains("quota_key") ? std::make_optional(options["quota_key"].as<std::string>()) : std::nullopt,
            .proto_send_chunked = proto_send_chunked,
            .proto_recv_chunked = proto_recv_chunked,
        }),
        connection_name(options.contains("connection") ? std::make_optional(options["connection"].as<std::string>()) : std::nullopt),
        round_robin(options.contains("roundrobin")),
        concurrency(options["concurrency"].as<unsigned>()),
        max_concurrency(std::max(concurrency, options["max_concurrency"].as<unsigned>())),
        threads(concurrency),
        delay(options["delay"].as<double>()),
        precise(options.contains("precise")),
        queue(max_concurrency),
        randomize(options.contains("randomize")),
        cumulative(options.contains("cumulative")),
        max_iterations(options["iterations"].as<size_t>()),
        max_time(options["timelimit"].as<double>()),
        confidence(options["confidence"].as<size_t>()),
        query_id(options["query_id"].as<std::string>()),
        query_id_prefix(options["query_id_prefix"].as<std::string>()),
        query_to_execute(options["query"].as<std::string>()),
        continue_on_errors(options.contains("ignore-error")),
        max_consecutive_errors(options["max-consecutive-errors"].as<size_t>()),
        reconnect(options["reconnect"].as<size_t>()),
        display_client_side_time(options.contains("client-side-time")),
        print_stacktrace(print_stacktrace_),
        settings(std::move(settings_)),
        shared_context(Context::createShared()),
        global_context(Context::createGlobal(shared_context.get())),
        pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, max_concurrency)
    {
        global_context->makeGlobalContext();
        global_context->setSettings(settings);
        global_context->setClientName(std::string(DEFAULT_CLIENT_NAME));
        global_context->setQueryKindInitial();

        /// This is needed to receive blocks with columns of AggregateFunction data type
        /// (example: when using stage = 'with_mergeable_state')
        registerAggregateFunctions();

        query_processing_stage = QueryProcessingStage::fromString(options["stage"].as<std::string>());

        if (options.contains("config"))
            config().setString("config-file", options["config"].as<std::string>());
    }

    void initialize(Poco::Util::Application & self [[maybe_unused]]) override
    {
        std::string home_path;
        const char * home_path_cstr = getenv("HOME"); // NOLINT(concurrency-mt-unsafe)
        if (home_path_cstr)
            home_path = home_path_cstr;

        std::optional<std::string> config_path;
        if (config().has("config-file"))
            config_path.emplace(config().getString("config-file"));
        else
            config_path = getClientConfigPath(home_path);
        if (config_path.has_value())
        {
            ConfigProcessor config_processor(*config_path);
            auto loaded_config = config_processor.loadConfig();
            config().add(loaded_config.configuration);
        }

        if (connection_name.has_value())
        {
            std::string default_host;
            if (connection_arguments.hosts.has_value())
                default_host = connection_arguments.hosts->front();
            else if (const char * env_host = getenv("CLICKHOUSE_HOST")) // NOLINT(concurrency-mt-unsafe)
                default_host = env_host;
            else
                default_host = "localhost";

            auto overrides = parseConnectionsCredentials(config(), default_host, connection_name);
            if (overrides.hostname.has_value() && !connection_arguments.hosts.has_value())
                connection_arguments.hosts.emplace({overrides.hostname.value()});
            if (overrides.port.has_value() && !connection_arguments.ports.has_value())
                connection_arguments.ports.emplace({overrides.port.value()});
            if (overrides.secure.has_value() && !connection_arguments.secure.has_value())
                connection_arguments.secure.emplace(overrides.secure.value());
            if (overrides.accept_invalid_certificate.has_value() && !connection_arguments.accept_invalid_certificate.has_value())
                connection_arguments.accept_invalid_certificate.emplace(overrides.accept_invalid_certificate.value());
            if (overrides.user.has_value() && !connection_arguments.user.has_value())
                connection_arguments.user.emplace(overrides.user.value());
            if (overrides.password.has_value() && !connection_arguments.password.has_value())
                connection_arguments.password.emplace(overrides.password.value());
            if (overrides.database.has_value() && !connection_arguments.database.has_value())
                connection_arguments.database.emplace(overrides.database.value());
        }

        if (connection_arguments.accept_invalid_certificate.value_or(false))
        {
            config().setString("openSSL.client.invalidCertificateHandler.name", "AcceptCertificateHandler");
            config().setString("openSSL.client.verificationMode", "none");
        }

        makeConnections();
    }

    int main(const std::vector<std::string> &) override
    {
        try
        {
            readQueries();
            runBenchmark();
            return 0;
        }
        catch (...)
        {
            log << getCurrentExceptionMessage(print_stacktrace, true) << '\n';
            auto code = getCurrentExceptionCode();
            return static_cast<UInt8>(code) ? code : 1;
        }
    }

private:
    using Entry = ConnectionPool::Entry;
    using EntryPtr = std::shared_ptr<Entry>;
    using EntryPtrs = std::vector<EntryPtr>;

    struct ConnectionArguments
    {
        std::optional<Strings> hosts;
        std::optional<Ports> ports;
        std::optional<String> user;
        std::optional<String> password;
        std::optional<String> database;
        std::optional<bool> secure;
        std::optional<bool> accept_invalid_certificate;

        std::optional<String> quota_key;
        const String proto_send_chunked;
        const String proto_recv_chunked;
    } connection_arguments;
    std::optional<String> connection_name;

    bool round_robin;
    unsigned concurrency;
    unsigned max_concurrency;
    unsigned threads;
    double delay;
    bool precise;

    using Query = std::string;
    using Queries = std::vector<Query>;
    Queries queries;

    using Queue = ConcurrentBoundedQueue<Query>;
    Queue queue;

    using ConnectionPoolUniq = std::unique_ptr<ConnectionPool>;
    using ConnectionPoolUniqs = std::vector<ConnectionPoolUniq>;
    ConnectionPoolUniqs connections;

    bool randomize;
    bool cumulative;
    size_t max_iterations;
    double max_time;
    size_t confidence;
    String query_id;
    String query_id_prefix;
    String query_to_execute;
    bool continue_on_errors;
    size_t max_consecutive_errors;
    size_t reconnect;
    bool display_client_side_time;
    bool print_stacktrace;
    const Settings & settings;
    SharedContextHolder shared_context;
    ContextMutablePtr global_context;
    QueryProcessingStage::Enum query_processing_stage;

    std::mutex mutex;
    AutoFinalizedWriteBuffer<WriteBufferFromFileDescriptor> log TSA_GUARDED_BY(mutex) {STDERR_FILENO};

    std::atomic<size_t> consecutive_errors{0};

    /// Don't execute new queries after timelimit or SIGINT or exception
    std::atomic<bool> shutdown{false};

    std::atomic<size_t> queries_executed{0};

    std::mutex queries_per_connection_mutex;
    std::vector<size_t> queries_per_connection TSA_GUARDED_BY(queries_per_connection_mutex);

    struct Stats
    {
        double queries = 0;
        size_t finished_queries = 0;
        size_t errors = 0;
        double read_rows = 0;
        double read_bytes = 0;
        double result_rows = 0;
        double result_bytes = 0;

        using Sampler = ReservoirSampler<double>;
        Sampler sampler {1 << 16};

        void addWeighted(size_t read_rows_inc, size_t read_bytes_inc, size_t result_rows_inc, size_t result_bytes_inc, double weight)
        {
            queries += weight;
            read_rows += weight * read_rows_inc;
            read_bytes += weight * read_bytes_inc;
            result_rows += weight * result_rows_inc;
            result_bytes += weight * result_bytes_inc;
        }

        void sample(double duration)
        {
            ++finished_queries;
            sampler.insert(duration);
        }

        void add(double duration, size_t read_rows_inc, size_t read_bytes_inc, size_t result_rows_inc, size_t result_bytes_inc)
        {
            addWeighted(read_rows_inc, read_bytes_inc, result_rows_inc, result_bytes_inc, 1.0);
            sample(duration);
        }
    };

    using MultiStats = std::vector<std::shared_ptr<Stats>>;

    struct IntervalStats;
    friend struct IntervalStats;
    struct IntervalStats
    {
        Benchmark & benchmark;
        bool reported = false;

        UInt64 start_ns;
        UInt64 end_ns = std::numeric_limits<UInt64>::max();
        MultiStats stats;
        size_t threads;

        // NOTE: We keep reference to the next interval for --precise mode
        std::shared_ptr<IntervalStats> next;

        IntervalStats(Benchmark & benchmark_, UInt64 start_ns_)
            : benchmark(benchmark_)
            , start_ns(start_ns_)
        {}

        ~IntervalStats()
        {
            report();
        }

        // Closes the interval. Note that stats maybe updated after close by queries started during this interval, but finished in a later one
        void close(UInt64 end_ns_, size_t threads_)
        {
            end_ns = end_ns_;
            threads = threads_;
        }

        // In --precise mode: it must be called on a closed interval when all queries started during it are finished
        // Otherwise: it is called just after close()
        void report()
        {
            if (reported || end_ns == std::numeric_limits<UInt64>::max())
                return;
            reported = true;
            benchmark.report(stats, (end_ns - start_ns) / 1e9, threads);
        }
    };

    std::mutex interval_mutex;
    std::shared_ptr<IntervalStats> interval TSA_GUARDED_BY(interval_mutex);
    MultiStats total_stats; // requires mutex
    StudentTTest t_test;

    Stopwatch total_watch;
    Stopwatch delay_watch;

    ThreadPool pool;

    void makeConnections()
    {
        const bool is_secure = connection_arguments.secure.value_or(false);
        const auto secure = is_secure ? Protocol::Secure::Enable : Protocol::Secure::Disable;

        /// Note: according to the standard, subsequent calls to getenv can mangle previous result.
        /// So we copy the results to std::string.
        std::optional<std::string> env_user_str;
        std::optional<std::string> env_password_str;
        std::optional<std::string> env_host_str;
        std::optional<std::string> env_quota_key_str;

        const char * env_user = getenv("CLICKHOUSE_USER"); // NOLINT(concurrency-mt-unsafe)
        if (env_user != nullptr)
            env_user_str.emplace(std::string(env_user));

        const char * env_password = getenv("CLICKHOUSE_PASSWORD"); // NOLINT(concurrency-mt-unsafe)
        if (env_password != nullptr)
            env_password_str.emplace(std::string(env_password));

        const char * env_host = getenv("CLICKHOUSE_HOST"); // NOLINT(concurrency-mt-unsafe)
        if (env_host != nullptr)
            env_host_str.emplace(std::string(env_host));

        const char * env_quota_key = getenv("CLICKHOUSE_QUOTA_KEY"); // NOLINT(concurrency-mt-unsafe)
        if (env_quota_key != nullptr)
            env_quota_key_str.emplace(std::string(env_quota_key));

        UInt16 default_port = is_secure ? DBMS_DEFAULT_SECURE_PORT : DBMS_DEFAULT_PORT;
        Ports ports = connection_arguments.ports.value_or(Ports({default_port}));
        Strings hosts = connection_arguments.hosts.value_or(Strings({env_host_str.value_or("localhost")}));

        size_t connections_cnt = std::max(ports.size(), hosts.size());
        for (size_t i = 0; i < connections_cnt; ++i)
        {
            UInt16 cur_port = i >= ports.size() ? default_port : ports[i];
            std::string cur_host = i >= hosts.size() ? "localhost" : hosts[i];

            connections.emplace_back(std::make_unique<ConnectionPool>(
                max_concurrency,
                cur_host,
                cur_port,
                connection_arguments.database.value_or("default"),
                connection_arguments.user.value_or(env_user_str.value_or("default")),
                connection_arguments.password.value_or(env_password_str.value_or("")),
                connection_arguments.proto_send_chunked,
                connection_arguments.proto_recv_chunked,
                connection_arguments.quota_key.value_or(env_quota_key_str.value_or("")),
                /* cluster_= */ "",
                /* cluster_secret_= */ "",
                /* client_name_= */ std::string(DEFAULT_CLIENT_NAME),
                Protocol::Compression::Enable,
                secure,
                /* bind_host_= */ ""));

            if (!round_robin || total_stats.empty())
                total_stats.emplace_back(std::make_shared<Stats>());
        }

        connections.reserve(connections_cnt);
        total_stats.reserve(round_robin ? 1 : connections_cnt);

        // Initialize queries_per_connection to track queries for each connection
        {
            std::lock_guard lock(queries_per_connection_mutex);
            queries_per_connection.resize(connections.size(), 0);
        }
    }

    void readQueries()
    {
        if (query_to_execute.empty())
        {
            ReadBufferFromFileDescriptor in(STDIN_FILENO);

            while (!in.eof())
            {
                String query;
                readText(query, in);
                assertChar('\n', in);

                if (!query.empty())
                    queries.emplace_back(std::move(query));
            }

            if (queries.empty())
                throw Exception(ErrorCodes::EMPTY_DATA_PASSED, "Empty list of queries.");
        }
        else
        {
            queries.emplace_back(query_to_execute);
        }


        std::lock_guard lock(mutex);
        log << "Loaded " << queries.size() << " queries.\n" << flush;
    }

    /// Try push new query and check cancellation conditions
    bool tryPushQueryInteractively(const String & query, InterruptListener & interrupt_listener)
    {
        bool inserted = false;

        while (!inserted)
        {
            inserted = queue.tryPush(query, 100);

            if (shutdown)
            {
                /// An exception occurred in a worker
                return false;
            }

            if (max_time > 0 && total_watch.elapsedSeconds() >= max_time)
            {
                std::cout << "Stopping launch of queries."
                          << " Requested time limit " << max_time << " seconds is exhausted.\n";
                return false;
            }

            if (interrupt_listener.check())
            {
                std::cout << "Stopping launch of queries. SIGINT received.\n";
                return false;
            }
        }

        if (delay > 0 && delay_watch.elapsedSeconds() > delay)
        {
            startNextInterval();

            // Special mode: gradually increasing concurrency.
            // Concurrency is constant between reports and increased by one after every report.
            if (concurrency < max_concurrency)
            {
                if (threads == max_concurrency)
                    return false; // Reached maximum concurrency
                try
                {
                    pool.scheduleOrThrowOnError([this]() mutable { thread(); });
                    threads++;
                }
                catch (...)
                {
                    shutdown = true;
                    pool.wait();
                    throw;
                }
            }
        }

        return true;
    }

    void startNextInterval(bool first = false)
    {
        std::lock_guard lock(interval_mutex);

        delay_watch.restart();
        UInt64 now_ns = delay_watch.getStart();

        if (cumulative)
        {
            if (!first)
                report(total_stats, total_watch.elapsedSeconds(), threads);
        }
        else
        {
            // Report previous interval (if any)
            if (interval)
            {
                interval->close(now_ns, threads);
                if (!precise)
                    interval->report();
            }

            // Start the next interval
            auto next_interval = std::make_shared<IntervalStats>(*this, now_ns);
            next_interval->stats.reserve(round_robin ? 1 : connections.size());
            for (size_t i = 0; i < (round_robin ? 1 : connections.size()); ++i)
                next_interval->stats.emplace_back(std::make_shared<Stats>());
            if (interval)
                interval->next = next_interval;
            interval = next_interval;
        }
    }

    void runBenchmark()
    {
        pcg64 generator(randomSeed());
        std::uniform_int_distribution<size_t> distribution(0, queries.size() - 1);

        startNextInterval(true);

        try
        {
            for (size_t i = 0; i < concurrency; ++i)
                pool.scheduleOrThrowOnError([this]() mutable { thread(); });
        }
        catch (...)
        {
            shutdown = true;
            pool.wait();
            throw;
        }

        InterruptListener interrupt_listener;

        /// Push queries into queue
        for (size_t i = 0; !max_iterations || i < max_iterations; ++i)
        {
            size_t query_index = randomize ? distribution(generator) : i % queries.size();

            if (!tryPushQueryInteractively(queries[query_index], interrupt_listener))
            {
                shutdown = true;
                break;
            }
        }

        /// Now we don't block the Ctrl+C signal and second signal will terminate the program without waiting.
        interrupt_listener.unblock();

        pool.wait();
        total_watch.stop();

        report(total_stats, total_watch.elapsedSeconds());
    }


    void thread()
    {
        Query query;

        /// Randomly choosing connection index
        pcg64 generator(randomSeed());
        std::uniform_int_distribution<size_t> distribution(0, connections.size() - 1);

        /// In these threads we do not accept INT signal.
        sigset_t sig_set;
        if (sigemptyset(&sig_set)
            || sigaddset(&sig_set, SIGINT)
            || pthread_sigmask(SIG_BLOCK, &sig_set, nullptr))
        {
            throw ErrnoException(ErrorCodes::CANNOT_BLOCK_SIGNAL, "Cannot block signal");
        }

        while (true)
        {
            bool extracted = false;

            while (!extracted)
            {
                extracted = queue.tryPop(query, 100);

                if (shutdown || (max_iterations && queries_executed == max_iterations))
                    return;
            }

            const auto connection_index = distribution(generator);
            try
            {
                execute(query, connection_index);
                consecutive_errors = 0;
            }
            catch (...)
            {
                size_t info_index = round_robin ? 0 : connection_index;
                {
                    std::lock_guard lock(mutex);
                    if (!(continue_on_errors || max_consecutive_errors > ++consecutive_errors))
                    {
                        shutdown = true;
                        throw;
                    }

                    log << getCurrentExceptionMessage(print_stacktrace,
                        true /*check embedded stack trace*/) << "\n\n" << flush;

                    ++total_stats[info_index]->errors;
                }

                std::lock_guard lock(interval_mutex); // Locking order: first intervals_mutex, then mutex
                if (interval)
                    ++interval->stats[info_index]->errors;
            }
            // Count failed queries toward executed, so that we'd reach
            // max_iterations even if every run fails.
            ++queries_executed;
        }
    }

    void execute(const Query & query, size_t connection_index)
    {
        Stopwatch watch;

        std::shared_ptr<IntervalStats> cur_interval;
        if (precise)
        {
            std::lock_guard lock(interval_mutex);
            cur_interval = interval;
        }

        ConnectionPool::Entry entry = connections[connection_index]->get(ConnectionTimeouts::getTCPTimeoutsWithoutFailover(settings));

        bool should_reconnect = false;
        {
            std::lock_guard lock(queries_per_connection_mutex);
            should_reconnect = reconnect > 0 && (++queries_per_connection[connection_index] % reconnect == 0);
        }

        if (should_reconnect)
            entry->disconnect();

        RemoteQueryExecutor executor(*entry, query, std::make_shared<const Block>(), global_context, nullptr, Scalars(), Tables(), query_processing_stage);

        if (!query_id.empty())
            executor.setQueryId(query_id);
        else if (!query_id_prefix.empty())
            executor.setQueryId(query_id_prefix + "_" + Poco::UUIDGenerator().createRandom().toString());

        Progress progress;
        executor.setProgressCallback([&progress](const Progress & value) { progress.incrementPiecewiseAtomically(value); });

        executor.sendQuery(ClientInfo::QueryKind::INITIAL_QUERY);

        ProfileInfo info;
        for (Block block = executor.readBlock(); !block.empty(); block = executor.readBlock())
            info.update(block);

        executor.finish();

        watch.stop();
        double duration = (display_client_side_time || progress.elapsed_ns == 0)
            ? watch.elapsedSeconds()
            : progress.elapsed_ns / 1e9;
        size_t info_index = round_robin ? 0 : connection_index;

        if (precise && cur_interval)
        {
            // Stats weighting across all overlapped intervals
            UInt64 duration_ns = watch.getEnd() - watch.getStart();

            std::lock_guard lock(interval_mutex);

            // Distribute weights across intervals intersecting query execution span
            // Intervals:
            // [beg]    I0: [s0---------e0)
            //          I1:               [s1--------------e1)
            // [end]    I2:               |                  [s2-----------e2)
            //                            |                  |
            //  Query span:        [qs----+------------------+-------qe)
            //                         ^           ^              ^
            //                         w0          w1             w2
            // Weights (w0, w1, w2) are proportional to the lengths of intersections
            // and normalized to the total duration of a query to given 1 as a sum of weights.
            for (; cur_interval; cur_interval = cur_interval->next)
            {
                if (cur_interval->end_ns <= watch.getStart())
                    continue;
                if (cur_interval->start_ns >= watch.getEnd())
                    break;
                const double overlap_ns = std::min(cur_interval->end_ns, watch.getEnd()) - std::max(cur_interval->start_ns, watch.getStart());
                const double weight = overlap_ns / duration_ns;
                if (overlap_ns > 0 && duration_ns > 0)
                    cur_interval->stats[info_index]->addWeighted(progress.read_rows, progress.read_bytes, info.rows, info.bytes, weight);
            }

            // Latency goes to the last interval only (our sampler does not support weights)
            interval->stats[info_index]->sample(duration);
        }
        else if (!cumulative)
        {
            std::lock_guard lock(interval_mutex);
            interval->stats[info_index]->add(duration, progress.read_rows, progress.read_bytes, info.rows, info.bytes);
        }

        std::lock_guard lock(mutex);
        total_stats[info_index]->add(duration, progress.read_rows, progress.read_bytes, info.rows, info.bytes);
        t_test.add(info_index, duration);
    }

    void report(const MultiStats & infos, double seconds, size_t used_threads = 0)
    {
        std::lock_guard lock(mutex);

        size_t executed = queries_executed.load();
        log << "\nQueries executed: " << executed;
        if (max_iterations > 1)
            log << " (" << (executed * 100.0 / max_iterations) << "%)";
        else if (queries.size() > 1)
            log << " (" << (executed * 100.0 / queries.size()) << "%)";
        log << ".\n" << flush;

        if (used_threads > 0 && concurrency < max_concurrency)
        {
            log << "Concurrency: " << used_threads;
            log << " of " << max_concurrency;
            log << " parallel queries.\n" << flush;
        }

        log << "\n";
        for (size_t i = 0; i < infos.size(); ++i)
        {
            const auto & info = infos[i];

            /// Avoid zeros, nans or exceptions
            if (0 == info->finished_queries)
                return;

            std::string connection_description = connections[i]->getDescription();
            if (round_robin)
            {
                connection_description.clear();
                for (const auto & conn : connections)
                {
                    if (!connection_description.empty())
                        connection_description += ", ";
                    connection_description += conn->getDescription();
                }
            }
            log
                << connection_description << ", "
                << "queries: " << info->finished_queries << ", ";
            if (info->errors)
            {
                log << "errors: " << info->errors << ", ";
            }
            log
                << "QPS: " << fmt::format("{:.3f}", info->queries / seconds) << ", "
                << "RPS: " << fmt::format("{:.3f}", info->read_rows / seconds) << ", "
                << "MiB/s: " << fmt::format("{:.3f}", info->read_bytes / seconds / 1048576) << ", "
                << "result RPS: " << fmt::format("{:.3f}", info->result_rows / seconds) << ", "
                << "result MiB/s: " << fmt::format("{:.3f}", info->result_bytes / seconds / 1048576) << "."
                << "\n";
        }
        log << "\n";

        auto print_percentile = [&](double percent) TSA_REQUIRES(mutex)
        {
            log << percent << "%\t\t";
            for (const auto & info : infos)
            {
                log << fmt::format("{:.3f}", info->sampler.quantileNearest(percent / 100.0)) << " sec.\t";
            }
            log << "\n";
        };

        for (int percent = 0; percent <= 90; percent += 10)
            print_percentile(percent);

        print_percentile(95);
        print_percentile(99);
        print_percentile(99.9);
        print_percentile(99.99);

        log << "\n" << t_test.compareAndReport(confidence).second << "\n";

        log.next();
    }

public:

    ~Benchmark() override
    {
        shutdown = true;
    }
};

}


int mainEntryClickHouseBenchmark(int argc, char ** argv)
{
    using namespace DB;
    bool print_stacktrace = false;

    try
    {
        using boost::program_options::value;

        boost::program_options::options_description options_description = createOptionsDescription("Allowed options", getTerminalWidth());
        options_description.add_options()
            ("help", "Print usage summary and exit; combine with --verbose to display all options")
            ("verbose", "Increase output verbosity")
            ("query,q",       value<std::string>()->default_value(""),          "query to execute")
            ("concurrency,c", value<unsigned>()->default_value(1),              "number of parallel queries")
            ("max_concurrency,C", value<unsigned>()->default_value(0),          "gradually increase number of parallel queries up to specified value, making one report for every concurrency level")
            ("delay,d",       value<double>()->default_value(1),                "delay between intermediate reports in seconds (set 0 to disable reports)")
            ("precise",                                                         "enable precise per-interval reporting with weighted metrics")
            ("stage",         value<std::string>()->default_value("complete"),  "request query processing up to specified stage: complete,fetch_columns,with_mergeable_state,with_mergeable_state_after_aggregation,with_mergeable_state_after_aggregation_and_limit")
            ("iterations,i",  value<size_t>()->default_value(0),                "amount of queries to be executed")
            ("timelimit,t",   value<double>()->default_value(0.),               "stop launch of queries after specified time limit")
            ("randomize,r",                                                     "randomize order of execution")
            ("host,h",        value<Strings>()->multitoken(),                   "list of hosts")
            ("port",          value<Ports>()->multitoken(),                     "list of ports")
            ("connection",    value<std::string>(),                             "connection name (define under connections_credentials in client config)")
            ("roundrobin",    "Instead of comparing queries for different --host/--port just pick one random --host/--port for every query and send query to it.")
            ("cumulative",    "prints cumulative data instead of data per interval")
            ("secure,s",      "Use TLS connection")
            ("accept-invalid-certificate", "Ignore certificate verification errors, equal to config parameters " "openSSL.client.invalidCertificateHandler.name=AcceptCertificateHandler and openSSL.client.verificationMode=none")
            ("user,u",        value<std::string>(), "")
            ("password",      value<std::string>(), "")
            ("quota_key",     value<std::string>(), "")
            ("database",      value<std::string>(), "")
            ("config",        value<std::string>(), "Path to client config")
            ("stacktrace", "print stack traces of exceptions")
            ("confidence", value<size_t>()->default_value(5), "set the level of confidence for T-test [0=80%, 1=90%, 2=95%, 3=98%, 4=99%, 5=99.5%(default)")
            ("query_id", value<std::string>()->default_value(""), "")
            ("query_id_prefix", value<std::string>()->default_value(""), "")
            ("max-consecutive-errors", value<size_t>()->default_value(0), "set number of allowed consecutive errors")
            ("ignore-error,continue_on_errors", "continue testing even if a query fails")
            ("reconnect", value<size_t>()->default_value(0), "control reconnection behaviour: 0 (never reconnect), 1 (reconnect for every query), or N (reconnect after every N queries)")
            ("client-side-time", "display the time including network communication instead of server-side time; note that for server versions before 22.8 we always display client-side time")
            ("proto_caps", value<std::string>(), "Enable/disable chunked protocol (comma-separated): chunked_optional, notchunked, notchunked_optional, send_chunked, send_chunked_optional, send_notchunked, send_notchunked_optional, recv_chunked, recv_chunked_optional, recv_notchunked, recv_notchunked_optional")
        ;

        Settings settings;
        auto options_description_non_verbose = options_description;
        settings.addToProgramOptions(options_description);

        auto parser = po::command_line_parser(argc, argv)
                          .options(options_description)
                          .extra_parser(OptionsAliasParser(options_description))
                          .allow_unregistered();
        po::parsed_options parsed = parser.run();

        /// Check unrecognized options.
        auto unrecognized_options = po::collect_unrecognized(parsed.options, po::collect_unrecognized_mode::exclude_positional);
        if (!unrecognized_options.empty())
        {
            throw Exception(ErrorCodes::UNRECOGNIZED_ARGUMENTS, "Unrecognized option '{}'", unrecognized_options[0]);
        }

        /// Check positional options.
        for (const auto & op : parsed.options)
        {
            /// Skip all options after empty `--`. These are processed separately into the Application configuration.
            if (op.string_key.empty() && op.original_tokens[0].starts_with("--"))
                break;

            if (!op.unregistered && op.string_key.empty() && !op.original_tokens[0].starts_with("--")
                && !op.original_tokens[0].empty() && !op.value.empty())
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Positional option '{}' is not supported.", op.original_tokens[0]);
            }
        }

        po::variables_map options;
        po::store(parsed, options);
        po::notify(options);

        clearPasswordFromCommandLine(argc, argv);

        if (options.contains("help"))
        {
            std::cout << "Usage: " << argv[0] << " [options] < queries.txt\n";
            if (options.contains("verbose"))
                std::cout << options_description << "\n";
            else
                std::cout << options_description_non_verbose << "\n";
            std::cout << "\nSee also: https://clickhouse.com/docs/operations/utilities/clickhouse-benchmark/\n";
            return 0;
        }

        print_stacktrace = options.contains("stacktrace");

        /// NOTE Maybe clickhouse-benchmark should also respect .xml configuration of clickhouse-client.

        String proto_send_chunked {"notchunked"};
        String proto_recv_chunked {"notchunked"};

        if (options.contains("proto_caps"))
        {
            std::string proto_caps_str = options["proto_caps"].as<std::string>();

            std::vector<std::string_view> proto_caps;
            splitInto<','>(proto_caps, proto_caps_str);

            for (auto cap_str : proto_caps)
            {
                std::string direction;

                if (cap_str.starts_with("send_"))
                {
                    direction = "send";
                    cap_str = cap_str.substr(std::string_view("send_").size());
                }
                else if (cap_str.starts_with("recv_"))
                {
                    direction = "recv";
                    cap_str = cap_str.substr(std::string_view("recv_").size());
                }

                if (cap_str != "chunked" && cap_str != "notchunked" && cap_str != "chunked_optional" && cap_str != "notchunked_optional")
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "proto_caps option is incorrect ({})", proto_caps_str);

                if (direction.empty())
                {
                    proto_send_chunked = cap_str;
                    proto_recv_chunked = cap_str;
                }
                else
                {
                    if (direction == "send")
                        proto_send_chunked = cap_str;
                    else
                        proto_recv_chunked = cap_str;
                }
            }
        }

        Benchmark benchmark(
            options,
            std::move(proto_send_chunked),
            std::move(proto_recv_chunked),
            print_stacktrace,
            std::move(settings));
        return benchmark.run();
    }
    catch (const po::error & e)
    {
        std::cerr << "Bad arguments: " << e.what() << std::endl;
        return ErrorCodes::BAD_ARGUMENTS;
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(print_stacktrace, true) << '\n';
        auto code = getCurrentExceptionCode();
        return static_cast<UInt8>(code) ? code : 1;
    }
}
