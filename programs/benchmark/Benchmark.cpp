#include <unistd.h>
#include <cmath>
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
#include <IO/SharedThreadPools.h>
#include <Common/scope_guard_safe.h>
#include <AggregateFunctions/ReservoirSampler.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Client/ClientBaseHelpers.h>
#include <base/defines.h>
#include <boost/program_options.hpp>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/Exception.h>
#include <Common/ErrnoException.h>
#include <Common/randomSeed.h>
#include <Common/clearPasswordFromCommandLine.h>
#include <Core/Settings.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadBufferFromMemory.h>
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
#include <Parsers/parseQuery.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/ProfileEvents.h>
#include <base/scope_guard.h>
#include <base/sleep.h>

/** A tool for evaluating ClickHouse performance.
  * By default (closed loop) the tool emulates a case with fixed amount of simultaneously executing queries:
  * every client sends the next query when the previous one returns.
  * With --arrival-rate (open loop) queries arrive at random times with exponentially distributed
  * interarrival times (a Poisson process), whatever the server does.
  */

namespace ProfileEvents
{
    extern const Event UserTimeMicroseconds;
    extern const Event SystemTimeMicroseconds;
}

namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_settings_after_format_in_insert;
    extern const SettingsBool implicit_select;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_query_size;
}

using Ports = std::vector<UInt16>;
static constexpr std::string_view DEFAULT_CLIENT_NAME = "benchmark";

enum class QueriesFormat
{
    TSV,
    Script,
};

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_BLOCK_SIGNAL;
    extern const int EMPTY_DATA_PASSED;
    extern const int LOGICAL_ERROR;
    extern const int UNRECOGNIZED_ARGUMENTS;
}

/// The CPU time of a query on the server from a `ProfileEvents` packet.
/// The rows with thread id 0 are the increments for all threads of the query on one host.
static UInt64 getCPUMicroseconds(const Block & block)
{
    const auto & thread_ids = typeid_cast<const ColumnUInt64 &>(*block.getByName("thread_id").column).getData();
    const auto & names = typeid_cast<const ColumnString &>(*block.getByName("name").column);
    const auto & values = typeid_cast<const ColumnInt64 &>(*block.getByName("value").column).getData();
    const std::string_view user_time_name = ProfileEvents::getName(ProfileEvents::UserTimeMicroseconds);
    const std::string_view system_time_name = ProfileEvents::getName(ProfileEvents::SystemTimeMicroseconds);

    UInt64 result = 0;
    for (size_t i = 0; i < block.rows(); ++i)
    {
        if (thread_ids[i] != 0 || values[i] < 0)
            continue;
        const auto name = names.getDataAt(i);
        if (name == user_time_name || name == system_time_name)
            result += values[i];
    }
    return result;
}

static QueriesFormat parseQueriesFormat(const std::string & value)
{
    if (value == "tsv")
        return QueriesFormat::TSV;
    if (value == "script")
        return QueriesFormat::Script;
    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "Unknown value for --queries-format: '{}'. Allowed values: 'tsv', 'script'.", value);
}

class Benchmark : public Poco::Util::Application
{
public:
    Benchmark(
        const boost::program_options::variables_map & options,
        String && proto_send_chunked,
        String && proto_recv_chunked,
        bool print_stacktrace_,
        Settings && settings_,
        std::vector<double> && arrival_rates_)
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
        arrival_rates(std::move(arrival_rates_)),
        seed(options.contains("seed") ? options["seed"].as<UInt64>() : randomSeed()),
        /// In open-loop mode there is one worker thread and one pooled connection per in-flight slot.
        concurrency(arrival_rates.empty() ? options["concurrency"].as<unsigned>() : options["max-in-flight"].as<unsigned>()),
        max_concurrency(std::max(concurrency, options["max_concurrency"].as<unsigned>())),
        threads(concurrency),
        delay(options["delay"].as<double>()),
        precise(options.contains("precise")),
        /// In open-loop mode the queue holds the arrivals that wait for a free in-flight slot, so it is not bounded.
        queue(arrival_rates.empty() ? max_concurrency : std::numeric_limits<size_t>::max()),
        randomize(options.contains("randomize")),
        per_query(options.contains("per-query")),
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
        queries_format(parseQueriesFormat(options["queries-format"].as<std::string>())),
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

            if (connection_arguments.hosts.has_value())
            {
                if (isCloudEndpoint(connection_arguments.hosts->front()))
                    connection_arguments.secure.emplace(true);
            }
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
    /// Empty in the closed-loop mode. Otherwise the arrival rates (queries per second) to run one after another.
    std::vector<double> arrival_rates;
    UInt64 seed;
    unsigned concurrency;
    unsigned max_concurrency;
    unsigned threads;
    double delay;
    bool precise;

    using Query = std::string;
    using Queries = std::vector<Query>;
    Queries queries;

    struct Task
    {
        size_t query_index = 0;
        /// Open-loop mode only: the connection chosen at arrival.
        std::optional<size_t> connection_index;
    };

    using Queue = ConcurrentBoundedQueue<Task>;
    Queue queue;

    using ConnectionPoolUniq = std::unique_ptr<ConnectionPool>;
    using ConnectionPoolUniqs = std::vector<ConnectionPoolUniq>;
    ConnectionPoolUniqs connections;

    bool randomize;
    bool per_query;
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
    QueriesFormat queries_format;
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
        double max_duration = 0;
        double read_rows = 0;
        double read_bytes = 0;
        double result_rows = 0;
        double result_bytes = 0;
        /// CPU time of the queries on the server, from their `UserTimeMicroseconds` and `SystemTimeMicroseconds` profile events.
        double cpu_seconds = 0;

        using Sampler = ReservoirSampler<double>;
        Sampler sampler;

        explicit Stats(size_t sample_count = 1 << 16)
            : sampler(sample_count)
        {}

        void addWeighted(size_t read_rows_inc, size_t read_bytes_inc, size_t result_rows_inc, size_t result_bytes_inc, double cpu_seconds_inc, double weight)
        {
            queries += weight;
            cpu_seconds += weight * cpu_seconds_inc;
            read_rows += weight * static_cast<double>(read_rows_inc);
            read_bytes += weight * static_cast<double>(read_bytes_inc);
            result_rows += weight * static_cast<double>(result_rows_inc);
            result_bytes += weight * static_cast<double>(result_bytes_inc);
        }

        void sample(double duration)
        {
            ++finished_queries;
            max_duration = std::max(max_duration, duration);
            sampler.insert(duration);
        }

        void add(double duration, size_t read_rows_inc, size_t read_bytes_inc, size_t result_rows_inc, size_t result_bytes_inc, double cpu_seconds_inc)
        {
            addWeighted(read_rows_inc, read_bytes_inc, result_rows_inc, result_bytes_inc, cpu_seconds_inc, 1.0);
            sample(duration);
        }
    };

    using MultiStats = std::vector<std::shared_ptr<Stats>>;

    /// Open-loop mode: arrivals and in-flight queries over a report interval or over the whole run.
    struct ArrivalStats
    {
        size_t arrivals = 0;
        /// Arrivals that found all in-flight slots busy and waited in the client-side queue.
        size_t queued = 0;
        /// Arrivals that were still waiting in the client-side queue when the run stopped.
        size_t not_sent = 0;
        size_t max_in_flight = 0;
        size_t max_queue_size = 0;
        /// Integral of the number of in-flight queries over time, to compute the mean.
        double in_flight_ns = 0;
        UInt64 last_change_ns = 0;
        /// The duration of the arrival process, if it stopped before the reported period ended.
        std::optional<double> arrival_seconds;
    };

    struct IntervalStats;
    friend struct IntervalStats;
    struct IntervalStats
    {
        Benchmark & benchmark;
        bool reported = false;

        UInt64 start_ns;
        UInt64 end_ns = std::numeric_limits<UInt64>::max();
        MultiStats stats;
        size_t threads = 0;
        ArrivalStats arrival_stats;

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
            benchmark.report(stats, static_cast<double>(end_ns - start_ns) / 1e9, threads, &arrival_stats);
        }
    };

    std::mutex interval_mutex;
    std::shared_ptr<IntervalStats> interval TSA_GUARDED_BY(interval_mutex);
    MultiStats total_stats; // requires mutex
    MultiStats per_query_stats; // requires mutex
    StudentTTest t_test;

    /// Open-loop mode state.
    double arrival_rate = 0;
    ArrivalStats total_arrival_stats TSA_GUARDED_BY(interval_mutex);
    /// Queries that are executing now.
    size_t in_flight TSA_GUARDED_BY(interval_mutex) = 0;
    /// Queries that arrived and did not finish yet: the in-flight ones and the ones waiting in the queue.
    size_t arrived_not_finished TSA_GUARDED_BY(interval_mutex) = 0;
    /// All arrivals are pushed to the queue, the workers stop when it is empty.
    std::atomic<bool> arrivals_finished{false};
    /// Arrivals taken from the queue after the run stopped.
    std::atomic<size_t> not_sent{0};
    /// The end of the run by --timelimit, 0 if there is no time limit.
    std::atomic<UInt64> deadline_ns{0};

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

            switch (queries_format)
            {
                case QueriesFormat::Script:
                {
                    String all_queries_text;
                    readStringUntilEOF(all_queries_text, in);

                    auto parse_res = splitMultipartQuery(
                        all_queries_text,
                        queries,
                        settings[Setting::max_query_size],
                        settings[Setting::max_parser_depth],
                        settings[Setting::max_parser_backtracks],
                        settings[Setting::allow_settings_after_format_in_insert],
                        settings[Setting::implicit_select]);

                    if (!parse_res.second)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Cannot parse query near: {}", String(parse_res.first, std::min<size_t>(100, all_queries_text.data() + all_queries_text.size() - parse_res.first)));
                    break;
                }
                case QueriesFormat::TSV:
                {
                    /// Default: one query per line, tab-escaped
                    while (!in.eof())
                    {
                        String query;
                        readText(query, in);
                        assertChar('\n', in);

                        if (!query.empty())
                            queries.emplace_back(std::move(query));
                    }
                    break;
                }
            }

            if (queries.empty())
                throw Exception(ErrorCodes::EMPTY_DATA_PASSED, "Empty list of queries.");
        }
        else
        {
            queries.emplace_back(query_to_execute);
        }

        std::lock_guard lock(mutex);
        resetPerQueryStats();
        log << "Loaded " << queries.size() << " queries.\n" << flush;
        if (!arrival_rates.empty())
            log << "Open-loop mode: max " << concurrency << " queries in flight, seed " << seed << ".\n" << flush;
    }

    void resetPerQueryStats() TSA_REQUIRES(mutex)
    {
        per_query_stats.clear();
        if (!per_query)
            return;
        per_query_stats.reserve(queries.size());
        for (size_t i = 0; i < queries.size(); ++i)
            per_query_stats.emplace_back(std::make_shared<Stats>(1 << 12));
    }

    /// Try push new query and check cancellation conditions
    bool tryPushQueryInteractively(const Task & task, InterruptListener & interrupt_listener)
    {
        bool inserted = false;

        while (!inserted)
        {
            inserted = queue.tryPush(task, 100);

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
        updateInFlightIntegral(now_ns);

        if (cumulative)
        {
            if (!first)
            {
                ArrivalStats arrival_stats_snapshot = total_arrival_stats;
                report(total_stats, total_watch.elapsedSeconds(), threads, &arrival_stats_snapshot);
            }
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
            next_interval->arrival_stats.last_change_ns = now_ns;
            next_interval->arrival_stats.max_in_flight = in_flight;
            next_interval->arrival_stats.max_queue_size = queueSize();
            if (interval)
                interval->next = next_interval;
            interval = next_interval;
        }
    }

    /// Accounts the time since the last change of the number of in-flight queries.
    void updateInFlightIntegral(UInt64 now_ns) TSA_REQUIRES(interval_mutex)
    {
        auto update = [&](ArrivalStats & stats) TSA_REQUIRES(interval_mutex)
        {
            if (now_ns > stats.last_change_ns)
            {
                stats.in_flight_ns += static_cast<double>(in_flight) * static_cast<double>(now_ns - stats.last_change_ns);
                stats.last_change_ns = now_ns;
            }
        };
        update(total_arrival_stats);
        if (interval)
            update(interval->arrival_stats);
    }

    /// The number of arrivals waiting for a free in-flight slot.
    size_t queueSize() const TSA_REQUIRES(interval_mutex)
    {
        return arrived_not_finished > concurrency ? arrived_not_finished - concurrency : 0;
    }

    void onArrival() TSA_REQUIRES(interval_mutex)
    {
        /// There is one worker per in-flight slot, so an arrival waits iff earlier arrivals occupy all slots.
        const bool slot_free = arrived_not_finished < concurrency;
        ++arrived_not_finished;
        auto update = [&](ArrivalStats & stats) TSA_REQUIRES(interval_mutex)
        {
            ++stats.arrivals;
            if (!slot_free)
                ++stats.queued;
            stats.max_queue_size = std::max(stats.max_queue_size, queueSize());
        };
        update(total_arrival_stats);
        if (interval)
            update(interval->arrival_stats);
    }

    void onQueryStart(UInt64 now_ns) TSA_REQUIRES(interval_mutex)
    {
        updateInFlightIntegral(now_ns);
        ++in_flight;
        total_arrival_stats.max_in_flight = std::max(total_arrival_stats.max_in_flight, in_flight);
        if (interval)
            interval->arrival_stats.max_in_flight = std::max(interval->arrival_stats.max_in_flight, in_flight);
    }

    void onQueryFinish(UInt64 now_ns) TSA_REQUIRES(interval_mutex)
    {
        updateInFlightIntegral(now_ns);
        --in_flight;
        --arrived_not_finished;
    }

    void runBenchmark()
    {
        if (arrival_rates.empty())
        {
            runClosedLoop();
            return;
        }

        InterruptListener interrupt_listener;
        for (size_t i = 0; i < arrival_rates.size(); ++i)
        {
            if (i > 0)
                resetStats();
            if (!runOpenLoop(arrival_rates[i], i + 1 == arrival_rates.size(), interrupt_listener))
                break;
        }
    }

    /// Prepares the next run of a rate sweep.
    void resetStats()
    {
        {
            std::lock_guard lock(interval_mutex);
            interval.reset();
            total_arrival_stats = {};
            in_flight = 0;
            arrived_not_finished = 0;
        }
        {
            std::lock_guard lock(mutex);
            for (auto & stats : total_stats)
                stats = std::make_shared<Stats>();
            resetPerQueryStats();
            t_test.clear();
        }
        queue.clear();
        queries_executed = 0;
        consecutive_errors = 0;
        not_sent = 0;
        arrivals_finished = false;
        shutdown = false;
    }

    /// Runs the open-loop benchmark at one arrival rate.
    /// Returns false if the whole benchmark should stop: on SIGINT.
    bool runOpenLoop(double rate, bool last_run, InterruptListener & interrupt_listener)
    {
        arrival_rate = rate;
        pcg64 generator(seed);
        std::exponential_distribution<double> interarrival_distribution(rate);
        std::uniform_int_distribution<size_t> query_distribution(0, queries.size() - 1);
        std::uniform_int_distribution<size_t> connection_distribution(0, connections.size() - 1);

        total_watch.restart();
        const UInt64 start_ns = total_watch.getStart();
        {
            std::lock_guard lock(interval_mutex);
            total_arrival_stats.last_change_ns = start_ns;
        }
        startNextInterval(true);
        deadline_ns = max_time > 0 ? start_ns + static_cast<UInt64>(max_time * 1e9) : 0;

        try
        {
            for (size_t i = 0; i < concurrency; ++i)
                pool.scheduleOrThrowOnError([this]() mutable { openLoopThread(); });
        }
        catch (...)
        {
            shutdown = true;
            pool.wait();
            throw;
        }

        bool interrupted = false;
        bool stopped = false;
        double arrival_time = 0;
        for (size_t i = 0; !max_iterations || i < max_iterations; ++i)
        {
            arrival_time += interarrival_distribution(generator);
            if (max_time > 0 && arrival_time >= max_time)
            {
                /// Wait until the time limit to report the right arrival rate.
                if (waitUntil(deadline_ns, interrupt_listener, interrupted))
                    std::cout << "Stopping launch of queries."
                              << " Requested time limit " << max_time << " seconds is exhausted.\n";
                stopped = true;
                break;
            }

            Task task;
            task.query_index = randomize ? query_distribution(generator) : i % queries.size();
            task.connection_index = connection_distribution(generator);
            const UInt64 arrival_ns = start_ns + static_cast<UInt64>(arrival_time * 1e9);

            if (!waitUntil(arrival_ns, interrupt_listener, interrupted))
            {
                stopped = true;
                break;
            }

            /// The arrival process may fall behind the schedule, but no query starts after the time limit.
            if (pastDeadline())
            {
                std::cout << "Stopping launch of queries."
                          << " Requested time limit " << max_time << " seconds is exhausted.\n";
                stopped = true;
                break;
            }

            {
                std::lock_guard lock(interval_mutex);
                onArrival();
            }
            if (!queue.push(task))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot push to the queue of arrivals");
        }

        /// The achieved arrival rate is computed over the actual duration of the arrival process,
        /// also in the reports made while the queries in flight finish.
        {
            std::lock_guard lock(interval_mutex);
            const UInt64 arrivals_end_ns = clock_gettime_ns();
            total_arrival_stats.arrival_seconds = static_cast<double>(arrivals_end_ns - start_ns) / 1e9;
            if (interval)
                interval->arrival_stats.arrival_seconds = static_cast<double>(arrivals_end_ns - interval->start_ns) / 1e9;
        }

        /// The queries in flight finish. The arrivals in the queue are executed only if all
        /// --iterations arrived, otherwise they are reported as not sent.
        if (stopped)
            shutdown = true;
        arrivals_finished = true;

        /// Now we don't block the Ctrl+C signal and second signal will terminate the program without waiting.
        if (interrupted || last_run)
            interrupt_listener.unblock();

        /// Wait for the workers, while handling SIGINT if it is still blocked (in a sweep) and reporting intervals.
        while (!pool.waitUntil(std::chrono::steady_clock::now() + std::chrono::milliseconds(100)))
        {
            if (interrupt_listener.check())
            {
                std::cout << "Stopping the run. SIGINT received.\n";
                interrupted = true;
                shutdown = true;
                interrupt_listener.unblock();
            }

            if (delay > 0 && delay_watch.elapsedSeconds() > delay)
                startNextInterval();
        }

        pool.wait();
        total_watch.stop();

        ArrivalStats arrival_stats_snapshot;
        {
            std::lock_guard lock(interval_mutex);
            updateInFlightIntegral(total_watch.getEnd());
            total_arrival_stats.not_sent = queue.size() + not_sent;
            arrival_stats_snapshot = total_arrival_stats;
        }
        report(total_stats, total_watch.elapsedSeconds(), 0, &arrival_stats_snapshot);
        reportPerQuery(total_watch.elapsedSeconds());
        return !interrupted;
    }

    /// Sleeps until the scheduled arrival time, reporting intervals meanwhile.
    /// Returns false if the run should stop: on SIGINT or when a worker failed.
    bool waitUntil(UInt64 target_ns, InterruptListener & interrupt_listener, bool & interrupted)
    {
        while (true)
        {
            if (shutdown)
                return false;

            if (interrupt_listener.check())
            {
                std::cout << "Stopping launch of queries. SIGINT received.\n";
                interrupted = true;
                return false;
            }

            if (delay > 0 && delay_watch.elapsedSeconds() > delay)
                startNextInterval();

            UInt64 now_ns = clock_gettime_ns();
            if (now_ns >= target_ns)
                return true;

            /// Wake up at least every 100 ms to check for SIGINT and to report intervals.
            sleepForNanoseconds(std::min<UInt64>(target_ns - now_ns, 100'000'000));
        }
    }

    bool pastDeadline() const
    {
        const UInt64 deadline = deadline_ns;
        return deadline && clock_gettime_ns() >= deadline;
    }

    void openLoopThread()
    {
        blockSigint();

        Task task;
        while (true)
        {
            if (shutdown)
                return;

            /// Read before popping: if all arrivals were pushed and the queue is empty, no more tasks will come.
            bool finished = arrivals_finished;
            if (!queue.tryPop(task, 100))
            {
                if (finished)
                    return;
                continue;
            }

            if (shutdown || pastDeadline())
            {
                ++not_sent;
                return;
            }

            {
                std::lock_guard lock(interval_mutex);
                onQueryStart(clock_gettime_ns());
            }
            SCOPE_EXIT({
                std::lock_guard lock(interval_mutex);
                onQueryFinish(clock_gettime_ns());
            });

            executeAndHandleErrors(task, *task.connection_index);
        }
    }

    void runClosedLoop()
    {
        pcg64 generator(seed);
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

            Task task;
            task.query_index = query_index;
            if (!tryPushQueryInteractively(task, interrupt_listener))
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
        reportPerQuery(total_watch.elapsedSeconds());
    }


    /// In the worker threads we do not accept INT signal.
    static void blockSigint()
    {
        sigset_t sig_set;
        if (sigemptyset(&sig_set)
            || sigaddset(&sig_set, SIGINT)
            || pthread_sigmask(SIG_BLOCK, &sig_set, nullptr))
        {
            throw ErrnoException(ErrorCodes::CANNOT_BLOCK_SIGNAL, "Cannot block signal");
        }
    }

    void thread()
    {
        Task task;

        /// Randomly choosing connection index
        pcg64 generator(randomSeed());
        std::uniform_int_distribution<size_t> distribution(0, connections.size() - 1);

        blockSigint();

        while (true)
        {
            bool extracted = false;

            while (!extracted)
            {
                extracted = queue.tryPop(task, 100);

                if (shutdown || (max_iterations && queries_executed == max_iterations))
                    return;
            }

            executeAndHandleErrors(task, distribution(generator));
        }
    }

    void executeAndHandleErrors(const Task & task, size_t connection_index)
    {
        try
        {
            execute(task, connection_index);
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
                if (per_query)
                    ++per_query_stats[task.query_index]->errors;
            }

            std::lock_guard lock(interval_mutex); // Locking order: first intervals_mutex, then mutex
            if (interval)
                ++interval->stats[info_index]->errors;
        }
        // Count failed queries toward executed, so that we'd reach
        // max_iterations even if every run fails.
        ++queries_executed;
    }

    void execute(const Task & task, size_t connection_index)
    {
        const Query & query = queries[task.query_index];
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

        UInt64 cpu_microseconds = 0;
        executor.setProfileEventsCallback([&cpu_microseconds](const Block & block) { cpu_microseconds += getCPUMicroseconds(block); });

        executor.sendQuery(ClientInfo::QueryKind::INITIAL_QUERY);

        ProfileInfo info;
        for (Block block = executor.readBlock(); !block.empty(); block = executor.readBlock())
            info.update(block);

        executor.finish();

        watch.stop();
        double duration = (display_client_side_time || progress.elapsed_ns == 0)
            ? watch.elapsedSeconds()
            : static_cast<double>(progress.elapsed_ns) / 1e9;
        const double cpu_seconds = static_cast<double>(cpu_microseconds) / 1e6;
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
                const double overlap_ns = static_cast<double>(std::min(cur_interval->end_ns, watch.getEnd()) - std::max(cur_interval->start_ns, watch.getStart()));
                const double weight = overlap_ns / static_cast<double>(duration_ns);
                if (overlap_ns > 0 && duration_ns > 0)
                    cur_interval->stats[info_index]->addWeighted(progress.read_rows, progress.read_bytes, info.rows, info.bytes, cpu_seconds, weight);
            }

            // Latency goes to the last interval only (our sampler does not support weights)
            interval->stats[info_index]->sample(duration);
        }
        else if (!cumulative)
        {
            std::lock_guard lock(interval_mutex);
            interval->stats[info_index]->add(duration, progress.read_rows, progress.read_bytes, info.rows, info.bytes, cpu_seconds);
        }

        std::lock_guard lock(mutex);
        total_stats[info_index]->add(duration, progress.read_rows, progress.read_bytes, info.rows, info.bytes, cpu_seconds);
        if (per_query)
            per_query_stats[task.query_index]->add(duration, progress.read_rows, progress.read_bytes, info.rows, info.bytes, cpu_seconds);
        t_test.add(info_index, duration);
    }

    void report(const MultiStats & infos, double seconds, size_t used_threads = 0, const ArrivalStats * arrival_stats = nullptr)
    {
        std::lock_guard lock(mutex);

        size_t executed = queries_executed.load();
        log << "\nQueries executed: " << executed;
        if (max_iterations > 1)
            log << " (" << (static_cast<double>(executed) * 100.0 / static_cast<double>(max_iterations)) << "%)";
        else if (queries.size() > 1)
            log << " (" << (static_cast<double>(executed) * 100.0 / static_cast<double>(queries.size())) << "%)";
        log << ".\n" << flush;

        if (used_threads > 0 && concurrency < max_concurrency)
        {
            log << "Concurrency: " << used_threads;
            log << " of " << max_concurrency;
            log << " parallel queries.\n" << flush;
        }

        if (!arrival_rates.empty() && arrival_stats)
        {
            const double arrival_seconds = arrival_stats->arrival_seconds.value_or(seconds);
            log << "Arrivals: " << arrival_stats->arrivals
                << ", target rate: " << fmt::format("{:.3f}", arrival_rate) << "/s"
                << ", achieved rate: " << fmt::format("{:.3f}", arrival_seconds > 0 ? static_cast<double>(arrival_stats->arrivals) / arrival_seconds : 0.0) << "/s"
                << ", queued: " << arrival_stats->queued
                << " (max queue size " << arrival_stats->max_queue_size << ")";
            if (arrival_stats->not_sent)
                log << ", not sent: " << arrival_stats->not_sent;
            log << ".\n"
                << "In flight: mean " << fmt::format("{:.3f}", seconds > 0 ? arrival_stats->in_flight_ns / (seconds * 1e9) : 0.0)
                << ", max " << arrival_stats->max_in_flight << " of " << concurrency << ".\n" << flush;
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
                << "result MiB/s: " << fmt::format("{:.3f}", info->result_bytes / seconds / 1048576);
            /// With open-loop arrivals QPS follows the arrival rate, so the CPU usage shows the cost of the load.
            if (!arrival_rates.empty())
                log << ", CPU: " << fmt::format("{:.3f}", info->cpu_seconds / seconds) << " cores"
                    << ", CPU per query: " << fmt::format("{:.3f}", info->cpu_seconds / info->queries) << " sec";
            log << ".\n";
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

    void reportPerQuery(double seconds)
    {
        std::lock_guard lock(mutex);
        if (!per_query)
            return;

        log << "Per-query statistics:\n";
        log << "#\tqueries\terrors\tQPS\t50%\t90%\t99%\tmax\tCPU\tquery\n";
        for (size_t i = 0; i < queries.size(); ++i)
        {
            auto & stats = *per_query_stats[i];
            log << i << "\t" << stats.finished_queries << "\t" << stats.errors
                << "\t" << fmt::format("{:.3f}", stats.queries / seconds);
            for (double level : {0.5, 0.9, 0.99})
                log << "\t" << (stats.finished_queries ? fmt::format("{:.3f}", stats.sampler.quantileNearest(level)) : "-");
            log << "\t" << (stats.finished_queries ? fmt::format("{:.3f}", stats.max_duration) : "-");
            log << "\t" << (stats.finished_queries ? fmt::format("{:.3f}", stats.cpu_seconds / stats.queries) : "-");
            String query_text = queries[i].substr(0, 80);
            std::replace_if(query_text.begin(), query_text.end(), [](char c) { return c == '\n' || c == '\t'; }, ' ');
            log << "\t" << query_text << (queries[i].size() > 80 ? "..." : "") << "\n";
        }
        log << "\n";
        log.next();
    }

public:

    ~Benchmark() override
    {
        shutdown = true;
    }
};

}


int mainEntryClickHouseBenchmark(int argc, char ** argv);
int mainEntryClickHouseBenchmark(int argc, char ** argv)
{
    using namespace DB;
    bool print_stacktrace = false;

    /// Join global-pool threads before the statics they may have accessed are destroyed.
    /// That way, accesses happen-before destruction.
    SCOPE_EXIT_SAFE({
        DB::StaticThreadPool::shutdownAll();
        GlobalThreadPool::shutdown();
    });

    try
    {
        using boost::program_options::value;

        boost::program_options::options_description options_description = createOptionsDescription("Allowed options", getTerminalWidth());
        options_description.add_options()
            ("help", "Print usage summary and exit; combine with --verbose to display all options")
            ("verbose", "Increase output verbosity")
            ("query,q",       value<std::string>()->default_value(""),          "query to execute")
            ("queries-format", value<std::string>()->default_value("tsv"),      "format of queries read from standard input: 'tsv' (default, one tab-escaped query per line) or 'script' (parse the input as a script of multiple queries separated by semicolons)")
            ("concurrency,c", value<unsigned>()->default_value(1),              "number of parallel queries")
            ("max_concurrency,C", value<unsigned>()->default_value(0),          "gradually increase number of parallel queries up to specified value, making one report for every concurrency level")
            ("delay,d",       value<double>()->default_value(1),                "delay between intermediate reports in seconds (set 0 to disable reports)")
            ("precise",                                                         "enable precise per-interval reporting with weighted metrics")
            ("stage",         value<std::string>()->default_value("complete"),  "request query processing up to specified stage: complete,fetch_columns,with_mergeable_state,with_mergeable_state_after_aggregation,with_mergeable_state_after_aggregation_and_limit")
            ("iterations,i",  value<size_t>()->default_value(0),                "amount of queries to be executed")
            ("timelimit,t",   value<double>()->default_value(0.),               "stop launch of queries after specified time limit")
            ("randomize,r",                                                     "randomize order of execution")
            ("arrival-rate",  value<std::string>(),                             "open-loop mode: queries arrive at random times at this average rate (queries per second), with exponentially distributed interarrival times, whatever the server does; a comma-separated list of rates makes one run and report per rate")
            ("max-in-flight", value<unsigned>()->default_value(256),            "open-loop mode: max number of queries executing at the same time; the arrivals that find all slots busy wait in a client-side queue")
            ("seed",          value<UInt64>(),                                  "seed of the random generator for the arrival times, the order of queries with --randomize and the choice of host in the open-loop mode; random by default")
            ("per-query",                                                       "print statistics for every query at the end")
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
            ("reconnect", value<size_t>()->default_value(0)->implicit_value(1), "control reconnection behaviour: 0 (never reconnect), 1 (reconnect for every query), or N (reconnect after every N queries); when given without a value, behaves as 1")
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
            std::cout << "Usage: clickhouse benchmark [options] < queries.txt\n";
            std::cout << "Usage: clickhouse benchmark [options] --query \"query text\"\n\n";
            std::cout << "clickhouse-benchmark connects to ClickHouse server, repeatedly sends "
                         "specified queries and reports query statistics. "
                         "Multiple queries can be used if passed in TSV format.\n\n";
            if (options.contains("verbose"))
                std::cout << options_description << "\n";
            else
                std::cout << options_description_non_verbose << "\n";
            std::cout << "\nSee also: https://clickhouse.com/docs/operations/utilities/clickhouse-benchmark/\n";
            return 0;
        }

        print_stacktrace = options.contains("stacktrace");

        std::vector<double> arrival_rates;
        if (options.contains("arrival-rate"))
        {
            std::string arrival_rate_str = options["arrival-rate"].as<std::string>();
            std::vector<std::string_view> rates;
            splitInto<','>(rates, arrival_rate_str);
            for (auto rate_str : rates)
            {
                double rate = 0;
                ReadBufferFromMemory buf(rate_str);
                if (!tryReadText(rate, buf) || !buf.eof() || !std::isfinite(rate) || rate <= 0)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "--arrival-rate must be a positive number or a comma-separated list of them, got '{}'", arrival_rate_str);
                arrival_rates.push_back(rate);
            }

            if (!options["concurrency"].defaulted() || options["max_concurrency"].as<unsigned>() != 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "--concurrency and --max_concurrency cannot be used with --arrival-rate, use --max-in-flight to limit the number of queries in flight");
            if (options["max-in-flight"].as<unsigned>() == 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "--max-in-flight must be positive");
            if (arrival_rates.size() > 1 && options["iterations"].as<size_t>() == 0 && options["timelimit"].as<double>() <= 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "A list of arrival rates requires --iterations or --timelimit to end the run for every rate");
        }
        else if (!options["max-in-flight"].defaulted())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "--max-in-flight requires --arrival-rate");
        }

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
            std::move(settings),
            std::move(arrival_rates));
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
