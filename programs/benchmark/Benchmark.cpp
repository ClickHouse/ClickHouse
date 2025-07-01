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
}

class Benchmark : public Poco::Util::Application
{
public:
    Benchmark(unsigned concurrency_,
            double delay_,
            Strings && hosts_,
            Ports && ports_,
            bool round_robin_,
            bool cumulative_,
            bool secure_,
            const String & default_database_,
            const String & user_,
            const String & password_,
            const String & proto_send_chunked_,
            const String & proto_recv_chunked_,
            const String & quota_key_,
            const String & stage,
            bool randomize_,
            size_t max_iterations_,
            double max_time_,
            size_t confidence_,
            const String & query_id_,
            const String & query_id_prefix_,
            const String & query_to_execute_,
            size_t max_consecutive_errors_,
            bool continue_on_errors_,
            size_t reconnect_,
            bool display_client_side_time_,
            bool print_stacktrace_,
            const Settings & settings_)
        :
        round_robin(round_robin_),
        concurrency(concurrency_),
        delay(delay_),
        queue(concurrency),
        randomize(randomize_),
        cumulative(cumulative_),
        max_iterations(max_iterations_),
        max_time(max_time_),
        confidence(confidence_),
        query_id(query_id_),
        query_id_prefix(query_id_prefix_),
        query_to_execute(query_to_execute_),
        continue_on_errors(continue_on_errors_),
        max_consecutive_errors(max_consecutive_errors_),
        reconnect(reconnect_),
        display_client_side_time(display_client_side_time_),
        print_stacktrace(print_stacktrace_),
        settings(settings_),
        shared_context(Context::createShared()),
        global_context(Context::createGlobal(shared_context.get())),
        pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, concurrency)
    {
        const auto secure = secure_ ? Protocol::Secure::Enable : Protocol::Secure::Disable;
        size_t connections_cnt = std::max(ports_.size(), hosts_.size());

        connections.reserve(connections_cnt);
        comparison_info_total.reserve(round_robin ? 1 : connections_cnt);
        comparison_info_per_interval.reserve(round_robin ? 1 : connections_cnt);

        for (size_t i = 0; i < connections_cnt; ++i)
        {
            UInt16 cur_port = i >= ports_.size() ? 9000 : ports_[i];
            std::string cur_host = i >= hosts_.size() ? "localhost" : hosts_[i];

            connections.emplace_back(std::make_unique<ConnectionPool>(
                concurrency,
                cur_host, cur_port,
                default_database_, user_, password_,
                proto_send_chunked_, proto_recv_chunked_,
                quota_key_,
                /* cluster_= */ "",
                /* cluster_secret_= */ "",
                /* client_name_= */ std::string(DEFAULT_CLIENT_NAME),
                Protocol::Compression::Enable,
                secure,
                /* bind_host_= */ ""));

            if (!round_robin || comparison_info_per_interval.empty())
            {
                comparison_info_per_interval.emplace_back(std::make_shared<Stats>());
                comparison_info_total.emplace_back(std::make_shared<Stats>());
            }
        }

        // Initialize queries_per_connection to track queries for each connection
        queries_per_connection.resize(connections.size(), 0);

        global_context->makeGlobalContext();
        global_context->setSettings(settings);
        global_context->setClientName(std::string(DEFAULT_CLIENT_NAME));
        global_context->setQueryKindInitial();

        /// This is needed to receive blocks with columns of AggregateFunction data type
        /// (example: when using stage = 'with_mergeable_state')
        registerAggregateFunctions();

        query_processing_stage = QueryProcessingStage::fromString(stage);
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
    }

    int main(const std::vector<std::string> &) override
    {
        readQueries();
        runBenchmark();
        return 0;
    }

private:
    using Entry = ConnectionPool::Entry;
    using EntryPtr = std::shared_ptr<Entry>;
    using EntryPtrs = std::vector<EntryPtr>;

    bool round_robin;
    unsigned concurrency;
    double delay;

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
        std::atomic<size_t> queries{0};
        size_t errors = 0;
        size_t read_rows = 0;
        size_t read_bytes = 0;
        size_t result_rows = 0;
        size_t result_bytes = 0;

        using Sampler = ReservoirSampler<double>;
        Sampler sampler {1 << 16};

        void add(double duration, size_t read_rows_inc, size_t read_bytes_inc, size_t result_rows_inc, size_t result_bytes_inc)
        {
            ++queries;
            read_rows += read_rows_inc;
            read_bytes += read_bytes_inc;
            result_rows += result_rows_inc;
            result_bytes += result_bytes_inc;
            sampler.insert(duration);
        }

        void clear()
        {
            queries = 0;
            read_rows = 0;
            read_bytes = 0;
            result_rows = 0;
            result_bytes = 0;
            sampler.clear();
        }
    };

    using MultiStats = std::vector<std::shared_ptr<Stats>>;
    MultiStats comparison_info_per_interval;
    MultiStats comparison_info_total;
    StudentTTest t_test;

    Stopwatch total_watch;
    Stopwatch delay_watch;

    ThreadPool pool;

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


    void printNumberOfQueriesExecuted(size_t num)
    {
        std::lock_guard lock(mutex);

        log << "\nQueries executed: " << num;
        if (max_iterations > 1)
            log << " (" << (num * 100.0 / max_iterations) << "%)";
        else if (queries.size() > 1)
            log << " (" << (num * 100.0 / queries.size()) << "%)";
        log << ".\n" << flush;
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

        double seconds = delay_watch.elapsedSeconds();
        if (delay > 0 && seconds > delay)
        {
            printNumberOfQueriesExecuted(queries_executed);
            cumulative
                ? report(comparison_info_total, total_watch.elapsedSeconds())
                : report(comparison_info_per_interval, seconds);
            delay_watch.restart();
        }

        return true;
    }

    void runBenchmark()
    {
        pcg64 generator(randomSeed());
        std::uniform_int_distribution<size_t> distribution(0, queries.size() - 1);

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
        delay_watch.restart();

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

        printNumberOfQueriesExecuted(queries_executed);
        report(comparison_info_total, total_watch.elapsedSeconds());
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
                std::lock_guard lock(mutex);
                log << "An error occurred while processing the query " << "'" << query << "'"
                          << ": " << getCurrentExceptionMessage(false) << '\n';
                if (!(continue_on_errors || max_consecutive_errors > ++consecutive_errors))
                {
                    shutdown = true;
                    throw;
                }

                log << getCurrentExceptionMessage(print_stacktrace,
                    true /*check embedded stack trace*/) << '\n' << flush;

                size_t info_index = round_robin ? 0 : connection_index;
                ++comparison_info_per_interval[info_index]->errors;
                ++comparison_info_total[info_index]->errors;
            }
            // Count failed queries toward executed, so that we'd reach
            // max_iterations even if every run fails.
            ++queries_executed;
        }
    }

    void execute(const Query & query, size_t connection_index)
    {
        Stopwatch watch;

        ConnectionPool::Entry entry = connections[connection_index]->get(ConnectionTimeouts::getTCPTimeoutsWithoutFailover(settings));

        bool should_reconnect = false;
        {
            std::lock_guard lock(queries_per_connection_mutex);
            should_reconnect = reconnect > 0 && (++queries_per_connection[connection_index] % reconnect == 0);
        }

        if (should_reconnect)
            entry->disconnect();

        RemoteQueryExecutor executor(*entry, query, {}, global_context, nullptr, Scalars(), Tables(), query_processing_stage);

        if (!query_id.empty())
            executor.setQueryId(query_id);
        else if (!query_id_prefix.empty())
            executor.setQueryId(query_id_prefix + "_" + Poco::UUIDGenerator().createRandom().toString());

        Progress progress;
        executor.setProgressCallback([&progress](const Progress & value) { progress.incrementPiecewiseAtomically(value); });

        executor.sendQuery(ClientInfo::QueryKind::INITIAL_QUERY);

        ProfileInfo info;
        while (Block block = executor.readBlock())
            info.update(block);

        executor.finish();

        double duration = (display_client_side_time || progress.elapsed_ns == 0)
            ? watch.elapsedSeconds()
            : progress.elapsed_ns / 1e9;

        std::lock_guard lock(mutex);

        size_t info_index = round_robin ? 0 : connection_index;
        comparison_info_per_interval[info_index]->add(duration, progress.read_rows, progress.read_bytes, info.rows, info.bytes);
        comparison_info_total[info_index]->add(duration, progress.read_rows, progress.read_bytes, info.rows, info.bytes);
        t_test.add(info_index, duration);
    }

    void report(const MultiStats & infos, double seconds)
    {
        std::lock_guard lock(mutex);

        log << "\n";
        for (size_t i = 0; i < infos.size(); ++i)
        {
            const auto & info = infos[i];

            /// Avoid zeros, nans or exceptions
            if (0 == info->queries)
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
                << "queries: " << info->queries.load() << ", ";
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

        if (!cumulative)
        {
            for (const auto & info : infos)
                info->clear();
        }

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
    bool print_stacktrace = true;

    try
    {
        using boost::program_options::value;

        /// Note: according to the standard, subsequent calls to getenv can mangle previous result.
        /// So we copy the results to std::string.
        std::optional<std::string> env_user_str;
        std::optional<std::string> env_password_str;
        std::optional<std::string> env_quota_key_str;

        const char * env_user = getenv("CLICKHOUSE_USER"); // NOLINT(concurrency-mt-unsafe)
        if (env_user != nullptr)
            env_user_str.emplace(std::string(env_user));

        const char * env_password = getenv("CLICKHOUSE_PASSWORD"); // NOLINT(concurrency-mt-unsafe)
        if (env_password != nullptr)
            env_password_str.emplace(std::string(env_password));

        const char * env_quota_key = getenv("CLICKHOUSE_QUOTA_KEY"); // NOLINT(concurrency-mt-unsafe)
        if (env_quota_key != nullptr)
            env_quota_key_str.emplace(std::string(env_quota_key));

        boost::program_options::options_description options_description = createOptionsDescription("Allowed options", getTerminalWidth());
        options_description.add_options()
            ("help", "Print usage summary and exit; combine with --verbose to display all options")
            ("verbose", "Increase output verbosity")
            ("query,q",       value<std::string>()->default_value(""),          "query to execute")
            ("concurrency,c", value<unsigned>()->default_value(1),              "number of parallel queries")
            ("delay,d",       value<double>()->default_value(1),                "delay between intermediate reports in seconds (set 0 to disable reports)")
            ("stage",         value<std::string>()->default_value("complete"),  "request query processing up to specified stage: complete,fetch_columns,with_mergeable_state,with_mergeable_state_after_aggregation,with_mergeable_state_after_aggregation_and_limit")
            ("iterations,i",  value<size_t>()->default_value(0),                "amount of queries to be executed")
            ("timelimit,t",   value<double>()->default_value(0.),               "stop launch of queries after specified time limit")
            ("randomize,r",                                                     "randomize order of execution")
            ("host,h",        value<Strings>()->multitoken(),                   "list of hosts")
            ("port",          value<Ports>()->multitoken(),                     "list of ports")
            ("roundrobin",    "Instead of comparing queries for different --host/--port just pick one random --host/--port for every query and send query to it.")
            ("cumulative",    "prints cumulative data instead of data per interval")
            ("secure,s",      "Use TLS connection")
            ("user,u",        value<std::string>()->default_value(env_user_str.value_or("default")), "")
            ("password",      value<std::string>()->default_value(env_password_str.value_or("")), "")
            ("quota_key",     value<std::string>()->default_value(env_quota_key_str.value_or("")), "")
            ("database", value<std::string>()->default_value("default"), "")
            ("stacktrace", "print stack traces of exceptions")
            ("confidence", value<size_t>()->default_value(5), "set the level of confidence for T-test [0=80%, 1=90%, 2=95%, 3=98%, 4=99%, 5=99.5%(default)")
            ("query_id", value<std::string>()->default_value(""), "")
            ("query_id_prefix", value<std::string>()->default_value(""), "")
            ("max-consecutive-errors", value<size_t>()->default_value(0), "set number of allowed consecutive errors")
            ("ignore-error,continue_on_errors", "continue testing even if a query fails")
            ("reconnect", value<size_t>()->default_value(1), "control reconnection behaviour: 0 (never reconnect), 1 (reconnect for every query), or N (reconnect after every N queries)")
            ("client-side-time", "display the time including network communication instead of server-side time; note that for server versions before 22.8 we always display client-side time")
        ;

        Settings settings;
        auto options_description_non_verbose = options_description;
        settings.addToProgramOptions(options_description);

        boost::program_options::variables_map options;
        boost::program_options::store(boost::program_options::parse_command_line(argc, argv, options_description), options);
        boost::program_options::notify(options);

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

        UInt16 default_port = options.contains("secure") ? DBMS_DEFAULT_SECURE_PORT : DBMS_DEFAULT_PORT;

        Ports ports = options.contains("port")
            ? options["port"].as<Ports>()
            : Ports({default_port});

        Strings hosts = options.contains("host") ? options["host"].as<Strings>() : Strings({"localhost"});

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
            options["concurrency"].as<unsigned>(),
            options["delay"].as<double>(),
            std::move(hosts),
            std::move(ports),
            options.contains("roundrobin"),
            options.contains("cumulative"),
            options.contains("secure"),
            options["database"].as<std::string>(),
            options["user"].as<std::string>(),
            options["password"].as<std::string>(),
            proto_send_chunked,
            proto_recv_chunked,
            options["quota_key"].as<std::string>(),
            options["stage"].as<std::string>(),
            options.contains("randomize"),
            options["iterations"].as<size_t>(),
            options["timelimit"].as<double>(),
            options["confidence"].as<size_t>(),
            options["query_id"].as<std::string>(),
            options["query_id_prefix"].as<std::string>(),
            options["query"].as<std::string>(),
            options["max-consecutive-errors"].as<size_t>(),
            options.contains("ignore-error"),
            options["reconnect"].as<size_t>(),
            options.contains("client-side-time"),
            print_stacktrace,
            settings);
        return benchmark.run();
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(print_stacktrace, true) << '\n';
        return getCurrentExceptionCode();
    }
}
