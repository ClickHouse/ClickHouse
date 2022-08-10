#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <signal.h>
#include <time.h>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <random>
#include <pcg_random.hpp>
#include <Poco/Util/Application.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>
#include <AggregateFunctions/ReservoirSampler.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <boost/program_options.hpp>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/Exception.h>
#include <Common/randomSeed.h>
#include <Common/clearPasswordFromCommandLine.h>
#include <Core/Types.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/ConnectionTimeoutsContext.h>
#include <IO/UseSSL.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Interpreters/Context.h>
#include <Client/Connection.h>
#include <Common/InterruptListener.h>
#include <Common/Config/configReadClient.h>
#include <Common/TerminalSize.h>
#include <Common/StudentTTest.h>
#include <filesystem>


namespace fs = std::filesystem;

/** A tool for evaluating ClickHouse performance.
  * The tool emulates a case with fixed amount of simultaneously executing queries.
  */

namespace DB
{

using Ports = std::vector<UInt16>;

namespace ErrorCodes
{
    extern const int CANNOT_BLOCK_SIGNAL;
    extern const int EMPTY_DATA_PASSED;
}

class Benchmark : public Poco::Util::Application
{
public:
    Benchmark(unsigned concurrency_, double delay_,
            Strings && hosts_, Ports && ports_, bool round_robin_,
            bool cumulative_, bool secure_, const String & default_database_,
            const String & user_, const String & password_, const String & stage,
            bool randomize_, size_t max_iterations_, double max_time_,
            const String & json_path_, size_t confidence_,
            const String & query_id_, const String & query_to_execute_, bool continue_on_errors_,
            bool reconnect_, bool print_stacktrace_, const Settings & settings_)
        :
        round_robin(round_robin_), concurrency(concurrency_), delay(delay_), queue(concurrency), randomize(randomize_),
        cumulative(cumulative_), max_iterations(max_iterations_), max_time(max_time_),
        json_path(json_path_), confidence(confidence_), query_id(query_id_),
        query_to_execute(query_to_execute_), continue_on_errors(continue_on_errors_), reconnect(reconnect_),
        print_stacktrace(print_stacktrace_), settings(settings_),
        shared_context(Context::createShared()), global_context(Context::createGlobal(shared_context.get())),
        pool(concurrency)
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
                /* cluster_= */ "",
                /* cluster_secret_= */ "",
                /* client_name_= */ "benchmark",
                Protocol::Compression::Enable,
                secure));

            if (!round_robin || comparison_info_per_interval.empty())
            {
                comparison_info_per_interval.emplace_back(std::make_shared<Stats>());
                comparison_info_total.emplace_back(std::make_shared<Stats>());
            }
        }

        global_context->makeGlobalContext();
        global_context->setSettings(settings);

        std::cerr << std::fixed << std::setprecision(3);

        /// This is needed to receive blocks with columns of AggregateFunction data type
        /// (example: when using stage = 'with_mergeable_state')
        registerAggregateFunctions();

        query_processing_stage = QueryProcessingStage::fromString(stage);
    }

    void initialize(Poco::Util::Application & self [[maybe_unused]]) override
    {
        std::string home_path;
        const char * home_path_cstr = getenv("HOME");
        if (home_path_cstr)
            home_path = home_path_cstr;

        configReadClient(config(), home_path);
    }

    int main(const std::vector<std::string> &) override
    {
        if (!json_path.empty() && fs::exists(json_path)) /// Clear file with previous results
            fs::remove(json_path);

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
    String json_path;
    size_t confidence;
    String query_id;
    String query_to_execute;
    bool continue_on_errors;
    bool reconnect;
    bool print_stacktrace;
    const Settings & settings;
    SharedContextHolder shared_context;
    ContextMutablePtr global_context;
    QueryProcessingStage::Enum query_processing_stage;

    /// Don't execute new queries after timelimit or SIGINT or exception
    std::atomic<bool> shutdown{false};

    std::atomic<size_t> queries_executed{0};

    struct Stats
    {
        std::atomic<size_t> queries{0};
        size_t errors = 0;
        size_t read_rows = 0;
        size_t read_bytes = 0;
        size_t result_rows = 0;
        size_t result_bytes = 0;
        double work_time = 0;

        using Sampler = ReservoirSampler<double>;
        Sampler sampler {1 << 16};

        void add(double seconds, size_t read_rows_inc, size_t read_bytes_inc, size_t result_rows_inc, size_t result_bytes_inc)
        {
            ++queries;
            work_time += seconds;
            read_rows += read_rows_inc;
            read_bytes += read_bytes_inc;
            result_rows += result_rows_inc;
            result_bytes += result_bytes_inc;
            sampler.insert(seconds);
        }

        void clear()
        {
            queries = 0;
            work_time = 0;
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

    std::mutex mutex;

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
                throw Exception("Empty list of queries.", ErrorCodes::EMPTY_DATA_PASSED);
        }
        else
        {
            queries.emplace_back(query_to_execute);
        }


        std::cerr << "Loaded " << queries.size() << " queries.\n";
    }


    void printNumberOfQueriesExecuted(size_t num)
    {
        std::cerr << "\nQueries executed: " << num;
        if (queries.size() > 1)
            std::cerr << " (" << (num * 100.0 / queries.size()) << "%)";
        std::cerr << ".\n";
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
                std::cout << "Stopping launch of queries. SIGINT received." << std::endl;
                return false;
            }

            if (delay > 0 && delay_watch.elapsedSeconds() > delay)
            {
                printNumberOfQueriesExecuted(queries_executed);
                cumulative ? report(comparison_info_total) : report(comparison_info_per_interval);
                delay_watch.restart();
            }
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
            {
                EntryPtrs connection_entries;
                connection_entries.reserve(connections.size());

                for (const auto & connection : connections)
                    connection_entries.emplace_back(std::make_shared<Entry>(
                            connection->get(ConnectionTimeouts::getTCPTimeoutsWithoutFailover(settings))));

                pool.scheduleOrThrowOnError([this, connection_entries]() mutable { thread(connection_entries); });
            }
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

        if (!json_path.empty())
            reportJSON(comparison_info_total, json_path);

        printNumberOfQueriesExecuted(queries_executed);
        report(comparison_info_total);
    }


    void thread(EntryPtrs & connection_entries)
    {
        Query query;

        /// Randomly choosing connection index
        pcg64 generator(randomSeed());
        std::uniform_int_distribution<size_t> distribution(0, connection_entries.size() - 1);

        /// In these threads we do not accept INT signal.
        sigset_t sig_set;
        if (sigemptyset(&sig_set)
            || sigaddset(&sig_set, SIGINT)
            || pthread_sigmask(SIG_BLOCK, &sig_set, nullptr))
        {
            throwFromErrno("Cannot block signal.", ErrorCodes::CANNOT_BLOCK_SIGNAL);
        }

        while (true)
        {
            bool extracted = false;

            while (!extracted)
            {
                extracted = queue.tryPop(query, 100);

                if (shutdown || (max_iterations && queries_executed == max_iterations))
                {
                    return;
                }
            }

            const auto connection_index = distribution(generator);
            try
            {
                execute(connection_entries, query, connection_index);
            }
            catch (...)
            {
                std::lock_guard lock(mutex);
                std::cerr << "An error occurred while processing the query " << "'" << query << "'"
                          << ": " << getCurrentExceptionMessage(false) << std::endl;
                if (!continue_on_errors)
                {
                    shutdown = true;
                    throw;
                }
                else
                {
                    std::cerr << getCurrentExceptionMessage(print_stacktrace,
                        true /*check embedded stack trace*/) << std::endl;

                    size_t info_index = round_robin ? 0 : connection_index;
                    comparison_info_per_interval[info_index]->errors++;
                    comparison_info_total[info_index]->errors++;
                }
            }
            // Count failed queries toward executed, so that we'd reach
            // max_iterations even if every run fails.
            ++queries_executed;
        }
    }

    void execute(EntryPtrs & connection_entries, Query & query, size_t connection_index)
    {
        Stopwatch watch;

        Connection & connection = **connection_entries[connection_index];

        if (reconnect)
            connection.disconnect();

        RemoteQueryExecutor executor(
            connection, query, {}, global_context, nullptr, Scalars(), Tables(), query_processing_stage);
        if (!query_id.empty())
            executor.setQueryId(query_id);

        Progress progress;
        executor.setProgressCallback([&progress](const Progress & value) { progress.incrementPiecewiseAtomically(value); });

        executor.sendQuery(ClientInfo::QueryKind::INITIAL_QUERY);

        ProfileInfo info;
        while (Block block = executor.read())
            info.update(block);

        executor.finish();

        double seconds = watch.elapsedSeconds();

        std::lock_guard lock(mutex);

        size_t info_index = round_robin ? 0 : connection_index;
        comparison_info_per_interval[info_index]->add(seconds, progress.read_rows, progress.read_bytes, info.rows, info.bytes);
        comparison_info_total[info_index]->add(seconds, progress.read_rows, progress.read_bytes, info.rows, info.bytes);
        t_test.add(info_index, seconds);
    }

    void report(MultiStats & infos)
    {
        std::lock_guard lock(mutex);

        std::cerr << "\n";
        for (size_t i = 0; i < infos.size(); ++i)
        {
            const auto & info = infos[i];

            /// Avoid zeros, nans or exceptions
            if (0 == info->queries)
                return;

            double seconds = info->work_time / concurrency;

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
            std::cerr
                    << connection_description << ", "
                    << "queries " << info->queries << ", ";
            if (info->errors)
            {
                std::cerr << "errors " << info->errors << ", ";
            }
            std::cerr
                    << "QPS: " << (info->queries / seconds) << ", "
                    << "RPS: " << (info->read_rows / seconds) << ", "
                    << "MiB/s: " << (info->read_bytes / seconds / 1048576) << ", "
                    << "result RPS: " << (info->result_rows / seconds) << ", "
                    << "result MiB/s: " << (info->result_bytes / seconds / 1048576) << "."
                    << "\n";
        }
        std::cerr << "\n";

        auto print_percentile = [&](double percent)
        {
            std::cerr << percent << "%\t\t";
            for (const auto & info : infos)
            {
                std::cerr << info->sampler.quantileNearest(percent / 100.0) << " sec.\t";
            }
            std::cerr << "\n";
        };

        for (int percent = 0; percent <= 90; percent += 10)
            print_percentile(percent);

        print_percentile(95);
        print_percentile(99);
        print_percentile(99.9);
        print_percentile(99.99);

        std::cerr << "\n" << t_test.compareAndReport(confidence).second << "\n";

        if (!cumulative)
        {
            for (auto & info : infos)
                info->clear();
        }
    }

    void reportJSON(MultiStats & infos, const std::string & filename)
    {
        WriteBufferFromFile json_out(filename);

        std::lock_guard lock(mutex);

        auto print_key_value = [&](auto key, auto value, bool with_comma = true)
        {
            json_out << double_quote << key << ": " << value << (with_comma ? ",\n" : "\n");
        };

        auto print_percentile = [&json_out](Stats & info, auto percent, bool with_comma = true)
        {
            json_out << "\"" << percent << "\": " << info.sampler.quantileNearest(percent / 100.0) << (with_comma ? ",\n" : "\n");
        };

        json_out << "{\n";

        for (size_t i = 0; i < infos.size(); ++i)
        {
            const auto & info = infos[i];

            json_out << double_quote << connections[i]->getDescription() << ": {\n";
            json_out << double_quote << "statistics" << ": {\n";

            print_key_value("QPS", info->queries / info->work_time);
            print_key_value("RPS", info->read_rows / info->work_time);
            print_key_value("MiBPS", info->read_bytes / info->work_time);
            print_key_value("RPS_result", info->result_rows / info->work_time);
            print_key_value("MiBPS_result", info->result_bytes / info->work_time);
            print_key_value("num_queries", info->queries.load());
            print_key_value("num_errors", info->errors, false);

            json_out << "},\n";
            json_out << double_quote << "query_time_percentiles" << ": {\n";

            if (info->queries != 0)
            {
                for (int percent = 0; percent <= 90; percent += 10)
                    print_percentile(*info, percent);

                print_percentile(*info, 95);
                print_percentile(*info, 99);
                print_percentile(*info, 99.9);
                print_percentile(*info, 99.99, false);
            }

            json_out << "}\n";
            json_out << (i == infos.size() - 1 ? "}\n" : "},\n");
        }

        json_out << "}\n";
    }

public:

    ~Benchmark() override
    {
        shutdown = true;
    }
};

}


#ifndef __clang__
#pragma GCC optimize("-fno-var-tracking-assignments")
#endif

int mainEntryClickHouseBenchmark(int argc, char ** argv)
{
    using namespace DB;
    bool print_stacktrace = true;

    try
    {
        using boost::program_options::value;

        boost::program_options::options_description desc = createOptionsDescription("Allowed options", getTerminalWidth());
        desc.add_options()
            ("help",                                                            "produce help message")
            ("query",      value<std::string>()->default_value(""),             "query to execute")
            ("concurrency,c", value<unsigned>()->default_value(1),              "number of parallel queries")
            ("delay,d",       value<double>()->default_value(1),                "delay between intermediate reports in seconds (set 0 to disable reports)")
            ("stage",         value<std::string>()->default_value("complete"),  "request query processing up to specified stage: complete,fetch_columns,with_mergeable_state,with_mergeable_state_after_aggregation,with_mergeable_state_after_aggregation_and_limit")
            ("iterations,i",  value<size_t>()->default_value(0),                "amount of queries to be executed")
            ("timelimit,t",   value<double>()->default_value(0.),               "stop launch of queries after specified time limit")
            ("randomize,r",   value<bool>()->default_value(false),              "randomize order of execution")
            ("json",          value<std::string>()->default_value(""),          "write final report to specified file in JSON format")
            ("host,h",        value<Strings>()->multitoken(),                   "list of hosts")
            ("port,p",        value<Ports>()->multitoken(),                     "list of ports")
            ("roundrobin",                                                      "Instead of comparing queries for different --host/--port just pick one random --host/--port for every query and send query to it.")
            ("cumulative",                                                      "prints cumulative data instead of data per interval")
            ("secure,s",                                                        "Use TLS connection")
            ("user",          value<std::string>()->default_value("default"),   "")
            ("password",      value<std::string>()->default_value(""),          "")
            ("database",      value<std::string>()->default_value("default"),   "")
            ("stacktrace",                                                      "print stack traces of exceptions")
            ("confidence",    value<size_t>()->default_value(5), "set the level of confidence for T-test [0=80%, 1=90%, 2=95%, 3=98%, 4=99%, 5=99.5%(default)")
            ("query_id",      value<std::string>()->default_value(""),         "")
            ("continue_on_errors", "continue testing even if a query fails")
            ("reconnect", "establish new connection for every query")
        ;

        Settings settings;
        settings.addProgramOptions(desc);

        boost::program_options::variables_map options;
        boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), options);
        boost::program_options::notify(options);

        clearPasswordFromCommandLine(argc, argv);

        if (options.count("help"))
        {
            std::cout << "Usage: " << argv[0] << " [options] < queries.txt\n";
            std::cout << desc << "\n";
            return 1;
        }

        print_stacktrace = options.count("stacktrace");

        /// NOTE Maybe clickhouse-benchmark should also respect .xml configuration of clickhouse-client.

        UInt16 default_port = options.count("secure") ? DBMS_DEFAULT_SECURE_PORT : DBMS_DEFAULT_PORT;

        UseSSL use_ssl;
        Ports ports = options.count("port")
            ? options["port"].as<Ports>()
            : Ports({default_port});

        Strings hosts = options.count("host") ? options["host"].as<Strings>() : Strings({"localhost"});

        Benchmark benchmark(
            options["concurrency"].as<unsigned>(),
            options["delay"].as<double>(),
            std::move(hosts),
            std::move(ports),
            options.count("roundrobin"),
            options.count("cumulative"),
            options.count("secure"),
            options["database"].as<std::string>(),
            options["user"].as<std::string>(),
            options["password"].as<std::string>(),
            options["stage"].as<std::string>(),
            options["randomize"].as<bool>(),
            options["iterations"].as<size_t>(),
            options["timelimit"].as<double>(),
            options["json"].as<std::string>(),
            options["confidence"].as<size_t>(),
            options["query_id"].as<std::string>(),
            options["query"].as<std::string>(),
            options.count("continue_on_errors"),
            options.count("reconnect"),
            print_stacktrace,
            settings);
        return benchmark.run();
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(print_stacktrace, true) << std::endl;
        return getCurrentExceptionCode();
    }
}
