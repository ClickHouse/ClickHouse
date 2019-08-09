#include <port/unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <signal.h>
#include <time.h>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <random>
#include <pcg_random.hpp>
#include <Poco/File.h>
#include <Poco/Util/Application.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>
#include <AggregateFunctions/ReservoirSampler.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <boost/program_options.hpp>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/Exception.h>
#include <Common/randomSeed.h>
#include <Core/Types.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/UseSSL.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Client/Connection.h>
#include <Common/InterruptListener.h>
#include <Common/Config/configReadClient.h>


/** A tool for evaluating ClickHouse performance.
  * The tool emulates a case with fixed amount of simultaneously executing queries.
  */

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int EMPTY_DATA_PASSED;
}

class Benchmark : public Poco::Util::Application
{
public:
    Benchmark(unsigned concurrency_, double delay_,
            const std::vector<std::string> & hosts_, const std::vector<UInt16> & ports_,
            bool cumulative_, bool secure_, const String & default_database_,
            const String & user_, const String & password_, const String & stage,
            bool randomize_, size_t max_iterations_, double max_time_,
            const String & json_path_, size_t confidence_, const Settings & settings_)
        :
        concurrency(concurrency_), delay(delay_), queue(concurrency), randomize(randomize_),
        cumulative(cumulative_), max_iterations(max_iterations_), max_time(max_time_),
        confidence(confidence_), json_path(json_path_), settings(settings_),
        global_context(Context::createGlobal()), pool(concurrency)
    {
        const auto secure = secure_ ? Protocol::Secure::Enable : Protocol::Secure::Disable;
        size_t connections_cnt = std::max(ports_.size(), hosts_.size());

        connections.reserve(connections_cnt);
        comparison_info_total.reserve(connections_cnt);
        comparison_info_per_interval.reserve(connections_cnt);
        comparison_relative.data.resize(connections_cnt);

        for (size_t i = 0; i < connections_cnt; ++i)
        {
            UInt16 cur_port = i >= ports_.size() ? 9000 : ports_[i];
            std::string cur_host = i >= hosts_.size() ? "localhost" : hosts_[i];

            connections.emplace_back(std::make_shared<ConnectionPool>(concurrency, cur_host, cur_port, default_database_, user_, password_, "benchmark", Protocol::Compression::Enable, secure));
            comparison_info_per_interval.emplace_back(std::make_shared<Stats>());
            comparison_info_total.emplace_back(std::make_shared<Stats>());
        }

        if (confidence > 5)
        {
            std::cerr << "Confidence can't be set to " + toString(confidence) + ". It was set to 5 instead." << '\n';
            confidence = 5;
        }

        global_context.makeGlobalContext();

        std::cerr << std::fixed << std::setprecision(3);

        /// This is needed to receive blocks with columns of AggregateFunction data type
        /// (example: when using stage = 'with_mergeable_state')
        registerAggregateFunctions();

        if (stage == "complete")
            query_processing_stage = QueryProcessingStage::Complete;
        else if (stage == "fetch_columns")
            query_processing_stage = QueryProcessingStage::FetchColumns;
        else if (stage == "with_mergeable_state")
            query_processing_stage = QueryProcessingStage::WithMergeableState;
        else
            throw Exception("Unknown query processing stage: " + stage, ErrorCodes::BAD_ARGUMENTS);

    }

    void initialize(Poco::Util::Application & self [[maybe_unused]])
    {
        std::string home_path;
        const char * home_path_cstr = getenv("HOME");
        if (home_path_cstr)
            home_path = home_path_cstr;

        configReadClient(config(), home_path);
    }

    int main(const std::vector<std::string> &)
    {
        if (!json_path.empty() && Poco::File(json_path).exists()) /// Clear file with previous results
            Poco::File(json_path).remove();

        readQueries();
        runBenchmark();
        return 0;
    }

private:
    using Entry = ConnectionPool::Entry;
    using EntryPtr = std::shared_ptr<Entry>;
    using EntryPtrs = std::vector<EntryPtr>;

    unsigned concurrency;
    double delay;

    using Query = std::string;
    using Queries = std::vector<Query>;
    Queries queries;

    using Queue = ConcurrentBoundedQueue<Query>;
    Queue queue;

    ConnectionPoolPtrs connections;

    bool randomize;
    bool cumulative;
    size_t max_iterations;
    double max_time;
    size_t confidence;
    String json_path;
    Settings settings;
    Context global_context;
    QueryProcessingStage::Enum query_processing_stage;

    /// Don't execute new queries after timelimit or SIGINT or exception
    std::atomic<bool> shutdown{false};

    std::atomic<size_t> queries_executed{0};

    struct Stats
    {
        std::atomic<size_t> queries{0};
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

    struct RelativeAnalysis
    {
        struct RelativeStats
        {
            size_t cnt = 0;
            double sum = 0;
            double squares_sum = 0;

            void add(double seconds)
            {
                ++cnt;
                sum += seconds;
                squares_sum += seconds * seconds;
            }

            double avg() const
            {
                return sum / cnt;
            }

            double var() const
            {
                return (squares_sum - (sum * sum / cnt)) / static_cast<double>(cnt - 1);
            }
        };

        const std::vector<double> confidence_level = { 80, 90, 95, 98, 99, 99.5 };

        const std::vector<std::vector<double>> students_table = {
        /* inf */	{	1.282,	1.645,	1.960,	2.326,	2.576,	3.090  },
        /* 1. */	{	3.078,	6.314,	12.706,	31.821,	63.657,	318.313},
        /* 2. */	{	1.886,	2.920,	4.303,	6.965,	9.925,	22.327 },
        /* 3. */	{	1.638,	2.353,	3.182,	4.541,	5.841,	10.215 },
        /* 4. */	{	1.533,	2.132,	2.776,	3.747,	4.604,	7.173  },
        /* 5. */	{	1.476,	2.015,	2.571,	3.365,	4.032,	5.893  },
        /* 6. */	{	1.440,	1.943,	2.447,	3.143,	3.707,	5.208  },
        /* 7. */	{	1.415,	1.895,	2.365,	2.998,	3.499,	4.782  },
        /* 8. */	{	1.397,	1.860,	2.306,	2.896,	3.355,	4.499  },
        /* 9. */	{	1.383,	1.833,	2.262,	2.821,	3.250,	4.296  },
        /* 10. */	{	1.372,	1.812,	2.228,	2.764,	3.169,	4.143  },
        /* 11. */	{	1.363,	1.796,	2.201,	2.718,	3.106,	4.024  },
        /* 12. */	{	1.356,	1.782,	2.179,	2.681,	3.055,	3.929  },
        /* 13. */	{	1.350,	1.771,	2.160,	2.650,	3.012,	3.852  },
        /* 14. */	{	1.345,	1.761,	2.145,	2.624,	2.977,	3.787  },
        /* 15. */	{	1.341,	1.753,	2.131,	2.602,	2.947,	3.733  },
        /* 16. */	{	1.337,	1.746,	2.120,	2.583,	2.921,	3.686  },
        /* 17. */	{	1.333,	1.740,	2.110,	2.567,	2.898,	3.646  },
        /* 18. */	{	1.330,	1.734,	2.101,	2.552,	2.878,	3.610  },
        /* 19. */	{	1.328,	1.729,	2.093,	2.539,	2.861,	3.579  },
        /* 20. */	{	1.325,	1.725,	2.086,	2.528,	2.845,	3.552  },
        /* 21. */	{	1.323,	1.721,	2.080,	2.518,	2.831,	3.527  },
        /* 22. */	{	1.321,	1.717,	2.074,	2.508,	2.819,	3.505  },
        /* 23. */	{	1.319,	1.714,	2.069,	2.500,	2.807,	3.485  },
        /* 24. */	{	1.318,	1.711,	2.064,	2.492,	2.797,	3.467  },
        /* 25. */	{	1.316,	1.708,	2.060,	2.485,	2.787,	3.450  },
        /* 26. */	{	1.315,	1.706,	2.056,	2.479,	2.779,	3.435  },
        /* 27. */	{	1.314,	1.703,	2.052,	2.473,	2.771,	3.421  },
        /* 28. */	{	1.313,	1.701,	2.048,	2.467,	2.763,	3.408  },
        /* 29. */	{	1.311,	1.699,	2.045,	2.462,	2.756,	3.396  },
        /* 30. */	{	1.310,	1.697,	2.042,	2.457,	2.750,	3.385  },
        /* 31. */	{	1.309,	1.696,	2.040,	2.453,	2.744,	3.375  },
        /* 32. */	{	1.309,	1.694,	2.037,	2.449,	2.738,	3.365  },
        /* 33. */	{	1.308,	1.692,	2.035,	2.445,	2.733,	3.356  },
        /* 34. */	{	1.307,	1.691,	2.032,	2.441,	2.728,	3.348  },
        /* 35. */	{	1.306,	1.690,	2.030,	2.438,	2.724,	3.340  },
        /* 36. */	{	1.306,	1.688,	2.028,	2.434,	2.719,	3.333  },
        /* 37. */	{	1.305,	1.687,	2.026,	2.431,	2.715,	3.326  },
        /* 38. */	{	1.304,	1.686,	2.024,	2.429,	2.712,	3.319  },
        /* 39. */	{	1.304,	1.685,	2.023,	2.426,	2.708,	3.313  },
        /* 40. */	{	1.303,	1.684,	2.021,	2.423,	2.704,	3.307  },
        /* 41. */	{	1.303,	1.683,	2.020,	2.421,	2.701,	3.301  },
        /* 42. */	{	1.302,	1.682,	2.018,	2.418,	2.698,	3.296  },
        /* 43. */	{	1.302,	1.681,	2.017,	2.416,	2.695,	3.291  },
        /* 44. */	{	1.301,	1.680,	2.015,	2.414,	2.692,	3.286  },
        /* 45. */	{	1.301,	1.679,	2.014,	2.412,	2.690,	3.281  },
        /* 46. */	{	1.300,	1.679,	2.013,	2.410,	2.687,	3.277  },
        /* 47. */	{	1.300,	1.678,	2.012,	2.408,	2.685,	3.273  },
        /* 48. */	{	1.299,	1.677,	2.011,	2.407,	2.682,	3.269  },
        /* 49. */	{	1.299,	1.677,	2.010,	2.405,	2.680,	3.265  },
        /* 50. */	{	1.299,	1.676,	2.009,	2.403,	2.678,	3.261  },
        /* 51. */	{	1.298,	1.675,	2.008,	2.402,	2.676,	3.258  },
        /* 52. */	{	1.298,	1.675,	2.007,	2.400,	2.674,	3.255  },
        /* 53. */	{	1.298,	1.674,	2.006,	2.399,	2.672,	3.251  },
        /* 54. */	{	1.297,	1.674,	2.005,	2.397,	2.670,	3.248  },
        /* 55. */	{	1.297,	1.673,	2.004,	2.396,	2.668,	3.245  },
        /* 56. */	{	1.297,	1.673,	2.003,	2.395,	2.667,	3.242  },
        /* 57. */	{	1.297,	1.672,	2.002,	2.394,	2.665,	3.239  },
        /* 58. */	{	1.296,	1.672,	2.002,	2.392,	2.663,	3.237  },
        /* 59. */	{	1.296,	1.671,	2.001,	2.391,	2.662,	3.234  },
        /* 60. */	{	1.296,	1.671,	2.000,	2.390,	2.660,	3.232  },
        /* 61. */	{	1.296,	1.670,	2.000,	2.389,	2.659,	3.229  },
        /* 62. */	{	1.295,	1.670,	1.999,	2.388,	2.657,	3.227  },
        /* 63. */	{	1.295,	1.669,	1.998,	2.387,	2.656,	3.225  },
        /* 64. */	{	1.295,	1.669,	1.998,	2.386,	2.655,	3.223  },
        /* 65. */	{	1.295,	1.669,	1.997,	2.385,	2.654,	3.220  },
        /* 66. */	{	1.295,	1.668,	1.997,	2.384,	2.652,	3.218  },
        /* 67. */	{	1.294,	1.668,	1.996,	2.383,	2.651,	3.216  },
        /* 68. */	{	1.294,	1.668,	1.995,	2.382,	2.650,	3.214  },
        /* 69. */	{	1.294,	1.667,	1.995,	2.382,	2.649,	3.213  },
        /* 70. */	{	1.294,	1.667,	1.994,	2.381,	2.648,	3.211  },
        /* 71. */	{	1.294,	1.667,	1.994,	2.380,	2.647,	3.209  },
        /* 72. */	{	1.293,	1.666,	1.993,	2.379,	2.646,	3.207  },
        /* 73. */	{	1.293,	1.666,	1.993,	2.379,	2.645,	3.206  },
        /* 74. */	{	1.293,	1.666,	1.993,	2.378,	2.644,	3.204  },
        /* 75. */	{	1.293,	1.665,	1.992,	2.377,	2.643,	3.202  },
        /* 76. */	{	1.293,	1.665,	1.992,	2.376,	2.642,	3.201  },
        /* 77. */	{	1.293,	1.665,	1.991,	2.376,	2.641,	3.199  },
        /* 78. */	{	1.292,	1.665,	1.991,	2.375,	2.640,	3.198  },
        /* 79. */	{	1.292,	1.664,	1.990,	2.374,	2.640,	3.197  },
        /* 80. */	{	1.292,	1.664,	1.990,	2.374,	2.639,	3.195  },
        /* 81. */	{	1.292,	1.664,	1.990,	2.373,	2.638,	3.194  },
        /* 82. */	{	1.292,	1.664,	1.989,	2.373,	2.637,	3.193  },
        /* 83. */	{	1.292,	1.663,	1.989,	2.372,	2.636,	3.191  },
        /* 84. */	{	1.292,	1.663,	1.989,	2.372,	2.636,	3.190  },
        /* 85. */	{	1.292,	1.663,	1.988,	2.371,	2.635,	3.189  },
        /* 86. */	{	1.291,	1.663,	1.988,	2.370,	2.634,	3.188  },
        /* 87. */	{	1.291,	1.663,	1.988,	2.370,	2.634,	3.187  },
        /* 88. */	{	1.291,	1.662,	1.987,	2.369,	2.633,	3.185  },
        /* 89. */	{	1.291,	1.662,	1.987,	2.369,	2.632,	3.184  },
        /* 90. */	{	1.291,	1.662,	1.987,	2.368,	2.632,	3.183  },
        /* 91. */	{	1.291,	1.662,	1.986,	2.368,	2.631,	3.182  },
        /* 92. */	{	1.291,	1.662,	1.986,	2.368,	2.630,	3.181  },
        /* 93. */	{	1.291,	1.661,	1.986,	2.367,	2.630,	3.180  },
        /* 94. */	{	1.291,	1.661,	1.986,	2.367,	2.629,	3.179  },
        /* 95. */	{	1.291,	1.661,	1.985,	2.366,	2.629,	3.178  },
        /* 96. */	{	1.290,	1.661,	1.985,	2.366,	2.628,	3.177  },
        /* 97. */	{	1.290,	1.661,	1.985,	2.365,	2.627,	3.176  },
        /* 98. */	{	1.290,	1.661,	1.984,	2.365,	2.627,	3.175  },
        /* 99. */	{	1.290,	1.660,	1.984,	2.365,	2.626,	3.175  },
        /* 100. */	{	1.290,	1.660,	1.984,	2.364,	2.626,	3.174  }
                    };

        std::vector<RelativeStats> data;

        bool report(size_t confidence_index)
        {
            if (data.size() != 2) /// Works for two connections only
                return true;

            size_t i = (data[0].cnt - 1) + (data[1].cnt - 1);

            double t = students_table[i > 100 ? 0 : i][confidence_index];

            double spool = (data[0].cnt - 1) * data[0].var() + (data[1].cnt - 1) * data[1].var();
            spool = sqrt(spool / i);

            double s = spool * sqrt(1.0 / data[0].cnt + 1.0 / data[1].cnt);

            double d = data[1].avg() - data[0].avg();

            double e = t * s;

            std::cerr << '\n';
            if (fabs(d) > e)
            {
                std::cerr << std::setprecision(1) << "Difference at " << confidence_level[confidence_index] <<  "% confidence\n" << std::setprecision(6);
                std::cerr << "\t" << d << " +/- " << e << "\n";
                std::cerr << "\t" << d * 100 / data[0].avg() << "% +/- " << e * 100 / data[0].avg() << "%\n";
                std::cerr << "\t(Student's t, pooled s = " << spool << ")\n" << std::setprecision(3);
                return false;
            }
            else
            {
                std::cerr << std::setprecision(1) << "No difference proven at " << confidence_level[confidence_index] <<  "% confidence\n" << std::setprecision(3);
                return true;
            }
        }
    };

    RelativeAnalysis comparison_relative;

    using MultiStats = std::vector<std::shared_ptr<Stats>>;
    MultiStats comparison_info_per_interval;
    MultiStats comparison_info_total;

    Stopwatch total_watch;
    Stopwatch delay_watch;

    std::mutex mutex;

    ThreadPool pool;

    void readQueries()
    {
        ReadBufferFromFileDescriptor in(STDIN_FILENO);

        while (!in.eof())
        {
            std::string query;
            readText(query, in);
            assertChar('\n', in);

            if (!query.empty())
                queries.emplace_back(query);
        }

        if (queries.empty())
            throw Exception("Empty list of queries.", ErrorCodes::EMPTY_DATA_PASSED);

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
                std::cout << "Stopping launch of queries. Requested time limit is exhausted.\n";
                return false;
            }

            if (interrupt_listener.check())
            {
                std::cout << "Stopping launch of queries. SIGINT recieved.\n";
                return false;
            }

            if (delay > 0 && delay_watch.elapsedSeconds() > delay)
            {
                printNumberOfQueriesExecuted(queries_executed);
                cumulative ? report(comparison_info_total) : report(comparison_info_per_interval);
                comparison_relative.report(confidence);

                delay_watch.restart();
            }
        }

        return true;
    }

    void runBenchmark()
    {
        pcg64 generator(randomSeed());
        std::uniform_int_distribution<size_t> distribution(0, queries.size() - 1);

        for (size_t i = 0; i < concurrency; ++i)
        {
            EntryPtrs connection_entries;
            connection_entries.reserve(connections.size());

            for (const auto & connection : connections)
                connection_entries.emplace_back(std::make_shared<Entry>(connection->get(ConnectionTimeouts::getTCPTimeoutsWithoutFailover(settings))));

            pool.schedule(std::bind(&Benchmark::thread, this, connection_entries));
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

        pool.wait();
        total_watch.stop();

        if (!json_path.empty())
            reportJSON(comparison_info_total, json_path);

        printNumberOfQueriesExecuted(queries_executed);
        report(comparison_info_total);
        comparison_relative.report(confidence);
    }


    void thread(EntryPtrs & connection_entries)
    {
        Query query;

        /// Randomly choosing connection index
        pcg64 generator(randomSeed());
        std::uniform_int_distribution<size_t> distribution(0, connection_entries.size() - 1);

        try
        {
            /// In these threads we do not accept INT signal.
            sigset_t sig_set;
            if (sigemptyset(&sig_set)
                || sigaddset(&sig_set, SIGINT)
                || pthread_sigmask(SIG_BLOCK, &sig_set, nullptr))
                throwFromErrno("Cannot block signal.", ErrorCodes::CANNOT_BLOCK_SIGNAL);

            while (true)
            {
                bool extracted = false;

                while (!extracted)
                {
                    extracted = queue.tryPop(query, 100);

                    if (shutdown || (max_iterations && queries_executed == max_iterations))
                        return;
                }
                execute(connection_entries, query, distribution(generator));
                ++queries_executed;
            }
        }
        catch (...)
        {
            shutdown = true;
            std::cerr << "An error occurred while processing query:\n" << query << "\n";
            throw;
        }
    }

    void execute(EntryPtrs & connection_entries, Query & query, size_t connection_index)
    {
        Stopwatch watch;
        RemoteBlockInputStream stream(
            *(*connection_entries[connection_index]),
            query, {}, global_context, &settings, nullptr, Tables(), query_processing_stage);

        Progress progress;
        stream.setProgressCallback([&progress](const Progress & value) { progress.incrementPiecewiseAtomically(value); });

        stream.readPrefix();
        while (Block block = stream.read());

        stream.readSuffix();

        const BlockStreamProfileInfo & info = stream.getProfileInfo();

        double seconds = watch.elapsedSeconds();

        std::lock_guard lock(mutex);

        comparison_info_per_interval[connection_index]->add(seconds, progress.read_rows, progress.read_bytes, info.rows, info.bytes);
        comparison_info_total[connection_index]->add(seconds, progress.read_rows, progress.read_bytes, info.rows, info.bytes);
        comparison_relative.data[connection_index].add(seconds);
    }

    void report(MultiStats & infos)
    {
        std::lock_guard lock(mutex);

        std::cerr << "\n";
        for (size_t i = 1; i <= infos.size(); ++i)
        {
            const auto & info = infos[i - 1];

            /// Avoid zeros, nans or exceptions
            if (0 == info->queries)
                return;

            double seconds = info->work_time / concurrency;

            std::cerr
                    << "connection " << i << ", "
                    << "queries " << info->queries << ", "
                    << "QPS: " << (info->queries / seconds) << ", "
                    << "RPS: " << (info->read_rows / seconds) << ", "
                    << "MiB/s: " << (info->read_bytes / seconds / 1048576) << ", "
                    << "result RPS: " << (info->result_rows / seconds) << ", "
                    << "result MiB/s: " << (info->result_bytes / seconds / 1048576) << "."
                    << "\n";
        }

        std::cerr << "\n\t\t";

        for (size_t i = 1; i <= infos.size(); ++i)
            std::cerr << "connection " << i << "\t";

        std::cerr << "\n";
        auto print_percentile = [&](double percent)
        {
            std::cerr << percent << "%\t\t";
            for (const auto & info : infos)
            {
                std::cerr << info->sampler.quantileInterpolated(percent / 100.0) << " sec." << "\t";
            }
            std::cerr << "\n";
        };

        for (int percent = 0; percent <= 90; percent += 10)
            print_percentile(percent);

        print_percentile(95);
        print_percentile(99);
        print_percentile(99.9);
        print_percentile(99.99);

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
            json_out << "\"" << percent << "\"" << ": " << info.sampler.quantileInterpolated(percent / 100.0) << (with_comma ? ",\n" : "\n");
        };

        json_out << "{\n";

        for (size_t i = 1; i <= infos.size(); ++i)
        {
            const auto & info = infos[i - 1];

            json_out << double_quote << "connection_" + toString(i) << ": {\n";
            json_out << double_quote << "statistics" << ": {\n";

            print_key_value("QPS", info->queries / info->work_time);
            print_key_value("RPS", info->read_rows / info->work_time);
            print_key_value("MiBPS", info->read_bytes / info->work_time);
            print_key_value("RPS_result", info->result_rows / info->work_time);
            print_key_value("MiBPS_result", info->result_bytes / info->work_time);
            print_key_value("num_queries", info->queries.load(), false);

            json_out << "},\n";
            json_out << double_quote << "query_time_percentiles" << ": {\n";

            for (int percent = 0; percent <= 90; percent += 10)
                print_percentile(*info, percent);

            print_percentile(*info, 95);
            print_percentile(*info, 99);
            print_percentile(*info, 99.9);
            print_percentile(*info, 99.99, false);

            json_out << "}\n";
            json_out << (i == infos.size() ? "}\n" : "},\n");
        }

        json_out << "}\n";
    }

public:

    ~Benchmark()
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

        boost::program_options::options_description desc("Allowed options");
        desc.add_options()
            ("help",                                                            "produce help message")
            ("concurrency,c", value<unsigned>()->default_value(1),              "number of parallel queries")
            ("delay,d",       value<double>()->default_value(1),                "delay between intermediate reports in seconds (set 0 to disable reports)")
            ("stage",         value<std::string>()->default_value("complete"),  "request query processing up to specified stage: complete,fetch_columns,with_mergeable_state")
            ("iterations,i",  value<size_t>()->default_value(0),                "amount of queries to be executed")
            ("timelimit,t",   value<double>()->default_value(0.),               "stop launch of queries after specified time limit")
            ("randomize,r",   value<bool>()->default_value(false),              "randomize order of execution")
            ("json",          value<std::string>()->default_value(""),          "write final report to specified file in JSON format")
            ("host,h",        value<std::vector<std::string>>()->default_value(std::vector<std::string>{"localhost"}, "localhost"), "note that more than one host can be described")
            ("port,p",        value<std::vector<UInt16>>()->default_value(std::vector<UInt16>{9000}, "9000"),                       "note that more than one port can be described")
            ("cumulative",                                                      "prints cumulative data instead of data per interval")
            ("secure,s",                                                        "Use TLS connection")
            ("user",          value<std::string>()->default_value("default"),   "")
            ("password",      value<std::string>()->default_value(""),          "")
            ("database",      value<std::string>()->default_value("default"),   "")
            ("stacktrace",                                                      "print stack traces of exceptions")
            ("confidence",    value<size_t>()->default_value(5),                "set the level of confidence for T-test [0=80%, 1=90%, 2=95%, 3=98%, 4=99%, 5=99.5%(default)")
        ;

        Settings settings;
        settings.addProgramOptions(desc);

        boost::program_options::variables_map options;
        boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), options);
        boost::program_options::notify(options);

        if (options.count("help"))
        {
            std::cout << "Usage: " << argv[0] << " [options] < queries.txt\n";
            std::cout << desc << "\n";
            return 1;
        }

        print_stacktrace = options.count("stacktrace");

        UseSSL use_ssl;

        Benchmark benchmark(
            options["concurrency"].as<unsigned>(),
            options["delay"].as<double>(),
            options["host"].as<std::vector<std::string>>(),
            options["port"].as<std::vector<UInt16>>(),
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
            settings);
        return benchmark.run();
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(print_stacktrace, true) << std::endl;
        return getCurrentExceptionCode();
    }
}
