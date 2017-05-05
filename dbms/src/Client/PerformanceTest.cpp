#include <iostream>
#include <limits>
#include <unistd.h>

#include <boost/program_options.hpp>
#include <sys/stat.h>

#include <AggregateFunctions/ReservoirSampler.h>
#include <Client/Connection.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>
#include <Core/Types.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Settings.h>


#include <Poco/AutoPtr.h>
#include <Poco/Exception.h>
#include <Poco/SAX/InputSource.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/XML/XMLStream.h>

#include "InterruptListener.h"

/** Tests launcher for ClickHouse.
  * The tool walks through given or default folder in order to find files with
  * tests' descriptions and launches it.
  */
namespace DB
{
namespace ErrorCodes
{
    extern const int POCO_EXCEPTION;
    extern const int STD_EXCEPTION;
    extern const int UNKNOWN_EXCEPTION;
}

const std::string FOUR_SPACES = "    ";

bool isNumber(const std::string & str)
{
    if (str.empty())
    {
        return false;
    }

    size_t dots_counter = 0;

    if (str[0] == '.' || str[str.size() - 1] == '.')
    {
        return false;
    }

    for (char chr : str)
    {
        if (chr == '.')
        {
            if (dots_counter)
                return false;
            else
                ++dots_counter;
            continue;
        }

        if (chr < '0' || chr > '9')
        {
            return false;
        }
    }

    return true;
}

class JSONString
{
private:
    std::map<std::string, std::string> content;
    std::string current_key;
    size_t _padding = 1;

public:
    JSONString(){};
    JSONString(size_t padding) : _padding(padding){};

    JSONString & operator[](const std::string & key)
    {
        current_key = key;
        return *this;
    }

    template <typename T>
    typename std::enable_if<std::is_arithmetic<T>::value, JSONString &>::type operator[](const T key)
    {
        current_key = std::to_string(key);
        return *this;
    }

    void set(std::string value)
    {
        if (current_key.empty())
        {
            throw "cannot use set without key";
        }

        if (value.empty())
        {
            value = "null";
        }

        bool reserved = (value[0] == '[' || value[0] == '{' || value == "null");

        if (!reserved && !isNumber(value))
        {
            value = '\"' + value + '\"';
        }

        content[current_key] = value;
        current_key = "";
    }

    void set(const JSONString & inner_json)
    {
        set(inner_json.constructOutput());
    }

    void set(const std::vector<JSONString> & run_infos)
    {
        if (current_key.empty())
        {
            throw "cannot use set without key";
        }

        content[current_key] = "[\n";

        for (size_t i = 0; i < run_infos.size(); ++i)
        {
            for (size_t i = 0; i < _padding + 1; ++i)
            {
                content[current_key] += FOUR_SPACES;
            }
            content[current_key] += run_infos[i].constructOutput(_padding + 2);

            if (i != run_infos.size() - 1)
            {
                content[current_key] += ',';
            }

            content[current_key] += "\n";
        }

        for (size_t i = 0; i < _padding; ++i)
        {
            content[current_key] += FOUR_SPACES;
        }
        content[current_key] += ']';
        current_key = "";
    }

    template <typename T>
    typename std::enable_if<std::is_arithmetic<T>::value, void>::type set(T value)
    {
        set(std::to_string(value));
    }
    std::string constructOutput() const
    {
        return constructOutput(_padding);
    }

    std::string constructOutput(size_t padding) const
    {
        std::string output = "{";

        bool first = true;

        for (auto it = content.begin(); it != content.end(); ++it)
        {
            if (!first)
            {
                output += ',';
            }
            else
            {
                first = false;
            }

            output += "\n";
            for (size_t i = 0; i < padding; ++i)
            {
                output += FOUR_SPACES;
            }

            std::string key = '\"' + it->first + '\"';
            std::string value = it->second;

            output += key + ": " + value;
        }

        output += "\n";
        for (size_t i = 0; i < padding - 1; ++i)
        {
            output += FOUR_SPACES;
        }
        output += "}";
        return output;
    }
};

std::ostream & operator<<(std::ostream & stream, const JSONString & json_obj)
{
    stream << json_obj.constructOutput();

    return stream;
}

enum PriorityType
{
    min,
    max
};

struct CriterionWithPriority
{
    PriorityType priority;
    size_t value;
    bool fulfilled;

    CriterionWithPriority() : value(0), fulfilled(false)
    {
    }
    CriterionWithPriority(const CriterionWithPriority &) = default;
};

/// Termination criterions. The running test will be terminated in either of two conditions:
/// 1. All criterions marked 'min' are fulfilled
/// or
/// 2. Any criterion  marked 'max' is  fulfilled
class StopCriterions
{
private:
    using AbstractConfiguration = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;
    using Keys = std::vector<std::string>;

    void initializeStruct(const std::string & priority, const AbstractConfiguration & stop_criterions_view)
    {
        Keys keys;
        stop_criterions_view->keys(priority, keys);

        PriorityType priority_type = (priority == "min" ? min : max);

        for (const std::string & key : keys)
        {
            if (key == "timeout_ms")
            {
                timeout_ms.value = stop_criterions_view->getUInt64(priority + ".timeout_ms");
                timeout_ms.priority = priority_type;
            }
            else if (key == "rows_read")
            {
                rows_read.value = stop_criterions_view->getUInt64(priority + ".rows_read");
                rows_read.priority = priority_type;
            }
            else if (key == "bytes_read_uncompressed")
            {
                bytes_read_uncompressed.value = stop_criterions_view->getUInt64(priority + ".bytes_read_uncompressed");
                bytes_read_uncompressed.priority = priority_type;
            }
            else if (key == "iterations")
            {
                iterations.value = stop_criterions_view->getUInt64(priority + ".iterations");
                iterations.priority = priority_type;
            }
            else if (key == "min_time_not_changing_for_ms")
            {
                min_time_not_changing_for_ms.value = stop_criterions_view->getUInt64(priority + ".min_time_not_changing_for_ms");
                min_time_not_changing_for_ms.priority = priority_type;
            }
            else if (key == "max_speed_not_changing_for_ms")
            {
                max_speed_not_changing_for_ms.value = stop_criterions_view->getUInt64(priority + ".max_speed_not_changing_for_ms");
                max_speed_not_changing_for_ms.priority = priority_type;
            }
            else if (key == "average_speed_not_changing_for_ms")
            {
                average_speed_not_changing_for_ms.value = stop_criterions_view->getUInt64(priority + ".average_speed_not_changing_for_ms");
                average_speed_not_changing_for_ms.priority = priority_type;
            }
            else
            {
                throw Poco::Exception("Met unkown stop criterion: " + key, 1);
            }

            if (priority == "min")
            {
                ++number_of_initialized_min;
            };
            if (priority == "max")
            {
                ++number_of_initialized_max;
            };
        }
    }

public:
    StopCriterions() : number_of_initialized_min(0), number_of_initialized_max(0), fulfilled_criterions_min(0), fulfilled_criterions_max(0)
    {
    }

    StopCriterions(const StopCriterions & another_criterions)
        : timeout_ms(another_criterions.timeout_ms),
          rows_read(another_criterions.rows_read),
          bytes_read_uncompressed(another_criterions.bytes_read_uncompressed),
          iterations(another_criterions.iterations),
          min_time_not_changing_for_ms(another_criterions.min_time_not_changing_for_ms),
          max_speed_not_changing_for_ms(another_criterions.max_speed_not_changing_for_ms),
          average_speed_not_changing_for_ms(another_criterions.average_speed_not_changing_for_ms),

          number_of_initialized_min(another_criterions.number_of_initialized_min),
          number_of_initialized_max(another_criterions.number_of_initialized_max),
          fulfilled_criterions_min(another_criterions.fulfilled_criterions_min),
          fulfilled_criterions_max(another_criterions.fulfilled_criterions_max)
    {
    }

    void loadFromConfig(const AbstractConfiguration & stop_criterions_view)
    {
        if (stop_criterions_view->has("min"))
        {
            initializeStruct("min", stop_criterions_view);
        }

        if (stop_criterions_view->has("max"))
        {
            initializeStruct("max", stop_criterions_view);
        }
    }

    void reset()
    {
        timeout_ms.fulfilled = false;
        rows_read.fulfilled = false;
        bytes_read_uncompressed.fulfilled = false;
        iterations.fulfilled = false;
        min_time_not_changing_for_ms.fulfilled = false;
        max_speed_not_changing_for_ms.fulfilled = false;
        average_speed_not_changing_for_ms.fulfilled = false;

        fulfilled_criterions_min = 0;
        fulfilled_criterions_max = 0;
    }

    CriterionWithPriority timeout_ms;
    CriterionWithPriority rows_read;
    CriterionWithPriority bytes_read_uncompressed;
    CriterionWithPriority iterations;
    CriterionWithPriority min_time_not_changing_for_ms;
    CriterionWithPriority max_speed_not_changing_for_ms;
    CriterionWithPriority average_speed_not_changing_for_ms;

    /// Hereafter 'min' and 'max', in context of critetions, mean a level of importance
    /// Number of initialized properties met in configuration
    size_t number_of_initialized_min;
    size_t number_of_initialized_max;

    size_t fulfilled_criterions_min;
    size_t fulfilled_criterions_max;
};

struct Stats
{
    Stopwatch watch;
    Stopwatch watch_per_query;
    Stopwatch min_time_watch;
    Stopwatch max_rows_speed_watch;
    Stopwatch max_bytes_speed_watch;
    Stopwatch avg_rows_speed_watch;
    Stopwatch avg_bytes_speed_watch;
    size_t queries;
    size_t rows_read;
    size_t bytes_read;

    using Sampler = ReservoirSampler<double>;
    Sampler sampler{1 << 16};

    /// min_time in ms
    UInt64 min_time = std::numeric_limits<UInt64>::max();
    double total_time = 0;

    double max_rows_speed = 0;
    double max_bytes_speed = 0;

    double avg_rows_speed_value = 0;
    double avg_rows_speed_first = 0;
    static double avg_rows_speed_precision;

    double avg_bytes_speed_value = 0;
    double avg_bytes_speed_first = 0;
    static double avg_bytes_speed_precision;

    size_t number_of_rows_speed_info_batches = 0;
    size_t number_of_bytes_speed_info_batches = 0;

    bool ready = false; // check if a query wasn't interrupted by SIGINT

    std::string getStatisticByName(const std::string & statistic_name)
    {
        if (statistic_name == "min_time")
        {
            return std::to_string(min_time) + "ms";
        }
        if (statistic_name == "quantiles")
        {
            std::string result = "\n";

            for (double percent = 10; percent <= 90; percent += 10)
            {
                result += FOUR_SPACES + std::to_string((percent / 100));
                result += ": " + std::to_string(sampler.quantileInterpolated(percent / 100.0));
                result += "\n";
            }
            result += FOUR_SPACES + "0.95:   " + std::to_string(sampler.quantileInterpolated(95 / 100.0)) + "\n";
            result += FOUR_SPACES + "0.99: "   + std::to_string(sampler.quantileInterpolated(99 / 100.0)) + "\n";
            result += FOUR_SPACES + "0.999: "  + std::to_string(sampler.quantileInterpolated(99.9 / 100.)) + "\n";
            result += FOUR_SPACES + "0.9999: " + std::to_string(sampler.quantileInterpolated(99.99 / 100.));

            return result;
        }
        if (statistic_name == "total_time")
        {
            return std::to_string(total_time) + "s";
        }
        if (statistic_name == "queries_per_second")
        {
            return std::to_string(queries / total_time);
        }
        if (statistic_name == "rows_per_second")
        {
            return std::to_string(rows_read / total_time);
        }
        if (statistic_name == "bytes_per_second")
        {
            return std::to_string(bytes_read / total_time);
        }

        if (statistic_name == "max_rows_per_second")
        {
            return std::to_string(max_rows_speed);
        }
        if (statistic_name == "max_bytes_per_second")
        {
            return std::to_string(max_bytes_speed);
        }
        if (statistic_name == "avg_rows_per_second")
        {
            return std::to_string(avg_rows_speed_value);
        }
        if (statistic_name == "avg_bytes_per_second")
        {
            return std::to_string(avg_bytes_speed_value);
        }

        return "";
    }

    void update_min_time(const UInt64 min_time_candidate)
    {
        if (min_time_candidate < min_time)
        {
            min_time = min_time_candidate;
            min_time_watch.restart();
        }
    }

    void update_average_speed(const double new_speed_info,
        Stopwatch & avg_speed_watch,
        size_t & number_of_info_batches,
        double precision,
        double & avg_speed_first,
        double & avg_speed_value)
    {
        avg_speed_value = ((avg_speed_value * number_of_info_batches) + new_speed_info);
        avg_speed_value /= (++number_of_info_batches);

        if (avg_speed_first == 0)
        {
            avg_speed_first = avg_speed_value;
        }

        if (abs(avg_speed_value - avg_speed_first) >= precision)
        {
            avg_speed_first = avg_speed_value;
            avg_speed_watch.restart();
        }
    }

    void update_max_speed(const size_t max_speed_candidate, Stopwatch & max_speed_watch, double & max_speed)
    {
        if (max_speed_candidate > max_speed)
        {
            max_speed = max_speed_candidate;
            max_speed_watch.restart();
        }
    }

    void add(size_t rows_read_inc, size_t bytes_read_inc)
    {
        rows_read += rows_read_inc;
        bytes_read += bytes_read_inc;

        double new_rows_speed = rows_read_inc / watch_per_query.elapsedSeconds();
        double new_bytes_speed = bytes_read_inc / watch_per_query.elapsedSeconds();

        /// Update rows speed
        update_max_speed(new_rows_speed, max_rows_speed_watch, max_rows_speed);
        update_average_speed(new_rows_speed,
            avg_rows_speed_watch,
            number_of_rows_speed_info_batches,
            avg_rows_speed_precision,
            avg_rows_speed_first,
            avg_rows_speed_value);
        /// Update bytes speed
        update_max_speed(new_bytes_speed, max_bytes_speed_watch, max_bytes_speed);
        update_average_speed(new_bytes_speed,
            avg_bytes_speed_watch,
            number_of_bytes_speed_info_batches,
            avg_bytes_speed_precision,
            avg_bytes_speed_first,
            avg_bytes_speed_value);
    }

    void updateQueryInfo()
    {
        ++queries;
        sampler.insert(watch_per_query.elapsedSeconds());
        update_min_time(watch_per_query.elapsed() / (1000 * 1000)); /// ns to ms
    }

    void setTotalTime()
    {
        total_time = watch.elapsedSeconds();
    }

    void clear()
    {
        watch.restart();
        watch_per_query.restart();
        min_time_watch.restart();
        max_rows_speed_watch.restart();
        max_bytes_speed_watch.restart();
        avg_rows_speed_watch.restart();
        avg_bytes_speed_watch.restart();

        sampler.clear();

        queries = 0;
        rows_read = 0;
        bytes_read = 0;

        min_time = std::numeric_limits<UInt64>::max();
        total_time = 0;
        max_rows_speed = 0;
        max_bytes_speed = 0;
        avg_rows_speed_value = 0;
        avg_bytes_speed_value = 0;
        avg_rows_speed_first = 0;
        avg_bytes_speed_first = 0;
        avg_rows_speed_precision = 0.001;
        avg_bytes_speed_precision = 0.001;
        number_of_rows_speed_info_batches = 0;
        number_of_bytes_speed_info_batches = 0;
    }
};

double Stats::avg_rows_speed_precision = 0.001;
double Stats::avg_bytes_speed_precision = 0.001;

class PerformanceTest
{
public:
    PerformanceTest(
        const String & host_,
        const UInt16 port_,
        const String & default_database_,
        const String & user_,
        const String & password_,
        const bool & lite_output_,
        const std::vector<std::string> & input_files,
        const std::vector<std::string> & tags,
        const std::vector<std::string> & without_tags,
        const std::vector<std::string> & names,
        const std::vector<std::string> & without_names,
        const std::vector<std::string> & names_regexp,
        const std::vector<std::string> & without_names_regexp)
        : connection(host_, port_, default_database_, user_, password_),
          tests_configurations(input_files.size()),
          gotSIGINT(false),
          lite_output(lite_output_)
    {
        if (input_files.size() < 1)
        {
            throw Poco::Exception("No tests were specified", 0);
        }

        std::cerr << std::fixed << std::setprecision(3);
        std::cout << std::fixed << std::setprecision(3);
        readTestsConfiguration(input_files);
    }

private:
    unsigned concurrency;
    std::string test_name;

    using Query = std::string;
    using Queries = std::vector<Query>;
    using QueriesWithIndexes = std::vector<std::pair<Query, size_t>>;
    Queries queries;

    Connection connection;

    using Keys = std::vector<std::string>;

    Settings settings;

    InterruptListener interrupt_listener;

    double average_speed_precision = 0.001;

    using XMLConfiguration = Poco::Util::XMLConfiguration;
    using AbstractConfig = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;
    using Config = Poco::AutoPtr<XMLConfiguration>;
    using Paths = std::vector<std::string>;
    using StringToVector = std::map<std::string, std::vector<std::string>>;
    StringToVector substitutions;
    std::vector<Config> tests_configurations;

    using StringKeyValue = std::map<std::string, std::string>;
    std::vector<StringKeyValue> substitutions_maps;

    bool gotSIGINT;
    std::vector<StopCriterions> stop_criterions;
    std::string main_metric;
    bool lite_output;

// TODO: create enum class instead of string
#define incFulfilledCriterions(index, CRITERION)                                                            \
    if (!stop_criterions[index].CRITERION.fulfilled)                                                         \
    {                                                                                                       \
        stop_criterions[index].CRITERION.priority == min ? ++stop_criterions[index].fulfilled_criterions_min  \
                                                        : ++stop_criterions[index].fulfilled_criterions_max; \
        stop_criterions[index].CRITERION.fulfilled = true;                                                   \
    }

    enum ExecutionType
    {
        loop,
        once
    };
    ExecutionType exec_type;

    size_t times_to_run = 1;
    std::vector<Stats> statistics;

    void readTestsConfiguration(const Paths & input_files)
    {
        tests_configurations.resize(input_files.size());

        for (size_t i = 0; i != input_files.size(); ++i)
        {
            const std::string path = input_files[i];
            tests_configurations[i] = Config(new XMLConfiguration(path));
        }

        // TODO: here will be tests filter on tags, names, regexp matching, etc.
        // { ... }

        // for now let's launch one test only
        if (tests_configurations.size())
        {
            for (auto & test_config : tests_configurations)
            {
                runTest(test_config);
            }
        }
    }

    void runTest(Config & test_config)
    {
        test_name = test_config->getString("name");
        std::cout << "Running: " << test_name << "\n";

        /// Preprocess configuration file
        if (test_config->has("settings"))
        {
            Keys config_settings;
            test_config->keys("settings", config_settings);

            /// This macro goes through all settings in the Settings.h
            /// and, if found any settings in test's xml configuration
            /// with the same name, sets its value to settings
            std::vector<std::string>::iterator it;
#define EXTRACT_SETTING(TYPE, NAME, DEFAULT)                             \
    it = std::find(config_settings.begin(), config_settings.end(), #NAME); \
    if (it != config_settings.end())                                      \
        settings.set(#NAME, test_config->getString("settings." #NAME));
            APPLY_FOR_SETTINGS(EXTRACT_SETTING)
            APPLY_FOR_LIMITS(EXTRACT_SETTING)
#undef EXTRACT_SETTING

            if (std::find(config_settings.begin(), config_settings.end(), "profile") != config_settings.end())
            {
                // TODO: proceed profile settings in a proper way
            }

            if (std::find(config_settings.begin(), config_settings.end(), "average_rows_speed_precision") != config_settings.end())
            {
                Stats::avg_rows_speed_precision = test_config->getDouble("settings.average_rows_speed_precision");
            }

            if (std::find(config_settings.begin(), config_settings.end(), "average_bytes_speed_precision") != config_settings.end())
            {
                Stats::avg_bytes_speed_precision = test_config->getDouble("settings.average_bytes_speed_precision");
            }
        }

        Query query;

        if (!test_config->has("query"))
        {
            throw Poco::Exception("Missing query field in test's config: " + test_name, 1);
        }

        query = test_config->getString("query");

        if (query.empty())
        {
            throw Poco::Exception("The query is empty in test's config: " + test_name, 1);
        }

        if (test_config->has("substitutions"))
        {
            /// Make "subconfig" of inner xml block
            AbstractConfig substitutions_view(test_config->createView("substitutions"));
            constructSubstitutions(substitutions_view, substitutions);

            queries = formatQueries(query, substitutions);
        }
        else
        {
            // TODO: probably it will be a good practice to check if
            // query string has {substitution pattern}, but no substitution field
            // was found in xml configuration

            queries.push_back(query);
        }

        if (!test_config->has("type"))
        {
            throw Poco::Exception("Missing type property in config: " + test_name, 1);
        }

        std::string config_exec_type = test_config->getString("type");
        if (config_exec_type == "loop")
            exec_type = loop;
        else if (config_exec_type == "once")
            exec_type = once;
        else
            throw Poco::Exception("Unknown type " + config_exec_type + " in :" + test_name, 1);

        if (test_config->has("times_to_run"))
        {
            times_to_run = test_config->getUInt("times_to_run");
        }

        stop_criterions.resize(times_to_run * queries.size());

        if (test_config->has("stop"))
        {
            AbstractConfig stop_criterions_view(test_config->createView("stop"));
            for (StopCriterions & stop_criterion : stop_criterions)
            {
                stop_criterion.loadFromConfig(stop_criterions_view);
            }
        }
        else
        {
            throw Poco::Exception("No termination conditions were found in config", 1);
        }

        AbstractConfig metrics_view(test_config->createView("metrics"));
        Keys metrics;
        metrics_view->keys(metrics);
        main_metric = test_config->getString("main_metric", "");

        if (!main_metric.empty()) {
            if (std::find(metrics.begin(), metrics.end(), main_metric) == metrics.end())
                metrics.push_back(main_metric);
        } else {
            if (lite_output)
                throw Poco::Exception("Specify main_metric for lite output", 1);
        }

        if (metrics.size() > 0)
            checkMetricsInput(metrics);

        statistics.resize(times_to_run * queries.size());
        for (size_t number_of_launch = 0; number_of_launch < times_to_run; ++number_of_launch)
        {
            QueriesWithIndexes queries_with_indexes;

            for (size_t query_index = 0; query_index < queries.size(); ++query_index)
            {
                size_t statistic_index = number_of_launch * queries.size() + query_index;
                stop_criterions[statistic_index].reset();

                queries_with_indexes.push_back({queries[query_index], statistic_index});
            }

            if (interrupt_listener.check())
                gotSIGINT = true;

            if (gotSIGINT)
                break;

            runQueries(queries_with_indexes);
        }

        if (lite_output)
            minOutput(main_metric);
        else
            constructTotalInfo();
    }

    void checkMetricsInput(const Strings & metrics) const
    {
        std::vector<std::string> loop_metrics
            = {"min_time", "quantiles", "total_time", "queries_per_second", "rows_per_second", "bytes_per_second"};

        std::vector<std::string> non_loop_metrics
            = {"max_rows_per_second", "max_bytes_per_second", "avg_rows_per_second", "avg_bytes_per_second"};

        if (exec_type == loop)
        {
            for (const std::string & metric : metrics)
            {
                if (std::find(non_loop_metrics.begin(), non_loop_metrics.end(), metric) != non_loop_metrics.end())
                {
                    throw Poco::Exception("Wrong type of metric for loop execution type (" + metric + ")", 1);
                }
            }
        }
        else
        {
            for (const std::string & metric : metrics)
            {
                if (std::find(loop_metrics.begin(), loop_metrics.end(), metric) != loop_metrics.end())
                {
                    throw Poco::Exception("Wrong type of metric for non-loop execution type (" + metric + ")", 1);
                }
            }
        }
    }

    void runQueries(const QueriesWithIndexes & queries_with_indexes)
    {
        for (const std::pair<Query, const size_t> & query_and_index : queries_with_indexes)
        {
            Query query = query_and_index.first;
            const size_t statistic_index = query_and_index.second;

            size_t max_iterations = stop_criterions[statistic_index].iterations.value;
            size_t iteration = 0;

            statistics[statistic_index].clear();
            execute(query, statistic_index);

            if (exec_type == loop)
            {
                while (!gotSIGINT)
                {
                    ++iteration;

                    /// check stop criterions
                    if (max_iterations && iteration >= max_iterations)
                    {
                        incFulfilledCriterions(statistic_index, iterations);
                    }

                    if (stop_criterions[statistic_index].number_of_initialized_min
                        && (stop_criterions[statistic_index].fulfilled_criterions_min
                               >= stop_criterions[statistic_index].number_of_initialized_min))
                    {
                        /// All 'min' criterions are fulfilled
                        break;
                    }

                    if (stop_criterions[statistic_index].number_of_initialized_max && stop_criterions[statistic_index].fulfilled_criterions_max)
                    {
                        /// Some 'max' criterions are fulfilled
                        break;
                    }

                    execute(query, statistic_index);
                }
            }

            if (!gotSIGINT)
            {
                statistics[statistic_index].ready = true;
            }
        }
    }

    void execute(const Query & query, const size_t statistic_index)
    {
        statistics[statistic_index].watch_per_query.restart();

        RemoteBlockInputStream stream(connection, query, &settings, nullptr, Tables() /*, query_processing_stage*/);

        Progress progress;
        stream.setProgressCallback([&progress, &stream, statistic_index, this](const Progress & value) {
            progress.incrementPiecewiseAtomically(value);

            this->checkFulfilledCriterionsAndUpdate(progress, stream, statistic_index);
        });

        stream.readPrefix();
        while (Block block = stream.read())
            ;
        stream.readSuffix();

        statistics[statistic_index].updateQueryInfo();
        statistics[statistic_index].setTotalTime();
    }

    void checkFulfilledCriterionsAndUpdate(const Progress & progress,
        RemoteBlockInputStream & stream,
        const size_t statistic_index)
    {
        statistics[statistic_index].add(progress.rows, progress.bytes);

        size_t max_rows_to_read = stop_criterions[statistic_index].rows_read.value;
        if (max_rows_to_read && statistics[statistic_index].rows_read >= max_rows_to_read)
        {
            incFulfilledCriterions(statistic_index, rows_read);
        }

        size_t max_bytes_to_read = stop_criterions[statistic_index].bytes_read_uncompressed.value;
        if (max_bytes_to_read && statistics[statistic_index].bytes_read >= max_bytes_to_read)
        {
            incFulfilledCriterions(statistic_index, bytes_read_uncompressed);
        }

        if (UInt64 max_timeout_ms = stop_criterions[statistic_index].timeout_ms.value)
        {
            /// cast nanoseconds to ms
            if ((statistics[statistic_index].watch.elapsed() / (1000 * 1000)) > max_timeout_ms)
            {
                incFulfilledCriterions(statistic_index, timeout_ms);
            }
        }

        size_t min_time_not_changing_for_ms = stop_criterions[statistic_index].min_time_not_changing_for_ms.value;
        if (min_time_not_changing_for_ms)
        {
            size_t min_time_did_not_change_for = statistics[statistic_index].min_time_watch.elapsed() / (1000 * 1000);

            if (min_time_did_not_change_for >= min_time_not_changing_for_ms)
            {
                incFulfilledCriterions(statistic_index, min_time_not_changing_for_ms);
            }
        }

        size_t max_speed_not_changing_for_ms = stop_criterions[statistic_index].max_speed_not_changing_for_ms.value;
        if (max_speed_not_changing_for_ms)
        {
            UInt64 speed_not_changing_time = statistics[statistic_index].max_rows_speed_watch.elapsed() / (1000 * 1000);
            if (speed_not_changing_time >= max_speed_not_changing_for_ms)
            {
                incFulfilledCriterions(statistic_index, max_speed_not_changing_for_ms);
            }
        }

        size_t average_speed_not_changing_for_ms = stop_criterions[statistic_index].average_speed_not_changing_for_ms.value;
        if (average_speed_not_changing_for_ms)
        {
            UInt64 speed_not_changing_time = statistics[statistic_index].avg_rows_speed_watch.elapsed() / (1000 * 1000);
            if (speed_not_changing_time >= average_speed_not_changing_for_ms)
            {
                incFulfilledCriterions(statistic_index, average_speed_not_changing_for_ms);
            }
        }

        if (stop_criterions[statistic_index].number_of_initialized_min
            && (stop_criterions[statistic_index].fulfilled_criterions_min >= stop_criterions[statistic_index].number_of_initialized_min))
        {
            /// All 'min' criterions are fulfilled
            stream.cancel();
        }

        if (stop_criterions[statistic_index].number_of_initialized_max && stop_criterions[statistic_index].fulfilled_criterions_max)
        {
            /// Some 'max' criterions are fulfilled
            stream.cancel();
        }

        if (interrupt_listener.check())
        {
            gotSIGINT = true;
            stream.cancel();
        }
    }

    void constructSubstitutions(AbstractConfig & substitutions_view, StringToVector & substitutions)
    {
        Keys xml_substitutions;
        substitutions_view->keys(xml_substitutions);

        for (size_t i = 0; i != xml_substitutions.size(); ++i)
        {
            const AbstractConfig xml_substitution(substitutions_view->createView("substitution[" + std::to_string(i) + "]"));

            /// Property values for substitution will be stored in a vector
            /// accessible by property name
            std::vector<std::string> xml_values;
            xml_substitution->keys("values", xml_values);

            std::string name = xml_substitution->getString("name");

            for (size_t j = 0; j != xml_values.size(); ++j)
            {
                substitutions[name].push_back(xml_substitution->getString("values.value[" + std::to_string(j) + "]"));
            }
        }
    }

    std::vector<std::string> formatQueries(const std::string & query, StringToVector substitutions)
    {
        std::vector<std::string> queries;

        StringToVector::iterator substitutions_first = substitutions.begin();
        StringToVector::iterator substitutions_last = substitutions.end();
        --substitutions_last;

        std::map<std::string, std::string> substitutions_map;

        runThroughAllOptionsAndPush(substitutions_first, substitutions_last, query, queries, substitutions_map);

        return queries;
    }

    /// Recursive method which goes through all substitution blocks in xml
    /// and replaces property {names} by their values
    void runThroughAllOptionsAndPush(StringToVector::iterator substitutions_left,
        StringToVector::iterator substitutions_right,
        const std::string & template_query,
        std::vector<std::string> & queries,
        const StringKeyValue & template_substitutions_map = StringKeyValue())
    {
        std::string name = substitutions_left->first;
        std::vector<std::string> values = substitutions_left->second;

        for (const std::string & value : values)
        {
            /// Copy query string for each unique permutation
            Query query = template_query;
            StringKeyValue substitutions_map = template_substitutions_map;
            size_t substr_pos = 0;

            while (substr_pos != std::string::npos)
            {
                substr_pos = query.find("{" + name + "}");

                if (substr_pos != std::string::npos)
                {
                    query.replace(substr_pos, 1 + name.length() + 1, value);
                }
            }

            substitutions_map[name] = value;

            /// If we've reached the end of substitution chain
            if (substitutions_left == substitutions_right)
            {
                queries.push_back(query);
                substitutions_maps.push_back(substitutions_map);
            }
            else
            {
                StringToVector::iterator next_it = substitutions_left;
                ++next_it;

                runThroughAllOptionsAndPush(next_it, substitutions_right, query, queries, substitutions_map);
            }
        }
    }

public:
    void constructTotalInfo()
    {
        JSONString json_output;
        std::string hostname;

        char hostname_buffer[256];
        if (gethostname(hostname_buffer, 256) == 0)
        {
            hostname = std::string(hostname_buffer);
        }

        json_output["hostname"].set(hostname);
        json_output["Number of CPUs"].set(sysconf(_SC_NPROCESSORS_ONLN));
        json_output["test_name"].set(test_name);
        json_output["main_metric"].set(main_metric);

        if (substitutions.size())
        {
            JSONString json_parameters(2); /// here, 2 is the size of \t padding

            for (auto it = substitutions.begin(); it != substitutions.end(); ++it)
            {
                std::string parameter = it->first;
                std::vector<std::string> values = it->second;

                std::string array_string = "[";
                for (size_t i = 0; i != values.size(); ++i)
                {
                    array_string += '\"' + values[i] + '\"';
                    if (i != values.size() - 1)
                    {
                        array_string += ", ";
                    }
                }
                array_string += ']';

                json_parameters[parameter].set(array_string);
            }

            json_output["parameters"].set(json_parameters);
        }

        std::vector<JSONString> run_infos;
        for (size_t query_index = 0; query_index < queries.size(); ++query_index)
        {
            for (size_t number_of_launch = 0; number_of_launch < statistics.size(); ++number_of_launch)
            {
                if (!statistics[number_of_launch].ready)
                    continue;

                JSONString runJSON;

                if (substitutions_maps.size())
                {
                    JSONString parameters(4);

                    for (auto it = substitutions_maps[query_index].begin(); it != substitutions_maps[query_index].end(); ++it)
                    {
                        parameters[it->first].set(it->second);
                    }

                    runJSON["parameters"].set(parameters);
                }

                if (exec_type == loop)
                {
                    /// in seconds
                    runJSON["min_time"].set(statistics[number_of_launch].min_time / double(1000));

                    JSONString quantiles(4); /// here, 4 is the size of \t padding
                    for (double percent = 10; percent <= 90; percent += 10)
                    {
                        quantiles[percent / 100].set(statistics[number_of_launch].sampler.quantileInterpolated(percent / 100.0));
                    }
                    quantiles[0.95].set(statistics[number_of_launch].sampler.quantileInterpolated(95 / 100.0));
                    quantiles[0.99].set(statistics[number_of_launch].sampler.quantileInterpolated(99 / 100.0));
                    quantiles[0.999].set(statistics[number_of_launch].sampler.quantileInterpolated(99.9 / 100.0));
                    quantiles[0.9999].set(statistics[number_of_launch].sampler.quantileInterpolated(99.99 / 100.0));

                    runJSON["quantiles"].set(quantiles);

                    runJSON["total_time"].set(statistics[number_of_launch].total_time);
                    runJSON["queries_per_second"].set(double(statistics[number_of_launch].queries) / statistics[number_of_launch].total_time);
                    runJSON["rows_per_second"].set(double(statistics[number_of_launch].rows_read) / statistics[number_of_launch].total_time);
                    runJSON["bytes_per_second"].set(double(statistics[number_of_launch].bytes_read) / statistics[number_of_launch].total_time);
                }
                else
                {
                    runJSON["max_rows_per_second"].set(statistics[number_of_launch].max_rows_speed);
                    runJSON["max_bytes_per_second"].set(statistics[number_of_launch].max_bytes_speed);
                    runJSON["avg_rows_per_second"].set(statistics[number_of_launch].avg_rows_speed_value);
                    runJSON["avg_bytes_per_second"].set(statistics[number_of_launch].avg_bytes_speed_value);
                }

                run_infos.push_back(runJSON);
            }
        }

        json_output["runs"].set(run_infos);

        std::cout << std::endl << json_output << std::endl;
    }

    void minOutput(const std::string & main_metric)
    {
        for (size_t query_index = 0; query_index < queries.size(); ++query_index)
        {
            for (size_t number_of_launch = 0; number_of_launch < times_to_run; ++number_of_launch)
            {
                std::cout << test_name << ", ";

                if (substitutions_maps.size())
                {
                    for (auto it = substitutions_maps[query_index].begin(); it != substitutions_maps[query_index].end(); ++it)
                    {
                        std::cout << it->first << " = " << it->second << ", ";
                    }
                }

                std::cout << "run " << number_of_launch + 1 << ": ";
                std::cout << main_metric << " = ";
                std::cout << statistics[number_of_launch * queries.size() + query_index].getStatisticByName(main_metric);
                std::cout << std::endl;
            }
        }
    }
};
}


int mainEntryClickhousePerformanceTest(int argc, char ** argv)
{
    using namespace DB;

    try
    {
        using boost::program_options::value;
        using Strings = std::vector<std::string>;

        boost::program_options::options_description desc("Allowed options");
        desc.add_options()
            ("help",                                                                  "produce help message")
            ("lite",                                                                  "use lite version of output")
            ("host,h",              value<std::string>()->default_value("localhost"), "")
            ("port",                value<UInt16>()->default_value(9000),             "")
            ("user",                value<std::string>()->default_value("default"),   "")
            ("password",            value<std::string>()->default_value(""),          "")
            ("database",            value<std::string>()->default_value("default"),   "")
            ("tag",                 value<Strings>(),                                 "Run only tests with tag")
            ("without-tag",         value<Strings>(),                                 "Do not run tests with tag")
            ("name",                value<Strings>(),                                 "Run tests with specific name")
            ("without-name",        value<Strings>(),                                 "Do not run tests with name")
            ("name-regexp",         value<Strings>(),                                 "Run tests with names matching regexp")
            ("without-name-regexp", value<Strings>(),                                 "Do not run tests with names matching regexp");

        /// These options will not be displayed in --help
        boost::program_options::options_description hidden("Hidden options");
        hidden.add_options()("input-files", value<std::vector<std::string>>(), "");

        /// But they will be legit, though. And they must be given without name
        boost::program_options::positional_options_description positional;
        positional.add("input-files", -1);

        boost::program_options::options_description cmdline_options;
        cmdline_options.add(desc).add(hidden);

        boost::program_options::variables_map options;
        boost::program_options::store(
            boost::program_options::command_line_parser(argc, argv).options(cmdline_options).positional(positional).run(), options);
        boost::program_options::notify(options);

        if (options.count("help"))
        {
            std::cout << "Usage: " << argv[0] << " [options] [test_file ...] [tests_folder]\n";
            std::cout << desc << "\n";
            return 0;
        }

        if (!options.count("input-files"))
        {
            std::cerr << "No tests files were specified. See --help"
                      << "\n";
            return 1;
        }

        Strings tests_tags;
        Strings skip_tags;
        Strings tests_names;
        Strings skip_names;
        Strings name_regexp;
        Strings skip_matching_regexp;

        if (options.count("tag"))
        {
            tests_tags = options["tag"].as<Strings>();
        }

        if (options.count("without-tag"))
        {
            skip_tags = options["without-tag"].as<Strings>();
        }

        if (options.count("name"))
        {
            tests_names = options["name"].as<Strings>();
        }

        if (options.count("without-name"))
        {
            skip_names = options["without-name"].as<Strings>();
        }

        if (options.count("name-regexp"))
        {
            name_regexp = options["name-regexp"].as<Strings>();
        }

        if (options.count("without-name-regexp"))
        {
            skip_matching_regexp = options["without-name-regexp"].as<Strings>();
        }

        PerformanceTest performanceTest(
            options["host"].as<std::string>(),
            options["port"].as<UInt16>(),
            options["database"].as<std::string>(),
            options["user"].as<std::string>(),
            options["password"].as<std::string>(),
            options.count("lite") > 0,
            options["input-files"].as<Strings>(),
            tests_tags,
            skip_tags,
            tests_names,
            skip_names,
            name_regexp,
            skip_matching_regexp);
    }
    catch (const Exception & e)
    {
        std::string text = e.displayText();

        std::cerr << "Code: " << e.code() << ". " << text << "\n\n";

        /// Если есть стек-трейс на сервере, то не будем писать стек-трейс на клиенте.
        if (std::string::npos == text.find("Stack trace"))
            std::cerr << "Stack trace:\n" << e.getStackTrace().toString();

        return e.code();
    }
    catch (const Poco::Exception & e)
    {
        std::cerr << "Poco::Exception: " << e.displayText() << "\n";
        return ErrorCodes::POCO_EXCEPTION;
    }
    catch (const std::exception & e)
    {
        std::cerr << "std::exception: " << e.what() << "\n";
        return ErrorCodes::STD_EXCEPTION;
    }
    catch (...)
    {
        std::cerr << "Unknown exception\n";
        return ErrorCodes::UNKNOWN_EXCEPTION;
    }

    return 0;
}
