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

const std::string four_spaces = "    ";

bool isNumber(const std::string & str)
{
    if (str.empty())
    {
        return false;
    }

    size_t dotsCounter = 0;

    if (str[0] == '.' || str[str.size() - 1] == '.')
    {
        return false;
    }

    for (char chr : str)
    {
        if (chr == '.')
        {
            if (dotsCounter)
                return false;
            else
                ++dotsCounter;
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

    void set(const JSONString & innerJSON)
    {
        set(innerJSON.constructOutput());
    }

    void set(const std::vector<JSONString> & runInfos)
    {
        if (current_key.empty())
        {
            throw "cannot use set without key";
        }

        content[current_key] = "[\n";

        for (size_t i = 0; i < runInfos.size(); ++i)
        {
            for (size_t i = 0; i < _padding + 1; ++i)
            {
                content[current_key] += four_spaces;
            }
            content[current_key] += runInfos[i].constructOutput(_padding + 2);

            if (i != runInfos.size() - 1)
            {
                content[current_key] += ',';
            }

            content[current_key] += "\n";
        }

        for (size_t i = 0; i < _padding; ++i)
        {
            content[current_key] += four_spaces;
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
                output += four_spaces;
            }

            std::string key = '\"' + it->first + '\"';
            std::string value = it->second;

            output += key + ": " + value;
        }

        output += "\n";
        for (size_t i = 0; i < padding - 1; ++i)
        {
            output += four_spaces;
        }
        output += "}";
        return output;
    }
};

std::ostream & operator<<(std::ostream & stream, const JSONString & jsonObj)
{
    stream << jsonObj.constructOutput();

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

    void initializeStruct(const std::string & priority, const AbstractConfiguration & stopCriterionsView)
    {
        Keys keys;
        stopCriterionsView->keys(priority, keys);

        PriorityType priorityType = (priority == "min" ? min : max);

        for (const std::string & key : keys)
        {
            if (key == "timeout_ms")
            {
                timeout_ms.value = stopCriterionsView->getUInt64(priority + ".timeout_ms");
                timeout_ms.priority = priorityType;
            }
            else if (key == "rows_read")
            {
                rows_read.value = stopCriterionsView->getUInt64(priority + ".rows_read");
                rows_read.priority = priorityType;
            }
            else if (key == "bytes_read_uncompressed")
            {
                bytes_read_uncompressed.value = stopCriterionsView->getUInt64(priority + ".bytes_read_uncompressed");
                bytes_read_uncompressed.priority = priorityType;
            }
            else if (key == "iterations")
            {
                iterations.value = stopCriterionsView->getUInt64(priority + ".iterations");
                iterations.priority = priorityType;
            }
            else if (key == "min_time_not_changing_for_ms")
            {
                min_time_not_changing_for_ms.value = stopCriterionsView->getUInt64(priority + ".min_time_not_changing_for_ms");
                min_time_not_changing_for_ms.priority = priorityType;
            }
            else if (key == "max_speed_not_changing_for_ms")
            {
                max_speed_not_changing_for_ms.value = stopCriterionsView->getUInt64(priority + ".max_speed_not_changing_for_ms");
                max_speed_not_changing_for_ms.priority = priorityType;
            }
            else if (key == "average_speed_not_changing_for_ms")
            {
                average_speed_not_changing_for_ms.value = stopCriterionsView->getUInt64(priority + ".average_speed_not_changing_for_ms");
                average_speed_not_changing_for_ms.priority = priorityType;
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

    StopCriterions(const StopCriterions & anotherCriterions)
        : timeout_ms(anotherCriterions.timeout_ms),
          rows_read(anotherCriterions.rows_read),
          bytes_read_uncompressed(anotherCriterions.bytes_read_uncompressed),
          iterations(anotherCriterions.iterations),
          min_time_not_changing_for_ms(anotherCriterions.min_time_not_changing_for_ms),
          max_speed_not_changing_for_ms(anotherCriterions.max_speed_not_changing_for_ms),
          average_speed_not_changing_for_ms(anotherCriterions.average_speed_not_changing_for_ms),

          number_of_initialized_min(anotherCriterions.number_of_initialized_min),
          number_of_initialized_max(anotherCriterions.number_of_initialized_max),
          fulfilled_criterions_min(anotherCriterions.fulfilled_criterions_min),
          fulfilled_criterions_max(anotherCriterions.fulfilled_criterions_max)
    {
    }

    void loadFromConfig(const AbstractConfiguration & stopCriterionsView)
    {
        if (stopCriterionsView->has("min"))
        {
            initializeStruct("min", stopCriterionsView);
        }

        if (stopCriterionsView->has("max"))
        {
            initializeStruct("max", stopCriterionsView);
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

    std::string getStatisticByName(const std::string & statisticName)
    {
        if (statisticName == "min_time")
        {
            return std::to_string(min_time) + "ms";
        }
        if (statisticName == "quantiles")
        {
            std::string result = "\n";

            for (double percent = 10; percent <= 90; percent += 10)
            {
                result += four_spaces + std::to_string((percent / 100));
                result += ": " + std::to_string(sampler.quantileInterpolated(percent / 100.0));
                result += "\n";
            }
            result += four_spaces + "0.95:   " + std::to_string(sampler.quantileInterpolated(95 / 100.0)) + "\n";
            result += four_spaces + "0.99: "   + std::to_string(sampler.quantileInterpolated(99 / 100.0)) + "\n";
            result += four_spaces + "0.999: "  + std::to_string(sampler.quantileInterpolated(99.9 / 100.)) + "\n";
            result += four_spaces + "0.9999: " + std::to_string(sampler.quantileInterpolated(99.99 / 100.));

            return result;
        }
        if (statisticName == "total_time")
        {
            return std::to_string(total_time) + "s";
        }
        if (statisticName == "queries_per_second")
        {
            return std::to_string(queries / total_time);
        }
        if (statisticName == "rows_per_second")
        {
            return std::to_string(rows_read / total_time);
        }
        if (statisticName == "bytes_per_second")
        {
            return std::to_string(bytes_read / total_time);
        }

        if (statisticName == "max_rows_per_second")
        {
            return std::to_string(max_rows_speed);
        }
        if (statisticName == "max_bytes_per_second")
        {
            return std::to_string(max_bytes_speed);
        }
        if (statisticName == "avg_rows_per_second")
        {
            return std::to_string(avg_rows_speed_value);
        }
        if (statisticName == "avg_bytes_per_second")
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
          testsConfigurations(input_files.size()),
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
    std::string testName;

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
    std::vector<Config> testsConfigurations;

    using StringKeyValue = std::map<std::string, std::string>;
    std::vector<StringKeyValue> substitutionsMaps;

    bool gotSIGINT;
    std::vector<StopCriterions> stopCriterions;
    std::string main_metric;
    bool lite_output;

// TODO: create enum class instead of string
#define incFulfilledCriterions(index, CRITERION)                                                            \
    if (!stopCriterions[index].CRITERION.fulfilled)                                                         \
    {                                                                                                       \
        stopCriterions[index].CRITERION.priority == min ? ++stopCriterions[index].fulfilled_criterions_min  \
                                                        : ++stopCriterions[index].fulfilled_criterions_max; \
        stopCriterions[index].CRITERION.fulfilled = true;                                                   \
    }

    enum ExecutionType
    {
        loop,
        once
    };
    ExecutionType execType;

    size_t times_to_run = 1;
    std::vector<Stats> statistics;

    void readTestsConfiguration(const Paths & input_files)
    {
        testsConfigurations.resize(input_files.size());

        for (size_t i = 0; i != input_files.size(); ++i)
        {
            const std::string path = input_files[i];
            testsConfigurations[i] = Config(new XMLConfiguration(path));
        }

        // TODO: here will be tests filter on tags, names, regexp matching, etc.
        // { ... }

        // for now let's launch one test only
        if (testsConfigurations.size())
        {
            for (auto & testConfig : testsConfigurations)
            {
                runTest(testConfig);
            }
        }
    }

    void runTest(Config & testConfig)
    {
        testName = testConfig->getString("name");
        std::cout << "Running: " << testName << "\n";

        /// Preprocess configuration file
        if (testConfig->has("settings"))
        {
            Keys configSettings;
            testConfig->keys("settings", configSettings);

            /// This macro goes through all settings in the Settings.h
            /// and, if found any settings in test's xml configuration
            /// with the same name, sets its value to settings
            std::vector<std::string>::iterator it;
#define EXTRACT_SETTING(TYPE, NAME, DEFAULT)                             \
    it = std::find(configSettings.begin(), configSettings.end(), #NAME); \
    if (it != configSettings.end())                                      \
        settings.set(#NAME, testConfig->getString("settings." #NAME));
            APPLY_FOR_SETTINGS(EXTRACT_SETTING)
            APPLY_FOR_LIMITS(EXTRACT_SETTING)
#undef EXTRACT_SETTING

            if (std::find(configSettings.begin(), configSettings.end(), "profile") != configSettings.end())
            {
                // TODO: proceed profile settings in a proper way
            }

            if (std::find(configSettings.begin(), configSettings.end(), "average_rows_speed_precision") != configSettings.end())
            {
                Stats::avg_rows_speed_precision = testConfig->getDouble("settings.average_rows_speed_precision");
            }

            if (std::find(configSettings.begin(), configSettings.end(), "average_bytes_speed_precision") != configSettings.end())
            {
                Stats::avg_bytes_speed_precision = testConfig->getDouble("settings.average_bytes_speed_precision");
            }
        }

        Query query;

        if (!testConfig->has("query"))
        {
            throw Poco::Exception("Missing query field in test's config: " + testName, 1);
        }

        query = testConfig->getString("query");

        if (query.empty())
        {
            throw Poco::Exception("The query is empty in test's config: " + testName, 1);
        }

        if (testConfig->has("substitutions"))
        {
            /// Make "subconfig" of inner xml block
            AbstractConfig substitutionsView(testConfig->createView("substitutions"));
            constructSubstitutions(substitutionsView, substitutions);

            queries = formatQueries(query, substitutions);
        }
        else
        {
            // TODO: probably it will be a good practice to check if
            // query string has {substitution pattern}, but no substitution field
            // was found in xml configuration

            queries.push_back(query);
        }

        if (!testConfig->has("type"))
        {
            throw Poco::Exception("Missing type property in config: " + testName, 1);
        }

        std::string configExecType = testConfig->getString("type");
        if (configExecType == "loop")
            execType = loop;
        else if (configExecType == "once")
            execType = once;
        else
            throw Poco::Exception("Unknown type " + configExecType + " in :" + testName, 1);

        if (testConfig->has("times_to_run"))
        {
            times_to_run = testConfig->getUInt("times_to_run");
        }

        stopCriterions.resize(times_to_run * queries.size());

        if (testConfig->has("stop"))
        {
            AbstractConfig stopCriterionsView(testConfig->createView("stop"));
            for (StopCriterions & stopCriterion : stopCriterions)
            {
                stopCriterion.loadFromConfig(stopCriterionsView);
            }
        }
        else
        {
            throw Poco::Exception("No termination conditions were found in config", 1);
        }

        AbstractConfig metricsView(testConfig->createView("metrics"));
        Keys metrics;
        metricsView->keys(metrics);
        main_metric = testConfig->getString("main_metric", "");

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
        for (size_t numberOfLaunch = 0; numberOfLaunch < times_to_run; ++numberOfLaunch)
        {
            QueriesWithIndexes queriesWithIndexes;

            for (size_t queryIndex = 0; queryIndex < queries.size(); ++queryIndex)
            {
                size_t statisticIndex = numberOfLaunch * queries.size() + queryIndex;
                stopCriterions[statisticIndex].reset();

                queriesWithIndexes.push_back({queries[queryIndex], statisticIndex});
            }

            if (interrupt_listener.check())
                gotSIGINT = true;

            if (gotSIGINT)
                break;

            runQueries(queriesWithIndexes);
        }

        if (lite_output)
            minOutput(main_metric);
        else
            constructTotalInfo();
    }

    void checkMetricsInput(const Strings & metrics) const
    {
        std::vector<std::string> loopMetrics
            = {"min_time", "quantiles", "total_time", "queries_per_second", "rows_per_second", "bytes_per_second"};

        std::vector<std::string> nonLoopMetrics
            = {"max_rows_per_second", "max_bytes_per_second", "avg_rows_per_second", "avg_bytes_per_second"};

        if (execType == loop)
        {
            for (const std::string & metric : metrics)
            {
                if (std::find(nonLoopMetrics.begin(), nonLoopMetrics.end(), metric) != nonLoopMetrics.end())
                {
                    throw Poco::Exception("Wrong type of metric for loop execution type (" + metric + ")", 1);
                }
            }
        }
        else
        {
            for (const std::string & metric : metrics)
            {
                if (std::find(loopMetrics.begin(), loopMetrics.end(), metric) != loopMetrics.end())
                {
                    throw Poco::Exception("Wrong type of metric for non-loop execution type (" + metric + ")", 1);
                }
            }
        }
    }

    void runQueries(const QueriesWithIndexes & queriesWithIndexes)
    {
        for (const std::pair<Query, const size_t> & queryAndIndex : queriesWithIndexes)
        {
            Query query = queryAndIndex.first;
            const size_t statisticIndex = queryAndIndex.second;

            size_t max_iterations = stopCriterions[statisticIndex].iterations.value;
            size_t iteration = 0;

            statistics[statisticIndex].clear();
            execute(query, statisticIndex);

            if (execType == loop)
            {
                while (!gotSIGINT)
                {
                    ++iteration;

                    /// check stop criterions
                    if (max_iterations && iteration >= max_iterations)
                    {
                        incFulfilledCriterions(statisticIndex, iterations);
                    }

                    if (stopCriterions[statisticIndex].number_of_initialized_min
                        && (stopCriterions[statisticIndex].fulfilled_criterions_min
                               >= stopCriterions[statisticIndex].number_of_initialized_min))
                    {
                        /// All 'min' criterions are fulfilled
                        break;
                    }

                    if (stopCriterions[statisticIndex].number_of_initialized_max && stopCriterions[statisticIndex].fulfilled_criterions_max)
                    {
                        /// Some 'max' criterions are fulfilled
                        break;
                    }

                    execute(query, statisticIndex);
                }
            }

            if (!gotSIGINT)
            {
                statistics[statisticIndex].ready = true;
            }
        }
    }

    void execute(const Query & query, const size_t statisticIndex)
    {
        statistics[statisticIndex].watch_per_query.restart();

        RemoteBlockInputStream stream(connection, query, &settings, nullptr, Tables() /*, query_processing_stage*/);

        Progress progress;
        stream.setProgressCallback([&progress, &stream, statisticIndex, this](const Progress & value) {
            progress.incrementPiecewiseAtomically(value);

            this->checkFulfilledCriterionsAndUpdate(progress, stream, statisticIndex);
        });

        stream.readPrefix();
        while (Block block = stream.read())
            ;
        stream.readSuffix();

        statistics[statisticIndex].updateQueryInfo();
        statistics[statisticIndex].setTotalTime();
    }

    void checkFulfilledCriterionsAndUpdate(const Progress & progress,
        RemoteBlockInputStream & stream,
        const size_t statisticIndex)
    {
        statistics[statisticIndex].add(progress.rows, progress.bytes);

        size_t max_rows_to_read = stopCriterions[statisticIndex].rows_read.value;
        if (max_rows_to_read && statistics[statisticIndex].rows_read >= max_rows_to_read)
        {
            incFulfilledCriterions(statisticIndex, rows_read);
        }

        size_t max_bytes_to_read = stopCriterions[statisticIndex].bytes_read_uncompressed.value;
        if (max_bytes_to_read && statistics[statisticIndex].bytes_read >= max_bytes_to_read)
        {
            incFulfilledCriterions(statisticIndex, bytes_read_uncompressed);
        }

        if (UInt64 max_timeout_ms = stopCriterions[statisticIndex].timeout_ms.value)
        {
            /// cast nanoseconds to ms
            if ((statistics[statisticIndex].watch.elapsed() / (1000 * 1000)) > max_timeout_ms)
            {
                incFulfilledCriterions(statisticIndex, timeout_ms);
            }
        }

        size_t min_time_not_changing_for_ms = stopCriterions[statisticIndex].min_time_not_changing_for_ms.value;
        if (min_time_not_changing_for_ms)
        {
            size_t min_time_did_not_change_for = statistics[statisticIndex].min_time_watch.elapsed() / (1000 * 1000);

            if (min_time_did_not_change_for >= min_time_not_changing_for_ms)
            {
                incFulfilledCriterions(statisticIndex, min_time_not_changing_for_ms);
            }
        }

        size_t max_speed_not_changing_for_ms = stopCriterions[statisticIndex].max_speed_not_changing_for_ms.value;
        if (max_speed_not_changing_for_ms)
        {
            UInt64 speed_not_changing_time = statistics[statisticIndex].max_rows_speed_watch.elapsed() / (1000 * 1000);
            if (speed_not_changing_time >= max_speed_not_changing_for_ms)
            {
                incFulfilledCriterions(statisticIndex, max_speed_not_changing_for_ms);
            }
        }

        size_t average_speed_not_changing_for_ms = stopCriterions[statisticIndex].average_speed_not_changing_for_ms.value;
        if (average_speed_not_changing_for_ms)
        {
            UInt64 speed_not_changing_time = statistics[statisticIndex].avg_rows_speed_watch.elapsed() / (1000 * 1000);
            if (speed_not_changing_time >= average_speed_not_changing_for_ms)
            {
                incFulfilledCriterions(statisticIndex, average_speed_not_changing_for_ms);
            }
        }

        if (stopCriterions[statisticIndex].number_of_initialized_min
            && (stopCriterions[statisticIndex].fulfilled_criterions_min >= stopCriterions[statisticIndex].number_of_initialized_min))
        {
            /// All 'min' criterions are fulfilled
            stream.cancel();
        }

        if (stopCriterions[statisticIndex].number_of_initialized_max && stopCriterions[statisticIndex].fulfilled_criterions_max)
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

    void constructSubstitutions(AbstractConfig & substitutionsView, StringToVector & substitutions)
    {
        Keys xml_substitutions;
        substitutionsView->keys(xml_substitutions);

        for (size_t i = 0; i != xml_substitutions.size(); ++i)
        {
            const AbstractConfig xml_substitution(substitutionsView->createView("substitution[" + std::to_string(i) + "]"));

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

        std::map<std::string, std::string> substitutionsMap;

        runThroughAllOptionsAndPush(substitutions_first, substitutions_last, query, queries, substitutionsMap);

        return queries;
    }

    /// Recursive method which goes through all substitution blocks in xml
    /// and replaces property {names} by their values
    void runThroughAllOptionsAndPush(StringToVector::iterator substitutions_left,
        StringToVector::iterator substitutions_right,
        const std::string & template_query,
        std::vector<std::string> & queries,
        const StringKeyValue & templateSubstitutionsMap = StringKeyValue())
    {
        std::string name = substitutions_left->first;
        std::vector<std::string> values = substitutions_left->second;

        for (const std::string & value : values)
        {
            /// Copy query string for each unique permutation
            Query query = template_query;
            StringKeyValue substitutionsMap = templateSubstitutionsMap;
            size_t substrPos = 0;

            while (substrPos != std::string::npos)
            {
                substrPos = query.find("{" + name + "}");

                if (substrPos != std::string::npos)
                {
                    query.replace(substrPos, 1 + name.length() + 1, value);
                }
            }

            substitutionsMap[name] = value;

            /// If we've reached the end of substitution chain
            if (substitutions_left == substitutions_right)
            {
                queries.push_back(query);
                substitutionsMaps.push_back(substitutionsMap);
            }
            else
            {
                StringToVector::iterator next_it = substitutions_left;
                ++next_it;

                runThroughAllOptionsAndPush(next_it, substitutions_right, query, queries, substitutionsMap);
            }
        }
    }

public:
    void constructTotalInfo()
    {
        JSONString jsonOutput;
        std::string hostname;

        char hostname_buffer[256];
        if (gethostname(hostname_buffer, 256) == 0)
        {
            hostname = std::string(hostname_buffer);
        }

        jsonOutput["hostname"].set(hostname);
        jsonOutput["Number of CPUs"].set(sysconf(_SC_NPROCESSORS_ONLN));
        jsonOutput["test_name"].set(testName);
        jsonOutput["main_metric"].set(main_metric);

        if (substitutions.size())
        {
            JSONString jsonParameters(2); /// here, 2 is the size of \t padding

            for (auto it = substitutions.begin(); it != substitutions.end(); ++it)
            {
                std::string parameter = it->first;
                std::vector<std::string> values = it->second;

                std::string arrayString = "[";
                for (size_t i = 0; i != values.size(); ++i)
                {
                    arrayString += '\"' + values[i] + '\"';
                    if (i != values.size() - 1)
                    {
                        arrayString += ", ";
                    }
                }
                arrayString += ']';

                jsonParameters[parameter].set(arrayString);
            }

            jsonOutput["parameters"].set(jsonParameters);
        }

        std::vector<JSONString> runInfos;
        for (size_t queryIndex = 0; queryIndex < queries.size(); ++queryIndex)
        {
            for (size_t numberOfLaunch = 0; numberOfLaunch < statistics.size(); ++numberOfLaunch)
            {
                if (!statistics[numberOfLaunch].ready)
                    continue;

                JSONString runJSON;

                if (substitutionsMaps.size())
                {
                    JSONString parameters(4);

                    for (auto it = substitutionsMaps[queryIndex].begin(); it != substitutionsMaps[queryIndex].end(); ++it)
                    {
                        parameters[it->first].set(it->second);
                    }

                    runJSON["parameters"].set(parameters);
                }

                if (execType == loop)
                {
                    /// in seconds
                    runJSON["min_time"].set(statistics[numberOfLaunch].min_time / double(1000));

                    JSONString quantiles(4); /// here, 4 is the size of \t padding
                    for (double percent = 10; percent <= 90; percent += 10)
                    {
                        quantiles[percent / 100].set(statistics[numberOfLaunch].sampler.quantileInterpolated(percent / 100.0));
                    }
                    quantiles[0.95].set(statistics[numberOfLaunch].sampler.quantileInterpolated(95 / 100.0));
                    quantiles[0.99].set(statistics[numberOfLaunch].sampler.quantileInterpolated(99 / 100.0));
                    quantiles[0.999].set(statistics[numberOfLaunch].sampler.quantileInterpolated(99.9 / 100.0));
                    quantiles[0.9999].set(statistics[numberOfLaunch].sampler.quantileInterpolated(99.99 / 100.0));

                    runJSON["quantiles"].set(quantiles);

                    runJSON["total_time"].set(statistics[numberOfLaunch].total_time);
                    runJSON["queries_per_second"].set(double(statistics[numberOfLaunch].queries) / statistics[numberOfLaunch].total_time);
                    runJSON["rows_per_second"].set(double(statistics[numberOfLaunch].rows_read) / statistics[numberOfLaunch].total_time);
                    runJSON["bytes_per_second"].set(double(statistics[numberOfLaunch].bytes_read) / statistics[numberOfLaunch].total_time);
                }
                else
                {
                    runJSON["max_rows_per_second"].set(statistics[numberOfLaunch].max_rows_speed);
                    runJSON["max_bytes_per_second"].set(statistics[numberOfLaunch].max_bytes_speed);
                    runJSON["avg_rows_per_second"].set(statistics[numberOfLaunch].avg_rows_speed_value);
                    runJSON["avg_bytes_per_second"].set(statistics[numberOfLaunch].avg_bytes_speed_value);
                }

                runInfos.push_back(runJSON);
            }
        }

        jsonOutput["runs"].set(runInfos);

        std::cout << std::endl << jsonOutput << std::endl;
    }

    void minOutput(const std::string & main_metric)
    {
        for (size_t queryIndex = 0; queryIndex < queries.size(); ++queryIndex)
        {
            for (size_t numberOfLaunch = 0; numberOfLaunch < times_to_run; ++numberOfLaunch)
            {
                std::cout << testName << ", ";

                if (substitutionsMaps.size())
                {
                    for (auto it = substitutionsMaps[queryIndex].begin(); it != substitutionsMaps[queryIndex].end(); ++it)
                    {
                        std::cout << it->first << " = " << it->second << ", ";
                    }
                }

                std::cout << "run " << numberOfLaunch + 1 << ": ";
                std::cout << main_metric << " = ";
                std::cout << statistics[numberOfLaunch * queries.size() + queryIndex].getStatisticByName(main_metric);
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
