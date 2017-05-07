#include <functional>
#include <iostream>
#include <limits>
#include <regex>
#include <unistd.h>

#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>
#include <sys/stat.h>

#include <AggregateFunctions/ReservoirSampler.h>
#include <Client/Connection.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>
#include <Core/Types.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <IO/ReadBufferFromFile.h>
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
namespace FS = boost::filesystem;

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
    using Strings = std::vector<std::string>;

    PerformanceTest(
        const String & host_,
        const UInt16 port_,
        const String & default_database_,
        const String & user_,
        const String & password_,
        const bool & lite_output_,
        const std::string & profiles_file_,
        Strings && input_files_,
        Strings && tests_tags_,
        Strings && skip_tags_,
        Strings && tests_names_,
        Strings && skip_names_,
        Strings && tests_names_regexp_,
        Strings && skip_names_regexp_
    )
        : connection(host_, port_, default_database_, user_, password_),
          gotSIGINT(false),
          lite_output(lite_output_),
          profiles_file(profiles_file_),
          input_files(input_files_),
          tests_tags(std::move(tests_tags_)),
          skip_tags(std::move(skip_tags_)),
          tests_names(std::move(tests_names_)),
          skip_names(std::move(skip_names_)),
          tests_names_regexp(std::move(tests_names_regexp_)),
          skip_names_regexp(std::move(skip_names_regexp_))
    {
        if (input_files.size() < 1)
        {
            throw Poco::Exception("No tests were specified", 0);
        }

        std::cerr << std::fixed << std::setprecision(3);
        std::cout << std::fixed << std::setprecision(3);
        processTestsConfigurations(input_files);
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

    using StringKeyValue = std::map<std::string, std::string>;
    std::vector<StringKeyValue> substitutions_maps;

    bool gotSIGINT;
    std::vector<StopCriterions> stop_criterions;
    std::string main_metric;
    bool lite_output;
    std::string profiles_file;

    Strings input_files;
    std::vector<Config> tests_configurations;

    Strings tests_tags;
    Strings skip_tags;
    Strings tests_names;
    Strings skip_names;
    Strings tests_names_regexp;
    Strings skip_names_regexp;

    #define incFulfilledCriterions(index, CRITERION)                                                          \
    if (!stop_criterions[index].CRITERION.fulfilled)                                                          \
    {                                                                                                         \
        stop_criterions[index].CRITERION.priority == min ? ++stop_criterions[index].fulfilled_criterions_min  \
                                                         : ++stop_criterions[index].fulfilled_criterions_max; \
        stop_criterions[index].CRITERION.fulfilled = true;                                                    \
    }

    enum ExecutionType
    {
        loop,
        once
    };
    ExecutionType exec_type;

    enum FilterType
    {
        tag,
        name,
        name_regexp
    };

    size_t times_to_run = 1;
    std::vector<Stats> statistics;

    /// Removes configurations that has a given value. If leave is true, the logic is reversed.
    void removeConfigurationsIf(std::vector<Config> & configs, FilterType filter_type, const Strings & values, bool leave = false)
    {
        std::function<bool(Config &)> checker = [&filter_type, &values, &leave](Config & config) {
            if (values.size() == 0)
                return false;

            bool remove_or_not = false;

            if (filter_type == tag)
            {
                Keys tags_keys;
                config->keys("tags", tags_keys);

                Strings tags(tags_keys.size());
                for (size_t i = 0; i != tags_keys.size(); ++i)
                    tags[i] = config->getString("tags.tag[" + std::to_string(i) + "]");

                for (const std::string & config_tag : tags) {
                    if (std::find(values.begin(), values.end(), config_tag) != values.end())
                        remove_or_not = true;
                }
            }

            if (filter_type == name)
            {
                remove_or_not = (std::find(values.begin(), values.end(), config->getString("name", "")) != values.end());
            }

            if (filter_type == name_regexp)
            {
                std::string config_name = config->getString("name", "");
                std::function<bool(const std::string &)> regex_checker = [&config_name](const std::string & name_regexp) {
                    std::regex pattern(name_regexp);
                    return std::regex_search(config_name, pattern);
                };

                remove_or_not = config->has("name") ? (std::find_if(values.begin(), values.end(), regex_checker) != values.end())
                                                    : false;
            }

            if (leave)
                remove_or_not = !remove_or_not;
            return remove_or_not;
        };

        std::vector<Config>::iterator new_end = std::remove_if(configs.begin(), configs.end(), checker);
        configs.erase(new_end, configs.end());
    }

    /// Filter tests by tags, names, regexp matching, etc.
    void filterConfigurations()
    {
        /// Leave tests:
        removeConfigurationsIf(tests_configurations, FilterType::tag, tests_tags, true);
        removeConfigurationsIf(tests_configurations, FilterType::name, tests_names, true);
        removeConfigurationsIf(tests_configurations, FilterType::name_regexp, tests_names_regexp, true);


        /// Skip tests
        removeConfigurationsIf(tests_configurations, FilterType::tag, skip_tags, false);
        removeConfigurationsIf(tests_configurations, FilterType::name, skip_names, false);
        removeConfigurationsIf(tests_configurations, FilterType::name_regexp, skip_names_regexp, false);
    }

    void processTestsConfigurations(const Paths & input_files)
    {
        tests_configurations.resize(input_files.size());

        for (size_t i = 0; i != input_files.size(); ++i)
        {
            const std::string path = input_files[i];
            tests_configurations[i] = Config(new XMLConfiguration(path));
        }

        filterConfigurations();

        if (tests_configurations.size())
        {
            Strings outputs;

            for (auto & test_config : tests_configurations)
            {
                std::string output = runTest(test_config);
                if (lite_output)
                    std::cout << output << std::endl;
                else
                    outputs.push_back(output);
            }

            if (!lite_output && outputs.size())
            {
                std::cout << "[" << std::endl;

                for (size_t i = 0; i != outputs.size(); ++i)
                {
                    std::cout << outputs[i];
                    if (i != outputs.size() - 1)
                        std::cout << ",";

                    std::cout << std::endl;
                }

                std::cout << "]" << std::endl;
            }
        }
    }

    void extractSettings(const Config & config, const std::string & key,
                         const Strings & settings_list,
                         std::map<std::string, std::string> settings_to_apply)
    {
        for (const std::string & setup : settings_list)
        {
            if (setup == "profile")
                continue;

            std::string value = config->getString(key + "." + setup);
            if (value.empty())
                value = "true";

            settings_to_apply[setup] = value;
        }
    }

    std::string runTest(Config & test_config)
    {
        queries.clear();

        test_name = test_config->getString("name");
        std::cerr << "Running: " << test_name << "\n";

        if (test_config->has("settings"))
        {
            std::map<std::string, std::string> settings_to_apply;
            Keys config_settings;
            test_config->keys("settings", config_settings);

            /// Preprocess configuration file
            if (std::find(config_settings.begin(), config_settings.end(), "profile") != config_settings.end())
            {
                if (!profiles_file.empty())
                {
                    std::string profile_name = test_config->getString("settings.profile");
                    Config profiles_config(new XMLConfiguration(profiles_file));

                    Keys profile_settings;
                    profiles_config->keys("profiles." + profile_name, profile_settings);

                    extractSettings(profiles_config, "profiles." + profile_name, profile_settings, settings_to_apply);
                }
            }

            extractSettings(test_config, "settings", config_settings, settings_to_apply);

            /// This macro goes through all settings in the Settings.h
            /// and, if found any settings in test's xml configuration
            /// with the same name, sets its value to settings
            std::map<std::string, std::string>::iterator it;
            #define EXTRACT_SETTING(TYPE, NAME, DEFAULT) \
                it = settings_to_apply.find(#NAME);      \
                if (it != settings_to_apply.end())       \
                    settings.set(#NAME, settings_to_apply[#NAME]);

            APPLY_FOR_SETTINGS(EXTRACT_SETTING)
            APPLY_FOR_LIMITS(EXTRACT_SETTING)

            #undef EXTRACT_SETTING

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

        if (!test_config->has("query") && !test_config->has("query_file"))
        {
            throw Poco::Exception("Missing query fields in test's config: " + test_name, 1);
        }

        if (test_config->has("query") && test_config->has("query_file"))
        {
            throw Poco::Exception("Found both query and query_file fields. Choose only one", 1);
        }

        if (test_config->has("query"))
            queries.push_back(test_config->getString("query"));

        if (test_config->has("query_file"))
        {
            const std::string filename = test_config->getString("query_file");
            if (filename.empty())
                throw Poco::Exception("Empty file name", 1);

            bool tsv = FS::path(filename).extension().string() == ".tsv";

            ReadBufferFromFile query_file(filename);
            while (!query_file.eof())
            {
                tsv ? readEscapedString(query, query_file, true)
                    : readString(query, query_file, true);

                if (!query.empty())
                    queries.push_back(query);
            }
        }

        if (queries.empty())
        {
            throw Poco::Exception("Did not find any query to execute: " + test_name, 1);
        }

        if (test_config->has("substitutions"))
        {
            if (queries.size() > 1)
                throw Poco::Exception("Only one query is allowed when using substitutions", 1);

            /// Make "subconfig" of inner xml block
            AbstractConfig substitutions_view(test_config->createView("substitutions"));
            constructSubstitutions(substitutions_view, substitutions);

            queries = formatQueries(queries[0], substitutions);
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

        if (test_config->has("main_metric"))
        {
            Keys main_metrics;
            test_config->keys("main_metric", main_metrics);
            if (main_metrics.size())
                main_metric = main_metrics[0];
        }

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
            return minOutput(main_metric);
        else
            return constructTotalInfo();
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
    std::string constructTotalInfo()
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

        return json_output.constructOutput();
    }

    std::string minOutput(const std::string & main_metric)
    {
        std::string output;

        for (size_t query_index = 0; query_index < queries.size(); ++query_index)
        {
            for (size_t number_of_launch = 0; number_of_launch < times_to_run; ++number_of_launch)
            {
                output += test_name  + ", ";

                if (substitutions_maps.size())
                {
                    for (auto it = substitutions_maps[query_index].begin(); it != substitutions_maps[query_index].end(); ++it)
                    {
                        output += it->first + " = " + it->second + ", ";
                    }
                }

                output += "run " + std::to_string(number_of_launch + 1) + ": ";
                output += main_metric + " = ";
                output += statistics[number_of_launch * queries.size() + query_index].getStatisticByName(main_metric);
                output += "\n";
            }
        }

        return output;
    }
};
}

void getFilesFromDir(FS::path && dir, std::vector<std::string> & input_files)
{
    if (dir.extension().string() == ".xml")
        std::cerr << "Warning: \"" + dir.string() + "\" is a directory, but has .xml extension" << std::endl;

    FS::directory_iterator end;
    for (FS::directory_iterator it(dir); it != end; ++it)
    {
        const FS::path file = (*it);
        if (!FS::is_directory(file) && file.extension().string() == ".xml")
            input_files.push_back(file.string());
    }
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
            ("help",                                                                     "produce help message")
            ("lite",                                                                     "use lite version of output")
            ("profiles-file",        value<std::string>()->default_value(""),            "Specify a file with global profiles")
            ("host,h",               value<std::string>()->default_value("localhost"),   "")
            ("port",                 value<UInt16>()->default_value(9000),               "")
            ("database",             value<std::string>()->default_value("default"),     "")
            ("user",                 value<std::string>()->default_value("default"),     "")
            ("password",             value<std::string>()->default_value(""),            "")
            ("tags",                 value<Strings>()->multitoken(),                     "Run only tests with tag")
            ("skip-tags",            value<Strings>()->multitoken(),                     "Do not run tests with tag")
            ("names",                value<Strings>()->multitoken(),                     "Run tests with specific name")
            ("skip-names",           value<Strings>()->multitoken(),                     "Do not run tests with name")
            ("names-regexp",         value<Strings>()->multitoken(),                     "Run tests with names matching regexp")
            ("skip-names-regexp",    value<Strings>()->multitoken(),                     "Do not run tests with names matching regexp");

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

        Strings input_files;

        if (!options.count("input-files"))
        {
            std::cout << "Trying to find tests in current folder" << std::endl;

            getFilesFromDir(FS::path("."), input_files);

            if (input_files.empty())
                throw Poco::Exception("Did not find any xml files", 1);
        } else {
            input_files = options["input-files"].as<Strings>();

            for (const std::string filename : input_files)
            {
                FS::path file(filename);

                if (!FS::exists(file))
                    throw Poco::Exception("File \"" + filename + "\" does not exist", 1);

                if (FS::is_directory(file))
                {
                    input_files.erase( std::remove(input_files.begin(), input_files.end(), filename) , input_files.end());
                    getFilesFromDir(std::move(file), input_files);
                } else {
                    if (file.extension().string() != ".xml")
                        throw Poco::Exception("File \"" + filename + "\" does not have .xml extension", 1);
                }
            }
        }

        Strings tests_tags = options.count("tags")
            ? options["tags"].as<Strings>()
            : Strings({});
        Strings skip_tags = options.count("skip-tags")
            ? options["skip-tags"].as<Strings>()
            : Strings({});
        Strings tests_names = options.count("names")
            ? options["names"].as<Strings>()
            : Strings({});
        Strings skip_names = options.count("skip-names")
            ? options["skip-names"].as<Strings>()
            : Strings({});
        Strings tests_names_regexp = options.count("names-regexp")
            ? options["names-regexp"].as<Strings>()
            : Strings({});
        Strings skip_names_regexp = options.count("skip-names-regexp")
            ? options["skip-names-regexp"].as<Strings>()
            : Strings({});

        PerformanceTest performanceTest(
            options["host"].as<std::string>(),
            options["port"].as<UInt16>(),
            options["database"].as<std::string>(),
            options["user"].as<std::string>(),
            options["password"].as<std::string>(),
            options.count("lite") > 0,
            options["profiles-file"].as<std::string>(),
            std::move(input_files),
            std::move(tests_tags),
            std::move(skip_tags),
            std::move(tests_names),
            std::move(skip_names),
            std::move(tests_names_regexp),
            std::move(skip_names_regexp)
        );
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
