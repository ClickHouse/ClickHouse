#include <functional>
#include <iostream>
#include <limits>
#include <regex>
#include <sys/sysinfo.h>
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
#include <IO/WriteBufferFromFile.h>
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
using String = std::string;
const String FOUR_SPACES = "    ";

namespace DB
{
namespace ErrorCodes
{
    extern const int POCO_EXCEPTION;
    extern const int STD_EXCEPTION;
    extern const int UNKNOWN_EXCEPTION;
}

static String pad(size_t padding) {
    return String(padding * 4, ' ');
}

class JSONString
{
private:
    std::map<String, String> content;
    size_t padding;

public:
    JSONString(size_t padding_ = 1) : padding(padding_){};

    void set(const String key, String value, bool wrap = true)
    {
        if (value.empty())
            value = "null";

        bool reserved = (value[0] == '[' || value[0] == '{' || value == "null");
        if (!reserved && wrap)
            value = '\"' + value + '\"';

        content[key] = value;
    }

    template <typename T>
    typename std::enable_if<std::is_arithmetic<T>::value>::type set(const String key, T value)
    {
        set(key, std::to_string(value), /*wrap= */false);
    }

    void set(const String key, const std::vector<JSONString> & run_infos)
    {
        String value = "[\n";

        for (size_t i = 0; i < run_infos.size(); ++i)
        {
            value += pad(padding + 1) + run_infos[i].asString(padding + 2);
            if (i != run_infos.size() - 1)
                value += ',';

            value += "\n";
        }

        value += pad(padding) + ']';
        content[key] = value;
    }

    String asString() const { return asString(padding); }
    String asString(size_t padding) const
    {
        String repr = "{";

        for (auto it = content.begin(); it != content.end(); ++it)
        {
            if (it != content.begin())
                repr += ',';
            /// construct "key": "value" string with padding
            repr += "\n" + pad(padding) + '\"' + it->first + '\"' + ": " + it->second;
        }

        repr += "\n" + pad(padding - 1) + '}';
        return repr;
    }
};

enum class PriorityType
{
    Min,
    Max
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
    using Keys = std::vector<String>;

    void initializeStruct(const String & priority, const AbstractConfiguration & stop_criterions_view)
    {
        Keys keys;
        stop_criterions_view->keys(priority, keys);

        PriorityType priority_type = (priority == "min" ? PriorityType::Min : PriorityType::Max);

        for (const String & key : keys)
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
                throw DB::Exception("Met unkown stop criterion: " + key, 1);
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

    String getStatisticByName(const String & statistic_name)
    {
        if (statistic_name == "min_time")
        {
            return std::to_string(min_time) + "ms";
        }
        if (statistic_name == "quantiles")
        {
            String result = "\n";

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

double Stats::avg_rows_speed_precision  = 0.001;
double Stats::avg_bytes_speed_precision = 0.001;

class PerformanceTest
{
public:
    using Strings = std::vector<String>;

    PerformanceTest(
        const String & host_,
        const UInt16 port_,
        const String & default_database_,
        const String & user_,
        const String & password_,
        const bool & lite_output_,
        const String & profiles_file_,
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
            throw DB::Exception("No tests were specified", 0);
        }

        std::cerr << std::fixed << std::setprecision(3);
        std::cout << std::fixed << std::setprecision(3);
        processTestsConfigurations(input_files);
    }

private:
    String test_name;

    using Query = String;
    using Queries = std::vector<Query>;
    using QueriesWithIndexes = std::vector<std::pair<Query, size_t>>;
    Queries queries;

    Connection connection;

    using Keys = std::vector<String>;

    Settings settings;

    InterruptListener interrupt_listener;

    using XMLConfiguration = Poco::Util::XMLConfiguration;
    using AbstractConfig = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;
    using Config = Poco::AutoPtr<XMLConfiguration>;

    using Paths = std::vector<String>;
    using StringToVector = std::map<String, std::vector<String>>;
    StringToVector substitutions;

    using StringKeyValue = std::map<String, String>;
    std::vector<StringKeyValue> substitutions_maps;

    bool gotSIGINT;
    std::vector<StopCriterions> stop_criterions;
    String main_metric;
    bool lite_output;
    String profiles_file;

    Strings input_files;
    std::vector<Config> tests_configurations;

    Strings tests_tags;
    Strings skip_tags;
    Strings tests_names;
    Strings skip_names;
    Strings tests_names_regexp;
    Strings skip_names_regexp;

    #define incFulfilledCriterions(index, CRITERION)                                                                        \
    if (!stop_criterions[index].CRITERION.fulfilled)                                                                        \
    {                                                                                                                       \
        stop_criterions[index].CRITERION.priority == PriorityType::Min ? ++stop_criterions[index].fulfilled_criterions_min  \
                                                                       : ++stop_criterions[index].fulfilled_criterions_max; \
        stop_criterions[index].CRITERION.fulfilled = true;                                                                  \
    }

    enum class ExecutionType
    {
        Loop,
        Once
    };
    ExecutionType exec_type;

    enum class FilterType
    {
        Tag,
        Name,
        Name_regexp
    };

    size_t times_to_run = 1;
    std::vector<Stats> statistics;

    /// Removes configurations that has a given value. If leave is true, the logic is reversed.
    void removeConfigurationsIf(std::vector<Config> & configs, FilterType filter_type, const Strings & values, bool leave = false)
    {
        auto checker = [&filter_type, &values, &leave](Config & config) {
            if (values.size() == 0)
                return false;

            bool remove_or_not = false;

            if (filter_type == FilterType::Tag)
            {
                Keys tags_keys;
                config->keys("tags", tags_keys);

                Strings tags(tags_keys.size());
                for (size_t i = 0; i != tags_keys.size(); ++i)
                    tags[i] = config->getString("tags.tag[" + std::to_string(i) + "]");

                for (const String & config_tag : tags) {
                    if (std::find(values.begin(), values.end(), config_tag) != values.end())
                        remove_or_not = true;
                }
            }

            if (filter_type == FilterType::Name)
            {
                remove_or_not = (std::find(values.begin(), values.end(), config->getString("name", "")) != values.end());
            }

            if (filter_type == FilterType::Name_regexp)
            {
                String config_name = config->getString("name", "");
                auto regex_checker = [&config_name](const String & name_regexp) {
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
        removeConfigurationsIf(tests_configurations, FilterType::Tag, tests_tags, true);
        removeConfigurationsIf(tests_configurations, FilterType::Name, tests_names, true);
        removeConfigurationsIf(tests_configurations, FilterType::Name_regexp, tests_names_regexp, true);


        /// Skip tests
        removeConfigurationsIf(tests_configurations, FilterType::Tag, skip_tags, false);
        removeConfigurationsIf(tests_configurations, FilterType::Name, skip_names, false);
        removeConfigurationsIf(tests_configurations, FilterType::Name_regexp, skip_names_regexp, false);
    }

    /// Checks specified preconditions per test (process cache, table existence, etc.)
    bool checkPreconditions(const Config & config)
    {
        if (!config->has("preconditions"))
            return true;

        Keys preconditions;
        config->keys("preconditions", preconditions);
        size_t table_precondition_index = 0;

        for (const String & precondition : preconditions)
        {
            if (precondition == "reset_cpu_cache")
                if (system("(>&2 echo 'Flushing cache...') && (sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches') && (>&2 echo 'Flushed.')")) {
                    std::cerr << "Failed to flush cache" << std::endl;
                    return false;
                }

            if (precondition == "ram_size")
            {
                struct sysinfo *system_information = new struct sysinfo();
                if (sysinfo(system_information))
                {
                    std::cerr << "Failed to check system RAM size" << std::endl;
                    delete system_information;
                }
                else
                {
                    size_t ram_size_needed = config->getUInt64("preconditions.ram_size");
                    size_t actual_ram = system_information->totalram / 1024 / 1024;
                    if (ram_size_needed > actual_ram)
                    {
                        std::cerr << "Not enough RAM" << std::endl;
                        delete system_information;
                        return false;
                    }
                }
            }

            if (precondition == "table_exists")
            {
                String precondition_key = "preconditions.table_exists[" + std::to_string(table_precondition_index++) + "]";
                String table_to_check = config->getString(precondition_key);
                String query = "EXISTS TABLE " + table_to_check + ";";

                size_t exist = 0;

                connection.sendQuery(query, "", QueryProcessingStage::Complete, &settings, nullptr, false);

                while (true)
                {
                    Connection::Packet packet = connection.receivePacket();

                    if (packet.type == Protocol::Server::Data) {
                        for (const ColumnWithTypeAndName & column : packet.block.getColumns())
                        {
                            if (column.name == "result" && column.column->getDataAt(0).data != nullptr) {
                                exist = column.column->get64(0);
                            }
                        }
                    }

                    if (packet.type == Protocol::Server::Exception || packet.type == Protocol::Server::EndOfStream)
                        break;
                }

                if (exist == 0) {
                    std::cerr << "Table " + table_to_check + " doesn't exist" << std::endl;
                    return false;
                }
            }
        }

        return true;
    }

    void processTestsConfigurations(const Paths & input_files)
    {
        tests_configurations.resize(input_files.size());

        for (size_t i = 0; i != input_files.size(); ++i)
        {
            const String path = input_files[i];
            tests_configurations[i] = Config(new XMLConfiguration(path));
        }

        filterConfigurations();

        if (tests_configurations.size())
        {
            Strings outputs;

            for (auto & test_config : tests_configurations)
            {
                if (!checkPreconditions(test_config))
                {
                    std::cerr << "Preconditions are not fulfilled for test \"" + test_config->getString("name", "") + "\"";
                    continue;
                }

                String output = runTest(test_config);
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

    void extractSettings(const Config & config, const String & key,
                         const Strings & settings_list,
                         std::map<String, String> settings_to_apply)
    {
        for (const String & setup : settings_list)
        {
            if (setup == "profile")
                continue;

            String value = config->getString(key + "." + setup);
            if (value.empty())
                value = "true";

            settings_to_apply[setup] = value;
        }
    }

    String runTest(Config & test_config)
    {
        queries.clear();

        test_name = test_config->getString("name");
        std::cerr << "Running: " << test_name << "\n";

        if (test_config->has("settings"))
        {
            std::map<String, String> settings_to_apply;
            Keys config_settings;
            test_config->keys("settings", config_settings);

            /// Preprocess configuration file
            if (std::find(config_settings.begin(), config_settings.end(), "profile") != config_settings.end())
            {
                if (!profiles_file.empty())
                {
                    String profile_name = test_config->getString("settings.profile");
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
            std::map<String, String>::iterator it;
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
            throw DB::Exception("Missing query fields in test's config: " + test_name, 1);
        }

        if (test_config->has("query") && test_config->has("query_file"))
        {
            throw DB::Exception("Found both query and query_file fields. Choose only one", 1);
        }

        if (test_config->has("query"))
            queries.push_back(test_config->getString("query"));

        if (test_config->has("query_file"))
        {
            const String filename = test_config->getString("query_file");
            if (filename.empty())
                throw DB::Exception("Empty file name", 1);

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
            throw DB::Exception("Did not find any query to execute: " + test_name, 1);
        }

        if (test_config->has("substitutions"))
        {
            if (queries.size() > 1)
                throw DB::Exception("Only one query is allowed when using substitutions", 1);

            /// Make "subconfig" of inner xml block
            AbstractConfig substitutions_view(test_config->createView("substitutions"));
            constructSubstitutions(substitutions_view, substitutions);

            queries = formatQueries(queries[0], substitutions);
        }

        if (!test_config->has("type"))
        {
            throw DB::Exception("Missing type property in config: " + test_name, 1);
        }

        String config_exec_type = test_config->getString("type");
        if (config_exec_type == "loop")
            exec_type = ExecutionType::Loop;
        else if (config_exec_type == "once")
            exec_type = ExecutionType::Once;
        else
            throw DB::Exception("Unknown type " + config_exec_type + " in :" + test_name, 1);

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
            throw DB::Exception("No termination conditions were found in config", 1);
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

        if (!main_metric.empty())
        {
            if (std::find(metrics.begin(), metrics.end(), main_metric) == metrics.end())
                metrics.push_back(main_metric);
        }
        else
        {
            if (lite_output)
                throw DB::Exception("Specify main_metric for lite output", 1);
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
            return constructTotalInfo(metrics);
    }

    void checkMetricsInput(const Strings & metrics) const
    {
        std::vector<String> loop_metrics
            = {"min_time", "quantiles", "total_time", "queries_per_second", "rows_per_second", "bytes_per_second"};

        std::vector<String> non_loop_metrics
            = {"max_rows_per_second", "max_bytes_per_second", "avg_rows_per_second", "avg_bytes_per_second"};

        if (exec_type == ExecutionType::Loop)
        {
            for (const String & metric : metrics)
            {
                if (std::find(non_loop_metrics.begin(), non_loop_metrics.end(), metric) != non_loop_metrics.end())
                {
                    throw DB::Exception("Wrong type of metric for loop execution type (" + metric + ")", 1);
                }
            }
        }
        else
        {
            for (const String & metric : metrics)
            {
                if (std::find(loop_metrics.begin(), loop_metrics.end(), metric) != loop_metrics.end())
                {
                    throw DB::Exception("Wrong type of metric for non-loop execution type (" + metric + ")", 1);
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

            if (exec_type == ExecutionType::Loop)
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
            std::vector<String> xml_values;
            xml_substitution->keys("values", xml_values);

            String name = xml_substitution->getString("name");

            for (size_t j = 0; j != xml_values.size(); ++j)
            {
                substitutions[name].push_back(xml_substitution->getString("values.value[" + std::to_string(j) + "]"));
            }
        }
    }

    std::vector<String> formatQueries(const String & query, StringToVector substitutions)
    {
        std::vector<String> queries;

        StringToVector::iterator substitutions_first = substitutions.begin();
        StringToVector::iterator substitutions_last = substitutions.end();
        --substitutions_last;

        std::map<String, String> substitutions_map;

        runThroughAllOptionsAndPush(substitutions_first, substitutions_last, query, queries, substitutions_map);

        return queries;
    }

    /// Recursive method which goes through all substitution blocks in xml
    /// and replaces property {names} by their values
    void runThroughAllOptionsAndPush(StringToVector::iterator substitutions_left,
        StringToVector::iterator substitutions_right,
        const String & template_query,
        std::vector<String> & queries,
        const StringKeyValue & template_substitutions_map = StringKeyValue())
    {
        String name = substitutions_left->first;
        std::vector<String> values = substitutions_left->second;

        for (const String & value : values)
        {
            /// Copy query string for each unique permutation
            Query query = template_query;
            StringKeyValue substitutions_map = template_substitutions_map;
            size_t substr_pos = 0;

            while (substr_pos != String::npos)
            {
                substr_pos = query.find("{" + name + "}");

                if (substr_pos != String::npos)
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
    String constructTotalInfo(Strings metrics)
    {
        JSONString json_output;
        String hostname;

        char hostname_buffer[256];
        if (gethostname(hostname_buffer, 256) == 0)
        {
            hostname = String(hostname_buffer);
        }

        json_output.set("hostname", hostname);
        json_output.set("cpu_num", sysconf(_SC_NPROCESSORS_ONLN));
        json_output.set("test_name", test_name);
        json_output.set("main_metric", main_metric);

        if (substitutions.size())
        {
            JSONString json_parameters(2); /// here, 2 is the size of \t padding

            for (auto it = substitutions.begin(); it != substitutions.end(); ++it)
            {
                String parameter = it->first;
                std::vector<String> values = it->second;

                String array_string = "[";
                for (size_t i = 0; i != values.size(); ++i)
                {
                    array_string += '\"' + values[i] + '\"';
                    if (i != values.size() - 1)
                    {
                        array_string += ", ";
                    }
                }
                array_string += ']';

                json_parameters.set(parameter, array_string);
            }

            json_output.set("parameters", json_parameters.asString());
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
                        parameters.set(it->first, it->second);
                    }

                    runJSON.set("parameters", parameters.asString());
                }

                if (exec_type == ExecutionType::Loop)
                {
                    /// in seconds
                    if (std::find(metrics.begin(), metrics.end(), "min_time") != metrics.end())
                        runJSON.set("min_time", statistics[number_of_launch].min_time / double(1000));

                    if (std::find(metrics.begin(), metrics.end(), "quantiles") != metrics.end())
                    {
                        JSONString quantiles(4); /// here, 4 is the size of \t padding
                        for (double percent = 10; percent <= 90; percent += 10)
                        {
                            String quantile_key = std::to_string(percent / 100.0);
                            while (quantile_key.back() == '0')
                                quantile_key.pop_back();

                            quantiles.set(quantile_key, statistics[number_of_launch].sampler.quantileInterpolated(percent / 100.0));
                        }
                        quantiles.set("0.95",   statistics[number_of_launch].sampler.quantileInterpolated(95    / 100.0));
                        quantiles.set("0.99",   statistics[number_of_launch].sampler.quantileInterpolated(99    / 100.0));
                        quantiles.set("0.999",  statistics[number_of_launch].sampler.quantileInterpolated(99.9  / 100.0));
                        quantiles.set("0.9999", statistics[number_of_launch].sampler.quantileInterpolated(99.99 / 100.0));

                        runJSON.set("quantiles", quantiles.asString());
                    }

                    if (std::find(metrics.begin(), metrics.end(), "total_time") != metrics.end())
                        runJSON.set("total_time", statistics[number_of_launch].total_time);

                    if (std::find(metrics.begin(), metrics.end(), "queries_per_second") != metrics.end())
                        runJSON.set("queries_per_second", double(statistics[number_of_launch].queries) /
                                                          statistics[number_of_launch].total_time);

                    if (std::find(metrics.begin(), metrics.end(), "rows_per_second") != metrics.end())
                        runJSON.set("rows_per_second", double(statistics[number_of_launch].rows_read) /
                                                       statistics[number_of_launch].total_time);

                    if (std::find(metrics.begin(), metrics.end(), "bytes_per_second") != metrics.end())
                        runJSON.set("bytes_per_second", double(statistics[number_of_launch].bytes_read) /
                                                        statistics[number_of_launch].total_time);
                }
                else
                {
                    if (std::find(metrics.begin(), metrics.end(), "max_rows_per_second") != metrics.end())
                        runJSON.set("max_rows_per_second", statistics[number_of_launch].max_rows_speed);

                    if (std::find(metrics.begin(), metrics.end(), "max_bytes_per_second") != metrics.end())
                        runJSON.set("max_bytes_per_second", statistics[number_of_launch].max_bytes_speed);

                    if (std::find(metrics.begin(), metrics.end(), "avg_rows_per_second") != metrics.end())
                        runJSON.set("avg_rows_per_second", statistics[number_of_launch].avg_rows_speed_value);

                    if (std::find(metrics.begin(), metrics.end(), "avg_bytes_per_second") != metrics.end())
                        runJSON.set("avg_bytes_per_second", statistics[number_of_launch].avg_bytes_speed_value);
                }

                run_infos.push_back(runJSON);
            }
        }

        json_output.set("runs", run_infos);

        return json_output.asString();
    }

    String minOutput(const String & main_metric)
    {
        String output;

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

static void getFilesFromDir(const FS::path & dir, std::vector<String> & input_files)
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
        using Strings = std::vector<String>;

        boost::program_options::options_description desc("Allowed options");
        desc.add_options()
            ("help",                                                                     "produce help message")
            ("lite",                                                                     "use lite version of output")
            ("profiles-file",        value<String>()->default_value(""),            "Specify a file with global profiles")
            ("host,h",               value<String>()->default_value("localhost"),   "")
            ("port",                 value<UInt16>()->default_value(9000),               "")
            ("database",             value<String>()->default_value("default"),     "")
            ("user",                 value<String>()->default_value("default"),     "")
            ("password",             value<String>()->default_value(""),            "")
            ("tags",                 value<Strings>()->multitoken(),                     "Run only tests with tag")
            ("skip-tags",            value<Strings>()->multitoken(),                     "Do not run tests with tag")
            ("names",                value<Strings>()->multitoken(),                     "Run tests with specific name")
            ("skip-names",           value<Strings>()->multitoken(),                     "Do not run tests with name")
            ("names-regexp",         value<Strings>()->multitoken(),                     "Run tests with names matching regexp")
            ("skip-names-regexp",    value<Strings>()->multitoken(),                     "Do not run tests with names matching regexp");

        /// These options will not be displayed in --help
        boost::program_options::options_description hidden("Hidden options");
        hidden.add_options()("input-files", value<std::vector<String>>(), "");

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
            std::cerr << "Trying to find tests in current folder" << std::endl;
            FS::path curr_dir(".");

            getFilesFromDir(curr_dir, input_files);

            if (input_files.empty())
                throw DB::Exception("Did not find any xml files", 1);
        }
        else
        {
            input_files = options["input-files"].as<Strings>();

            for (const String filename : input_files)
            {
                FS::path file(filename);

                if (!FS::exists(file))
                    throw DB::Exception("File \"" + filename + "\" does not exist", 1);

                if (FS::is_directory(file))
                {
                    input_files.erase( std::remove(input_files.begin(), input_files.end(), filename) , input_files.end() );
                    getFilesFromDir(file, input_files);
                }
                else
                {
                    if (file.extension().string() != ".xml")
                        throw DB::Exception("File \"" + filename + "\" does not have .xml extension", 1);
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
            options["host"].as<String>(),
            options["port"].as<UInt16>(),
            options["database"].as<String>(),
            options["user"].as<String>(),
            options["password"].as<String>(),
            options.count("lite") > 0,
            options["profiles-file"].as<String>(),
            std::move(input_files),
            std::move(tests_tags),
            std::move(skip_tags),
            std::move(tests_names),
            std::move(skip_names),
            std::move(tests_names_regexp),
            std::move(skip_names_regexp)
        );
    }
    catch (...)
    {
        std::cout << getCurrentExceptionMessage(/*with stacktrace = */true) << std::endl;
        return getCurrentExceptionCode();
    }

    return 0;
}
