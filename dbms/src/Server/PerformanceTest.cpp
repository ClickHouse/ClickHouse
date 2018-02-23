#include <functional>
#include <iostream>
#include <limits>
#include <regex>
#include <thread>
#include <unistd.h>

#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <sys/stat.h>

#include <common/DateLUT.h>

#include <AggregateFunctions/ReservoirSampler.h>
#include <Client/Connection.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/Stopwatch.h>
#include <Common/getFQDNOrHostName.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Core/Types.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ConnectionTimeouts.h>
#include <Interpreters/Settings.h>
#include <common/ThreadPool.h>
#include <common/getMemoryAmount.h>

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
namespace fs = boost::filesystem;
using String = std::string;
const String FOUR_SPACES = "    ";

namespace DB
{
namespace ErrorCodes
{
    extern const int POCO_EXCEPTION;
    extern const int STD_EXCEPTION;
    extern const int UNKNOWN_EXCEPTION;
    extern const int NOT_IMPLEMENTED;
}

static String pad(size_t padding)
{
    return String(padding * 4, ' ');
}


/// NOTE The code is totally wrong.
class JSONString
{
private:
    std::map<String, String> content;
    size_t padding;

public:
    explicit JSONString(size_t padding_ = 1) : padding(padding_){};

    void set(const String key, String value, bool wrap = true)
    {
        if (value.empty())
            value = "null";

        bool reserved = (value[0] == '[' || value[0] == '{' || value == "null");
        if (!reserved && wrap)
            value = '"' + value + '"';

        content[key] = value;
    }

    template <typename T>
    std::enable_if_t<std::is_arithmetic_v<T>> set(const String key, T value)
    {
        set(key, std::to_string(value), /*wrap= */ false);
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

    String asString() const
    {
        return asString(padding);
    }
    String asString(size_t padding) const
    {
        String repr = "{";

        for (auto it = content.begin(); it != content.end(); ++it)
        {
            if (it != content.begin())
                repr += ',';
            /// construct "key": "value" string with padding
            repr += "\n" + pad(padding) + '"' + it->first + '"' + ": " + it->second;
        }

        repr += "\n" + pad(padding - 1) + '}';
        return repr;
    }
};


using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

/// A set of supported stop conditions.
struct StopConditionsSet
{
    void loadFromConfig(const ConfigurationPtr & stop_conditions_view)
    {
        using Keys = std::vector<String>;
        Keys keys;
        stop_conditions_view->keys(keys);

        for (const String & key : keys)
        {
            if (key == "total_time_ms")
                total_time_ms.value = stop_conditions_view->getUInt64(key);
            else if (key == "rows_read")
                rows_read.value = stop_conditions_view->getUInt64(key);
            else if (key == "bytes_read_uncompressed")
                bytes_read_uncompressed.value = stop_conditions_view->getUInt64(key);
            else if (key == "iterations")
                iterations.value = stop_conditions_view->getUInt64(key);
            else if (key == "min_time_not_changing_for_ms")
                min_time_not_changing_for_ms.value = stop_conditions_view->getUInt64(key);
            else if (key == "max_speed_not_changing_for_ms")
                max_speed_not_changing_for_ms.value = stop_conditions_view->getUInt64(key);
            else if (key == "average_speed_not_changing_for_ms")
                average_speed_not_changing_for_ms.value = stop_conditions_view->getUInt64(key);
            else
                throw DB::Exception("Met unkown stop condition: " + key);

            ++initialized_count;
        }
    }

    void reset()
    {
        total_time_ms.fulfilled = false;
        rows_read.fulfilled = false;
        bytes_read_uncompressed.fulfilled = false;
        iterations.fulfilled = false;
        min_time_not_changing_for_ms.fulfilled = false;
        max_speed_not_changing_for_ms.fulfilled = false;
        average_speed_not_changing_for_ms.fulfilled = false;

        fulfilled_count = 0;
    }

    /// Note: only conditions with UInt64 minimal thresholds are supported.
    /// I.e. condition is fulfilled when value is exceeded.
    struct StopCondition
    {
        UInt64 value = 0;
        bool fulfilled = false;
    };

    void report(UInt64 value, StopCondition & condition)
    {
        if (condition.value && !condition.fulfilled && value >= condition.value)
        {
            condition.fulfilled = true;
            ++fulfilled_count;
        }
    }

    StopCondition total_time_ms;
    StopCondition rows_read;
    StopCondition bytes_read_uncompressed;
    StopCondition iterations;
    StopCondition min_time_not_changing_for_ms;
    StopCondition max_speed_not_changing_for_ms;
    StopCondition average_speed_not_changing_for_ms;

    size_t initialized_count = 0;
    size_t fulfilled_count = 0;
};

/// Stop conditions for a test run. The running test will be terminated in either of two conditions:
/// 1. All conditions marked 'all_of' are fulfilled
/// or
/// 2. Any condition  marked 'any_of' is  fulfilled
class TestStopConditions
{
public:
    void loadFromConfig(ConfigurationPtr & stop_conditions_config)
    {
        if (stop_conditions_config->has("all_of"))
        {
            ConfigurationPtr config_all_of(stop_conditions_config->createView("all_of"));
            conditions_all_of.loadFromConfig(config_all_of);
        }
        if (stop_conditions_config->has("any_of"))
        {
            ConfigurationPtr config_any_of(stop_conditions_config->createView("any_of"));
            conditions_any_of.loadFromConfig(config_any_of);
        }
    }

    bool empty() const
    {
        return !conditions_all_of.initialized_count && !conditions_any_of.initialized_count;
    }

#define DEFINE_REPORT_FUNC(FUNC_NAME, CONDITION)                      \
    void FUNC_NAME(UInt64 value)                                      \
    {                                                                 \
        conditions_all_of.report(value, conditions_all_of.CONDITION); \
        conditions_any_of.report(value, conditions_any_of.CONDITION); \
    }

    DEFINE_REPORT_FUNC(reportTotalTime, total_time_ms);
    DEFINE_REPORT_FUNC(reportRowsRead, rows_read);
    DEFINE_REPORT_FUNC(reportBytesReadUncompressed, bytes_read_uncompressed);
    DEFINE_REPORT_FUNC(reportIterations, iterations);
    DEFINE_REPORT_FUNC(reportMinTimeNotChangingFor, min_time_not_changing_for_ms);
    DEFINE_REPORT_FUNC(reportMaxSpeedNotChangingFor, max_speed_not_changing_for_ms);
    DEFINE_REPORT_FUNC(reportAverageSpeedNotChangingFor, average_speed_not_changing_for_ms);

#undef REPORT

    bool areFulfilled() const
    {
        return (conditions_all_of.initialized_count && conditions_all_of.fulfilled_count >= conditions_all_of.initialized_count)
            || (conditions_any_of.initialized_count && conditions_any_of.fulfilled_count);
    }

    void reset()
    {
        conditions_all_of.reset();
        conditions_any_of.reset();
    }

private:
    StopConditionsSet conditions_all_of;
    StopConditionsSet conditions_any_of;
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

    bool last_query_was_cancelled = false;

    size_t queries = 0;

    size_t total_rows_read = 0;
    size_t total_bytes_read = 0;

    size_t last_query_rows_read = 0;
    size_t last_query_bytes_read = 0;

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
    String exception;

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
            result += FOUR_SPACES + "0.99: " + std::to_string(sampler.quantileInterpolated(99 / 100.0)) + "\n";
            result += FOUR_SPACES + "0.999: " + std::to_string(sampler.quantileInterpolated(99.9 / 100.)) + "\n";
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
            return std::to_string(total_rows_read / total_time);
        }
        if (statistic_name == "bytes_per_second")
        {
            return std::to_string(total_bytes_read / total_time);
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
        ++number_of_info_batches;
        avg_speed_value /= number_of_info_batches;

        if (avg_speed_first == 0)
        {
            avg_speed_first = avg_speed_value;
        }

        if (std::abs(avg_speed_value - avg_speed_first) >= precision)
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
        total_rows_read += rows_read_inc;
        total_bytes_read += bytes_read_inc;
        last_query_rows_read += rows_read_inc;
        last_query_bytes_read += bytes_read_inc;

        double new_rows_speed = last_query_rows_read / watch_per_query.elapsedSeconds();
        double new_bytes_speed = last_query_bytes_read / watch_per_query.elapsedSeconds();

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

        last_query_was_cancelled = false;

        sampler.clear();

        queries = 0;
        total_rows_read = 0;
        total_bytes_read = 0;
        last_query_rows_read = 0;
        last_query_bytes_read = 0;

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
    using Strings = std::vector<String>;

    PerformanceTest(const String & host_,
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
        Strings && skip_names_regexp_,
        const ConnectionTimeouts & timeouts)
        : connection(host_, port_, default_database_, user_, password_, timeouts),
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

        std::string name;
        UInt64 version_major;
        UInt64 version_minor;
        UInt64 version_revision;
        connection.getServerVersion(name, version_major, version_minor, version_revision);

        std::stringstream ss;
        ss << version_major << "." << version_minor << "." << version_revision;
        server_version = ss.str();

        processTestsConfigurations(input_files);
    }

private:
    String test_name;

    using Query = String;
    using Queries = std::vector<Query>;
    using QueriesWithIndexes = std::vector<std::pair<Query, size_t>>;
    Queries queries;

    Connection connection;
    std::string server_version;

    using Keys = std::vector<String>;

    Settings settings;
    Context global_context = Context::createGlobal();

    InterruptListener interrupt_listener;

    using XMLConfiguration = Poco::Util::XMLConfiguration;
    using XMLConfigurationPtr = Poco::AutoPtr<XMLConfiguration>;

    using Paths = std::vector<String>;
    using StringToVector = std::map<String, std::vector<String>>;
    StringToVector substitutions;

    using StringKeyValue = std::map<String, String>;
    std::vector<StringKeyValue> substitutions_maps;

    bool gotSIGINT;
    std::vector<TestStopConditions> stop_conditions_by_run;
    String main_metric;
    bool lite_output;
    String profiles_file;

    Strings input_files;
    std::vector<XMLConfigurationPtr> tests_configurations;

    Strings tests_tags;
    Strings skip_tags;
    Strings tests_names;
    Strings skip_names;
    Strings tests_names_regexp;
    Strings skip_names_regexp;

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
    std::vector<Stats> statistics_by_run;

    /// Removes configurations that has a given value. If leave is true, the logic is reversed.
    void removeConfigurationsIf(
        std::vector<XMLConfigurationPtr> & configs, FilterType filter_type, const Strings & values, bool leave = false)
    {
        auto checker = [&filter_type, &values, &leave](XMLConfigurationPtr & config)
        {
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

                for (const String & config_tag : tags)
                {
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
                auto regex_checker = [&config_name](const String & name_regexp)
                {
                    std::regex pattern(name_regexp);
                    return std::regex_search(config_name, pattern);
                };

                remove_or_not = config->has("name") ? (std::find_if(values.begin(), values.end(), regex_checker) != values.end()) : false;
            }

            if (leave)
                remove_or_not = !remove_or_not;
            return remove_or_not;
        };

        auto new_end = std::remove_if(configs.begin(), configs.end(), checker);
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
    bool checkPreconditions(const XMLConfigurationPtr & config)
    {
        if (!config->has("preconditions"))
            return true;

        Keys preconditions;
        config->keys("preconditions", preconditions);
        size_t table_precondition_index = 0;

        for (const String & precondition : preconditions)
        {
            if (precondition == "flush_disk_cache")
            {
                if (system(
                        "(>&2 echo 'Flushing disk cache...') && (sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches') && (>&2 echo 'Flushed.')"))
                {
                    std::cerr << "Failed to flush disk cache" << std::endl;
                    return false;
                }
            }

            if (precondition == "ram_size")
            {
                size_t ram_size_needed = config->getUInt64("preconditions.ram_size");
                size_t actual_ram = getMemoryAmount();
                if (!actual_ram)
                    throw DB::Exception("ram_size precondition not available on this platform", ErrorCodes::NOT_IMPLEMENTED);

                if (ram_size_needed > actual_ram)
                {
                    std::cerr << "Not enough RAM: need = " << ram_size_needed << ", present = " << actual_ram << std::endl;
                    return false;
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

                    if (packet.type == Protocol::Server::Data)
                    {
                        for (const ColumnWithTypeAndName & column : packet.block)
                        {
                            if (column.name == "result" && column.column->size() > 0)
                            {
                                exist = column.column->get64(0);
                                if (exist)
                                    break;
                            }
                        }
                    }

                    if (packet.type == Protocol::Server::Exception || packet.type == Protocol::Server::EndOfStream)
                        break;
                }

                if (!exist)
                {
                    std::cerr << "Table " << table_to_check << " doesn't exist" << std::endl;
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
            tests_configurations[i] = XMLConfigurationPtr(new XMLConfiguration(path));
        }

        filterConfigurations();

        if (tests_configurations.size())
        {
            Strings outputs;

            for (auto & test_config : tests_configurations)
            {
                if (!checkPreconditions(test_config))
                {
                    std::cerr << "Preconditions are not fulfilled for test '" + test_config->getString("name", "") + "' ";
                    continue;
                }

                String output = runTest(test_config);
                if (lite_output)
                    std::cout << output;
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

    void extractSettings(
        const XMLConfigurationPtr & config, const String & key, const Strings & settings_list, std::map<String, String> & settings_to_apply)
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

    String runTest(XMLConfigurationPtr & test_config)
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
                    XMLConfigurationPtr profiles_config(new XMLConfiguration(profiles_file));

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
#define EXTRACT_SETTING(TYPE, NAME, DEFAULT, DESCRIPTION) \
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
            throw DB::Exception("Missing query fields in test's config: " + test_name);
        }

        if (test_config->has("query") && test_config->has("query_file"))
        {
            throw DB::Exception("Found both query and query_file fields. Choose only one");
        }

        if (test_config->has("query"))
        {
            queries = DB::getMultipleValuesFromConfig(*test_config, "", "query");
        }

        if (test_config->has("query_file"))
        {
            const String filename = test_config->getString("query_file");
            if (filename.empty())
                throw DB::Exception("Empty file name");

            bool tsv = fs::path(filename).extension().string() == ".tsv";

            ReadBufferFromFile query_file(filename);

            if (tsv)
            {
                while (!query_file.eof())
                {
                    readEscapedString(query, query_file);
                    assertChar('\n', query_file);
                    queries.push_back(query);
                }
            }
            else
            {
                readStringUntilEOF(query, query_file);
                queries.push_back(query);
            }
        }

        if (queries.empty())
        {
            throw DB::Exception("Did not find any query to execute: " + test_name);
        }

        if (test_config->has("substitutions"))
        {
            /// Make "subconfig" of inner xml block
            ConfigurationPtr substitutions_view(test_config->createView("substitutions"));
            constructSubstitutions(substitutions_view, substitutions);

            auto queries_pre_format = queries;
            queries.clear();
            for (const auto & query : queries_pre_format)
            {
                auto formatted = formatQueries(query, substitutions);
                queries.insert(queries.end(), formatted.begin(), formatted.end());
            }
        }

        if (!test_config->has("type"))
        {
            throw DB::Exception("Missing type property in config: " + test_name);
        }

        String config_exec_type = test_config->getString("type");
        if (config_exec_type == "loop")
            exec_type = ExecutionType::Loop;
        else if (config_exec_type == "once")
            exec_type = ExecutionType::Once;
        else
            throw DB::Exception("Unknown type " + config_exec_type + " in :" + test_name);

        times_to_run = test_config->getUInt("times_to_run", 1);

        stop_conditions_by_run.clear();
        TestStopConditions stop_conditions_template;
        if (test_config->has("stop_conditions"))
        {
            ConfigurationPtr stop_conditions_config(test_config->createView("stop_conditions"));
            stop_conditions_template.loadFromConfig(stop_conditions_config);
        }

        if (stop_conditions_template.empty())
            throw DB::Exception("No termination conditions were found in config");

        for (size_t i = 0; i < times_to_run * queries.size(); ++i)
            stop_conditions_by_run.push_back(stop_conditions_template);


        ConfigurationPtr metrics_view(test_config->createView("metrics"));
        Keys metrics;
        metrics_view->keys(metrics);

        main_metric.clear();
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
                throw DB::Exception("Specify main_metric for lite output");
        }

        if (metrics.size() > 0)
            checkMetricsInput(metrics);

        statistics_by_run.resize(times_to_run * queries.size());
        for (size_t number_of_launch = 0; number_of_launch < times_to_run; ++number_of_launch)
        {
            QueriesWithIndexes queries_with_indexes;

            for (size_t query_index = 0; query_index < queries.size(); ++query_index)
            {
                size_t statistic_index = number_of_launch * queries.size() + query_index;
                stop_conditions_by_run[statistic_index].reset();

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
                    throw DB::Exception("Wrong type of metric for loop execution type (" + metric + ")");
                }
            }
        }
        else
        {
            for (const String & metric : metrics)
            {
                if (std::find(loop_metrics.begin(), loop_metrics.end(), metric) != loop_metrics.end())
                {
                    throw DB::Exception("Wrong type of metric for non-loop execution type (" + metric + ")");
                }
            }
        }
    }

    void runQueries(const QueriesWithIndexes & queries_with_indexes)
    {
        for (const std::pair<Query, const size_t> & query_and_index : queries_with_indexes)
        {
            Query query = query_and_index.first;
            const size_t run_index = query_and_index.second;

            TestStopConditions & stop_conditions = stop_conditions_by_run[run_index];
            Stats & statistics = statistics_by_run[run_index];

            statistics.clear();
            try
            {
                execute(query, statistics, stop_conditions);

                if (exec_type == ExecutionType::Loop)
                {
                    for (size_t iteration = 1; !gotSIGINT; ++iteration)
                    {
                        stop_conditions.reportIterations(iteration);
                        if (stop_conditions.areFulfilled())
                            break;

                        execute(query, statistics, stop_conditions);
                    }
                }
            }
            catch (const DB::Exception & e)
            {
                statistics.exception = e.what() + String(", ") + e.displayText();
            }

            if (!gotSIGINT)
            {
                statistics.ready = true;
            }
        }
    }

    void execute(const Query & query, Stats & statistics, TestStopConditions & stop_conditions)
    {
        statistics.watch_per_query.restart();
        statistics.last_query_was_cancelled = false;
        statistics.last_query_rows_read = 0;
        statistics.last_query_bytes_read = 0;

        RemoteBlockInputStream stream(connection, query, {}, global_context, &settings);

        stream.setProgressCallback(
            [&](const Progress & value) { this->checkFulfilledConditionsAndUpdate(value, stream, statistics, stop_conditions); });

        stream.readPrefix();
        while (Block block = stream.read())
            ;
        stream.readSuffix();

        if (!statistics.last_query_was_cancelled)
            statistics.updateQueryInfo();

        statistics.setTotalTime();
    }

    void checkFulfilledConditionsAndUpdate(
        const Progress & progress, RemoteBlockInputStream & stream, Stats & statistics, TestStopConditions & stop_conditions)
    {
        statistics.add(progress.rows, progress.bytes);

        stop_conditions.reportRowsRead(statistics.total_rows_read);
        stop_conditions.reportBytesReadUncompressed(statistics.total_bytes_read);
        stop_conditions.reportTotalTime(statistics.watch.elapsed() / (1000 * 1000));
        stop_conditions.reportMinTimeNotChangingFor(statistics.min_time_watch.elapsed() / (1000 * 1000));
        stop_conditions.reportMaxSpeedNotChangingFor(statistics.max_rows_speed_watch.elapsed() / (1000 * 1000));
        stop_conditions.reportAverageSpeedNotChangingFor(statistics.avg_rows_speed_watch.elapsed() / (1000 * 1000));

        if (stop_conditions.areFulfilled())
        {
            statistics.last_query_was_cancelled = true;
            stream.cancel();
        }

        if (interrupt_listener.check())
        {
            gotSIGINT = true;
            statistics.last_query_was_cancelled = true;
            stream.cancel();
        }
    }

    void constructSubstitutions(ConfigurationPtr & substitutions_view, StringToVector & substitutions)
    {
        Keys xml_substitutions;
        substitutions_view->keys(xml_substitutions);

        for (size_t i = 0; i != xml_substitutions.size(); ++i)
        {
            const ConfigurationPtr xml_substitution(substitutions_view->createView("substitution[" + std::to_string(i) + "]"));

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

        json_output.set("hostname", getFQDNOrHostName());
        json_output.set("num_cores", getNumberOfPhysicalCPUCores());
        json_output.set("num_threads", std::thread::hardware_concurrency());
        json_output.set("ram", getMemoryAmount());
        json_output.set("server_version", server_version);
        json_output.set("time", DateLUT::instance().timeToString(time(nullptr)));
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
                    array_string += '"' + values[i] + '"';
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
            for (size_t number_of_launch = 0; number_of_launch < times_to_run; ++number_of_launch)
            {
                Stats & statistics = statistics_by_run[number_of_launch * queries.size() + query_index];

                if (!statistics.ready)
                    continue;

                JSONString runJSON;

                runJSON.set("query", queries[query_index]);
                if (!statistics.exception.empty())
                    runJSON.set("exception", statistics.exception);

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
                        runJSON.set("min_time", statistics.min_time / double(1000));

                    if (std::find(metrics.begin(), metrics.end(), "quantiles") != metrics.end())
                    {
                        JSONString quantiles(4); /// here, 4 is the size of \t padding
                        for (double percent = 10; percent <= 90; percent += 10)
                        {
                            String quantile_key = std::to_string(percent / 100.0);
                            while (quantile_key.back() == '0')
                                quantile_key.pop_back();

                            quantiles.set(quantile_key, statistics.sampler.quantileInterpolated(percent / 100.0));
                        }
                        quantiles.set("0.95", statistics.sampler.quantileInterpolated(95 / 100.0));
                        quantiles.set("0.99", statistics.sampler.quantileInterpolated(99 / 100.0));
                        quantiles.set("0.999", statistics.sampler.quantileInterpolated(99.9 / 100.0));
                        quantiles.set("0.9999", statistics.sampler.quantileInterpolated(99.99 / 100.0));

                        runJSON.set("quantiles", quantiles.asString());
                    }

                    if (std::find(metrics.begin(), metrics.end(), "total_time") != metrics.end())
                        runJSON.set("total_time", statistics.total_time);

                    if (std::find(metrics.begin(), metrics.end(), "queries_per_second") != metrics.end())
                        runJSON.set("queries_per_second", double(statistics.queries) / statistics.total_time);

                    if (std::find(metrics.begin(), metrics.end(), "rows_per_second") != metrics.end())
                        runJSON.set("rows_per_second", double(statistics.total_rows_read) / statistics.total_time);

                    if (std::find(metrics.begin(), metrics.end(), "bytes_per_second") != metrics.end())
                        runJSON.set("bytes_per_second", double(statistics.total_bytes_read) / statistics.total_time);
                }
                else
                {
                    if (std::find(metrics.begin(), metrics.end(), "max_rows_per_second") != metrics.end())
                        runJSON.set("max_rows_per_second", statistics.max_rows_speed);

                    if (std::find(metrics.begin(), metrics.end(), "max_bytes_per_second") != metrics.end())
                        runJSON.set("max_bytes_per_second", statistics.max_bytes_speed);

                    if (std::find(metrics.begin(), metrics.end(), "avg_rows_per_second") != metrics.end())
                        runJSON.set("avg_rows_per_second", statistics.avg_rows_speed_value);

                    if (std::find(metrics.begin(), metrics.end(), "avg_bytes_per_second") != metrics.end())
                        runJSON.set("avg_bytes_per_second", statistics.avg_bytes_speed_value);
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
                if (queries.size() > 1)
                {
                    output += "query \"" + queries[query_index] + "\", ";
                }

                if (substitutions_maps.size())
                {
                    for (auto it = substitutions_maps[query_index].begin(); it != substitutions_maps[query_index].end(); ++it)
                    {
                        output += it->first + " = " + it->second + ", ";
                    }
                }

                output += "run " + std::to_string(number_of_launch + 1) + ": ";
                output += main_metric + " = ";
                output += statistics_by_run[number_of_launch * queries.size() + query_index].getStatisticByName(main_metric);
                output += "\n";
            }
        }

        return output;
    }
};
}

static void getFilesFromDir(const fs::path & dir, std::vector<String> & input_files, const bool recursive = false)
{
    if (dir.extension().string() == ".xml")
        std::cerr << "Warning: '" + dir.string() + "' is a directory, but has .xml extension" << std::endl;

    fs::directory_iterator end;
    for (fs::directory_iterator it(dir); it != end; ++it)
    {
        const fs::path file = (*it);
        if (recursive && fs::is_directory(file))
            getFilesFromDir(file, input_files, recursive);
        else if (!fs::is_directory(file) && file.extension().string() == ".xml")
            input_files.push_back(file.string());
    }
}

int mainEntryClickHousePerformanceTest(int argc, char ** argv)
try
{
    using boost::program_options::value;
    using Strings = std::vector<String>;

    boost::program_options::options_description desc("Allowed options");
    desc.add_options()("help", "produce help message")("lite", "use lite version of output")(
        "profiles-file", value<String>()->default_value(""), "Specify a file with global profiles")(
        "host,h", value<String>()->default_value("localhost"), "")("port", value<UInt16>()->default_value(9000), "")(
        "database", value<String>()->default_value("default"), "")("user", value<String>()->default_value("default"), "")(
        "password", value<String>()->default_value(""), "")("tags", value<Strings>()->multitoken(), "Run only tests with tag")(
        "skip-tags", value<Strings>()->multitoken(), "Do not run tests with tag")("names",
        value<Strings>()->multitoken(),
        "Run tests with specific name")("skip-names", value<Strings>()->multitoken(), "Do not run tests with name")(
        "names-regexp", value<Strings>()->multitoken(), "Run tests with names matching regexp")("skip-names-regexp",
        value<Strings>()->multitoken(),
        "Do not run tests with names matching regexp")("recursive,r", "Recurse in directories to find all xml's");

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
    bool recursive = options.count("recursive");

    if (!options.count("input-files"))
    {
        std::cerr << "Trying to find test scenario files in the current folder...";
        fs::path curr_dir(".");

        getFilesFromDir(curr_dir, input_files, recursive);

        if (input_files.empty())
        {
            std::cerr << std::endl;
            throw DB::Exception("Did not find any xml files");
        }
        else
            std::cerr << " found " << input_files.size() << " files." << std::endl;
    }
    else
    {
        input_files = options["input-files"].as<Strings>();
        Strings collected_files;

        for (const String filename : input_files)
        {
            fs::path file(filename);

            if (!fs::exists(file))
                throw DB::Exception("File '" + filename + "' does not exist");

            if (fs::is_directory(file))
            {
                getFilesFromDir(file, collected_files, recursive);
            }
            else
            {
                if (file.extension().string() != ".xml")
                    throw DB::Exception("File '" + filename + "' does not have .xml extension");
                collected_files.push_back(filename);
            }
        }

        input_files = std::move(collected_files);
    }

    Strings tests_tags = options.count("tags") ? options["tags"].as<Strings>() : Strings({});
    Strings skip_tags = options.count("skip-tags") ? options["skip-tags"].as<Strings>() : Strings({});
    Strings tests_names = options.count("names") ? options["names"].as<Strings>() : Strings({});
    Strings skip_names = options.count("skip-names") ? options["skip-names"].as<Strings>() : Strings({});
    Strings tests_names_regexp = options.count("names-regexp") ? options["names-regexp"].as<Strings>() : Strings({});
    Strings skip_names_regexp = options.count("skip-names-regexp") ? options["skip-names-regexp"].as<Strings>() : Strings({});

    auto timeouts = DB::ConnectionTimeouts::getTCPTimeouts(DB::Settings());

    DB::PerformanceTest performanceTest(options["host"].as<String>(),
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
        std::move(skip_names_regexp),
        timeouts);

    return 0;
}
catch (...)
{
    std::cout << DB::getCurrentExceptionMessage(/*with stacktrace = */ true) << std::endl;
    int code = DB::getCurrentExceptionCode();
    return code ? code : 1;
}
