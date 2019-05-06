#include "PerformanceTestInfo.h"
#include <Common/getMultipleKeysFromConfig.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <boost/filesystem.hpp>
#include "applySubstitutions.h"
#include <iostream>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace
{

void extractSettings(
    const XMLConfigurationPtr & config,
    const std::string & key,
    const Strings & settings_list,
    SettingsChanges & settings_to_apply)
{
    for (const std::string & setup : settings_list)
    {
        if (setup == "profile")
            continue;

        std::string value = config->getString(key + "." + setup);
        if (value.empty())
            value = "true";

        settings_to_apply.emplace_back(SettingChange{setup, value});
    }
}

}


namespace fs = boost::filesystem;

PerformanceTestInfo::PerformanceTestInfo(
    XMLConfigurationPtr config,
    const std::string & profiles_file_,
    const Settings & global_settings_)
    : profiles_file(profiles_file_)
    , settings(global_settings_)
{
    test_name = config->getString("name");
    path = config->getString("path");
    if (config->has("main_metric"))
    {
        Strings main_metrics;
        config->keys("main_metric", main_metrics);
        if (main_metrics.size())
            main_metric = main_metrics[0];
    }

    applySettings(config);
    extractQueries(config);
    processSubstitutions(config);
    getExecutionType(config);
    getStopConditions(config);
    extractAuxiliaryQueries(config);
}

void PerformanceTestInfo::applySettings(XMLConfigurationPtr config)
{
    if (config->has("settings"))
    {
        SettingsChanges settings_to_apply;
        Strings config_settings;
        config->keys("settings", config_settings);

        auto settings_contain = [&config_settings] (const std::string & setting)
        {
            auto position = std::find(config_settings.begin(), config_settings.end(), setting);
            return position != config_settings.end();

        };
        /// Preprocess configuration file
        if (settings_contain("profile"))
        {
            if (!profiles_file.empty())
            {
                std::string profile_name = config->getString("settings.profile");
                XMLConfigurationPtr profiles_config(new XMLConfiguration(profiles_file));

                Strings profile_settings;
                profiles_config->keys("profiles." + profile_name, profile_settings);

                extractSettings(profiles_config, "profiles." + profile_name, profile_settings, settings_to_apply);
            }
        }

        extractSettings(config, "settings", config_settings, settings_to_apply);
        settings.applyChanges(settings_to_apply);

        if (settings_contain("average_rows_speed_precision"))
            TestStats::avg_rows_speed_precision =
                config->getDouble("settings.average_rows_speed_precision");

        if (settings_contain("average_bytes_speed_precision"))
            TestStats::avg_bytes_speed_precision =
                config->getDouble("settings.average_bytes_speed_precision");
    }
}

void PerformanceTestInfo::extractQueries(XMLConfigurationPtr config)
{
    if (config->has("query"))
        queries = getMultipleValuesFromConfig(*config, "", "query");

    if (config->has("query_file"))
    {
        const std::string filename = config->getString("query_file");
        if (filename.empty())
            throw Exception("Empty file name", ErrorCodes::BAD_ARGUMENTS);

        bool tsv = fs::path(filename).extension().string() == ".tsv";

        ReadBufferFromFile query_file(filename);
        std::string query;

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
        throw Exception("Did not find any query to execute: " + test_name,
            ErrorCodes::BAD_ARGUMENTS);
}

void PerformanceTestInfo::processSubstitutions(XMLConfigurationPtr config)
{
    if (config->has("substitutions"))
    {
        /// Make "subconfig" of inner xml block
        ConfigurationPtr substitutions_view(config->createView("substitutions"));
        constructSubstitutions(substitutions_view, substitutions);

        auto queries_pre_format = queries;
        queries.clear();
        for (const auto & query : queries_pre_format)
        {
            auto formatted = formatQueries(query, substitutions);
            queries.insert(queries.end(), formatted.begin(), formatted.end());
        }
    }
}

void PerformanceTestInfo::getExecutionType(XMLConfigurationPtr config)
{
    if (!config->has("type"))
        throw Exception("Missing type property in config: " + test_name,
            ErrorCodes::BAD_ARGUMENTS);

    std::string config_exec_type = config->getString("type");
    if (config_exec_type == "loop")
        exec_type = ExecutionType::Loop;
    else if (config_exec_type == "once")
        exec_type = ExecutionType::Once;
    else
        throw Exception("Unknown type " + config_exec_type + " in :" + test_name,
            ErrorCodes::BAD_ARGUMENTS);
}


void PerformanceTestInfo::getStopConditions(XMLConfigurationPtr config)
{
    TestStopConditions stop_conditions_template;
    if (config->has("stop_conditions"))
    {
        ConfigurationPtr stop_conditions_config(config->createView("stop_conditions"));
        stop_conditions_template.loadFromConfig(stop_conditions_config);
    }

    if (stop_conditions_template.empty())
        throw Exception("No termination conditions were found in config",
            ErrorCodes::BAD_ARGUMENTS);

    times_to_run = config->getUInt("times_to_run", 1);

    for (size_t i = 0; i < times_to_run * queries.size(); ++i)
        stop_conditions_by_run.push_back(stop_conditions_template);

}

void PerformanceTestInfo::extractAuxiliaryQueries(XMLConfigurationPtr config)
{
    if (config->has("create_query"))
        create_queries = getMultipleValuesFromConfig(*config, "", "create_query");

    if (config->has("fill_query"))
        fill_queries = getMultipleValuesFromConfig(*config, "", "fill_query");

    if (config->has("drop_query"))
        drop_queries = getMultipleValuesFromConfig(*config, "", "drop_query");
}

}
