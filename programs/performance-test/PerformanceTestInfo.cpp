#include "PerformanceTestInfo.h"
#include <Common/getMultipleKeysFromConfig.h>
#include <Common/SettingsChanges.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include "applySubstitutions.h"
#include <filesystem>
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


namespace fs = std::filesystem;

PerformanceTestInfo::PerformanceTestInfo(
    XMLConfigurationPtr config,
    const Settings & global_settings_)
    : settings(global_settings_)
{
    path = config->getString("path");
    test_name = fs::path(path).stem().string();
    applySettings(config);
    extractQueries(config);
    extractAuxiliaryQueries(config);
    processSubstitutions(config);
    getExecutionType(config);
    getStopConditions(config);
}

void PerformanceTestInfo::applySettings(XMLConfigurationPtr config)
{
    if (config->has("settings"))
    {
        SettingsChanges settings_to_apply;
        Strings config_settings;
        config->keys("settings", config_settings);
        extractSettings(config, "settings", config_settings, settings_to_apply);
        settings.applyChanges(settings_to_apply);
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

        auto create_and_fill_queries_preformat = create_and_fill_queries;
        create_and_fill_queries.clear();
        for (const auto & query : create_and_fill_queries_preformat)
        {
            auto formatted = formatQueries(query, substitutions);
            create_and_fill_queries.insert(create_and_fill_queries.end(), formatted.begin(), formatted.end());
        }

        auto queries_preformat = queries;
        queries.clear();
        for (const auto & query : queries_preformat)
        {
            auto formatted = formatQueries(query, substitutions);
            queries.insert(queries.end(), formatted.begin(), formatted.end());
        }

        auto drop_queries_preformat = drop_queries;
        drop_queries.clear();
        for (const auto & query : drop_queries_preformat)
        {
            auto formatted = formatQueries(query, substitutions);
            drop_queries.insert(drop_queries.end(), formatted.begin(), formatted.end());
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
    {
        create_and_fill_queries = getMultipleValuesFromConfig(*config, "", "create_query");
    }

    if (config->has("fill_query"))
    {
        auto fill_queries = getMultipleValuesFromConfig(*config, "", "fill_query");
        create_and_fill_queries.insert(create_and_fill_queries.end(), fill_queries.begin(), fill_queries.end());
    }

    if (config->has("drop_query"))
    {
        drop_queries = getMultipleValuesFromConfig(*config, "", "drop_query");
    }
}

}
