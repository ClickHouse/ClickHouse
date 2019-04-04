#pragma once
#include <string>
#include <vector>
#include <map>
#include <Core/Settings.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/AutoPtr.h>

#include "StopConditionsSet.h"
#include "TestStopConditions.h"
#include "TestStats.h"

namespace DB
{
enum class ExecutionType
{
    Loop,
    Once
};

using XMLConfiguration = Poco::Util::XMLConfiguration;
using XMLConfigurationPtr = Poco::AutoPtr<XMLConfiguration>;
using StringToVector = std::map<std::string, Strings>;

/// Class containing all info to run performance test
class PerformanceTestInfo
{
public:
    PerformanceTestInfo(XMLConfigurationPtr config, const std::string & profiles_file_, const Settings & global_settings_);

    std::string test_name;
    std::string path;
    std::string main_metric;

    Strings queries;

    std::string profiles_file;
    Settings settings;
    ExecutionType exec_type;
    StringToVector substitutions;
    size_t times_to_run;

    std::vector<TestStopConditions> stop_conditions_by_run;

    Strings create_queries;
    Strings fill_queries;
    Strings drop_queries;

private:
    void applySettings(XMLConfigurationPtr config);
    void extractQueries(XMLConfigurationPtr config);
    void processSubstitutions(XMLConfigurationPtr config);
    void getExecutionType(XMLConfigurationPtr config);
    void getStopConditions(XMLConfigurationPtr config);
    void getMetrics(XMLConfigurationPtr config);
    void extractAuxiliaryQueries(XMLConfigurationPtr config);
};

}
