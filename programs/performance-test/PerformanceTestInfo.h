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

using XMLConfiguration = Poco::Util::XMLConfiguration;
using XMLConfigurationPtr = Poco::AutoPtr<XMLConfiguration>;
using StringToVector = std::map<std::string, Strings>;

/// Class containing all info to run performance test
class PerformanceTestInfo
{
public:
    PerformanceTestInfo(XMLConfigurationPtr config, const Settings & global_settings_);

    std::string test_name;
    std::string path;

    Strings queries;

    Settings settings;
    StringToVector substitutions;
    size_t times_to_run;

    std::vector<TestStopConditions> stop_conditions_by_run;

    Strings create_and_fill_queries;
    Strings drop_queries;

private:
    void applySettings(XMLConfigurationPtr config);
    void extractQueries(XMLConfigurationPtr config);
    void processSubstitutions(XMLConfigurationPtr config);
    void getStopConditions(XMLConfigurationPtr config);
    void extractAuxiliaryQueries(XMLConfigurationPtr config);
};

}
