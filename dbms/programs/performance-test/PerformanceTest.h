#pragma once

#include <Client/Connection.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Common/InterruptListener.h>
#include "PerformanceTestInfo.h"


namespace DB
{

using XMLConfiguration = Poco::Util::XMLConfiguration;
using XMLConfigurationPtr = Poco::AutoPtr<XMLConfiguration>;
using QueriesWithIndexes = std::vector<std::pair<std::string, size_t>>;


class PerformanceTest
{
public:

    PerformanceTest(
        const XMLConfigurationPtr & config_,
        Connection & connection_,
        InterruptListener & interrupt_listener_,
        const PerformanceTestInfo & test_info_);

    bool checkPreconditions() const;
    std::vector<TestStats> execute();

    const PerformanceTestInfo & getTestInfo() const
    {
        return test_info;
    }

private:
    void runQueries(
        const QueriesWithIndexes & queries_with_indexes,
        std::vector<TestStats> & statistics_by_run);


private:
    XMLConfigurationPtr config;
    Connection & connection;
    InterruptListener & interrupt_listener;

    PerformanceTestInfo test_info;

};
}
