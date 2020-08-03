#pragma once

#include <Client/Connection.h>
#include <Common/InterruptListener.h>
#include <common/logger_useful.h>
#include <Poco/Util/XMLConfiguration.h>

#include <IO/ConnectionTimeouts.h>
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
        const ConnectionTimeouts & timeouts_,
        InterruptListener & interrupt_listener_,
        const PerformanceTestInfo & test_info_,
        Context & context_,
        const std::vector<size_t> & queries_to_run_);

    bool checkPreconditions() const;
    void prepare() const;
    std::vector<TestStats> execute();
    void finish() const;

    bool checkSIGINT() const
    {
        return got_SIGINT;
    }

private:
    void runQueries(
        const QueriesWithIndexes & queries_with_indexes,
        std::vector<TestStats> & statistics_by_run);

    UInt64 calculateMaxExecTime() const;

private:
    XMLConfigurationPtr config;
    Connection & connection;
    const ConnectionTimeouts & timeouts;
    InterruptListener & interrupt_listener;

    PerformanceTestInfo test_info;
    Context & context;

    std::vector<size_t> queries_to_run;
    Poco::Logger * log;

    bool got_SIGINT = false;
};

}
