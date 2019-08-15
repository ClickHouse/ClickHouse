#pragma once
#include "PerformanceTestInfo.h"
#include <IO/ConnectionTimeouts.h>
#include <Client/Connection.h>
#include <vector>
#include <string>

namespace DB
{

class ReportBuilder
{
public:
    ReportBuilder();

    std::string buildFullReport(
        const PerformanceTestInfo & test_info,
        const std::vector<TestStatsPtrs> & stats,
        const Connections & connections,
        const ConnectionTimeouts & timeouts,
        size_t connection_index,
        const std::vector<std::size_t> & queries_to_run) const;


    std::string buildCompactReport(
        const PerformanceTestInfo & test_info,
        const std::vector<TestStatsPtrs> & stats,
        const Connections & connections,
        const ConnectionTimeouts & timeouts,
        size_t connection_index,
        const std::vector<std::size_t> & queries_to_run) const;

private:
    std::string hostname;
    size_t num_cores;
    size_t num_threads;
    size_t ram;

private:
    std::string getCurrentTime() const;

};

}
