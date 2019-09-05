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

    std::string buildFullReport(
        const PerformanceTestInfo & test_info,
        std::vector<TestStats> & stats,
        const Connections & connections,
        const ConnectionTimeouts & timeouts,
        size_t connection_index,
        const std::vector<std::size_t> & queries_to_run) const;


    std::string buildCompactReport(
        const PerformanceTestInfo & test_info,
        std::vector<TestStats> & stats,
        const Connections & connections,
        const ConnectionTimeouts & timeouts,
        size_t connection_index,
        const std::vector<std::size_t> & queries_to_run) const;

private:

    std::string getCurrentTime() const;

};

}
