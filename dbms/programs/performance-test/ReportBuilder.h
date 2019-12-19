#pragma once
#include "PerformanceTestInfo.h"
#include "JSONString.h"
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
        const std::vector<std::size_t> & queries_to_run,
        const Connections & connections,
        const ConnectionTimeouts & timeouts,
        StudentTTest & t_test) const;


    std::string buildCompactReport(
        const PerformanceTestInfo & test_info,
        std::vector<TestStats> & stats,
        const std::vector<std::size_t> & queries_to_run,
        const Connections & connections) const;

private:

    std::string getCurrentTime() const
    {
        return DateLUT::instance().timeToString(time(nullptr));
    }

    void buildRunsReport(
        const PerformanceTestInfo & test_info,
        std::vector<TestStats> & stats,
        const std::vector<std::size_t> & queries_to_run,
        const Connections & connections,
        const ConnectionTimeouts & timeouts,
        JSONString & json_output,
        StudentTTest & t_test) const;

};

}
