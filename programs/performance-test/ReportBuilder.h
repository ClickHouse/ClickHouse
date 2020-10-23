#pragma once
#include "PerformanceTestInfo.h"
#include <vector>
#include <string>

namespace DB
{

class ReportBuilder
{
public:
    ReportBuilder(const std::string & server_version_);
    std::string buildFullReport(
        const PerformanceTestInfo & test_info,
        std::vector<TestStats> & stats,
        const std::vector<std::size_t> & queries_to_run) const;


    std::string buildCompactReport(
        const PerformanceTestInfo & test_info,
        std::vector<TestStats> & stats,
        const std::vector<std::size_t> & queries_to_run) const;

private:
    std::string server_version;
    std::string hostname;
    size_t num_cores;
    size_t num_threads;
    size_t ram;

private:
    std::string getCurrentTime() const;

};

}
