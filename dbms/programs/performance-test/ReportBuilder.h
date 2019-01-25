#pragma once
#include "PerformanceTestInfo.h"

namespace DB
{

class ReportBuilder
{
public:
    explicit ReportBuilder(const std::string & server_version_);
    std::string buildFullReport(
        const PerformanceTestInfo & test_info,
        std::vector<TestStats> & stats) const;

    std::string buildCompactReport(
        const PerformanceTestInfo & test_info,
        std::vector<TestStats> & stats) const;
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
