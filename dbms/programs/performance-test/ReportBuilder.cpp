#include "ReportBuilder.h"

#include <algorithm>
#include <regex>
#include <sstream>
#include <thread>

#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/getFQDNOrHostName.h>
#include <common/getMemoryAmount.h>

#include "JSONString.h"

namespace DB
{

namespace
{
const std::regex QUOTE_REGEX{"\""};
std::string getMainMetric(const PerformanceTestInfo & test_info)
{
    std::string main_metric;
    if (test_info.main_metric.empty())
        if (test_info.exec_type == ExecutionType::Loop)
            main_metric = "min_time";
        else
            main_metric = "rows_per_second";
    else
        main_metric = test_info.main_metric;
    return main_metric;
}
}

ReportBuilder::ReportBuilder(const std::string & server_version_)
    : server_version(server_version_)
    , hostname(getFQDNOrHostName())
    , num_cores(getNumberOfPhysicalCPUCores())
    , num_threads(std::thread::hardware_concurrency())
    , ram(getMemoryAmount())
{
}

std::string ReportBuilder::getCurrentTime() const
{
    return DateLUT::instance().timeToString(time(nullptr));
}

std::string ReportBuilder::buildFullReport(
    const PerformanceTestInfo & test_info,
    std::vector<TestStats> & stats,
    const std::vector<std::size_t> & queries_to_run) const
{
    JSONString json_output;

    json_output.set("hostname", hostname);
    json_output.set("num_cores", num_cores);
    json_output.set("num_threads", num_threads);
    json_output.set("ram", ram);
    json_output.set("server_version", server_version);
    json_output.set("time", getCurrentTime());
    json_output.set("test_name", test_info.test_name);
    json_output.set("path", test_info.path);
    json_output.set("main_metric", getMainMetric(test_info));

    if (test_info.substitutions.size())
    {
        JSONString json_parameters(2); /// here, 2 is the size of \t padding

        for (auto it = test_info.substitutions.begin(); it != test_info.substitutions.end(); ++it)
        {
            std::string parameter = it->first;
            Strings values = it->second;

            std::ostringstream array_string;
            array_string << "[";
            for (size_t i = 0; i != values.size(); ++i)
            {
                array_string << '"' << std::regex_replace(values[i], QUOTE_REGEX, "\\\"") << '"';
                if (i != values.size() - 1)
                {
                    array_string << ", ";
                }
            }
            array_string << ']';

            json_parameters.set(parameter, array_string.str());
        }

        json_output.set("parameters", json_parameters.asString());
    }

    std::vector<JSONString> run_infos;
    for (size_t query_index = 0; query_index < test_info.queries.size(); ++query_index)
    {
        if (!queries_to_run.empty() && std::find(queries_to_run.begin(), queries_to_run.end(), query_index) == queries_to_run.end())
            continue;

        for (size_t number_of_launch = 0; number_of_launch < test_info.times_to_run; ++number_of_launch)
        {
            size_t stat_index = number_of_launch * test_info.queries.size() + query_index;
            TestStats & statistics = stats[stat_index];

            if (!statistics.ready)
                continue;

            JSONString runJSON;

            auto query = std::regex_replace(test_info.queries[query_index], QUOTE_REGEX, "\\\"");
            runJSON.set("query", query);
            runJSON.set("query_index", query_index);
            if (!statistics.exception.empty())
                runJSON.set("exception", statistics.exception);

            if (test_info.exec_type == ExecutionType::Loop)
            {
                /// in seconds
                runJSON.set("min_time", statistics.min_time / double(1000));

                if (statistics.sampler.size() != 0)
                {
                    JSONString quantiles(4); /// here, 4 is the size of \t padding
                    for (double percent = 10; percent <= 90; percent += 10)
                    {
                        std::string quantile_key = std::to_string(percent / 100.0);
                        while (quantile_key.back() == '0')
                            quantile_key.pop_back();

                        quantiles.set(quantile_key,
                            statistics.sampler.quantileInterpolated(percent / 100.0));
                    }
                    quantiles.set("0.95",
                        statistics.sampler.quantileInterpolated(95 / 100.0));
                    quantiles.set("0.99",
                        statistics.sampler.quantileInterpolated(99 / 100.0));
                    quantiles.set("0.999",
                        statistics.sampler.quantileInterpolated(99.9 / 100.0));
                    quantiles.set("0.9999",
                        statistics.sampler.quantileInterpolated(99.99 / 100.0));

                    runJSON.set("quantiles", quantiles.asString());
                }

                runJSON.set("total_time", statistics.total_time);

                if (statistics.total_time != 0)
                {
                    runJSON.set("queries_per_second", static_cast<double>(statistics.queries) / statistics.total_time);
                    runJSON.set("rows_per_second", static_cast<double>(statistics.total_rows_read) / statistics.total_time);
                    runJSON.set("bytes_per_second", static_cast<double>(statistics.total_bytes_read) / statistics.total_time);
                }
            }
            else
            {
                runJSON.set("max_rows_per_second", statistics.max_rows_speed);
                runJSON.set("max_bytes_per_second", statistics.max_bytes_speed);
                runJSON.set("avg_rows_per_second", statistics.avg_rows_speed_value);
                runJSON.set("avg_bytes_per_second", statistics.avg_bytes_speed_value);
            }

            run_infos.push_back(runJSON);
        }
    }

    json_output.set("runs", run_infos);

    return json_output.asString();
}

std::string ReportBuilder::buildCompactReport(
    const PerformanceTestInfo & test_info,
    std::vector<TestStats> & stats,
    const std::vector<std::size_t> & queries_to_run) const
{

    std::ostringstream output;

    for (size_t query_index = 0; query_index < test_info.queries.size(); ++query_index)
    {
        if (!queries_to_run.empty() && std::find(queries_to_run.begin(), queries_to_run.end(), query_index) == queries_to_run.end())
            continue;

        for (size_t number_of_launch = 0; number_of_launch < test_info.times_to_run; ++number_of_launch)
        {
            if (test_info.queries.size() > 1)
                output << "query \"" << test_info.queries[query_index] << "\", ";

            output << "run " << std::to_string(number_of_launch + 1) << ": ";

            std::string main_metric = getMainMetric(test_info);

            output << main_metric << " = ";
            size_t index = number_of_launch * test_info.queries.size() + query_index;
            output << stats[index].getStatisticByName(main_metric);
            output << "\n";
        }
    }
    return output.str();
}
}
