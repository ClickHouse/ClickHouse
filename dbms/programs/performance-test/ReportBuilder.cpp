#include "ReportBuilder.h"

#include <algorithm>
#include <regex>
#include <sstream>
#include <thread>

#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/getFQDNOrHostName.h>
#include <common/getMemoryAmount.h>
#include <Common/StringUtils/StringUtils.h>


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
            main_metric = "max_rows_per_second";
    else
        main_metric = test_info.main_metric;
    return main_metric;
}

bool isASCIIString(const std::string & str)
{
    return std::all_of(str.begin(), str.end(), isASCII);
}

}

std::string ReportBuilder::buildFullReport(
        const PerformanceTestInfo & test_info,
        std::vector<TestStats> & stats,
        const std::vector<std::size_t> & queries_to_run,
        const Connections & connections,
        const ConnectionTimeouts & timeouts,
        StudentTTest & t_test) const
{
    JSONString json_output;

    json_output.set("time", getCurrentTime());
    json_output.set("test_name", test_info.test_name);
    json_output.set("path", test_info.path);
    json_output.set("main_metric", getMainMetric(test_info));

    if (!test_info.substitutions.empty())
    {
        JSONString json_parameters(2); /// here, 2 is the size of \t padding

        for (auto & [parameter, values] : test_info.substitutions)
        {
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

    buildRunsReport(test_info, stats, queries_to_run, connections, timeouts, json_output, t_test);

    return json_output.asString();
}

void ReportBuilder::buildRunsReport(
        const PerformanceTestInfo & test_info,
        std::vector<TestStats> & stats,
        const std::vector<std::size_t> & queries_to_run,
        const Connections & connections,
        const ConnectionTimeouts & timeouts,
        JSONString & json_output,
        StudentTTest & t_test) const
{
    std::vector<JSONString> run_infos;

    for (size_t query_index = 0; query_index < test_info.queries.size(); ++query_index)
    {
        if (!queries_to_run.empty() && std::find(queries_to_run.begin(), queries_to_run.end(), query_index) == queries_to_run.end())
            continue;

        for (size_t run_index = 0; run_index < test_info.times_to_run; ++run_index)
        {
            JSONString run_info(2);

            run_info.set("query", std::regex_replace(test_info.queries[query_index], QUOTE_REGEX, "\\\""));
            run_info.set("query_index", query_index);

            bool t_test_status = test_info.stop_conditions_by_run[run_index][0].isInitializedTTestWithConfidenceLevel();
            if (t_test_status)
                run_info.set("t_test_status", t_test.reportResults(ConnectionTestStats::t_test_confidence_level, ConnectionTestStats::t_test_comparison_precision));

            std::vector<JSONString> run_by_connection_infos;

            for (size_t connection_index = 0; connection_index < connections.size(); ++connection_index)
            {
                size_t stat_index = run_index * test_info.queries.size() + query_index;
                ConnectionTestStats & statistics = stats[stat_index][connection_index];

                if (!statistics.ready)
                    continue;

                JSONString run_by_connection(3);
                run_by_connection.set("connection", connections[connection_index]->getDescription());

                std::string name;
                UInt64 version_major;
                UInt64 version_minor;
                UInt64 version_patch;
                UInt64 version_revision;

                connections[connection_index]->getServerVersion(timeouts, name, version_major, version_minor, version_patch, version_revision);

                std::stringstream ss;
                ss << version_major << "." << version_minor << "." << version_patch;
                std::string connection_server_version = ss.str();

                run_by_connection.set("server_version", connection_server_version);

                if (!statistics.exception.empty())
                {
                    if (isASCIIString(statistics.exception))
                        run_by_connection.set("exception", std::regex_replace(statistics.exception, QUOTE_REGEX, "\\\""));
                    else
                        run_by_connection.set("exception", "Some exception occurred with non ASCII message. This may produce invalid JSON. Try reproduce locally.");
                }

                if (test_info.exec_type == ExecutionType::Loop)
                {
                    /// in seconds
                    run_by_connection.set("min_time", statistics.min_time / 1000000.0);

                    if (statistics.sampler.size() != 0)
                    {
                        JSONString quantiles(4); /// here, 4 is the size of \t padding
                        for (int percent = 10; percent <= 90; percent += 10)
                        {
                            std::string quantile_key = std::to_string(percent / 100.0).substr(0, 3);
                            quantiles.set(quantile_key, statistics.sampler.quantileInterpolated(percent / 100.0));
                        }
                        quantiles.set("0.95", statistics.sampler.quantileInterpolated(95 / 100.0));
                        quantiles.set("0.99", statistics.sampler.quantileInterpolated(99 / 100.0));
                        quantiles.set("0.999", statistics.sampler.quantileInterpolated(99.9 / 100.0));
                        quantiles.set("0.9999", statistics.sampler.quantileInterpolated(99.99 / 100.0));

                        run_by_connection.set("quantiles", quantiles.asString());
                    }

                    run_by_connection.set("total_time", statistics.total_time);

                    if (statistics.total_time != 0)
                    {
                        run_by_connection.set("queries_number", statistics.queries);
                        run_by_connection.set("queries_per_second", (statistics.queries) / statistics.total_time);
                        run_by_connection.set("rows_per_second", (statistics.total_rows_read) / statistics.total_time);
                        run_by_connection.set("bytes_per_second", (statistics.total_bytes_read) / statistics.total_time);
                    }
                }
                else
                {
                    run_by_connection.set("max_rows_per_second", statistics.max_rows_speed);
                    run_by_connection.set("max_bytes_per_second", statistics.max_bytes_speed);
                    run_by_connection.set("avg_rows_per_second", statistics.avg_rows_speed_value);
                    run_by_connection.set("avg_bytes_per_second", statistics.avg_bytes_speed_value);
                }

                run_by_connection.set("memory_usage", statistics.memory_usage);

                run_by_connection_infos.push_back(run_by_connection);
            }
            run_info.set("runs_by_connections", run_by_connection_infos);
            run_infos.push_back(run_info);
        }
    }
    json_output.set("runs", run_infos);
}

std::string ReportBuilder::buildCompactReport(
    const PerformanceTestInfo & test_info,
    std::vector<TestStats> & stats,
    const std::vector<std::size_t> & queries_to_run,
    const Connections & connections) const
{
    std::ostringstream output;
    for (size_t connection_index = 0; connection_index < connections.size(); ++connection_index)
    {
        output << "connection \"" << connections[connection_index]->getDescription() << "\"\n";

        for (size_t query_index = 0; query_index < test_info.queries.size(); ++query_index)
        {
            if (!queries_to_run.empty() && std::find(queries_to_run.begin(), queries_to_run.end(), query_index) == queries_to_run.end())
                continue;

            for (size_t run_index = 0; run_index < test_info.times_to_run; ++run_index)
            {
                if (test_info.queries.size() > 1)
                    output << "query \"" << test_info.queries[query_index] << "\", ";

                output << "run " << std::to_string(run_index + 1) << ": ";

                std::string main_metric = getMainMetric(test_info);

                output << main_metric << " = ";
                size_t index = run_index * test_info.queries.size() + query_index;
                output << stats[index][connection_index].getStatisticByName(main_metric);
                output << "\n";
            }
        }
    }
    return output.str();
}

}
