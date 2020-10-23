#include "TestStats.h"
#include <algorithm>
namespace DB
{

namespace
{
const std::string FOUR_SPACES = "    ";
}

std::string TestStats::getStatisticByName(const std::string & statistic_name)
{
    if (statistic_name == "min_time")
        return std::to_string(min_time) + "ms";

    if (statistic_name == "quantiles")
    {
        std::string result = "\n";

        for (double percent = 10; percent <= 90; percent += 10)
        {
            result += FOUR_SPACES + std::to_string((percent / 100));
            result += ": " + std::to_string(sampler.quantileInterpolated(percent / 100.0));
            result += "\n";
        }
        result += FOUR_SPACES + "0.95:   " + std::to_string(sampler.quantileInterpolated(95 / 100.0)) + "\n";
        result += FOUR_SPACES + "0.99: " + std::to_string(sampler.quantileInterpolated(99 / 100.0)) + "\n";
        result += FOUR_SPACES + "0.999: " + std::to_string(sampler.quantileInterpolated(99.9 / 100.)) + "\n";
        result += FOUR_SPACES + "0.9999: " + std::to_string(sampler.quantileInterpolated(99.99 / 100.));

        return result;
    }
    if (statistic_name == "total_time")
        return std::to_string(total_time) + "s";

    if (statistic_name == "queries_per_second")
        return std::to_string(queries / total_time);

    if (statistic_name == "rows_per_second")
        return std::to_string(total_rows_read / total_time);

    if (statistic_name == "bytes_per_second")
        return std::to_string(total_bytes_read / total_time);

    if (statistic_name == "max_rows_per_second")
        return std::to_string(max_rows_speed);

    if (statistic_name == "max_bytes_per_second")
        return std::to_string(max_bytes_speed);

    if (statistic_name == "avg_rows_per_second")
        return std::to_string(avg_rows_speed_value);

    if (statistic_name == "avg_bytes_per_second")
        return std::to_string(avg_bytes_speed_value);

    return "";
}


void TestStats::update_min_time(UInt64 min_time_candidate)
{
    if (min_time_candidate < min_time)
    {
        min_time = min_time_candidate;
        min_time_watch.restart();
    }
}


void TestStats::add(size_t rows_read_inc, size_t bytes_read_inc)
{
    total_rows_read += rows_read_inc;
    total_bytes_read += bytes_read_inc;
    last_query_rows_read += rows_read_inc;
    last_query_bytes_read += bytes_read_inc;
}

void TestStats::updateQueryInfo()
{
    ++queries;
    sampler.insert(watch_per_query.elapsedSeconds());
    update_min_time(watch_per_query.elapsed() / (1000 * 1000)); /// ns to ms
}


TestStats::TestStats()
{
    watch.reset();
    watch_per_query.reset();
    min_time_watch.reset();
}


void TestStats::startWatches()
{
    watch.start();
    watch_per_query.start();
    min_time_watch.start();
}

}
