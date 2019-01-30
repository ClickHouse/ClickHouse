#include "TestStats.h"
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

void TestStats::update_max_speed(
    size_t max_speed_candidate,
    Stopwatch & max_speed_watch,
    UInt64 & max_speed)
{
    if (max_speed_candidate > max_speed)
    {
        max_speed = max_speed_candidate;
        max_speed_watch.restart();
    }
}


void TestStats::update_average_speed(
    double new_speed_info,
    Stopwatch & avg_speed_watch,
    size_t & number_of_info_batches,
    double precision,
    double & avg_speed_first,
    double & avg_speed_value)
{
    avg_speed_value = ((avg_speed_value * number_of_info_batches) + new_speed_info);
    ++number_of_info_batches;
    avg_speed_value /= number_of_info_batches;

    if (avg_speed_first == 0)
    {
        avg_speed_first = avg_speed_value;
    }

    if (std::abs(avg_speed_value - avg_speed_first) >= precision)
    {
        avg_speed_first = avg_speed_value;
        avg_speed_watch.restart();
    }
}

void TestStats::add(size_t rows_read_inc, size_t bytes_read_inc)
{
    total_rows_read += rows_read_inc;
    total_bytes_read += bytes_read_inc;
    last_query_rows_read += rows_read_inc;
    last_query_bytes_read += bytes_read_inc;

    double new_rows_speed = last_query_rows_read / watch_per_query.elapsedSeconds();
    double new_bytes_speed = last_query_bytes_read / watch_per_query.elapsedSeconds();

    /// Update rows speed
    update_max_speed(new_rows_speed, max_rows_speed_watch, max_rows_speed);
    update_average_speed(new_rows_speed,
        avg_rows_speed_watch,
        number_of_rows_speed_info_batches,
        avg_rows_speed_precision,
        avg_rows_speed_first,
        avg_rows_speed_value);
    /// Update bytes speed
    update_max_speed(new_bytes_speed, max_bytes_speed_watch, max_bytes_speed);
    update_average_speed(new_bytes_speed,
        avg_bytes_speed_watch,
        number_of_bytes_speed_info_batches,
        avg_bytes_speed_precision,
        avg_bytes_speed_first,
        avg_bytes_speed_value);
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
    max_rows_speed_watch.reset();
    max_bytes_speed_watch.reset();
    avg_rows_speed_watch.reset();
    avg_bytes_speed_watch.reset();
}


void TestStats::startWatches()
{
    watch.start();
    watch_per_query.start();
    min_time_watch.start();
    max_rows_speed_watch.start();
    max_bytes_speed_watch.start();
    avg_rows_speed_watch.start();
    avg_bytes_speed_watch.start();
}

}
