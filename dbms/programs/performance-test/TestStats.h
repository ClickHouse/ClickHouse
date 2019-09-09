#pragma once

#include <Core/Types.h>
#include <limits>
#include <Common/Stopwatch.h>
#include <AggregateFunctions/ReservoirSampler.h>

namespace DB
{
struct TestStats
{
    TestStats();
    Stopwatch watch;
    Stopwatch watch_per_query;
    Stopwatch min_time_watch;
    Stopwatch max_rows_speed_watch;
    Stopwatch max_bytes_speed_watch;
    Stopwatch avg_rows_speed_watch;
    Stopwatch avg_bytes_speed_watch;

    bool last_query_was_cancelled = false;
    std::string query_id;

    size_t queries = 0;

    size_t total_rows_read = 0;
    size_t total_bytes_read = 0;

    size_t last_query_rows_read = 0;
    size_t last_query_bytes_read = 0;

    using Sampler = ReservoirSampler<double>;
    Sampler sampler{1 << 16};

    /// min_time in ms
    UInt64 min_time = std::numeric_limits<UInt64>::max();
    double total_time = 0;

    UInt64 max_rows_speed = 0;
    UInt64 max_bytes_speed = 0;

    double avg_rows_speed_value = 0;
    double avg_rows_speed_first = 0;
    static inline double avg_rows_speed_precision = 0.005;

    double avg_bytes_speed_value = 0;
    double avg_bytes_speed_first = 0;
    static inline double avg_bytes_speed_precision = 0.005;

    size_t number_of_rows_speed_info_batches = 0;
    size_t number_of_bytes_speed_info_batches = 0;

    UInt64 memory_usage = 0;

    bool ready = false; // check if a query wasn't interrupted by SIGINT
    std::string exception;

    /// Hack, actually this field doesn't required for statistics
    bool got_SIGINT = false;

    std::string getStatisticByName(const std::string & statistic_name);

    void update_min_time(UInt64 min_time_candidate);

    void update_average_speed(
        double new_speed_info,
        Stopwatch & avg_speed_watch,
        size_t & number_of_info_batches,
        double precision,
        double & avg_speed_first,
        double & avg_speed_value);

    void update_max_speed(
        size_t max_speed_candidate,
        Stopwatch & max_speed_watch,
        UInt64 & max_speed);

    void add(size_t rows_read_inc, size_t bytes_read_inc);

    void updateQueryInfo();

    void setTotalTime()
    {
        total_time = watch.elapsedSeconds();
    }

    void startWatches();
};

}
