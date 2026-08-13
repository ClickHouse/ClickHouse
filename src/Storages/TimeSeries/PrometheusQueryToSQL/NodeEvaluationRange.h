#pragma once

#include <base/Decimal.h>


namespace DB::PrometheusQueryToSQL
{

/// Represents evaluation times found for a node in PrometheusQueryTree.
///
/// Example: http_requests_total[1h:10m]
///
/// The query `http_requests_total[1h:10m]` is evaluated as
/// last_over_time(http_requests_total[5m])[1h:10m]
/// (because `instant_selector_window` is `5m` by default)
///
/// So
/// start_time = now - 1h
/// end_time = now
/// step = 10m
/// window = 5m
///
/// Thus we read data from our table with timestamps in any of these windows:
///
/// TIME ---------->
///
/// (start_time - window)      start_time      (start_time + step - window)      (start_time + step)      (start_time + 2 * step - window)      (start_time + 2 * step)      ...      (end_time - window)      end_time
///  ************************************       ****************************************************       ************************************************************                ********************************
///             first window                                     second window                                                 third window                                                      last window

struct NodeEvaluationRange
{
    DateTime64 start_time;
    DateTime64 end_time;
    Decimal64 step;
    Decimal64 window;

    bool empty() const { return start_time > end_time; }  /// If `start_time == end_time` it's one point
};

}
