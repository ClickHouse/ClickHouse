-- `timeSeriesGroupArray` keeps the samples of a state sorted by timestamp, so merging two states combines
-- two sorted runs. `arrayReduce` merges the states in array order, which fixes which state is merged into
-- which - the two disjoint cases below differ only in that direction.

SET allow_experimental_time_series_aggregate_functions = 1;

WITH
    (SELECT timeSeriesGroupArrayState(toDateTime64(number, 3, 'UTC'), number::Float64) FROM numbers(3)) AS earlier,
    (SELECT timeSeriesGroupArrayState(toDateTime64(number + 10, 3, 'UTC'), (number + 10)::Float64) FROM numbers(3)) AS later,
    (SELECT timeSeriesGroupArrayState(toDateTime64(number * 2, 3, 'UTC'), (number * 2)::Float64) FROM numbers(3)) AS interleaved,
    (SELECT timeSeriesGroupArrayState(toDateTime64(pair.1, 3, 'UTC'), pair.2::Float64)
     FROM (SELECT arrayJoin([(12, 12.), (10, 10.), (11, 11.)]) AS pair)) AS out_of_order,
    (SELECT timeSeriesGroupArrayState(toDateTime64(number, 3, 'UTC'), number::Float64) FROM numbers(0)) AS empty
SELECT
    arrayReduce('timeSeriesGroupArrayMerge', [earlier, later]) AS disjoint_later_merged_in,
    arrayReduce('timeSeriesGroupArrayMerge', [later, earlier]) AS disjoint_earlier_merged_in,
    arrayReduce('timeSeriesGroupArrayMerge', [earlier, interleaved]) AS interleaved_ranges,
    arrayReduce('timeSeriesGroupArrayMerge', [earlier, out_of_order]) AS out_of_order_merged_in,
    arrayReduce('timeSeriesGroupArrayMerge', [out_of_order, earlier]) AS out_of_order_accumulated,
    arrayReduce('timeSeriesGroupArrayMerge', [earlier, empty]) AS empty_merged_in,
    arrayReduce('timeSeriesGroupArrayMerge', [empty, earlier]) AS empty_accumulated
FORMAT Vertical;
