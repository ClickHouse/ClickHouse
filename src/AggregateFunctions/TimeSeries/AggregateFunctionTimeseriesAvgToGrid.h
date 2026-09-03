#pragma once

#include <cmath>
#include <cstddef>
#include <optional>
#include <utility>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSamples.h>
#include <Common/DequeWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>


namespace DB
{

/// Traits for `timeSeriesAvgToGrid`, the ClickHouse counterpart of PromQL's `avg_over_time`.
template <typename TimestampType_, typename IntervalType_, typename ValueType_>
struct AggregateFunctionTimeseriesAvgToGridTraits
{
    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;

    using Samples = AggregateFunctionTimeseriesSamples<TimestampType, ValueType, /* keep_duplicates = */ true>;

    using ResultType = ValueType;

    static String getName() { return "timeSeriesAvgToGrid"; }

    /// The window keeps its buckets and folds their raw samples at each grid point, rather than
    /// combining a rounded per-bucket total. Merging per-bucket summaries is not equivalent to
    /// replaying the samples: a bucket would contribute its own rounded sum, so the reported average
    /// would depend on where the bucket boundaries fall, which is an internal detail of the grid and
    /// not part of `avg_over_time`. The buckets outlive this aggregator, so they are referenced.
    struct Aggregator
    {
        using OrderedSamples = VectorWithMemoryTracking<std::pair<TimestampType, ValueType>>;

        /// An already-ordered bucket is replayed in place. A rare out-of-order one is ordered once,
        /// here, because `forEachSample` iterates such a bucket through a fresh sorted copy every
        /// call - and this aggregator replays each bucket once per grid point it survives, unlike
        /// sum/count which fold it once on admission.
        struct WindowBucket
        {
            TimestampType bucket_end_timestamp{};
            const Samples * samples = nullptr;
            OrderedSamples ordered;
        };

        DequeWithMemoryTracking<WindowBucket> window;

        explicit Aggregator(size_t /*stack_size*/)
        {
        }

        void add(const Samples & samples, TimestampType bucket_end_timestamp)
        {
            WindowBucket bucket;
            bucket.bucket_end_timestamp = bucket_end_timestamp;

            if (samples.isSorted())
                bucket.samples = &samples;
            else
                samples.forEachSample([&](TimestampType timestamp, ValueType value)
                {
                    bucket.ordered.emplace_back(timestamp, value);
                });

            window.emplace_back(std::move(bucket));
        }

        void removeBefore(TimestampType cut_off)
        {
            while (!window.empty() && window.front().bucket_end_timestamp <= cut_off)
                window.pop_front();
        }

        std::optional<ResultType> getResult(TimestampType /*grid_timestamp*/) const
        {
            /// Kahan-Babuska-Neumaier compensated summation: the window's values arrive in timestamp
            /// order rather than by magnitude. `Samples` keeps duplicate timestamps, so every
            /// occurrence is folded in.
            Float64 sum = 0;
            Float64 compensation = 0;
            UInt64 count = 0;

            auto fold = [&](ValueType value)
            {
                const Float64 v = static_cast<Float64>(value);
                const Float64 t = sum + v;
                if (std::abs(sum) >= std::abs(v))
                    compensation += (sum - t) + v;
                else
                    compensation += (v - t) + sum;
                sum = t;
                ++count;
            };

            for (const auto & bucket : window)
            {
                if (bucket.samples)
                    bucket.samples->forEachSample([&](TimestampType, ValueType value) { fold(value); });
                else
                    for (const auto & [timestamp, value] : bucket.ordered)
                        fold(value);
            }

            /// No samples in the window: Prometheus produces no output point for this grid timestamp.
            if (count == 0)
                return std::nullopt;

            return static_cast<ResultType>((sum + compensation) / static_cast<Float64>(count));
        }
    };

    /// The bucket stores raw samples with duplicate timestamps kept, so every occurrence is averaged.
    using Bucket = Samples;

    /// Bumped from 1: buckets now keep duplicate timestamps, and an old binary would silently re-collapse them.
    static constexpr UInt16 FORMAT_VERSION = 2;

    /// No two-stacks thresholds: `getStackSizeForTwoStacks` only opts in for an aggregator that combines
    /// per-bucket summaries, and this one has none to combine. The queue would not help either, since it
    /// regroups partial results and no partial result is kept here.
    ///
    /// `timeSeriesSumToGrid` does keep per-bucket summaries and stays off the queue for a different reason:
    /// its plain Float64 additions commute but do not associate, which is what regrouping needs.
};


/// Aggregate function to calculate the average of timeseries values on the specified grid, i.e. the ClickHouse
/// counterpart of PromQL's `avg_over_time`.
template <typename TimestampType_, typename IntervalType_, typename ValueType_>
class AggregateFunctionTimeseriesAvgToGrid final :
    public AggregateFunctionTimeseriesBase<
        AggregateFunctionTimeseriesAvgToGrid<TimestampType_, IntervalType_, ValueType_>,
        AggregateFunctionTimeseriesAvgToGridTraits<TimestampType_, IntervalType_, ValueType_>>
{
public:
    using Traits = AggregateFunctionTimeseriesAvgToGridTraits<TimestampType_, IntervalType_, ValueType_>;
    using Aggregator = typename Traits::Aggregator;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesAvgToGrid, Traits>;
    using Base::Base;

    Aggregator createAggregator(size_t stack_size_for_two_stacks) const
    {
        return Aggregator{stack_size_for_two_stacks};
    }
};

}
