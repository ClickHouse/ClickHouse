#pragma once

#include <Columns/IColumn_fwd.h>

#include <cstddef>
#include <memory>


namespace DB
{

/// Typed implementations of `timeSeriesRateToGrid` expose this query-private
/// seam to the ordered PromQL raw-samples merge. The regular aggregate state
/// and its serialized representation are intentionally not involved.
class ITimeSeriesRateToGridStreaming
{
public:
    struct StreamingStateBase
    {
        virtual ~StreamingStateBase() = default;
    };

    using StatePtr = std::unique_ptr<StreamingStateBase>;

    virtual ~ITimeSeriesRateToGridStreaming() = default;

    virtual StatePtr createStreamingState() const = 0;
    virtual void addRawSamples(
        StreamingStateBase & state,
        const IColumn & timestamp_column,
        const IColumn & value_column,
        size_t row_begin,
        size_t row_end) const = 0;
    virtual void finishExternalBucket(StreamingStateBase & state) const = 0;
    virtual void insertStreamingResultInto(const StreamingStateBase & state, IColumn & to) const = 0;
};

}
