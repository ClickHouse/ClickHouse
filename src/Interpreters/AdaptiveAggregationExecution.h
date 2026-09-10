#pragma once

#include <Columns/IColumn_fwd.h>
#include <Interpreters/AdaptiveAggregation.h>

namespace DB
{

class AdaptiveAggregationMissesInfo;

/// Suspends a producer's post-block checks until the staging pipeline acknowledges its arguments.
struct AdaptiveAggregationExecution
{
    explicit AdaptiveAggregationExecution(AdaptiveAggregationProducer & producer_) : producer(producer_) {}

    bool hasPendingBlock() const { return pending_block; }

    /// The frozen kernel's recording for the block being forwarded; the producer attaches it to the
    /// forwarded columns. Null while no block is pending.
    std::shared_ptr<AdaptiveAggregationMissesInfo> misses;

    /// The key column the frozen kernel probed, when the recording reads key bytes from it: the
    /// block's key column materialized, or a constant key's single-row data column. Null otherwise.
    ColumnPtr key_column;

private:
    friend class Aggregator;
    AdaptiveAggregationProducer & producer;
    bool pending_block = false;
    bool use_own_memory_tracker = false;
    size_t input_rows = 0;
};

}
