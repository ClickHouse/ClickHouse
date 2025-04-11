#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Core/SortDescription.h>

#include <optional>

namespace DB
{

/// Base class for logical exchange steps.
/// Derived classes implement createSinkAndSourcePair method that is used to create a pair of send-recieve steps when converting
/// logical plan to a distributed plan.
/// By default the data that is sent via the exchange might be reordered, but in cases like distributed sorting it is required to
/// merge incoming sorted streams according to the sort description.
class LogicalExchangeStep : public ITransformingStep
{
protected:
    explicit LogicalExchangeStep(const Block & input_header_, std::optional<SortDescription> maintain_sort_description_ = std::nullopt)
        : ITransformingStep(input_header_, input_header_, {})
        , maintain_sort_description(std::move(maintain_sort_description_))
    {
    }

public:

    /// Number of buckets after the exchange. E.g. 1 for GatherExchange, num_buckets for ShuffleExchange.
    virtual size_t getResultBucketCount() const = 0;

    const std::optional<SortDescription> & getMaintainSortDescription() const
    {
        return maintain_sort_description;
    }

    /// Create a pair of sink and source steps for the exchange.
    /// They are "connected" to each other via exchange_id
    virtual std::pair<QueryPlanStepPtr, QueryPlanStepPtr> createSinkAndSourcePair(const String & exchange_id, const Strings & source_shards) const = 0;

protected:
    /// Describes required sort order of the output. Input(s) must also be sorted according to this description.
    std::optional<SortDescription> maintain_sort_description;
};

/// TODO: move to proper place
template <typename SourceBucketId, typename DestinationBucketId>
String streamNameForExchange(const String & exchange_id, const SourceBucketId & source_bucket, const DestinationBucketId & destination_bucket)
{
    return exchange_id + "__" + toString(source_bucket) + "_" + toString(destination_bucket);
}

/// TODO: move to proper place
/// Enumerates shard (bucket) names for the specified number of buckets.
inline Strings shardsForShuffleBuckets(size_t bucket_count)
{
    Strings shards;
    for (size_t bucket = 0; bucket < bucket_count; ++bucket)
        shards.push_back(toString(bucket));
    return shards;
}

}
