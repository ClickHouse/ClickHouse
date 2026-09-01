#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/StepIdentity.h>
#include <Core/SortDescription.h>

#include <optional>

namespace DB
{

/// Base class for logical exchange steps.
/// Derived classes implement createSinkAndSourcePair method that is used to create a pair of send-receive steps when converting
/// logical plan to a distributed plan.
/// By default the data that is sent via the exchange might be reordered, but in cases like distributed sorting it is required to
/// merge incoming sorted streams according to the sort description.
class LogicalExchangeStep : public ITransformingStep
{
protected:
    explicit LogicalExchangeStep(SharedHeader input_header_, std::optional<SortDescription> maintain_sort_description_ = std::nullopt)
        : ITransformingStep(input_header_, input_header_, {})
        , maintain_sort_description(std::move(maintain_sort_description_))
    {
    }

public:
    /// Full digest tag of the base field (see `writeExchangeBaseFullDigest`). A subclass numbers its
    /// own tags from `FIRST_DERIVED_FULL_DIGEST_TAG`, so all tags stay unique within one writer.
    static constexpr UInt64 MAINTAIN_SORT_DESCRIPTION_TAG = 1;
    static constexpr UInt64 FIRST_DERIVED_FULL_DIGEST_TAG = 2;

    /// Number of buckets before the exchange. E.g. 1 for ScatterExchange
    virtual size_t getSourceBucketCount() const = 0;
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
    /// Exchange steps have no wire `serialize`, so their full digest is extras-only: the shared
    /// preamble (serialization name, output header - which is the input header for every exchange)
    /// plus every field, framed, written by the subclass's `writeFullDigest`. Nothing here can throw,
    /// so there is no guard and no witness fallback.
    /// Excluded for every exchange: the `ITransformingStep` traits (all four subclasses pass `{}`)
    /// and `input_headers` (the frame compares the child groups, and the output header repeats it).
    /// This writes the one field the base owns; a subclass writes its own fields after it.
    void writeExchangeBaseFullDigest(StepDigestWriter & writer) const
    {
        /// Set only by `GatherExchangeStep`, but the field is the base's: when present, the exchange
        /// must deliver this order, and `createSinkAndSourcePair` passes it to both the send and the
        /// receive step, where it installs a merging transform instead of a plain resize.
        if (maintain_sort_description)
            writer.addSortDescription(MAINTAIN_SORT_DESCRIPTION_TAG, *maintain_sort_description);
        else
            writer.addAbsent(MAINTAIN_SORT_DESCRIPTION_TAG);
    }

    /// Describes required sort order of the output. Input(s) must also be sorted according to this description.
    std::optional<SortDescription> maintain_sort_description;
};

}
