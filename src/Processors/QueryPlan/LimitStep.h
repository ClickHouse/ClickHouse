#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Core/SortDescription.h>

namespace DB
{

/// Executes LIMIT. See LimitTransform.
class LimitStep : public ITransformingStep
{
public:
    LimitStep(
        const SharedHeader & input_header_,
        size_t limit_, size_t offset_,
        bool always_read_till_end_ = false, /// Read all data even if limit is reached. Needed for totals.
        bool with_ties_ = false, /// Limit with ties.
        SortDescription description_ = {});

    String getName() const override { return "Limit"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    size_t getLimit() const { return limit; }
    size_t getOffset() const { return offset; }

    size_t getLimitForSorting() const
    {
        if (limit > std::numeric_limits<UInt64>::max() - offset)
            return 0;

        return limit + offset;
    }

    bool withTies() const { return with_ties; }
    bool alwaysReadTillEnd() const { return always_read_till_end; }

    void markAsShardLimit() { is_shard_limit = true; }

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    QueryPlanStepPtr clone() const override;

    bool hasCorrelatedExpressions() const override { return false; }

    /// `transformPipeline` hands the updater to the `LimitTransform`, which records the post-limit
    /// output. Note that a `Limit` at the replica-output boundary is a shard limit, so its output is
    /// replicated rather than partitioned: every replica emits up to `limit` rows and ships all of them
    /// to the initiator. `considerEnablingParallelReplicas` accounts for that when pricing the network
    /// term, for this step and for a top-N `SortingStep` alike.
    bool supportsDataflowStatisticsCollection() const override { return true; }

private:
    void updateOutputHeader() override
    {
        output_header = input_headers.front();
    }

    size_t limit;
    size_t offset;
    bool always_read_till_end;

    bool with_ties;
    const SortDescription description;
    bool is_shard_limit = false;
};

}
