#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Core/SortDescription.h>

#include <optional>

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

    /// Number of leading rows a source must produce for this `LIMIT` to be satisfiable,
    /// i.e. `limit + offset`. Empty when that sum does not fit in `UInt64`, so there is no
    /// representable bound to push down.
    std::optional<size_t> getLimitWithOffset() const
    {
        if (limit > std::numeric_limits<UInt64>::max() - offset)
            return {};

        return limit + offset;
    }

    /// 0 means unlimited, as everywhere in the sorting code.
    size_t getLimitForSorting() const { return getLimitWithOffset().value_or(0); }

    bool withTies() const { return with_ties; }
    bool alwaysReadTillEnd() const { return always_read_till_end; }

    void markAsShardLimit() { is_shard_limit = true; }

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    QueryPlanStepPtr clone() const override;

    bool hasCorrelatedExpressions() const override { return false; }

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
