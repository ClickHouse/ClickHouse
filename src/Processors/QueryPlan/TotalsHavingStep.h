#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/Transforms/finalizeChunk.h>
#include <Interpreters/ActionsDAG.h>

namespace DB
{

enum class TotalsMode : uint8_t;

/// Execute HAVING and calculate totals. See TotalsHavingTransform.
class TotalsHavingStep : public ITransformingStep
{
public:
    TotalsHavingStep(
        SharedHeader input_header_,
        const AggregateDescriptions & aggregates_,
        bool overflow_row_,
        std::optional<ActionsDAG> actions_dag_,
        const std::string & filter_column_,
        bool remove_filter_,
        TotalsMode totals_mode_,
        float auto_include_threshold_,
        bool final_);

    /// `ActionsDAG` is move-only, so the implicit copy constructor is deleted. Define one that
    /// deep-clones `actions_dag` (like `FilterStep`) so the step can be cloned by `QueryPlan::clone`
    /// (used by `FutureSetFromSubquery::buildOrderedSetInplace` to preserve the source plan).
    TotalsHavingStep(const TotalsHavingStep & other)
        : ITransformingStep(other)
        , aggregates(other.aggregates)
        , overflow_row(other.overflow_row)
        , actions_dag(other.actions_dag ? std::optional<ActionsDAG>(other.actions_dag->clone()) : std::nullopt)
        , filter_column_name(other.filter_column_name)
        , remove_filter(other.remove_filter)
        , totals_mode(other.totals_mode)
        , auto_include_threshold(other.auto_include_threshold)
        , final(other.final)
    {}

    String getName() const override { return "TotalsHaving"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    const ActionsDAG * getActions() const { return actions_dag ? &*actions_dag : nullptr; }
    const String & getFilterColumnName() const { return filter_column_name; }

    bool hasCorrelatedExpressions() const override
    {
        if (actions_dag)
            return actions_dag->hasCorrelatedColumns();
        return false;
    }

    void serializeSettings(QueryPlanSerializationSettings & settings) const override;
    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    QueryPlanStepPtr clone() const override;

private:
    void updateOutputHeader() override;

    const AggregateDescriptions aggregates;

    bool overflow_row;
    std::optional<ActionsDAG> actions_dag;
    String filter_column_name;
    bool remove_filter;
    TotalsMode totals_mode;
    float auto_include_threshold;
    bool final;
};

}
