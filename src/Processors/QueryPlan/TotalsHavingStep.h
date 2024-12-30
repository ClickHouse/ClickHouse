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
        const Header & input_header_,
        const AggregateDescriptions & aggregates_,
        bool overflow_row_,
        std::optional<ActionsDAG> actions_dag_,
        const std::string & filter_column_,
        bool remove_filter_,
        TotalsMode totals_mode_,
        float auto_include_threshold_,
        bool final_);

    String getName() const override { return "TotalsHaving"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    const ActionsDAG * getActions() const { return actions_dag ? &*actions_dag : nullptr; }

    void serializeSettings(QueryPlanSerializationSettings & settings) const override;
    void serialize(Serialization & ctx) const override;

    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx);

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
