#pragma once

#include <optional>

#include <Processors/QueryPlan/ITransformingStep.h>
#include <Interpreters/ActionsDAG.h>

namespace DB
{

struct LimitRangeWire;

/** Executes LIMIT [n] AFTER expr [ALL] [UNTIL expr]. See LimitRangeTransform. */
class LimitRangeStep : public ITransformingStep
{
public:
    /// `conditions` computes the boundary columns `start_column_name` (`AFTER`) and `end_column_name`
    /// (`UNTIL`) from the input columns, so a subexpression shared by the two boundaries is computed once;
    /// a boundary the query does not have has no name.
    LimitRangeStep(
        const SharedHeader & input_header_,
        ActionsDAG conditions_,
        std::optional<String> start_column_name_,
        std::optional<String> end_column_name_,
        bool start_all_,
        std::optional<UInt64> limit_,
        bool always_read_till_end_);

    LimitRangeStep(const LimitRangeStep & other);

    String getName() const override { return "LimitRange"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    /// The framed format: the wire struct is what the manifest in `LimitRangeStep.cpp` declares.
    LimitRangeWire toWire() const;
    static QueryPlanStepPtr fromWire(LimitRangeWire wire, Deserialization & ctx);

    QueryPlanStepPtr clone() const override;

    bool hasCorrelatedExpressions() const override { return conditions.hasCorrelatedColumns(); }

private:
    /// Streams below the framed format.
    void serializeLegacy(Serialization & ctx) const;
    static QueryPlanStepPtr deserializeLegacy(Deserialization & ctx);

    void updateOutputHeader() override
    {
        output_header = input_headers.front();
    }

    ActionsDAG conditions;
    std::optional<String> start_column_name;
    std::optional<String> end_column_name;
    bool start_all = false;
    std::optional<UInt64> limit;
    bool always_read_till_end = false;
};

}
