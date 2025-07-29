#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

/// Implements WHERE, HAVING operations. See FilterTransform.
class BuildRuntimeFilterStep : public ITransformingStep
{
public:
    BuildRuntimeFilterStep(
        const SharedHeader & input_header_,
        String filter_column_name_,
        String filter_name_);

    BuildRuntimeFilterStep(const BuildRuntimeFilterStep & other) = default;

    String getName() const override { return "BuildRuntimeFilter"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    const String & getFilterColumnName() const { return filter_column_name; }

    void setConditionForQueryConditionCache(UInt64 condition_hash_, const String & condition_);

    static bool canUseType(const DataTypePtr & type);

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    QueryPlanStepPtr clone() const override;

private:
    void updateOutputHeader() override;

    String filter_column_name;
    String filter_name;
};

}
