#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

/// Adds a materialized const column with a specified value.
class AddingConstColumnStep : public ITransformingStep
{
public:
    AddingConstColumnStep(const DataStream & input_stream_, ColumnWithTypeAndName column_);

    String getName() const override { return "AddingConstColumn"; }

    void transformPipeline(QueryPipeline & pipeline) override;

private:
    ColumnWithTypeAndName column;
};

}

