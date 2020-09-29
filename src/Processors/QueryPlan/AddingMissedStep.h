#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

struct ColumnDefault;
using ColumnDefaults = std::unordered_map<std::string, ColumnDefault>;

/// Convert one block structure to another. See ConvertingTransform.
class AddingMissedStep : public ITransformingStep
{
public:
    AddingMissedStep(const DataStream & input_stream_,
                     Block result_header_,
                     const ColumnDefaults & column_defaults_,
                     const Context & context_);

    String getName() const override { return "AddingMissed"; }

    void transformPipeline(QueryPipeline & pipeline) override;

private:
    const ColumnDefaults column_defaults;
    const Context & context;
};

}
