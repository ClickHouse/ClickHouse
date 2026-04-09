#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>
#include <Interpreters/TemporaryDataOnDisk.h>

namespace DB
{

class ObfuscateStep : public ITransformingStep
{
public:
    ObfuscateStep(
        const SharedHeader & input_header_,
        TemporaryDataOnDiskScopePtr tmp_data_scope_);

    String getName() const override { return "Obfuscate"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    void updateOutputHeader() override
    {
        output_header = input_headers.front();
    }

private:
    TemporaryDataOnDiskScopePtr tmp_data_scope;
};

}
