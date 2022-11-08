#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Core/SortDescription.h>

namespace DB
{

class ObfuscateStep : public ITransformingStep
{
public:
    ObfuscateStep(
        const DataStream & input_stream_,
        TemporaryDataOnDiskScopePtr tmp_data_scope_);

    String getName() const override { return "Obfuscate"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

private:
    void updateOutputStream() override;

    TemporaryDataOnDiskScopePtr tmp_data_scope;
};

}
