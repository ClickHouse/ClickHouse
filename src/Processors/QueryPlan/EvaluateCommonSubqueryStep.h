#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;

class EvaluateCommonSubqueryStep : public ITransformingStep
{
public:
    EvaluateCommonSubqueryStep(
        const SharedHeader & header_,
        StoragePtr storage_,
        ContextPtr context_);

    String getName() const override { return "EvaluateCommonSubquery"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void updateOutputHeader() override
    {
        output_header = input_headers.front();
    }

private:
    StoragePtr storage;
    ContextPtr context;
};

}
