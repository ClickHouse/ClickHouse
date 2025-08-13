#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;

using ColumnIdentifier = std::string;
using ColumnIdentifiers = std::vector<ColumnIdentifier>;

class EvaluateCommonSubqueryStep : public ITransformingStep
{
public:
    EvaluateCommonSubqueryStep(
        const SharedHeader & header_,
        ColumnIdentifiers columns_to_save_,
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
    ColumnIdentifiers columns_to_save;
};

}
