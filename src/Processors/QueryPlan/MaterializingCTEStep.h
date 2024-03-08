#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/SizeLimits.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/PreparedSets.h>

namespace DB
{

/// Materializes CTE. See MaterializingCTETransform.
class MaterializingCTEStep : public ITransformingStep, public WithContext
{
public:
    MaterializingCTEStep(
        const DataStream & input_stream_,
        StoragePtr external_table_,
        String cte_table_name_,
        SizeLimits network_transfer_limits_,
        ContextPtr context_);

    String getName() const override { return "MaterializingCTEStep"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    void updateOutputStream() override;
    String cte_table_name;
    StoragePtr external_table;
    SizeLimits network_transfer_limits;
};

}
