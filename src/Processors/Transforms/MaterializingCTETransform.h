#pragma once

#include <Interpreters/MaterializedCTE.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/IAccumulatingTransform.h>
#include <QueryPipeline/QueryPipeline.h>

namespace DB
{

class MaterializingCTETransform : public IAccumulatingTransform
{
public:
    MaterializingCTETransform(
        const SharedHeader & input_header_,
        const SharedHeader & output_header_,
        MaterializedCTEPtr materialized_cte_);

    ~MaterializingCTETransform() override;

    String getName() const override { return "MaterializingCTETransform"; }

    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    MaterializedCTEPtr materialized_cte;
    QueryPipeline table_out;
    std::unique_ptr<PushingPipelineExecutor> executor;
};

}
