#pragma once

#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/IAccumulatingTransform.h>
#include <QueryPipeline/QueryPipeline.h>

namespace DB
{

struct TemporaryTableHolder;
using TemporaryTableHolderPtr = std::shared_ptr<TemporaryTableHolder>;

class MaterializingCTETransform : public IAccumulatingTransform
{
public:
    MaterializingCTETransform(
        const SharedHeader & input_header_,
        const SharedHeader & output_header_,
        TemporaryTableHolderPtr temporary_table_holder_);

    String getName() const override { return "MaterializingCTETransform"; }

    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    TemporaryTableHolderPtr temporary_table_holder;
    QueryPipeline table_out;
    std::unique_ptr<PushingPipelineExecutor> executor;
};

}
