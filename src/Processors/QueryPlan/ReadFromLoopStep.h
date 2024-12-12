#pragma once
#include <Core/QueryProcessingStage.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

    class ReadFromLoopStep final : public SourceStepWithFilter
    {
    public:
        ReadFromLoopStep(
                const Names & column_names_,
                const SelectQueryInfo & query_info_,
                const StorageSnapshotPtr & storage_snapshot_,
                const ContextPtr & context_,
                QueryProcessingStage::Enum processed_stage_,
                StoragePtr inner_storage_,
                size_t max_block_size_,
                size_t num_streams_);

        String getName() const override { return "ReadFromLoop"; }

        void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    private:

        Pipe makePipe();

        const Names column_names;
        QueryProcessingStage::Enum processed_stage;
        StoragePtr inner_storage;
        size_t max_block_size;
        size_t num_streams;
    };
}
