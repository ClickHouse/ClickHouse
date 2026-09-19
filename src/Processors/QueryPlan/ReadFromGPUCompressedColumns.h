#pragma once

#include <GPU/GPUTypes.h>
#include "config.h"

#if USE_GPU

#include <Processors/QueryPlan/ISourceStep.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageSnapshot.h>

namespace DB
{

class ReadFromGPUCompressedColumns : public ISourceStep
{
public:
    struct ColumnToReduce
    {
        NameAndTypePair column;
        DataTypePtr result_type;
        GPU::GPUAggregationKind aggregation;
    };

    ReadFromGPUCompressedColumns(
        SharedHeader output_header_,
        std::vector<ColumnToReduce> columns_,
        DataPartsVector parts_,
        StorageSnapshotPtr storage_snapshot_,
        ContextPtr context_,
        size_t batch_bytes_,
        size_t num_streams_);

    String getName() const override { return "ReadFromGPUCompressedColumns"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(FormatSettings & format_settings) const override;
    void describeActions(JSONBuilder::JSONMap & map) const override;

private:
    std::vector<ColumnToReduce> columns;
    DataPartsVector parts;

    StorageSnapshotPtr storage_snapshot;

    ContextPtr context;

    size_t batch_bytes;
    size_t num_streams;
};

}

#endif
