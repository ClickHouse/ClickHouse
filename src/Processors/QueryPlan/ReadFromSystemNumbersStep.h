#pragma once

#include <Core/QueryProcessingStage.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageSnapshot.h>


namespace DB
{

class ReadFromSystemNumbersStep final : public SourceStepWithFilter
{
public:
    ReadFromSystemNumbersStep(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        StoragePtr storage_,
        size_t max_block_size_,
        size_t num_streams_);

    String getName() const override { return "ReadFromSystemNumbers"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    /// Fail fast if estimated number of rows to read exceeds the limit
    void checkLimits(size_t rows);

    Pipe makePipe();

    const Names column_names;
    StoragePtr storage;
    ExpressionActionsPtr key_expression;
    size_t max_block_size;
    size_t num_streams;
    std::pair<UInt64, UInt64> limit_length_and_offset;
    bool should_pushdown_limit;
    UInt64 query_info_limit;
    std::shared_ptr<const StorageLimitsList> storage_limits;
};

}
