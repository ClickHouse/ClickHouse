#pragma once

#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/IProcessor.h>
#include <Storages/MergeTree/RangesInDataPart.h>

namespace DB
{

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

class LazyFinalKeyAnalysisTransform : public IProcessor
{
public:
    LazyFinalKeyAnalysisTransform(
        const Block & header,
        size_t max_rows_,
        ContextPtr query_context_,
        StorageMetadataPtr metadata_snapshot_,
        RangesInDataParts ranges_);

    String getName() const override { return "LazyFinalKeyAnalysisTransform"; }
    Status prepare() override;
    void work() override;

private:
    const size_t max_rows;
    const std::vector<size_t> key_columns;
    RangesInDataParts ranges;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr query_context;

    Chunks chunks;
    size_t rows_read = 0;
    bool pk_is_analyzed = false;
};

}
