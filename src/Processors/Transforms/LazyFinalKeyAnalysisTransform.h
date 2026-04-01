#pragma once

#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/PreparedSets.h>
#include <Processors/IProcessor.h>
#include <Storages/MergeTree/RangesInDataPart.h>

namespace DB
{

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

class LazyFinalKeyAnalysisTransform : public IProcessor
{
public:
    using MutableRangesInDataPartsPtr = std::shared_ptr<RangesInDataParts>;

    LazyFinalKeyAnalysisTransform(
        FutureSetPtr future_set_,
        ContextPtr query_context_,
        StorageMetadataPtr metadata_snapshot_,
        MutableRangesInDataPartsPtr ranges_);

    String getName() const override { return "LazyFinalKeyAnalysisTransform"; }
    Status prepare() override;
    void work() override;

private:
    FutureSetPtr future_set;
    MutableRangesInDataPartsPtr ranges;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr query_context;

    bool is_done = false;
};

}
