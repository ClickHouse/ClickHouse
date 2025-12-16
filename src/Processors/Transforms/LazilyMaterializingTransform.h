#pragma once

#include <Processors/IProcessor.h>
#include <Processors/Port.h>
#include <Storages/MergeTree/RangesInDataPart.h>


namespace DB
{

class RuntimeDataflowStatisticsCacheUpdater;
using RuntimeDataflowStatisticsCacheUpdaterPtr = std::shared_ptr<RuntimeDataflowStatisticsCacheUpdater>;

struct LazyMaterializingRows
{
    using PartOffsetInDataPart = PaddedPODArray<UInt64>;
    /// part_index_in_query -> row numbers
    using RowsInParts = std::map<size_t, PartOffsetInDataPart>;

    RowsInParts rows_in_parts;
    RangesInDataParts ranges_in_data_parts;

    explicit LazyMaterializingRows(RangesInDataParts ranges_in_data_parts_);

    void filterRangesAndFillRows(const PaddedPODArray<UInt64> & sorted_indexes);
};

using LazyMaterializingRowsPtr = std::shared_ptr<LazyMaterializingRows>;

class LazilyMaterializingTransform final : public IProcessor
{
public:
    LazilyMaterializingTransform(SharedHeader main_header, SharedHeader lazy_header, LazyMaterializingRowsPtr lazy_materializing_rows_, RuntimeDataflowStatisticsCacheUpdaterPtr updater_);

    static Block transformHeader(const Block & main_header, const Block & lazy_header);

    String getName() const override { return "LazilyMaterializingTransform"; }
    Status prepare() override;

    void work() override;

private:
    Chunks chunks;
    std::optional<Chunk> result_chunk;

    PaddedPODArray<UInt64> sorted_indexes;
    PaddedPODArray<UInt64> offsets;
    PaddedPODArray<size_t> permutation;

    LazyMaterializingRowsPtr lazy_materializing_rows;
    RuntimeDataflowStatisticsCacheUpdaterPtr updater;

    void prepareMainChunk();
    void prepareLazyChunk();
};

}
