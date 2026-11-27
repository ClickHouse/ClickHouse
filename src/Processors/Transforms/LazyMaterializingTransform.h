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

/// This transform has two ports for the main and lazy columns.
/// First, we read the main port and get the required row indexes.
/// Then, we prepare the main chunk and fill LazyMaterializingRows state.
/// Then, we read the lazy port, expecting data is sorted by the global row index.
/// Then, we prepare lazy columns by restoring the row order.
class LazyMaterializingTransform final : public IProcessor
{
public:
    LazyMaterializingTransform(SharedHeader main_header, SharedHeader lazy_header, LazyMaterializingRowsPtr lazy_materializing_rows_, RuntimeDataflowStatisticsCacheUpdaterPtr updater_);

    static Block transformHeader(const Block & main_header, const Block & lazy_header);

    String getName() const override { return "LazyMaterializingTransform"; }
    Status prepare() override;

    void work() override;

private:
    Chunks chunks;
    std::optional<Chunk> result_chunk;

    /// This fields are calculated after main chunks read in prepareMainChunk.
    /// The permutation is calculated to sort global row index.
    /// The sorted_indexes are those indexes after we removed duplicates.
    /// The offsets are prefix sum of duplicate indexes.
    PaddedPODArray<UInt64> sorted_indexes;
    PaddedPODArray<UInt64> offsets;
    PaddedPODArray<size_t> permutation;

    LazyMaterializingRowsPtr lazy_materializing_rows;
    RuntimeDataflowStatisticsCacheUpdaterPtr updater;

    /// Those functions are called once each after the corresponding port is finished.
    void prepareMainChunk();
    void prepareLazyChunk();
};

}
