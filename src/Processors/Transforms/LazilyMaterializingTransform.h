#pragma once

#include <Processors/IProcessor.h>
#include <Processors/Port.h>
#include <Storages/MergeTree/RangesInDataPart.h>


namespace DB
{


struct LazyMaterializingRows
{
    using PartOffsetInDataPart = PaddedPODArray<UInt64>;
    /// part_index_in_query -> row numbers
    using RowsInParts = std::map<size_t, PartOffsetInDataPart>;

    RowsInParts rows_in_parts;
    RangesInDataParts ranges_in_data_parts;
};

using LazyMaterializingRowsPtr = std::shared_ptr<LazyMaterializingRows>;

class LazilyMaterializingTransform final : public IProcessor
{
public:
    LazilyMaterializingTransform(SharedHeader main_header, SharedHeader lazy_header, LazyMaterializingRowsPtr lazy_materializing_rows_);

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

    void prepareMainChunk();
    void prepareLazyChunk();

    // enum class Stage
    // {
    //     ReadMain,
    //     PrepareLazyInfo,
    //     ReadLazy,
    // };
};

}
