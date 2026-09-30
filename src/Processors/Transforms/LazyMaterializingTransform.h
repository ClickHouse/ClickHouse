#pragma once

#include <Processors/IProcessor.h>
#include <Processors/Port.h>
#include <Storages/MergeTree/RangesInDataPart.h>


namespace DB
{

class RuntimeDataflowStatisticsCacheUpdater;
using RuntimeDataflowStatisticsCacheUpdaterPtr = std::shared_ptr<RuntimeDataflowStatisticsCacheUpdater>;

/// Shared state between the two branches of lazy materialization: the main branch (which decides
/// which rows survive the LIMIT) and the lazy branch (which reads the deferred columns for exactly
/// those rows). Each storage supporting lazy materialization provides its own implementation
/// that maps global row indexes back to storage-specific row locations.
struct ILazyMaterializingRows
{
    virtual ~ILazyMaterializingRows() = default;

    /// Called once, after the main branch is fully read, with the sorted and deduplicated
    /// `__global_row_index` values of the rows that survived the LIMIT.
    virtual void filterRangesAndFillRows(const PaddedPODArray<UInt64> & sorted_indexes) = 0;
};

using ILazyMaterializingRowsPtr = std::shared_ptr<ILazyMaterializingRows>;

struct LazyMaterializingRows : public ILazyMaterializingRows
{
    using PartOffsetInDataPart = PaddedPODArray<UInt64>;
    /// part_index_in_query -> row numbers
    using RowsInParts = std::map<size_t, PartOffsetInDataPart>;

    RowsInParts rows_in_parts;
    RangesInDataParts ranges_in_data_parts;

    explicit LazyMaterializingRows(RangesInDataParts ranges_in_data_parts_);

    void filterRangesAndFillRows(const PaddedPODArray<UInt64> & sorted_indexes) override;
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
    /// `index_column_name` names the column of the main input holding the row index the lazy input is
    /// addressed by. It is either `UInt64` or `Nullable(UInt64)`: a NULL index is a row that a join above
    /// the source matched nothing for, which has no row of the source to read, and gets the default of each
    /// lazy column instead - what the join itself would have stood there.
    LazyMaterializingTransform(
        SharedHeader main_header,
        SharedHeader lazy_header,
        ILazyMaterializingRowsPtr lazy_materializing_rows_,
        RuntimeDataflowStatisticsCacheUpdaterPtr updater_,
        String index_column_name_ = default_index_column_name);

    static constexpr auto default_index_column_name = "__global_row_index";

    static Block transformHeader(const Block & main_header, const Block & lazy_header, const String & index_column_name = default_index_column_name);

    void setPassThrough(bool value);

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

    ILazyMaterializingRowsPtr lazy_materializing_rows;
    RuntimeDataflowStatisticsCacheUpdaterPtr updater;
    String index_column_name;

    /// Set when the index is Nullable: which rows of the main chunk have a row of the source to read.
    /// The permutation and the offsets above cover those rows only.
    std::optional<IColumn::Filter> matched_rows;
    bool lazy_chunk_prepared = false;

    /// When true, pass lazy chunks directly to output without combining
    /// with main columns or permuting. Used when main input only has
    /// __global_row_index and output only needs lazy columns.
    bool pass_through = false;

    /// Those functions are called once each after the corresponding port is finished.
    void prepareMainChunk();
    void prepareLazyChunk();
};

}
