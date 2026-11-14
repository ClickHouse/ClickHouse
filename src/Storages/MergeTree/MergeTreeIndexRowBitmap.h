#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>

namespace DB
{

/// Stores scalar index expression values for every row in one row_bitmap granule. Query-time index
/// analysis evaluates the WHERE predicate on this block and converts the result to a dense row bitmap.
struct MergeTreeIndexGranuleRowBitmap final : public IMergeTreeIndexGranule
{
    explicit MergeTreeIndexGranuleRowBitmap(const Block & index_sample_block_);
    /// Used after the aggregator has filled a row-aligned scalar-value block for one index granule.
    explicit MergeTreeIndexGranuleRowBitmap(Block && block_);

    /// Persist row-level scalar values, not the query bitmap; the bitmap depends on the runtime WHERE.
    void serializeBinary(WriteBuffer & ostr) const override;
    /// Restore the row-aligned scalar block so query-time filtering can preserve physical row offsets.
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    /// Empty granules are not useful because every bitmap bit maps to one stored scalar row.
    bool empty() const override { return block.rows() == 0; }
    /// Memory accounting is based on stored scalar values; query bitmaps are temporary read hints.
    size_t memoryUsageBytes() const override { return block.bytes(); }
    /// Exposes the number of physical rows represented by this row_bitmap granule.
    size_t rows() const { return block.rows(); }

    /// The block is row-aligned with the physical rows covered by this index granule.
    Block block;
    Serializations serializations;
};

/// Builds row_bitmap granules during part writes, merges, and index materialization by copying the
/// indexed scalar expression values row by row from the source block.
struct MergeTreeIndexAggregatorRowBitmap final : public IMergeTreeIndexAggregator
{
    explicit MergeTreeIndexAggregatorRowBitmap(const Block & index_sample_block_);

    /// No row-level values means the current index granule has not accumulated data yet.
    bool empty() const override { return block.rows() == 0; }
    /// Flush the accumulated row-level scalar values as one persisted index granule.
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    /// Append the next rows for all indexed scalar expression columns without reducing them.
    void update(const Block & block_, size_t * pos, size_t limit) override;

private:
    Block block;
};

class MergeTreeIndexConditionRowBitmap final : public IMergeTreeIndexCondition
{
public:
    MergeTreeIndexConditionRowBitmap(const ActionsDAG::Node * predicate, ContextPtr context, const Block & index_sample_block);

    bool alwaysUnknownOrTrue() const override { return actions == nullptr; }
    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr granule, const UpdatePartialDisjunctionResultFn & update_partial_disjunction_result_fn) const override;
    /// Produces an exact bitmap only when the full predicate can be evaluated from this index block.
    /// The vector index later remaps it into vector-granule-local row offsets before exact filtered
    /// vector search.
    std::optional<VectorSearchFilter> calculateVectorSearchFilter(MergeTreeIndexGranulePtr granule) const override;
    std::string getDescription() const override { return actions ? "row_bitmap exact row filter" : ""; }

private:
    /// Executes the extracted filter DAG on the stored scalar values and packs the UInt8 result into
    /// VectorSearchFilter::DenseBitmap, where set bits mean rows allowed by WHERE.
    VectorSearchFilter buildFilter(const MergeTreeIndexGranuleRowBitmap & granule) const;

    ExpressionActionsPtr actions;
    String actions_output_column_name;
};

class MergeTreeIndexRowBitmap final : public IMergeTreeIndex
{
public:
    MergeTreeIndexRowBitmap(StorageMetadataPtr metadata_snapshot_, const IndexDescription & index_)
        : IMergeTreeIndex(std::move(metadata_snapshot_), index_)
    {
    }

    /// Create an empty row-aligned scalar-value container for index reads.
    MergeTreeIndexGranulePtr createIndexGranule() const override;
    /// Create a writer-side aggregator that preserves per-row scalar values.
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;
    /// Create a query condition capable of turning the runtime WHERE into a row bitmap.
    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const override;

    /// Used by ReadFromMergeTree to schedule row_bitmap before vector_similarity, so the vector index
    /// can consume the scalar bitmap during the same index-analysis pass.
    bool isRowBitmapIndex() const override { return true; }
};

MergeTreeIndexPtr rowBitmapIndexCreator(
    StorageMetadataPtr metadata_snapshot,
    const IndexDescription & index,
    const MergeTreeSettings & settings);

/// Validate row_bitmap DDL. The index has no arguments because its behavior is fully determined by
/// the indexed scalar expression and skip-index granularity.
void rowBitmapIndexValidator(const IndexDescription & index, bool attach, const MergeTreeSettings & settings);

}
