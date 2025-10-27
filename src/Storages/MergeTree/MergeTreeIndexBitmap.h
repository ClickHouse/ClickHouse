#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <map>


namespace DB
{

class MergeTreeIndexBitmap;

/// Granule for bitmap index
/// Stores a mapping from distinct values to roaring bitmaps of row positions
struct MergeTreeIndexGranuleBitmap final : public IMergeTreeIndexGranule
{
    explicit MergeTreeIndexGranuleBitmap(
        const String & index_name_,
        const Block & index_sample_block_,
        size_t max_cardinality_);

    MergeTreeIndexGranuleBitmap(
        const String & index_name_,
        const Block & index_sample_block_,
        size_t max_cardinality_,
        std::map<Field, std::unique_ptr<RoaringBitmapWithSmallSet<UInt32, 32>>> && bitmaps_,
        std::unique_ptr<RoaringBitmapWithSmallSet<UInt32, 32>> && null_bitmap_,
        size_t total_rows_);

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override { return bitmaps.empty() && (!null_bitmap || null_bitmap->size() == 0); }
    size_t memoryUsageBytes() const override;

    ~MergeTreeIndexGranuleBitmap() override = default;

    const String & index_name;
    const Block & index_sample_block;
    const size_t max_cardinality;

    /// Map from distinct values to bitmaps of row positions
    /// Using unique_ptr because RoaringBitmapWithSmallSet is non-copyable
    std::map<Field, std::unique_ptr<RoaringBitmapWithSmallSet<UInt32, 32>>> bitmaps;

    /// Bitmap for NULL values
    std::unique_ptr<RoaringBitmapWithSmallSet<UInt32, 32>> null_bitmap;

    /// Total number of rows in this granule
    size_t total_rows = 0;
};


/// Aggregator for building bitmap index granules
struct MergeTreeIndexAggregatorBitmap final : IMergeTreeIndexAggregator
{
    explicit MergeTreeIndexAggregatorBitmap(
        const String & index_name_,
        const Block & index_sample_block_,
        size_t max_cardinality_);

    ~MergeTreeIndexAggregatorBitmap() override = default;

    bool empty() const override { return bitmaps.empty() && (!null_bitmap || null_bitmap->size() == 0); }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;

private:
    String index_name;
    Block index_sample_block;
    size_t max_cardinality;

    /// Map from distinct values to bitmaps of row positions
    std::map<Field, std::unique_ptr<RoaringBitmapWithSmallSet<UInt32, 32>>> bitmaps;

    /// Bitmap for NULL values
    std::unique_ptr<RoaringBitmapWithSmallSet<UInt32, 32>> null_bitmap;

    /// Total number of rows processed
    size_t total_rows = 0;

    /// Current row offset within the granule
    UInt32 current_row_offset = 0;
};


/// Condition for evaluating bitmap index
class MergeTreeIndexConditionBitmap final : public IMergeTreeIndexCondition
{
public:
    MergeTreeIndexConditionBitmap(
        const IndexDescription & index,
        const ActionsDAGWithInversionPushDown & filter_dag,
        ContextPtr context);

    bool alwaysUnknownOrTrue() const override;
    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const override;

    ~MergeTreeIndexConditionBitmap() override = default;

private:
    /// Check if granule may contain matching rows based on RPN element
    bool checkInBitmap(
        const MergeTreeIndexGranuleBitmap & granule,
        const KeyCondition::RPNElement & element) const;

    DataTypePtr index_data_type;
    KeyCondition condition;
};


/// Main bitmap index class
class MergeTreeIndexBitmap : public IMergeTreeIndex
{
public:
    MergeTreeIndexBitmap(
        const IndexDescription & index_,
        size_t max_cardinality_)
        : IMergeTreeIndex(index_)
        , max_cardinality(max_cardinality_)
    {}

    ~MergeTreeIndexBitmap() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator(const MergeTreeWriterSettings & settings) const override;

    MergeTreeIndexConditionPtr createIndexCondition(
        const ActionsDAG::Node * predicate, ContextPtr context) const override;

    MergeTreeIndexSubstreams getSubstreams() const override
    {
        return {{MergeTreeIndexSubstream::Type::Regular, "", ".idx2"}};
    }

    MergeTreeIndexFormat getDeserializedFormat(
        const IDataPartStorage & data_part_storage,
        const std::string & path_prefix) const override;

    size_t max_cardinality = 0;
};

}

