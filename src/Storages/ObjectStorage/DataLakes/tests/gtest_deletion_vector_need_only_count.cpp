#include <gtest/gtest.h>

#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Processors/Chunk.h>
#include <Processors/Formats/IInputFormat.h>
#include <Storages/ObjectStorage/DataLakes/DeletionVectorTransform.h>

using namespace DB;

namespace
{

Chunk makeConstCountChunk(size_t num_rows, size_t row_num_offset)
{
    auto nested = ColumnUInt64::create();
    nested->insertDefault();
    Columns columns;
    columns.emplace_back(ColumnConst::create(std::move(nested), num_rows));
    Chunk chunk(std::move(columns), num_rows);
    chunk.getChunkInfos().add(std::make_shared<ChunkInfoRowNumbers>(row_num_offset));
    return chunk;
}

Chunk makeMaterializedChunk(const std::vector<UInt64> & values, size_t row_num_offset = 0)
{
    auto column = ColumnUInt64::create();
    for (UInt64 value : values)
        column->insert(value);

    Chunk chunk(Columns{std::move(column)}, values.size());
    chunk.getChunkInfos().add(std::make_shared<ChunkInfoRowNumbers>(row_num_offset));
    return chunk;
}

std::shared_ptr<DataLakeObjectMetadata::ExcludedRows> makeExcludedRows(std::initializer_list<UInt64> positions)
{
    auto excluded_rows = std::make_shared<DataLakeObjectMetadata::ExcludedRows>();
    for (UInt64 position : positions)
        excluded_rows->add(position);
    return excluded_rows;
}

}

TEST(RoaringBitmapRangeCardinality, CountsInclusiveStartExclusiveEnd)
{
    DataLakeObjectMetadata::ExcludedRows bitmap;
    bitmap.add(1);
    bitmap.add(5);
    bitmap.add(10);

    EXPECT_EQ(bitmap.rb_range_cardinality(0, 0), 0u);
    EXPECT_EQ(bitmap.rb_range_cardinality(5, 5), 0u);
    EXPECT_EQ(bitmap.rb_range_cardinality(0, 5), 1u);
    EXPECT_EQ(bitmap.rb_range_cardinality(0, 6), 2u);
    EXPECT_EQ(bitmap.rb_range_cardinality(1, 11), 3u);
    EXPECT_EQ(bitmap.rb_range_cardinality(2, 10), 1u);
    EXPECT_EQ(bitmap.rb_range_cardinality(11, 100), 0u);
}

TEST(RoaringBitmapRangeCardinality, MatchesRbRangeCountForLargeBitmap)
{
    DataLakeObjectMetadata::ExcludedRows bitmap;
    /// Force large roaring path (small-set threshold is 32).
    for (UInt64 i = 0; i < 64; ++i)
        bitmap.add(i * 3);
    ASSERT_TRUE(bitmap.isLarge());

    DataLakeObjectMetadata::ExcludedRows subset;
    const UInt64 via_range = bitmap.rb_range(10, 100, subset);
    EXPECT_EQ(bitmap.rb_range_cardinality(10, 100), via_range);
    EXPECT_EQ(subset.size(), via_range);
}

TEST(DeletionVectorNeedOnlyCount, ConstChunkUsesRangeCardinality)
{
    /// Large enough that a dense Filter would be expensive; const columns stay O(1).
    constexpr size_t num_rows = 5'000'000;
    Chunk chunk = makeConstCountChunk(num_rows, /*row_num_offset=*/0);

    DeletionVectorTransform::transform(chunk, *makeExcludedRows({0, 1, num_rows - 1, num_rows + 10}));
    EXPECT_EQ(chunk.getNumRows(), num_rows - 3);
    ASSERT_EQ(chunk.getNumColumns(), 1u);
    EXPECT_TRUE(isColumnConst(*chunk.getColumns()[0]));
    EXPECT_FALSE(chunk.getChunkInfos().get<ChunkInfoRowNumbers>()->applied_filter.has_value());
}

TEST(DeletionVectorNeedOnlyCount, ConstChunkHonorsRowNumOffset)
{
    Chunk chunk = makeConstCountChunk(/*num_rows=*/10, /*row_num_offset=*/100);
    /// Deletes at absolute positions 99 (before), 105 (inside), 110 (end exclusive / outside).
    DeletionVectorTransform::transform(chunk, *makeExcludedRows({99, 105, 110}));
    EXPECT_EQ(chunk.getNumRows(), 9u);
}

TEST(DeletionVectorNeedOnlyCount, MaterializedChunkStillUsesDenseFilter)
{
    Chunk chunk = makeMaterializedChunk({10, 11, 12, 13}, /*row_num_offset=*/10);
    DeletionVectorTransform::transform(chunk, *makeExcludedRows({11, 13}));
    EXPECT_EQ(chunk.getNumRows(), 2u);
    ASSERT_TRUE(chunk.getChunkInfos().get<ChunkInfoRowNumbers>()->applied_filter.has_value());
}
