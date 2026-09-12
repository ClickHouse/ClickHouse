#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/InsertDeduplication.h>
#include <Processors/Chunk.h>
#include <Processors/Transforms/DeduplicationTokenTransforms.h>

using namespace DB;

/// Row-less chunks (0 rows, header columns) reach `AddDeduplicationInfoTransform`
/// (`ISimpleTransform` only skips chunks with neither rows nor columns). They contribute
/// no token and are dropped by squashing, so numbering one only leaves a gap in the
/// otherwise-contiguous source block numbers of the data chunks. A unit test rather than
/// SQL: the interleaved row-less chunk only forms under real multi-host parallel-replicas
/// timing, which no local single-node query shape reproduces.

namespace
{

SharedHeader makeHeader()
{
    Block header{ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "a")};
    return std::make_shared<const Block>(std::move(header));
}

Chunk makeDataChunk(size_t rows)
{
    auto column = ColumnUInt64::create();
    for (size_t i = 0; i < rows; ++i)
        column->insertValue(i);
    return Chunk(Columns{std::move(column)}, rows);
}

/// 0 rows but the header's columns -- what `ISimpleTransform` manufactures downstream.
Chunk makeRowlessChunk(const SharedHeader & header)
{
    return Chunk(header->cloneEmpty().getColumns(), 0);
}

DeduplicationInfo::Ptr infoOf(const Chunk & chunk)
{
    return chunk.getChunkInfos().getSafe<DeduplicationInfo>();
}

}

/// Fails without the fix, passes with it. Shape: data / row-less / data / data.
/// Two adjacent data chunks separated only by the row-less chunk must merge into one
/// token (`getCount` 1) -- the row-less chunk must not split them. Two data chunks
/// separated by another data chunk carry non-adjacent numbers and must stay two tokens
/// (`getCount` 2); this second assertion also fails if source numbering is silently
/// removed, so it pins that numbers are actually assigned.
///
/// `getCount` (offsets, == deduplication hashes when enabled) is asserted, not
/// `getDeduplicationHashes`: the header-only ctor leaves `insert_dependencies` null, so
/// the info is disabled and `getDeduplicationHashes` early-returns {} -- while the
/// extend/concat and numbering logic is unaffected. So `getCount` is the disabled-agnostic
/// signal (a value check, not a `chassert` abort).
TEST(DedupSourceNumberHole, RowlessChunkDoesNotBreakTokenMerge)
{
    auto header = makeHeader();
    AddDeduplicationInfoTransform transform(header);

    Chunk first = makeDataChunk(2);
    transform.transform(first);

    Chunk rowless = makeRowlessChunk(header);
    transform.transform(rowless);

    Chunk second = makeDataChunk(2);
    transform.transform(second);

    Chunk third = makeDataChunk(2);
    transform.transform(third);

    auto merged_adjacent = infoOf(first)->mergeSelf(infoOf(second));
    EXPECT_EQ(merged_adjacent->getCount(), 1u)
        << "a row-less chunk must not consume a source block number and split the token "
           "of the surrounding data chunks";

    auto merged_non_adjacent = infoOf(first)->mergeSelf(infoOf(third));
    EXPECT_EQ(merged_non_adjacent->getCount(), 2u)
        << "data chunks with non-adjacent source numbers must stay two tokens; if numbering "
           "silently stopped this merge would wrongly collapse into one";
}

/// The gap a row-less chunk would leave: source numbers 0 and 2 concat into two tokens and
/// two hashes -- the state that trips the sink assert. Pins that `canBeExtended` must not
/// tolerate gaps, or the block id would depend on how many empty chunks interleaved.
TEST(DedupSourceNumberHole, HoleProducesTwoHashes)
{
    auto left = DeduplicationInfo::create(/*async_insert*/ false);
    left->setUserToken("x", 2);
    left->setSourceBlockNumber(0);

    auto right = DeduplicationInfo::create(/*async_insert*/ false);
    right->setUserToken("x", 2);
    right->setSourceBlockNumber(2);

    auto merged = left->mergeSelf(right);
    auto hashes = merged->getDeduplicationHashes("", /*deduplication_enabled*/ true);
    EXPECT_EQ(hashes.size(), 2u) << "non-contiguous source numbers must concat into two tokens";
}

/// Healthy path: contiguous source numbers 0 and 1 extend into one token and one hash.
TEST(DedupSourceNumberHole, ContiguousProducesOneHash)
{
    auto left = DeduplicationInfo::create(/*async_insert*/ false);
    left->setUserToken("x", 2);
    left->setSourceBlockNumber(0);

    auto right = DeduplicationInfo::create(/*async_insert*/ false);
    right->setUserToken("x", 2);
    right->setSourceBlockNumber(1);

    auto merged = left->mergeSelf(right);
    auto hashes = merged->getDeduplicationHashes("", /*deduplication_enabled*/ true);
    EXPECT_EQ(hashes.size(), 1u) << "contiguous source numbers must extend into one token";
}
