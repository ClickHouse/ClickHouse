#include <gtest/gtest.h>

#include <thread>
#include <vector>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Transforms/BlockNestedLoopJoinData.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>

using namespace DB;

namespace
{

SharedHeader makeHeader()
{
    Block header;
    header.insert(ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "x"));
    return std::make_shared<const Block>(std::move(header));
}

Block makeBlock(const std::vector<UInt64> & values)
{
    auto column = ColumnUInt64::create();
    for (auto value : values)
        column->insertValue(value);

    Block block;
    block.insert(ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeUInt64>(), "x"));
    return block;
}

std::shared_ptr<BlockNestedLoopJoinData> makeData(
    JoinKind kind = JoinKind::Inner, JoinStrictness strictness = JoinStrictness::All, const SizeLimits & limits = {})
{
    return std::make_shared<BlockNestedLoopJoinData>(makeHeader(), kind, strictness, limits);
}

/// The value of every stored row, in the store's own block order.
std::vector<UInt64> storedValues(const BlockNestedLoopJoinData & data)
{
    std::vector<UInt64> result;
    for (const auto & block : data.getBlocks())
    {
        const auto & column = assert_cast<const ColumnUInt64 &>(*block.columns.at(0));
        for (size_t i = 0; i < block.selector.size(); ++i)
            result.push_back(column.getElement(block.selector[i]));
    }
    return result;
}

}

TEST(BlockNestedLoopJoinData, RowOffsetsCoverEveryStoredRow)
{
    auto data = makeData();
    ASSERT_TRUE(data->addBlock(makeBlock({1, 2, 3}), 3));
    ASSERT_TRUE(data->addBlock(makeBlock({4}), 1));
    ASSERT_TRUE(data->addBlock(makeBlock({5, 6}), 2));
    data->finish();

    EXPECT_EQ(data->getTotalRows(), 6);
    EXPECT_GT(data->getTotalBytes(), 0);
    EXPECT_FALSE(data->isBuildSideEmpty());

    const auto & offsets = data->getRowOffsets();
    ASSERT_EQ(offsets.size(), data->getBlocks().size() + 1);
    EXPECT_EQ(offsets.back(), data->getTotalRows());
    for (size_t i = 0; i < data->getBlocks().size(); ++i)
        EXPECT_EQ(offsets[i + 1] - offsets[i], data->getBlocks()[i].selector.size());

    EXPECT_EQ(storedValues(*data), (std::vector<UInt64>{1, 2, 3, 4, 5, 6}));
}

TEST(BlockNestedLoopJoinData, EmptyBlocksAreNotStored)
{
    auto data = makeData();
    ASSERT_TRUE(data->addBlock(makeBlock({}), 0));
    data->finish();

    EXPECT_TRUE(data->getBlocks().empty());
    EXPECT_EQ(data->getRowOffsets(), (std::vector<size_t>{0}));
    EXPECT_EQ(data->getTotalRows(), 0);
    EXPECT_TRUE(data->isBuildSideEmpty());
}

TEST(BlockNestedLoopJoinData, ColumnlessBlocksKeepTheirRowCount)
{
    auto data = std::make_shared<BlockNestedLoopJoinData>(
        std::make_shared<const Block>(), JoinKind::Inner, JoinStrictness::All, SizeLimits{});
    ASSERT_TRUE(data->addBlock(Block{}, 5));
    data->finish();

    EXPECT_EQ(data->getTotalRows(), 5);
    EXPECT_FALSE(data->isBuildSideEmpty());
    ASSERT_EQ(data->getBlocks().size(), 1);
    EXPECT_EQ(data->getBlocks()[0].selector.size(), 5);
}

TEST(BlockNestedLoopJoinData, ConstAndSparseColumnsAreMaterialized)
{
    auto data = makeData();

    Block const_block;
    const_block.insert(ColumnWithTypeAndName(
        ColumnConst::create(ColumnUInt64::create(1, 7), 3), std::make_shared<DataTypeUInt64>(), "x"));
    ASSERT_TRUE(data->addBlock(std::move(const_block), 3));

    auto values = ColumnUInt64::create();
    values->insertDefault();
    values->insertValue(9);
    auto offsets = ColumnUInt64::create();
    offsets->insertValue(1);

    Block sparse_block;
    sparse_block.insert(ColumnWithTypeAndName(
        ColumnSparse::create(std::move(values), std::move(offsets), 3), std::make_shared<DataTypeUInt64>(), "x"));
    ASSERT_TRUE(data->addBlock(std::move(sparse_block), 3));

    data->finish();

    for (const auto & block : data->getBlocks())
    {
        EXPECT_FALSE(isColumnConst(*block.columns.at(0)));
        EXPECT_FALSE(block.columns.at(0)->isSparse());
    }
    EXPECT_EQ(storedValues(*data), (std::vector<UInt64>{7, 7, 7, 0, 9, 0}));
}

TEST(BlockNestedLoopJoinData, ReadingBeforeFinishThrows)
{
    auto data = makeData();
    ASSERT_TRUE(data->addBlock(makeBlock({1}), 1));

    EXPECT_FALSE(data->isFinished());
    EXPECT_THROW(data->getBlocks(), Exception);
    EXPECT_THROW(data->getRowOffsets(), Exception);
    EXPECT_THROW(data->isBuildSideEmpty(), Exception);

    data->finish();
    EXPECT_TRUE(data->isFinished());
    EXPECT_THROW(data->addBlock(makeBlock({2}), 1), Exception);
    EXPECT_THROW(data->finish(), Exception);
}

TEST(BlockNestedLoopJoinData, SizeLimitsThrow)
{
    auto data = makeData(JoinKind::Inner, JoinStrictness::All, SizeLimits(2, 0, OverflowMode::THROW));
    ASSERT_TRUE(data->addBlock(makeBlock({1, 2}), 2));
    EXPECT_THROW(data->addBlock(makeBlock({3}), 1), Exception);
}

TEST(BlockNestedLoopJoinData, SizeLimitsBreak)
{
    auto data = makeData(JoinKind::Inner, JoinStrictness::All, SizeLimits(3, 0, OverflowMode::BREAK));
    EXPECT_TRUE(data->addBlock(makeBlock({1, 2}), 2));
    /// `break` asks to stop once the limit is reached, but the block that reached it is kept.
    EXPECT_FALSE(data->addBlock(makeBlock({3, 4}), 2));
    data->finish();
    EXPECT_EQ(data->getTotalRows(), 4);
}

TEST(BlockNestedLoopJoinData, ConcurrentInsertsKeepEveryRow)
{
    constexpr size_t num_threads = 8;
    constexpr size_t blocks_per_thread = 32;

    auto data = makeData();
    std::vector<std::thread> threads;
    for (size_t thread_index = 0; thread_index < num_threads; ++thread_index)
    {
        threads.emplace_back([&, thread_index]
        {
            for (size_t i = 0; i < blocks_per_thread; ++i)
                data->addBlock(makeBlock({thread_index, i}), 2);
        });
    }
    for (auto & thread : threads)
        thread.join();
    data->finish();

    EXPECT_EQ(data->getTotalRows(), num_threads * blocks_per_thread * 2);
    EXPECT_EQ(data->getBlocks().size(), num_threads * blocks_per_thread);
    EXPECT_EQ(data->getRowOffsets().back(), data->getTotalRows());
    EXPECT_EQ(storedValues(*data).size(), data->getTotalRows());

    /// Every block is numbered once, so a global row number resolves to exactly one stored row.
    std::vector<bool> seen(data->getBlocks().size(), false);
    for (const auto & block : data->getBlocks())
    {
        ASSERT_LT(block.block_no, seen.size());
        EXPECT_FALSE(seen[block.block_no]);
        seen[block.block_no] = true;
    }
}

TEST(BlockNestedLoopJoinData, EmptyBuildSideAction)
{
    using enum EmptyBuildSideAction;

    for (auto strictness : {JoinStrictness::All, JoinStrictness::Any, JoinStrictness::RightAny})
    {
        EXPECT_EQ(emptyBuildSideActionFor(JoinKind::Inner, strictness), ProduceNothing);
        EXPECT_EQ(emptyBuildSideActionFor(JoinKind::Cross, strictness), ProduceNothing);
        EXPECT_EQ(emptyBuildSideActionFor(JoinKind::Right, strictness), ProduceNothing);
        EXPECT_EQ(emptyBuildSideActionFor(JoinKind::Left, strictness), PassProbeRowsPadded);
        EXPECT_EQ(emptyBuildSideActionFor(JoinKind::Full, strictness), PassProbeRowsPadded);
    }

    EXPECT_EQ(emptyBuildSideActionFor(JoinKind::Left, JoinStrictness::Semi), ProduceNothing);
    EXPECT_EQ(emptyBuildSideActionFor(JoinKind::Right, JoinStrictness::Semi), ProduceNothing);
    EXPECT_EQ(emptyBuildSideActionFor(JoinKind::Left, JoinStrictness::Anti), PassProbeRowsPadded);
    EXPECT_EQ(emptyBuildSideActionFor(JoinKind::Right, JoinStrictness::Anti), ProduceNothing);
}
