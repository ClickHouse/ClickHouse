#include <gtest/gtest.h>

#include <algorithm>
#include <numeric>
#include <thread>
#include <vector>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Processors/Transforms/BlockNestedLoopJoinData.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>

#include <Poco/TemporaryFile.h>

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

BlockNestedLoopJoinDataPtr makeData(
    JoinKind kind = JoinKind::Inner,
    JoinStrictness strictness = JoinStrictness::All,
    const SizeLimits & limits = {},
    BlockNestedLoopStoreSettings store_settings = {})
{
    return std::make_shared<BlockNestedLoopJoinData>(makeHeader(), kind, strictness, limits, std::move(store_settings));
}

/// The value of every stored row, in the store's own block order, read back the way the probe
/// reads it - so a compressed or spilled block goes through decompression or the temporary file.
std::vector<UInt64> storedValues(const BlockNestedLoopJoinDataPtr & data)
{
    BuildSideBlockReader reader(data);
    std::vector<UInt64> result;
    for (size_t index = 0; index < data->getNumBlocks(); ++index)
    {
        auto block = reader.read(index);
        EXPECT_EQ(block->num_rows, data->getBlockNumRows(index));
        const auto & column = assert_cast<const ColumnUInt64 &>(*block->columns.at(0));
        for (size_t i = 0; i < block->num_rows; ++i)
            result.push_back(column.getElement(i));
    }
    return result;
}

/// A temporary storage scope backed by a directory that lives as long as the returned holder.
struct TemporaryStorage
{
    std::unique_ptr<Poco::TemporaryFile> directory;
    TemporaryDataOnDiskScopePtr scope;
};

TemporaryStorage makeTemporaryStorage()
{
    TemporaryStorage storage;
    storage.directory = std::make_unique<Poco::TemporaryFile>();
    storage.directory->createDirectories();
    auto disk = std::make_shared<DiskLocal>("block_nested_loop_join_tmp", storage.directory->path() + "/");
    VolumePtr volume = std::make_shared<SingleDiskVolume>("block_nested_loop_join_tmp", disk, 0);
    storage.scope = std::make_shared<TemporaryDataOnDiskScope>(TemporaryDataOnDiskSettings{}, volume);
    return storage;
}

/// Every block is compressed: the very first one already passes the threshold.
BlockNestedLoopStoreSettings compressEverything()
{
    BlockNestedLoopStoreSettings settings;
    settings.min_rows_to_compress = 1;
    return settings;
}

/// Every block goes to disk: a threshold of one byte is passed by any of them.
BlockNestedLoopStoreSettings spillEverything(const TemporaryDataOnDiskScopePtr & scope)
{
    BlockNestedLoopStoreSettings settings;
    settings.max_bytes_in_memory = 1;
    settings.tmp_data = scope;
    return settings;
}

/// Nothing spills on its own; only an explicit request moves the blocks out of memory.
BlockNestedLoopStoreSettings spillOnRequest(const TemporaryDataOnDiskScopePtr & scope)
{
    BlockNestedLoopStoreSettings settings;
    settings.tmp_data = scope;
    return settings;
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

    const auto & offsets = data->getRowOffsets();
    ASSERT_EQ(offsets.size(), data->getNumBlocks() + 1);
    EXPECT_EQ(offsets.back(), data->getTotalRows());
    for (size_t i = 0; i < data->getNumBlocks(); ++i)
        EXPECT_EQ(offsets[i + 1] - offsets[i], data->getBlockNumRows(i));

    EXPECT_EQ(storedValues(data), (std::vector<UInt64>{1, 2, 3, 4, 5, 6}));
}

TEST(BlockNestedLoopJoinData, EmptyBlocksAreNotStored)
{
    auto data = makeData();
    ASSERT_TRUE(data->addBlock(makeBlock({}), 0));
    data->finish();

    EXPECT_EQ(data->getNumBlocks(), 0);
    EXPECT_EQ(data->getRowOffsets(), (std::vector<size_t>{0}));
    EXPECT_EQ(data->getTotalRows(), 0);
}

TEST(BlockNestedLoopJoinData, ColumnlessBlocksKeepTheirRowCount)
{
    auto data = std::make_shared<BlockNestedLoopJoinData>(
        std::make_shared<const Block>(), JoinKind::Inner, JoinStrictness::All, SizeLimits{});
    ASSERT_TRUE(data->addBlock(Block{}, 5));
    data->finish();

    EXPECT_EQ(data->getTotalRows(), 5);
    ASSERT_EQ(data->getNumBlocks(), 1);
    EXPECT_EQ(data->getBlockNumRows(0), 5);
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

    BuildSideBlockReader reader(data);
    for (size_t index = 0; index < data->getNumBlocks(); ++index)
    {
        auto block = reader.read(index);
        EXPECT_FALSE(isColumnConst(*block->columns.at(0)));
        EXPECT_FALSE(block->columns.at(0)->isSparse());
    }
    EXPECT_EQ(storedValues(data), (std::vector<UInt64>{7, 7, 7, 0, 9, 0}));
}

TEST(BlockNestedLoopJoinData, ReadingBeforeFinishThrows)
{
    auto data = makeData();
    ASSERT_TRUE(data->addBlock(makeBlock({1}), 1));

    EXPECT_FALSE(data->isFinished());
    EXPECT_THROW(data->getNumBlocks(), Exception);
    EXPECT_THROW(data->getRowOffsets(), Exception);

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
    EXPECT_EQ(data->getNumBlocks(), num_threads * blocks_per_thread);
    EXPECT_EQ(data->getRowOffsets().back(), data->getTotalRows());
    EXPECT_EQ(storedValues(data).size(), data->getTotalRows());

    /// Every block is numbered once, so a global row number resolves to exactly one stored row.
    BuildSideBlockReader reader(data);
    std::vector<bool> seen(data->getNumBlocks(), false);
    for (size_t index = 0; index < data->getNumBlocks(); ++index)
    {
        auto block = reader.read(index);
        ASSERT_LT(block->index, seen.size());
        EXPECT_FALSE(seen[block->index]);
        seen[block->index] = true;
    }
}

TEST(BlockNestedLoopJoinData, MatchFlagsAreKeptOnlyWhereTheResultNeedsThem)
{
    for (auto strictness : {JoinStrictness::All, JoinStrictness::Any, JoinStrictness::RightAny})
    {
        EXPECT_FALSE(needsBuildSideMatchFlags(JoinKind::Left, strictness));
        EXPECT_FALSE(needsBuildSideMatchFlags(JoinKind::Cross, strictness));
        EXPECT_TRUE(needsBuildSideMatchFlags(JoinKind::Right, strictness));
        EXPECT_TRUE(needsBuildSideMatchFlags(JoinKind::Full, strictness));

        EXPECT_FALSE(keepsUnmatchedBuildRows(JoinKind::Inner, strictness));
        EXPECT_FALSE(keepsUnmatchedBuildRows(JoinKind::Left, strictness));
        EXPECT_TRUE(keepsUnmatchedBuildRows(JoinKind::Right, strictness));
        EXPECT_TRUE(keepsUnmatchedBuildRows(JoinKind::Full, strictness));
    }

    /// `ANY INNER` keeps the flags to take each build row once, though it emits no build row itself.
    EXPECT_TRUE(needsBuildSideMatchFlags(JoinKind::Inner, JoinStrictness::Any));
    EXPECT_FALSE(needsBuildSideMatchFlags(JoinKind::Inner, JoinStrictness::All));
    EXPECT_FALSE(needsBuildSideMatchFlags(JoinKind::Inner, JoinStrictness::RightAny));

    /// The right-driven early-exit kinds select build rows by their flag, the left-driven ones do not.
    EXPECT_FALSE(needsBuildSideMatchFlags(JoinKind::Left, JoinStrictness::Semi));
    EXPECT_FALSE(needsBuildSideMatchFlags(JoinKind::Left, JoinStrictness::Anti));
    EXPECT_TRUE(needsBuildSideMatchFlags(JoinKind::Right, JoinStrictness::Semi));
    EXPECT_TRUE(needsBuildSideMatchFlags(JoinKind::Right, JoinStrictness::Anti));

    /// `RIGHT SEMI` emits the build rows that did match, which is the other half of the same scan.
    EXPECT_FALSE(keepsUnmatchedBuildRows(JoinKind::Right, JoinStrictness::Semi));
    EXPECT_TRUE(keepsUnmatchedBuildRows(JoinKind::Right, JoinStrictness::Anti));
    EXPECT_FALSE(keepsUnmatchedBuildRows(JoinKind::Left, JoinStrictness::Anti));

    auto without_flags = makeData(JoinKind::Left, JoinStrictness::All);
    EXPECT_FALSE(without_flags->hasBuildSideMatchFlags());
    auto with_flags = makeData(JoinKind::Full, JoinStrictness::All);
    EXPECT_TRUE(with_flags->hasBuildSideMatchFlags());
}

TEST(BlockNestedLoopJoinData, MatchFlagsStartUnset)
{
    auto data = makeData(JoinKind::Right, JoinStrictness::All);
    ASSERT_TRUE(data->addBlock(makeBlock({1, 2, 3}), 3));
    ASSERT_TRUE(data->addBlock(makeBlock({4, 5}), 2));
    data->finish();

    for (size_t row = 0; row < data->getTotalRows(); ++row)
        EXPECT_FALSE(data->isBuildRowMatched(row));

    data->setBuildRowMatched(0);
    data->setBuildRowMatched(4);
    /// Setting a flag twice is how two probe streams that matched the same build row behave.
    data->setBuildRowMatched(4);

    EXPECT_TRUE(data->isBuildRowMatched(0));
    EXPECT_FALSE(data->isBuildRowMatched(1));
    EXPECT_FALSE(data->isBuildRowMatched(2));
    EXPECT_FALSE(data->isBuildRowMatched(3));
    EXPECT_TRUE(data->isBuildRowMatched(4));
}

TEST(BlockNestedLoopJoinData, ConcurrentMatchFlagSetsKeepEveryFlag)
{
    constexpr size_t num_threads = 8;
    constexpr size_t num_rows = 4096;

    auto data = makeData(JoinKind::Full, JoinStrictness::All);
    for (size_t i = 0; i < num_rows; ++i)
        ASSERT_TRUE(data->addBlock(makeBlock({i}), 1));
    data->finish();
    ASSERT_EQ(data->getTotalRows(), num_rows);

    /// Every thread walks the whole range and sets the rows of its own residue class, so the threads
    /// contend on neighbouring flags throughout.
    std::vector<std::thread> threads;
    for (size_t thread_index = 0; thread_index < num_threads; ++thread_index)
    {
        threads.emplace_back([&, thread_index]
        {
            for (size_t row = 0; row < num_rows; ++row)
                if (row % num_threads == thread_index && row % 3 != 0)
                    data->setBuildRowMatched(row);
        });
    }
    for (auto & thread : threads)
        thread.join();

    for (size_t row = 0; row < num_rows; ++row)
        EXPECT_EQ(data->isBuildRowMatched(row), row % 3 != 0) << "row " << row;
}

TEST(BlockNestedLoopJoinData, ConcurrentClaimsGiveEveryBuildRowToOneProbeStream)
{
    constexpr size_t num_threads = 8;
    constexpr size_t num_rows = 4096;

    auto data = makeData(JoinKind::Right, JoinStrictness::Any);
    for (size_t i = 0; i < num_rows; ++i)
        ASSERT_TRUE(data->addBlock(makeBlock({i}), 1));
    data->finish();

    /// Every thread tries to take every row, so all of them contend on each one in turn.
    std::vector<std::vector<size_t>> claimed_by_thread(num_threads);
    std::vector<std::thread> threads;
    for (size_t thread_index = 0; thread_index < num_threads; ++thread_index)
    {
        threads.emplace_back([&, thread_index]
        {
            for (size_t row = 0; row < num_rows; ++row)
                if (data->claimBuildRow(row))
                    claimed_by_thread[thread_index].push_back(row);
        });
    }
    for (auto & thread : threads)
        thread.join();

    std::vector<size_t> claimed;
    for (const auto & rows : claimed_by_thread)
        claimed.insert(claimed.end(), rows.begin(), rows.end());
    std::sort(claimed.begin(), claimed.end());

    std::vector<size_t> expected(num_rows);
    std::iota(expected.begin(), expected.end(), 0);
    /// Exactly one thread takes each row, and a taken row counts as matched from then on.
    EXPECT_EQ(claimed, expected);
    for (size_t row = 0; row < num_rows; ++row)
        EXPECT_TRUE(data->isBuildRowMatched(row)) << "row " << row;
}

TEST(BlockNestedLoopJoinData, CompressedBlocksRoundTrip)
{
    /// Every block is compressed, because the very first one already passes the threshold.
    auto data = makeData(JoinKind::Inner, JoinStrictness::All, SizeLimits{}, compressEverything());
    ASSERT_TRUE(data->addBlock(makeBlock({1, 2, 3}), 3));
    ASSERT_TRUE(data->addBlock(makeBlock({4, 5}), 2));
    data->finish();

    EXPECT_EQ(data->getNumSpilledBlocks(), 0);
    EXPECT_GT(data->getInMemoryBytes(), 0);
    EXPECT_EQ(storedValues(data), (std::vector<UInt64>{1, 2, 3, 4, 5}));
}

TEST(BlockNestedLoopJoinData, CompressionShrinksWhatIsHeldInMemory)
{
    const std::vector<UInt64> compressible(4096, 7);

    auto plain = makeData();
    ASSERT_TRUE(plain->addBlock(makeBlock(compressible), compressible.size()));
    plain->finish();

    auto compressed = makeData(JoinKind::Inner, JoinStrictness::All, SizeLimits{}, compressEverything());
    ASSERT_TRUE(compressed->addBlock(makeBlock(compressible), compressible.size()));
    compressed->finish();

    EXPECT_LT(compressed->getInMemoryBytes(), plain->getInMemoryBytes());
    /// What the spill has to reserve is the block as it will be written out, not as it is held.
    EXPECT_EQ(compressed->getMaxInMemoryBlockBytes(), plain->getMaxInMemoryBlockBytes());
    EXPECT_EQ(storedValues(compressed), compressible);
}

TEST(BlockNestedLoopJoinData, SpilledBlocksRoundTrip)
{
    auto storage = makeTemporaryStorage();
    /// A threshold of one byte sends every block to disk, the first one included.
    auto data = makeData(
        JoinKind::Inner, JoinStrictness::All, SizeLimits{}, spillEverything(storage.scope));
    ASSERT_TRUE(data->canSpill());

    ASSERT_TRUE(data->addBlock(makeBlock({1, 2, 3}), 3));
    ASSERT_TRUE(data->addBlock(makeBlock({4}), 1));
    ASSERT_TRUE(data->addBlock(makeBlock({5, 6}), 2));
    data->finish();

    EXPECT_EQ(data->getNumSpilledBlocks(), 3);
    EXPECT_EQ(data->getInMemoryBytes(), 0);
    /// The rows and their numbering are what they would be without a spill: only where the columns
    /// live has changed.
    EXPECT_EQ(data->getTotalRows(), 6);
    EXPECT_GT(data->getTotalBytes(), 0);
    EXPECT_EQ(data->getRowOffsets(), (std::vector<size_t>{0, 3, 4, 6}));
    EXPECT_EQ(storedValues(data), (std::vector<UInt64>{1, 2, 3, 4, 5, 6}));
}

TEST(BlockNestedLoopJoinData, ARewoundReaderStartsTheTemporaryFileOver)
{
    auto storage = makeTemporaryStorage();
    auto data = makeData(
        JoinKind::Inner, JoinStrictness::All, SizeLimits{}, spillEverything(storage.scope));
    for (UInt64 i = 0; i < 4; ++i)
        ASSERT_TRUE(data->addBlock(makeBlock({i}), 1));
    data->finish();

    /// One reader serves every probe chunk, and each of them walks the build side from the start.
    BuildSideBlockReader reader(data);
    for (size_t pass = 0; pass < 3; ++pass)
        for (size_t index = 0; index < data->getNumBlocks(); ++index)
            EXPECT_EQ(assert_cast<const ColumnUInt64 &>(*reader.read(index)->columns.at(0)).getElement(0), index)
                << "pass " << pass << ", block " << index;

    /// A reader may also skip forward, which is what the scan for unmatched build rows does.
    BuildSideBlockReader skipping(data);
    EXPECT_EQ(assert_cast<const ColumnUInt64 &>(*skipping.read(1)->columns.at(0)).getElement(0), 1);
    EXPECT_EQ(assert_cast<const ColumnUInt64 &>(*skipping.read(3)->columns.at(0)).getElement(0), 3);
}

TEST(BlockNestedLoopJoinData, SpillingOnDemandMovesTheStoredBlocksOut)
{
    auto storage = makeTemporaryStorage();
    auto data = makeData(JoinKind::Inner, JoinStrictness::All, SizeLimits{}, spillOnRequest(storage.scope));

    /// Nothing spills on its own without a threshold, ...
    ASSERT_TRUE(data->addBlock(makeBlock({1, 2}), 2));
    ASSERT_TRUE(data->addBlock(makeBlock({3}), 1));
    EXPECT_EQ(data->getNumSpilledBlocks(), 0);
    EXPECT_GT(data->getInMemoryBytes(), 0);

    /// ... until the memory tracker asks for it.
    EXPECT_FALSE(data->spillInMemoryBlocks(data->getInMemoryBytes() + 1));
    EXPECT_TRUE(data->spillInMemoryBlocks(data->getInMemoryBytes()));
    EXPECT_EQ(data->getNumSpilledBlocks(), 2);
    EXPECT_EQ(data->getInMemoryBytes(), 0);
    EXPECT_FALSE(data->spillInMemoryBlocks(0));

    /// From then on the build side streams: a block that arrives later goes straight to the file,
    /// so the file order stays the block order.
    ASSERT_TRUE(data->addBlock(makeBlock({4, 5}), 2));
    EXPECT_EQ(data->getNumSpilledBlocks(), 3);
    data->finish();

    EXPECT_EQ(data->getRowOffsets(), (std::vector<size_t>{0, 2, 3, 5}));
    EXPECT_EQ(storedValues(data), (std::vector<UInt64>{1, 2, 3, 4, 5}));
}

TEST(BlockNestedLoopJoinData, SpillingOnAThresholdTakesTheEarlierBlocksWithIt)
{
    auto storage = makeTemporaryStorage();
    BlockNestedLoopStoreSettings settings;
    settings.tmp_data = storage.scope;
    /// Wide enough for the first block to stay in memory and narrow enough for the second to
    /// cross it, so that the spill starts with blocks already stored.
    auto probe = makeData();
    ASSERT_TRUE(probe->addBlock(makeBlock({1, 2}), 2));
    settings.max_bytes_in_memory = probe->getInMemoryBytes() + 1;

    auto data = makeData(JoinKind::Inner, JoinStrictness::All, SizeLimits{}, settings);
    ASSERT_TRUE(data->addBlock(makeBlock({1, 2}), 2));
    EXPECT_EQ(data->getNumSpilledBlocks(), 0);

    /// The block that crosses the threshold goes out behind the ones already in memory, so the
    /// file order is still the index order and a forward read walks the blocks in order.
    ASSERT_TRUE(data->addBlock(makeBlock({3}), 1));
    EXPECT_EQ(data->getNumSpilledBlocks(), 2);
    EXPECT_EQ(data->getInMemoryBytes(), 0);
    EXPECT_EQ(data->getMaxInMemoryBlockBytes(), 0);
    /// Nothing is left for the memory tracker to gain.
    EXPECT_FALSE(data->spillInMemoryBlocks(0));

    ASSERT_TRUE(data->addBlock(makeBlock({4, 5}), 2));
    data->finish();

    EXPECT_EQ(data->getNumSpilledBlocks(), 3);
    EXPECT_EQ(storedValues(data), (std::vector<UInt64>{1, 2, 3, 4, 5}));
}

TEST(BlockNestedLoopJoinData, MatchFlagsAreIndexedAcrossTheSpilledBoundary)
{
    auto storage = makeTemporaryStorage();
    auto data = makeData(JoinKind::Right, JoinStrictness::All, SizeLimits{}, spillOnRequest(storage.scope));

    ASSERT_TRUE(data->addBlock(makeBlock({10, 11, 12}), 3));
    ASSERT_TRUE(data->spillInMemoryBlocks(0));
    /// The store now holds one spilled block and, from here on, everything else spilled too.
    ASSERT_TRUE(data->addBlock(makeBlock({13, 14}), 2));
    data->finish();
    ASSERT_EQ(data->getNumSpilledBlocks(), 2);

    /// A global row number names the same row whether its block is in memory or on disk.
    data->setBuildRowMatched(0);
    data->setBuildRowMatched(4);

    BuildSideBlockReader reader(data);
    std::vector<UInt64> unmatched;
    for (size_t index = 0; index < data->getNumBlocks(); ++index)
    {
        auto block = reader.read(index);
        const auto & column = assert_cast<const ColumnUInt64 &>(*block->columns.at(0));
        for (size_t row = 0; row < data->getBlockNumRows(index); ++row)
            if (!data->isBuildRowMatched(data->getRowOffsets()[index] + row))
                unmatched.push_back(column.getElement(row));
    }
    EXPECT_EQ(unmatched, (std::vector<UInt64>{11, 12, 13}));
}

TEST(BlockNestedLoopJoinData, ColumnlessBuildSidesNeverSpill)
{
    auto storage = makeTemporaryStorage();
    /// A row count is all such a build side is; the `Native` format cannot persist one, and there
    /// is nothing to gain by trying.
    auto data = std::make_shared<BlockNestedLoopJoinData>(
        std::make_shared<const Block>(), JoinKind::Inner, JoinStrictness::All, SizeLimits{},
        spillEverything(storage.scope));

    EXPECT_FALSE(data->canSpill());
    ASSERT_TRUE(data->addBlock(Block{}, 5));
    EXPECT_FALSE(data->spillInMemoryBlocks(0));
    data->finish();

    EXPECT_EQ(data->getNumSpilledBlocks(), 0);
    EXPECT_EQ(data->getTotalRows(), 5);
    EXPECT_EQ(data->getBlockNumRows(0), 5);
}

TEST(BlockNestedLoopJoinData, SizeLimitsCountSpilledRowsToo)
{
    auto storage = makeTemporaryStorage();
    /// Spilling relieves memory pressure; it is not a way around `max_rows_in_join`, so
    /// `join_overflow_mode` means the same whether the build side fits in memory or not.
    auto data = makeData(
        JoinKind::Inner, JoinStrictness::All, SizeLimits(2, 0, OverflowMode::THROW),
        spillEverything(storage.scope));

    ASSERT_TRUE(data->addBlock(makeBlock({1, 2}), 2));
    EXPECT_EQ(data->getNumSpilledBlocks(), 1);
    EXPECT_THROW(data->addBlock(makeBlock({3}), 1), Exception);
}

