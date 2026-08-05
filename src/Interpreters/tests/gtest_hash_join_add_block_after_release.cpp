#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/SingleDiskVolume.h>
#include <Disks/tests/gtest_disk.h>
#include <Interpreters/GraceHashJoin.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Common/Exception.h>
#include <Common/scope_guard_safe.h>

/// Skipped under debug/sanitizers: LOGICAL_ERROR aborts there, so EXPECT_THROW can't catch it.
#ifndef DEBUG_OR_SANITIZER_BUILD

using namespace DB;

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int TOO_MANY_ROWS_OR_BYTES;
}

namespace
{

Block makeRightBlock(size_t rows)
{
    auto column = ColumnUInt64::create();
    for (size_t i = 0; i < rows; ++i)
        column->insertValue(i);

    return Block{ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeUInt64>(), "k")};
}

std::shared_ptr<TableJoin> makeTableJoin()
{
    return std::make_shared<TableJoin>(
        SizeLimits{}, /* use_nulls */ false, JoinKind::Inner, JoinStrictness::All, Names{"k"});
}

/// `GraceHashJoin` scatters both sides by key, so it needs the left key names, which the
/// constructor above does not fill in, and it needs the key column to survive `prepareRightBlock`.
/// A `RIGHT` join keeps it (`HashJoin::initRightBlockStructure`); an `INNER` one keeps it only
/// because the real settings put `grace_hash` in the enabled algorithms, which a hand-built
/// `TableJoin` cannot express.
std::shared_ptr<TableJoin> makeGraceTableJoin()
{
    auto table_join = std::make_shared<TableJoin>(
        SizeLimits{}, /* use_nulls */ false, JoinKind::Right, JoinStrictness::All, Names{"k"});
    table_join->setLeftKeys(Names{"k"});
    return table_join;
}

/// Poorly compressible values, so the bucket write reaches the temporary-data accounting instead
/// of sitting in the 1 MiB compression buffer.
Block makeSpreadRightBlock(size_t rows)
{
    auto key = ColumnUInt64::create();
    auto payload = ColumnUInt64::create();
    for (size_t i = 0; i < rows; ++i)
    {
        key->insertValue(i * 2654435761ULL);
        payload->insertValue(i * 11400714819323198485ULL);
    }

    return Block{
        ColumnWithTypeAndName(std::move(key), std::make_shared<DataTypeUInt64>(), "k"),
        ColumnWithTypeAndName(std::move(payload), std::make_shared<DataTypeUInt64>(), "v")};
}

std::shared_ptr<HashJoin> makeJoin()
{
    return std::make_shared<HashJoin>(makeTableJoin(), std::make_shared<const Block>(makeRightBlock(0)));
}

}

/// `releaseJoinedBlocks` resets `data` and only then allocates while extracting the blocks, so a
/// throw there leaves the instance alive with released data. The spilling wrappers can re-enter it
/// (`GraceHashJoin::addBlockToJoinImpl`, `SpillingHashJoin::switchToGraceHashJoin`), which must
/// report a logic error rather than dereference `data`.
TEST(HashJoin, AddBlockToJoinAfterDataReleaseThrows)
{
    auto join = makeJoin();
    ASSERT_TRUE(join->addBlockToJoin(makeRightBlock(8), /* check_limits = */ false));

    join->releaseJoinedBlocks(/* restructure */ false);

    /// The two-argument overload materializes against `data->sample_block` before delegating to
    /// the overload that carries the identical check, so it needs its own.
    try
    {
        join->addBlockToJoin(makeRightBlock(8), /* check_limits = */ false);
        FAIL() << "addBlockToJoin(Block, bool) did not throw after the join data was released";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::LOGICAL_ERROR);
        EXPECT_NE(e.message().find("Join data was released"), std::string::npos) << e.message();
    }

    /// Pin the scattered overload too, so the two stay symmetric.
    try
    {
        Block block = makeRightBlock(8);
        join->addBlockToJoin(block, ScatteredBlock::Selector(block.rows()), /* check_limits = */ false);
        FAIL() << "addBlockToJoin(Block, Selector, bool) did not throw after the join data was released";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::LOGICAL_ERROR);
        EXPECT_NE(e.message().find("Join data was released"), std::string::npos) << e.message();
    }
}

/// A throw while re-scattering the released blocks skips the rebuild at the end of
/// `addBlockToJoinImpl` and unwinds the lock, so `hash_join` is left null and visible to another
/// thread. `MemorySpillScheduler` reads these two counts before every `work()` of a spillable
/// processor, so they must report nothing rather than dereference the member.
TEST(GraceHashJoin, CountReadersToleratePendingRehash)
{
    DiskPtr disk;
    SCOPE_EXIT_SAFE(if (disk) destroyDisk(disk));
    disk = createDisk("grace_hash_join_pending_rehash_test_dir");

    /// Cap the temporary data so that flushing a bucket throws. The cap is inherited by the
    /// child scope the join creates, and the first write is already past it.
    TemporaryDataOnDiskSettings tmp_settings;
    tmp_settings.max_size_on_disk = 1;
    auto tmp_data = std::make_shared<TemporaryDataOnDiskScope>(
        tmp_settings, std::make_shared<SingleDiskVolume>("volume", disk));

    auto right_sample_block = std::make_shared<const Block>(makeSpreadRightBlock(0));
    /// One bucket to start with: `flushBlocksToBuckets` skips bucket 0, so nothing is written
    /// before the rehash and the first write to reach the accounting is the one inside the
    /// window where `hash_join` is null.
    GraceHashJoin join(
        /* initial_num_buckets_ */ 1,
        /* max_num_buckets_ */ 16,
        makeGraceTableJoin(),
        right_sample_block,
        right_sample_block,
        tmp_data);
    join.initialize(*right_sample_block);

    /// Force the rehash path: the block is added to the in-memory join, `hasMemoryOverflow`
    /// returns true, and the released blocks are then re-scattered into the new buckets.
    join.forceSpill();

    bool threw = false;
    try
    {
        join.addBlockToJoin(makeSpreadRightBlock(500000), /* check_limits = */ false);
    }
    catch (const Exception & e)
    {
        threw = true;
        EXPECT_EQ(e.code(), ErrorCodes::TOO_MANY_ROWS_OR_BYTES) << e.message();
    }

    ASSERT_TRUE(threw) << "flushing a bucket was expected to throw under the temporary data limit";

    /// Without the guards these dereference a null `hash_join`.
    EXPECT_EQ(join.getTotalRowCount(), 0u);
    EXPECT_EQ(join.getTotalByteCount(), 0u);
}

#endif
