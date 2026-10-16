#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/SingleDiskVolume.h>
#include <Disks/tests/gtest_disk.h>
#include <Interpreters/DiskSet.h>
#include <Interpreters/Set.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <QueryPipeline/SizeLimits.h>

using namespace DB;

/// A scheduler request can spill a set in either phase. A set that spills while it is built frees its table
/// before the disk set creates its directory and Bloom filters. A built set keeps its table for lookups until
/// the disk set replaces it, so the memory that the request needs also counts the directory and the filters.
TEST(SetMemoryStats, BuiltSetReservesDiskSetMemory)
{
    constexpr size_t keys = 1'000'000;
    constexpr size_t max_bytes_for_bloom_filter = 128 << 20;

    auto disk = createDisk("set_memory_stats");
    {
        auto type = std::make_shared<DataTypeUInt64>();
        Set set(SizeLimits{}, /*max_elements_to_fill_=*/ 0, /*transform_null_in_=*/ false);
        set.setHeader({ColumnWithTypeAndName(type->createColumn(), type, "x")});
        SetSpillSettings spill_settings;
        spill_settings.tmp_data = std::make_shared<TemporaryDataOnDiskScope>(
            TemporaryDataOnDiskSettings{}, std::make_shared<SingleDiskVolume>("temporary", disk));
        spill_settings.max_block_size = 65536;
        spill_settings.max_bytes_for_bloom_filter = max_bytes_for_bloom_filter;
        set.setSpillSettings(std::move(spill_settings));

        auto column = ColumnUInt64::create();
        for (size_t i = 0; i < keys; ++i)
            column->insertValue(i);
        set.insertFromColumns({std::move(column)});

        const auto building = set.getMemoryStats();
        set.finishInsert();
        const auto built = set.getMemoryStats();

        ASSERT_GT(building.spillable_memory_bytes, 0);
        EXPECT_EQ(built.spillable_memory_bytes, building.spillable_memory_bytes);
        EXPECT_EQ(
            built.need_reserved_memory_bytes - building.need_reserved_memory_bytes,
            static_cast<Int64>(estimateDiskSetMemory<UInt64>(keys, max_bytes_for_bloom_filter)));
    }
    destroyDisk(disk);
}
