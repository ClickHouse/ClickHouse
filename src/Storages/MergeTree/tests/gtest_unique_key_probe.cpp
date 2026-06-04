#include <gtest/gtest.h>

#include "config.h"

#if USE_ROCKSDB

#include <Storages/MergeTree/UniqueKey/UniqueKeyProbe.h>
#include <Storages/MergeTree/UniqueKey/UniqueKeyProbeSimple.h>
#include <Storages/MergeTree/UniqueKey/UniqueKeySSTProbe.h>
#include <Storages/MergeTree/UniqueKey/SSTIndexWriter.h>
#include <Storages/MergeTree/UniqueKey/UniqueKeyEncoding.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>

#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/Names.h>
#include <Core/SettingsEnums.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Common/tests/gtest_global_context.h>

#include <algorithm>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

using namespace DB;

namespace
{
    constexpr size_t MAX_ENC = 256;

    Block makeKeyBlock(const std::vector<UInt64> & keys)
    {
        auto col = ColumnUInt64::create();
        for (UInt64 k : keys)
            col->insertValue(k);
        Block b;
        b.insert({std::move(col), std::make_shared<DataTypeUInt64>(), "key"});
        return b;
    }

    /// Encode one UInt64 key with the same encoder the SST writer uses.
    String encodeKey(UInt64 k)
    {
        auto col = ColumnUInt64::create();
        col->insertValue(k);
        Columns cols{std::move(col)};
        std::vector<String> out;
        UniqueKeyEncoding::encodeBlock(cols, /*permutation=*/nullptr, MAX_ENC, out);
        return out.at(0);
    }
}

/// Fixture: owns a temp disk for the test's SST sidecars (kept alive for the
/// test duration) and builds real `SSTProbeTargetPart`s — one per "part" in a
/// newest-first snapshot, each written by the real `SSTIndexWriter` and given
/// its own delete bitmap.
class UniqueKeyProbeTest : public ::testing::Test
{
protected:
    std::filesystem::path base;
    std::shared_ptr<DiskLocal> disk;
    std::shared_ptr<SingleDiskVolume> volume;
    int counter = 0;

    void SetUp() override
    {
        base = std::filesystem::temp_directory_path()
            / ("gtest_uk_probe_" + std::to_string(reinterpret_cast<uintptr_t>(this)));
        std::filesystem::remove_all(base);
        std::filesystem::create_directories(base);
        disk = std::make_shared<DiskLocal>("test_disk", base.string());
        volume = std::make_shared<SingleDiskVolume>("test_volume", disk);

        /// `setTemporaryStoragePath` throws if called twice, so configure it
        /// once per binary lifetime (the SST writer streams through a temp dir).
        static std::once_flag tmp_storage_once;
        std::call_once(tmp_storage_once, []
        {
            auto shared_tmp = std::filesystem::temp_directory_path() / "ck_uk_probe_gtest_tmp";
            std::filesystem::create_directories(shared_tmp);
            getMutableContext().context->setTemporaryStoragePath(shared_tmp.string() + "/", 0);
        });
    }

    void TearDown() override
    {
        volume.reset();
        disk.reset();
        std::filesystem::remove_all(base);
    }

    /// Build an SST-backed probe target from `(key -> row)` entries (written via
    /// the real `SSTIndexWriter`), with `dead_rows` marked dead in the part's
    /// delete bitmap.
    ProbeTargetPartPtr makeTarget(
        std::vector<std::pair<UInt64, UInt32>> kv, std::vector<UInt64> dead_rows = {})
    {
        const String part_dir = "part_" + std::to_string(counter++);
        std::filesystem::create_directories(base / part_dir);
        auto storage = std::make_shared<DataPartStorageOnDiskFull>(volume, "", part_dir);

        /// `addEncoded` requires strictly-increasing encoded keys.
        std::vector<std::pair<String, UInt32>> enc;
        enc.reserve(kv.size());
        for (auto [k, r] : kv)
            enc.emplace_back(encodeKey(k), r);
        std::sort(enc.begin(), enc.end(), [](const auto & a, const auto & b) { return a.first < b.first; });

        SSTIndexWriter writer(*storage, getContext().context);
        for (const auto & [ek, r] : enc)
            writer.addEncoded(std::string_view(ek), r);
        writer.finalizeToStorage();

        auto handle = openSSTReaderFromPath(storage->getFullPath() + "/" + SSTIndexWriter::FILE_NAME);
        if (!handle.valid)
            return nullptr;
        auto bitmap = std::make_shared<DeleteBitmap>();
        for (UInt64 r : dead_rows)
            bitmap->add(r);
        return std::make_shared<SSTProbeTargetPart>(/*part=*/nullptr, bitmap, std::move(handle));
    }

    UniqueKeyProbeSimple probeOver(ProbeTargetsSnapshot snapshot)
    {
        return UniqueKeyProbeSimple(
            [snapshot](const String &) { return snapshot; }, Names{"key"}, MAX_ENC);
    }

    static ProbeResult probeKey(IUniqueKeyProbe & probe, UInt64 key)
    {
        return probe.probeBatch(makeKeyBlock({key}), "p0").at(0);
    }
};

/// ---------- reduction over real SST targets ----------

TEST_F(UniqueKeyProbeTest, EmptySnapshotReturnsNotFound)
{
    auto probe = probeOver({});
    EXPECT_EQ(probeKey(probe, 42).outcome, ProbeOutcome::NOT_FOUND);
}

TEST_F(UniqueKeyProbeTest, SinglePartLiveAndAbsent)
{
    auto t = makeTarget({{10, 0}, {20, 1}, {30, 2}});
    ASSERT_NE(t, nullptr);
    auto probe = probeOver({t});

    auto live = probeKey(probe, 20);
    EXPECT_EQ(live.outcome, ProbeOutcome::FOUND_LIVE);
    EXPECT_EQ(live.row_number, 1u);
    EXPECT_EQ(probeKey(probe, 99).outcome, ProbeOutcome::NOT_FOUND);
}

TEST_F(UniqueKeyProbeTest, SinglePartAllDead)
{
    auto t = makeTarget({{7, 3}}, /*dead_rows=*/{3});
    ASSERT_NE(t, nullptr);
    auto probe = probeOver({t});
    EXPECT_EQ(probeKey(probe, 7).outcome, ProbeOutcome::FOUND_ALL_DEAD);
}

TEST_F(UniqueKeyProbeTest, NewestLiveWinsOverOlder)
{
    auto newest = makeTarget({{5, 9}});
    auto older = makeTarget({{5, 1}});
    ASSERT_NE(newest, nullptr);
    ASSERT_NE(older, nullptr);
    auto probe = probeOver({newest, older}); /// newest-first

    auto r = probeKey(probe, 5);
    EXPECT_EQ(r.outcome, ProbeOutcome::FOUND_LIVE);
    EXPECT_EQ(r.row_number, 9u);
}

TEST_F(UniqueKeyProbeTest, SkipNewerDeadFindOlderLive)
{
    auto newest = makeTarget({{5, 9}}, /*dead_rows=*/{9});
    auto older = makeTarget({{5, 1}});
    ASSERT_NE(newest, nullptr);
    ASSERT_NE(older, nullptr);
    auto probe = probeOver({newest, older});

    auto r = probeKey(probe, 5);
    EXPECT_EQ(r.outcome, ProbeOutcome::FOUND_LIVE);
    EXPECT_EQ(r.row_number, 1u);
}

TEST_F(UniqueKeyProbeTest, AllOccurrencesDeadAcrossParts)
{
    auto newest = makeTarget({{5, 9}}, /*dead_rows=*/{9});
    auto older = makeTarget({{5, 1}}, /*dead_rows=*/{1});
    ASSERT_NE(newest, nullptr);
    ASSERT_NE(older, nullptr);
    auto probe = probeOver({newest, older});
    EXPECT_EQ(probeKey(probe, 5).outcome, ProbeOutcome::FOUND_ALL_DEAD);
}

TEST_F(UniqueKeyProbeTest, NullTargetsSkipped)
{
    auto t = makeTarget({{5, 2}});
    ASSERT_NE(t, nullptr);
    auto probe = probeOver({nullptr, t, nullptr});
    auto r = probeKey(probe, 5);
    EXPECT_EQ(r.outcome, ProbeOutcome::FOUND_LIVE);
    EXPECT_EQ(r.row_number, 2u);
}

TEST_F(UniqueKeyProbeTest, ProbeBatchMixedOutcomes)
{
    auto t = makeTarget({{1, 10}, {2, 20}, {3, 30}}, /*dead_rows=*/{20});
    ASSERT_NE(t, nullptr);
    auto probe = probeOver({t});

    std::vector<UInt64> keys{1, 2, 99};
    auto batch = probe.probeBatch(makeKeyBlock(keys), "p0");
    ASSERT_EQ(batch.size(), keys.size());
    EXPECT_EQ(batch[0].outcome, ProbeOutcome::FOUND_LIVE);
    EXPECT_EQ(batch[0].row_number, 10u);
    EXPECT_EQ(batch[1].outcome, ProbeOutcome::FOUND_ALL_DEAD);
    EXPECT_EQ(batch[2].outcome, ProbeOutcome::NOT_FOUND);
}

/// ---------- SST backend specifics ----------

TEST_F(UniqueKeyProbeTest, OpenMissingFileThrows)
{
    EXPECT_ANY_THROW(openSSTReaderFromPath((base / "does_not_exist.sst").string()));
}

TEST_F(UniqueKeyProbeTest, FindRowIndexBatchHitsExactKeyOnly)
{
    auto t = makeTarget({{10, 0}, {30, 2}});
    ASSERT_NE(t, nullptr);

    /// 20 lies between stored keys 10 and 30 — Seek lands on 30 but the exact
    /// compare must reject it.
    const String e20 = encodeKey(20);
    const String e30 = encodeKey(30);
    std::vector<std::string_view> views{
        {e20.data(), e20.size()}, {e30.data(), e30.size()}};
    std::vector<std::optional<UInt64>> out;
    t->findRowIndexBatch(views, out);

    ASSERT_EQ(out.size(), 2u);
    EXPECT_FALSE(out[0].has_value());
    EXPECT_EQ(out[1], std::optional<UInt64>(2));
}

TEST_F(UniqueKeyProbeTest, InvalidReaderHandleFailsClosed)
{
    /// A target whose SST cannot be read must fail closed: `findRowIndexBatch`
    /// throws rather than reporting misses, and the driver propagates the throw
    /// instead of reducing it to NOT_FOUND (which would risk a duplicate key).
    auto target = std::make_shared<SSTProbeTargetPart>(
        /*part=*/nullptr, std::make_shared<DeleteBitmap>(), SSTReaderHandle{});

    const String e = encodeKey(1);
    std::vector<std::string_view> views{{e.data(), e.size()}};
    std::vector<std::optional<UInt64>> out;
    EXPECT_ANY_THROW(target->findRowIndexBatch(views, out));

    auto probe = probeOver({target});
    EXPECT_ANY_THROW(probe.probeBatch(makeKeyBlock({1}), "p0"));
}

/// ---------- factory switch ----------

TEST_F(UniqueKeyProbeTest, FactoryBuildsWorkingProbe)
{
    auto t = makeTarget({{7, 4}});
    ASSERT_NE(t, nullptr);
    ProbeTargetsSnapshot snapshot{t};
    auto supplier = [snapshot](const String &) { return snapshot; };

    for (auto impl : {UniqueKeyProbeImplementation::Auto, UniqueKeyProbeImplementation::Simple})
    {
        auto probe = makeUniqueKeyProbe(impl, supplier, Names{"key"}, MAX_ENC);
        ASSERT_NE(probe, nullptr);
        EXPECT_NE(dynamic_cast<UniqueKeyProbeSimple *>(probe.get()), nullptr);
        EXPECT_EQ(probeKey(*probe, 7).row_number, 4u);
    }
}

#endif
