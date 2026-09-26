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
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDataPartBuilder.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/KeyDescription.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageMergeTree.h>

#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/Names.h>
#include <Core/SettingsEnums.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <IO/SharedThreadPools.h>
#include <Common/tests/gtest_global_context.h>

#include <rocksdb/env.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/sst_file_writer.h>
#include <rocksdb/table.h>

#include <algorithm>
#include <filesystem>
#include <memory>
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
        VectorWithMemoryTracking<String> out;
        UniqueKeyEncoding::encodeBlock(cols, /*permutation=*/nullptr, MAX_ENC, out);
        return out.at(0);
    }

    /// Write a single (encoded_key -> raw value) SST entry, bypassing
    /// `SSTIndexWriter` — used only to inject a malformed (non-4-byte) value the
    /// real writer cannot produce, for the corrupt-sidecar test.
    bool writeSSTRawValue(const String & path, const String & encoded_key, const String & value)
    {
        rocksdb::Options options;
        rocksdb::BlockBasedTableOptions tbl;
        tbl.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10.0));
        options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(tbl));
        rocksdb::SstFileWriter writer(rocksdb::EnvOptions(), options);
        if (!writer.Open(path).ok())
            return false;
        if (!writer.Put(rocksdb::Slice(encoded_key.data(), encoded_key.size()),
                        rocksdb::Slice(value.data(), value.size())).ok())
            return false;
        return writer.Finish().ok();
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

        /// The SST writer streams through the context's temporary storage. Set
        /// it only if no other fixture in this binary already did —
        /// `setTemporaryStoragePath` throws (LOGICAL_ERROR, aborts in Debug) if
        /// called twice, so two SST-using suites must guard on the shared state.
        if (!getContext().context->getSharedTempDataOnDisk())
        {
            auto shared_tmp = std::filesystem::temp_directory_path() / "ck_uk_probe_gtest_tmp";
            std::filesystem::create_directories(shared_tmp);
            getMutableContext().context->setTemporaryStoragePath(shared_tmp.string() + "/", 0);
        }
    }

    void TearDown() override
    {
        volume.reset();
        disk.reset();
        std::filesystem::remove_all(base);
    }

    /// Write `kv` to an SST via the real `SSTIndexWriter` and open a reader on it.
    SSTFileReaderPtr makeReader(const String & part_dir, std::vector<std::pair<UInt64, UInt32>> kv)
    {
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
        /// This test reads the SST back directly, so the recorded checksum is unused.
        MergeTreeDataPartChecksums sst_checksums;
        writer.finish(sst_checksums, /*fsync=*/false);

        return openSSTReaderFromStorage(storage, SSTIndexWriter::FILE_NAME, ReadSettings{});
    }

    /// Build an SST-backed probe target from `(key -> row)` entries (written via
    /// the real `SSTIndexWriter`), with `dead_rows` marked dead in the part's
    /// delete bitmap.
    ProbeTargetPartPtr makeTarget(
        std::vector<std::pair<UInt64, UInt32>> kv, std::vector<UInt64> dead_rows = {})
    {
        auto reader = makeReader("part_" + std::to_string(counter++), std::move(kv));
        auto bitmap = std::make_shared<DeleteBitmap>();
        for (UInt64 r : dead_rows)
            bitmap->add(r);
        return std::make_shared<SSTProbeTargetPart>(/*part=*/nullptr, bitmap, std::move(reader));
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

/// Newest-first resolution stops at the newest LIVE copy. The older copy is dead here on purpose:
/// two live copies of one key violate the single-live-part invariant, which
/// `UniqueKeyProbeSimple` throws on under `probe_validates_single_live_part` (debug), so a test
/// built that way asserts a state a unique-key table cannot be in.
TEST_F(UniqueKeyProbeTest, NewestLiveWinsOverOlderDead)
{
#ifdef NDEBUG
    auto newest = makeTarget({{5, 9}});
    auto older = makeTarget({{5, 1}}, /*dead_rows=*/{1});
    ASSERT_NE(newest, nullptr);
    ASSERT_NE(older, nullptr);
    auto probe = probeOver({newest, older}); /// newest-first

    auto r = probeKey(probe, 5);
    EXPECT_EQ(r.outcome, ProbeOutcome::FOUND_LIVE);
    EXPECT_EQ(r.row_number, 9u);
#else
    /// Two live copies of one key violate the single-live-part invariant that
    /// debug builds validate (and abort on) - newest-wins is release behavior.
    GTEST_SKIP() << "debug builds enforce the single-live-part invariant";
#endif
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

/// Results must map back to the input row order regardless of input key order.
TEST_F(UniqueKeyProbeTest, ProbeBatchMapsUnsortedRowsBack)
{
    auto t = makeTarget({{1, 10}, {2, 20}, {50, 30}});
    ASSERT_NE(t, nullptr);
    auto probe = probeOver({t});

    std::vector<UInt64> keys{99, 1, 50, 2}; /// not in key order
    auto batch = probe.probeBatch(makeKeyBlock(keys), "p0");
    ASSERT_EQ(batch.size(), keys.size());
    EXPECT_EQ(batch[0].outcome, ProbeOutcome::NOT_FOUND);
    EXPECT_EQ(batch[1].outcome, ProbeOutcome::FOUND_LIVE);
    EXPECT_EQ(batch[1].row_number, 10u);
    EXPECT_EQ(batch[2].outcome, ProbeOutcome::FOUND_LIVE);
    EXPECT_EQ(batch[2].row_number, 30u);
    EXPECT_EQ(batch[3].outcome, ProbeOutcome::FOUND_LIVE);
    EXPECT_EQ(batch[3].row_number, 20u);
}

/// ---------- SST backend specifics ----------

TEST_F(UniqueKeyProbeTest, OpenMissingFileThrows)
{
    auto missing_storage = std::make_shared<DataPartStorageOnDiskFull>(volume, "", "does_not_exist");
    EXPECT_ANY_THROW(openSSTReaderFromStorage(missing_storage, "does_not_exist.sst", ReadSettings{}));
}

TEST_F(UniqueKeyProbeTest, FindRowIndexBatchHitsExactKeyOnly)
{
    auto t = makeTarget({{10, 0}, {30, 2}});
    ASSERT_NE(t, nullptr);

    /// 20 lies between stored keys 10 and 30 and must simply miss.
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

/// Keys beyond the SST's min/max keys simply miss via the lookup; boundary
/// keys themselves hit.
TEST_F(UniqueKeyProbeTest, FindRowIndexBatchOutOfRangeKeysMiss)
{
    auto t = makeTarget({{10, 0}, {20, 1}, {30, 2}});
    ASSERT_NE(t, nullptr);

    const String e5 = encodeKey(5);
    const String e10 = encodeKey(10);
    const String e30 = encodeKey(30);
    const String e99 = encodeKey(99);
    std::vector<std::string_view> views{
        {e5.data(), e5.size()}, {e10.data(), e10.size()},
        {e30.data(), e30.size()}, {e99.data(), e99.size()}};

    std::vector<std::optional<UInt64>> out;
    t->findRowIndexBatch(views, out);

    ASSERT_EQ(out.size(), 4u);
    EXPECT_FALSE(out[0].has_value()) << "below-min key must miss";
    EXPECT_EQ(out[1], std::optional<UInt64>(0)) << "min boundary key hits";
    EXPECT_EQ(out[2], std::optional<UInt64>(2)) << "max boundary key hits";
    EXPECT_FALSE(out[3].has_value()) << "above-max key must miss";
}

/// A batch past the 32-key `MultiGet` cap is chunked internally; every key
/// still maps to its own row, misses interleaved. Input order is arbitrary
/// (descending here) - results must stay aligned with it.
TEST_F(UniqueKeyProbeTest, FindRowIndexBatchExceedsMultiGetBatchLimit)
{
    constexpr UInt64 N = 100; /// past the 32-key limit
    std::vector<std::pair<UInt64, UInt32>> kv;
    kv.reserve(N);
    for (UInt64 i = 0; i < N; ++i)
        kv.emplace_back(100 + i * 10, static_cast<UInt32>(i));
    auto t = makeTarget(std::move(kv));
    ASSERT_NE(t, nullptr);

    /// Descending input: above-max miss first, hits and in-range misses
    /// descending, below-min miss last.
    std::vector<String> storage;
    std::vector<std::optional<UInt64>> expected;
    storage.reserve(2 * N + 2);
    expected.reserve(2 * N + 2);
    storage.push_back(encodeKey(2000)); /// above max, misses
    expected.push_back(std::nullopt);
    for (UInt64 i = N; i-- > 0;)
    {
        storage.push_back(encodeKey(100 + i * 10 + 5));
        expected.push_back(std::nullopt);
        storage.push_back(encodeKey(100 + i * 10));
        expected.emplace_back(i);
    }
    storage.push_back(encodeKey(50));   /// below min, misses
    expected.push_back(std::nullopt);

    std::vector<std::string_view> views;
    views.reserve(storage.size());
    for (const auto & e : storage)
        views.emplace_back(e.data(), e.size());

    std::vector<std::optional<UInt64>> out;
    t->findRowIndexBatch(views, out);

    ASSERT_EQ(out.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i)
        EXPECT_EQ(out[i], expected[i]) << "mismatch at batch row " << i;
}

/// `BlockBasedTable::MultiGet` asserts on an empty range - an empty batch
/// must short-circuit as a no-op instead.
TEST_F(UniqueKeyProbeTest, MultiGetEmptyBatchIsNoOp)
{
    auto reader = makeReader("multiget_empty_part", {{1, 0}});

    std::vector<String> values{"stale"};
    std::vector<rocksdb::Slice> keys;
    const auto statuses = reader->multiGet(keys, values);
    EXPECT_TRUE(statuses.empty());
    EXPECT_TRUE(values.empty());
}

TEST_F(UniqueKeyProbeTest, InvalidReaderHandleFailsClosed)
{
    /// A target whose SST cannot be read must fail closed: `findRowIndexBatch`
    /// throws rather than reporting misses, and the driver propagates the throw
    /// instead of reducing it to NOT_FOUND (which would risk a duplicate key).
    auto target = std::make_shared<SSTProbeTargetPart>(
        /*part=*/nullptr, std::make_shared<DeleteBitmap>(), nullptr);

    const String e = encodeKey(1);
    std::vector<std::string_view> views{{e.data(), e.size()}};
    std::vector<std::optional<UInt64>> out;
    EXPECT_ANY_THROW(target->findRowIndexBatch(views, out));

    auto probe = probeOver({target});
    EXPECT_ANY_THROW(probe.probeBatch(makeKeyBlock({1}), "p0"));
}

TEST_F(UniqueKeyProbeTest, CorruptValueSizeFailsClosed)
{
    /// A value whose size isn't exactly 4 bytes is a corrupt/incompatible
    /// sidecar - decoding a prefix could point at the wrong row, so the probe
    /// must throw rather than return a (wrong) hit or a miss.
    const String part_dir = "corrupt_value_part";
    std::filesystem::create_directories(base / part_dir);
    auto storage = std::make_shared<DataPartStorageOnDiskFull>(volume, "", part_dir);
    ASSERT_TRUE(writeSSTRawValue(
        (base / part_dir / SSTIndexWriter::FILE_NAME).string(), encodeKey(1), String(5, '\0'))); /// 5-byte value

    auto reader = openSSTReaderFromStorage(storage, SSTIndexWriter::FILE_NAME, ReadSettings{});
    SSTProbeTargetPart target(/*part=*/nullptr, std::make_shared<DeleteBitmap>(), std::move(reader));

    const String e = encodeKey(1);
    std::vector<std::string_view> views{{e.data(), e.size()}};
    std::vector<std::optional<UInt64>> out;
    EXPECT_ANY_THROW(target.findRowIndexBatch(views, out));
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

/// ---------- decoded-row bounds check ----------

/// A valid 4-byte SST value that decodes to a row_number >= the part's
/// rows_count is a corrupt/incompatible sidecar (it would index past the
/// part's columns). With a non-null underlying part, `findRowIndexBatch` must
/// throw CORRUPTED_DATA rather than hand back an out-of-range `_part_offset`.
TEST_F(UniqueKeyProbeTest, DecodedRowOutOfPartBoundsThrows)
{
    /// Minimal MergeTree storage so we can build a part with a controlled
    /// rows_count (pattern from gtest_trivial_count_null_snapshot).
    getActivePartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
    getOutdatedPartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
    getUnexpectedPartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
    getPartsCleaningThreadPool().initializeWithDefaultSettingsIfNotInitialized();

    auto context = Context::createCopy(getContext().context);

    StorageInMemoryMetadata metadata;
    ColumnsDescription columns;
    columns.add(ColumnDescription("key", std::make_shared<DataTypeUInt64>()));
    metadata.setColumns(columns);
    auto order_by_ast = makeASTFunction("tuple");
    metadata.sorting_key = KeyDescription::getKeyFromAST(order_by_ast, metadata.columns, {}, context);
    metadata.primary_key = KeyDescription::getKeyFromAST(order_by_ast, metadata.columns, {}, context);
    metadata.primary_key.definition_ast = nullptr;
    metadata.partition_key = KeyDescription::getKeyFromAST(nullptr, metadata.columns, {}, context);

    auto storage_settings = std::make_unique<MergeTreeSettings>(context->getMergeTreeSettings());
    auto storage = std::make_shared<StorageMergeTree>(
        StorageID("test_db", "uk_probe_bounds"),
        "store/uk_probe_bounds/",
        metadata,
        LoadingStrictnessLevel::ATTACH,
        context,
        /*date_column_name=*/"",
        MergeTreeData::MergingParams{},
        std::move(storage_settings));

    const String part_dir = "bounds_part";
    std::filesystem::create_directories(base / part_dir);
    constexpr UInt32 PART_ROWS = 3;
    /// The directory is pre-created above, so `OpenExisting` matches what this test was written against.
    auto part = MergeTreeDataPartBuilder(*storage, "all_1_1_0", volume, "", part_dir, context->getReadSettings(), PartDirIntent::OpenExisting)
                    .withBytesAndRows(0, PART_ROWS, 0)
                    .build();
    part->rows_count = PART_ROWS;

    /// Seed an SST whose single entry's value decodes to row 5 — past the
    /// part's 3 rows. `writeFromBlock` always writes valid row numbers, so
    /// inject the malformed value directly via the raw SST writer.
    auto part_storage = std::make_shared<DataPartStorageOnDiskFull>(volume, "", part_dir);
    const String sst_path = part_storage->getFullPath() + "/" + SSTIndexWriter::FILE_NAME;
    const String out_of_range_value{'\0', '\0', '\0', '\x05'}; /// BE 5
    ASSERT_TRUE(writeSSTRawValue(sst_path, encodeKey(42), out_of_range_value));

    auto reader = openSSTReaderFromStorage(part_storage, SSTIndexWriter::FILE_NAME, ReadSettings{});
    SSTProbeTargetPart target(part.get(), std::make_shared<DeleteBitmap>(), std::move(reader));

    const String e = encodeKey(42);
    std::vector<std::string_view> views{{e.data(), e.size()}};
    std::vector<std::optional<UInt64>> out;
    EXPECT_ANY_THROW(target.findRowIndexBatch(views, out));

    storage->flushAndShutdown();
}

#endif
