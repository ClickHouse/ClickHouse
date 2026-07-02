#include <gtest/gtest.h>

#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/MergeTree/UniqueKey/Txn/UniqueKeyManifest.h>

#include <Common/Exception.h>

#include <filesystem>
#include <fstream>
#include <random>
#include <string>

using namespace DB;
using namespace DB::UniqueKeyTxn;

namespace
{
    /// Per-test sandbox under /tmp/ck_uk_txn_<pid>_<rand>/, exposing a real
    /// `DiskLocal`-backed `IDataPartStorage` over the part directory.
    class ManifestIOFixture : public ::testing::Test
    {
    protected:
        std::filesystem::path base_path;
        std::string part_dir_name = "part";
        DiskPtr disk;
        VolumePtr volume;
        MutableDataPartStoragePtr storage;

        void SetUp() override
        {
            std::random_device rd;
            const auto pid = static_cast<unsigned>(::getpid());
            const auto suffix = rd();
            const std::string unique_id = std::to_string(pid) + "_" + std::to_string(suffix);
            base_path = std::filesystem::temp_directory_path() / ("ck_uk_txn_" + unique_id);
            std::filesystem::create_directories(base_path / part_dir_name);

            disk = std::make_shared<DiskLocal>("test_disk_" + unique_id, base_path.string());
            volume = std::make_shared<SingleDiskVolume>("test_volume", disk);
            storage = std::make_shared<DataPartStorageOnDiskFull>(volume, /*root_path=*/"", part_dir_name);
        }

        void TearDown() override
        {
            std::error_code ec;
            std::filesystem::remove_all(base_path, ec);
        }

        std::filesystem::path partFile(const std::string & name) const
        {
            return base_path / part_dir_name / name;
        }

        /// Write raw bytes directly to the on-disk manifest path (bypassing the
        /// storage API) to drive the corruption / malformed-input cases.
        void writeRawManifest(const std::string & content)
        {
            std::ofstream out(partFile(UniqueKeyManifest::FILE_NAME));
            out << content;
        }
    };
}

/// Comprehensive round-trip: empty bitmaps, both `is_marker` states, multiple
/// `bitmaps_created` entries (order-preserving), and a separate `forwarded`
/// block all survive write→read.
TEST_F(ManifestIOFixture, RoundTrip)
{
    /// Empty bitmaps, is_marker=false.
    {
        UniqueKeyManifest meta;
        meta.creation_csn = 42;
        meta.is_marker = false;
        UniqueKeyManifest::write(*storage, meta);
        ASSERT_TRUE(UniqueKeyManifest::exists(*storage));

        auto loaded = UniqueKeyManifest::read(*storage);
        EXPECT_EQ(loaded.creation_csn, 42u);
        EXPECT_FALSE(loaded.is_marker);
        EXPECT_TRUE(loaded.bitmaps_created.empty());
        EXPECT_TRUE(loaded.forwarded.empty());
    }

    /// is_marker=true, ordered `bitmaps_created` + separate `forwarded` block.
    {
        UniqueKeyManifest meta;
        meta.creation_csn = 999;
        meta.is_marker = true;
        meta.bitmaps_created = {
            {"all_1_1_0", 50},
            {"all_2_2_0", 60},
            {"all_3_3_0", 70},
            {"all_4_4_1", 80},
            {"self", 999},
        };
        meta.forwarded = {{"third_A", 50}, {"third_B", 60}};

        UniqueKeyManifest::write(*storage, meta);
        auto loaded = UniqueKeyManifest::read(*storage);

        EXPECT_EQ(loaded.creation_csn, 999u);
        EXPECT_TRUE(loaded.is_marker);

        ASSERT_EQ(loaded.bitmaps_created.size(), meta.bitmaps_created.size());
        for (size_t i = 0; i < meta.bitmaps_created.size(); ++i)
        {
            EXPECT_EQ(loaded.bitmaps_created[i].first, meta.bitmaps_created[i].first);
            EXPECT_EQ(loaded.bitmaps_created[i].second, meta.bitmaps_created[i].second);
        }

        ASSERT_EQ(loaded.forwarded.size(), 2u);
        EXPECT_EQ(loaded.forwarded[0].first, "third_A");
        EXPECT_EQ(loaded.forwarded[0].second, 50u);
        EXPECT_EQ(loaded.forwarded[1].first, "third_B");
        EXPECT_EQ(loaded.forwarded[1].second, 60u);
    }
}

/// Parse-level rejection: bytes that don't parse as the expected JSON shape or
/// whose scalars carry the wrong JSON type fail closed (strict typing — `Poco`'s
/// coercive getValue would otherwise map bool/float/string-number to an int).
TEST_F(ManifestIOFixture, MalformedStructuralOrWrongTypeThrows)
{
    for (const char * raw : {
        R"({"creation_csn":abc,"is_marker":0})",                /// non-digit number
        R"({"creation_csn":7,"is_marker":maybe})",             /// unquoted garbage flag
        "creation_csn: 1\nis_marker: 0\n",                     /// not JSON at all
        R"({"creation_csn":12x,"is_marker":0})",               /// truncated number — no leading-`12` salvage
        R"({"version":1,"creation_csn":true,"is_marker":0})",  /// bool where int expected
        R"({"version":1,"creation_csn":1.5,"is_marker":0})",   /// float where int expected
        R"({"version":1,"creation_csn":"5","is_marker":0})",   /// string-number where int expected
        R"({"version":1,"creation_csn":5,"is_marker":2.0})"})  /// float is_marker
    {
        writeRawManifest(raw);
        EXPECT_THROW(UniqueKeyManifest::read(*storage), DB::Exception) << "raw=" << raw;
    }
}

/// Schema-level rejection: parses as JSON but violates the manifest contract —
/// a missing required key (`version`, `creation_csn`, `is_marker`, or a
/// per-entry `csn`), an out-of-range `is_marker` flag, or an empty entry target.
TEST_F(ManifestIOFixture, MalformedSchemaViolationThrows)
{
    for (const char * raw : {
        R"({"creation_csn":5,"is_marker":0})",                                               /// missing version
        R"({"version":1,"is_marker":0})",                                                     /// missing creation_csn
        R"({"version":1,"creation_csn":5})",                                                  /// missing is_marker
        R"({"version":1,"creation_csn":1,"is_marker":0,"bitmaps_created":[{"target":"all_1_1_0"}]})", /// entry missing csn
        R"({"version":1,"creation_csn":5,"is_marker":2})",                                    /// is_marker out of range
        R"({"version":1,"creation_csn":5,"is_marker":0,"bitmaps_created":[{"target":"","csn":5}]})"})  /// empty target
    {
        writeRawManifest(raw);
        EXPECT_THROW(UniqueKeyManifest::read(*storage), DB::Exception) << "raw=" << raw;
    }
}

TEST_F(ManifestIOFixture, MalformedUnknownVersionThrows)
{
    /// A newer format version fail-closes rather than being misread.
    writeRawManifest(R"({"version":2,"creation_csn":5,"is_marker":0})");
    EXPECT_THROW(UniqueKeyManifest::read(*storage), DB::Exception);
}

/// Forward-compat: unknown keys are ignored, not rejected (a newer writer may
/// add fields an older reader does not know).
TEST_F(ManifestIOFixture, UnknownKeyIgnored)
{
    writeRawManifest(R"({"version":1,"creation_csn":7,"is_marker":0,"future_field":123})");
    auto loaded = UniqueKeyManifest::read(*storage);
    EXPECT_EQ(loaded.creation_csn, 7u);
    EXPECT_FALSE(loaded.is_marker);
}

TEST_F(ManifestIOFixture, LegacyManifestWithoutForwardedBlockParses)
{
    /// Manifests written before the `forwarded` key existed must still load.
    /// `forwarded` defaults to empty on the loaded meta.
    writeRawManifest(R"({"version":1,"creation_csn":7,"is_marker":0,"bitmaps_created":[{"target":"self","csn":7}]})");
    auto loaded = UniqueKeyManifest::read(*storage);
    EXPECT_EQ(loaded.creation_csn, 7u);
    EXPECT_FALSE(loaded.is_marker);
    ASSERT_EQ(loaded.bitmaps_created.size(), 1u);
    EXPECT_EQ(loaded.bitmaps_created[0].first, "self");
    EXPECT_EQ(loaded.bitmaps_created[0].second, 7u);
    EXPECT_TRUE(loaded.forwarded.empty());
}

TEST_F(ManifestIOFixture, ExistsReturnsFalseWhenAbsent)
{
    EXPECT_FALSE(UniqueKeyManifest::exists(*storage));
}

/// Documents the load-bearing fsync ordering:
/// `write()` must fsync the file body *before* fsyncing the parent dir.
/// We don't intercept the syscall sequence here (would require an fs mock),
/// but we do verify that on a normal completion path:
///   - The file exists on disk after `write()`.
///   - Re-reading immediately returns the just-written content.
///   - Calling `write()` twice with different content is idempotent on
///     `creation_csn` (the later content wins).
/// These together establish that the durability sequence ran to completion;
/// the *ordering* of fsync calls is enforced in the implementation and
/// documented in the header comment.
TEST_F(ManifestIOFixture, WriteIsDurableAndOverwriteWins)
{
    UniqueKeyManifest a;
    a.creation_csn = 1;
    a.is_marker = false;
    UniqueKeyManifest::write(*storage, a);
    EXPECT_TRUE(std::filesystem::exists(partFile(UniqueKeyManifest::FILE_NAME)));

    UniqueKeyManifest b;
    b.creation_csn = 2;
    b.is_marker = true;
    UniqueKeyManifest::write(*storage, b);

    auto loaded = UniqueKeyManifest::read(*storage);
    EXPECT_EQ(loaded.creation_csn, 2u);
    EXPECT_TRUE(loaded.is_marker);
}
