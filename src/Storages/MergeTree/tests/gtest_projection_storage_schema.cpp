#include <gtest/gtest.h>

#include <base/defines.h>
#include <Common/Exception.h>
#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>

#include <filesystem>

using namespace DB;

/// LOGICAL_ERROR aborts in debug/sanitizer builds but throws in release (handle_error_code only aborts
/// when debug_or_sanitizer_build is set); branch the assertion on build type to match the runtime.
#ifdef DEBUG_OR_SANITIZER_BUILD
#    define EXPECT_LOGICAL_ERROR(statement) EXPECT_DEATH(statement, ".*")
#else
#    define EXPECT_LOGICAL_ERROR(statement) EXPECT_THROW(statement, Exception)
#endif

namespace
{

/// Records which directories get a sync guard requested; the fsync tests assert guard placement.
struct SyncGuardRecordingDisk : DiskLocal
{
    using DiskLocal::DiskLocal;
    mutable Strings sync_guard_paths;

    SyncGuardPtr getDirectorySyncGuard(const String & path) const override
    {
        /// Normalize the path-join trailing slash so assertions pin the directory, not its string form.
        sync_guard_paths.push_back(path.ends_with('/') ? path.substr(0, path.size() - 1) : path);
        return DiskLocal::getDirectorySyncGuard(path);
    }
};

struct PartStorageFixture
{
    std::filesystem::path base_path;
    std::string part_dir;
    std::shared_ptr<SyncGuardRecordingDisk> disk;
    VolumePtr volume;
    std::shared_ptr<DataPartStorageOnDiskFull> storage;

    PartStorageFixture()
    {
        auto base = std::filesystem::temp_directory_path();
        auto unique_id = std::to_string(::getpid()) + "_" + std::to_string(reinterpret_cast<uintptr_t>(this));
        base_path = base / ("projection_schema_gtest_" + unique_id);
        std::filesystem::create_directories(base_path);
        part_dir = "all_1_1_0";
        std::filesystem::create_directories(base_path / part_dir);

        disk = std::make_shared<SyncGuardRecordingDisk>("test_disk_" + unique_id, base_path.string());
        volume = std::make_shared<SingleDiskVolume>("test_volume", disk);
        storage = std::make_shared<DataPartStorageOnDiskFull>(volume, /*root_path=*/ "", part_dir);
    }

    ~PartStorageFixture()
    {
        std::error_code ec;
        std::filesystem::remove_all(base_path, ec);
    }
};

}

TEST(ProjectionDirNames, Classification)
{
    EXPECT_EQ(IDataPartStorage::Projection::dirNameType("p.proj"), IDataPartStorage::Projection::Status::Live);
    EXPECT_EQ(IDataPartStorage::Projection::dirNameType("p.tmp_proj"), IDataPartStorage::Projection::Status::Temp);
    EXPECT_EQ(IDataPartStorage::Projection::dirNameType("all_1_1_0"), IDataPartStorage::Projection::Status::None);
    EXPECT_EQ(IDataPartStorage::Projection::dirNameType("proj"), IDataPartStorage::Projection::Status::None);
    EXPECT_EQ(IDataPartStorage::Projection::dirNameType(""), IDataPartStorage::Projection::Status::None);
}

TEST(ProjectionStorageSchemaDeathTest, UnseededReadsAbort)
{
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    PartStorageFixture fixture;
    EXPECT_LOGICAL_ERROR(fixture.storage->getProjections());
    EXPECT_LOGICAL_ERROR(fixture.storage->getProjection("p.proj"));
    EXPECT_LOGICAL_ERROR(fixture.storage->getProjectionStorage("p.proj", true));
}

TEST(ProjectionStorageSchemaDeathTest, RemoveLiveProjectionAborts)
{
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    PartStorageFixture fixture;
    fixture.storage->setProjections({});
    fixture.storage->createProjection("p_1.tmp_proj", IDataPartStorage::ProjectionStorageFormat::LEGACY_NESTED);
    fixture.storage->renameProjection(fixture.storage->getProjection("p_1.tmp_proj"), "p.proj", /*fsync=*/ false);

    /// Only temporary projections may be removed individually; removing a live one is a logical error.
    EXPECT_LOGICAL_ERROR(fixture.storage->removeProjection(fixture.storage->getProjection("p.proj")));
}

TEST(ProjectionStorageSchemaDeathTest, WriteAccessRequiresOwnedProjection)
{
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    PartStorageFixture fixture;
    fixture.storage->setProjections({});

    /// A placement that never went through createProjection does not unlock writable storage.
    EXPECT_LOGICAL_ERROR(fixture.storage->getProjectionStorageForWrite(fixture.storage->projectionPlacement("p.proj", IDataPartStorage::ProjectionStorageFormat::LEGACY_NESTED), true));

    /// The descriptor produced by createProjection does.
    auto placement = fixture.storage->createProjection("p.proj", IDataPartStorage::ProjectionStorageFormat::LEGACY_NESTED);
    EXPECT_NE(fixture.storage->getProjectionStorageForWrite(placement, true), nullptr);
}

TEST(ProjectionStorageSchema, EmptySchemaIsValid)
{
    PartStorageFixture fixture;
    fixture.storage->setProjections({});
    EXPECT_TRUE(fixture.storage->getProjections().empty());
    EXPECT_FALSE(fixture.storage->hasProjection("p.proj"));
}

TEST(ProjectionStorageSchema, CreateRenameRemoveMaintainCache)
{
    PartStorageFixture fixture;
    fixture.storage->setProjections({});

    fixture.storage->createProjection("p_1.tmp_proj", IDataPartStorage::ProjectionStorageFormat::LEGACY_NESTED);
    ASSERT_TRUE(fixture.storage->hasProjection("p_1.tmp_proj"));
    ASSERT_TRUE(std::filesystem::exists(fixture.base_path / "all_1_1_0" / "p_1.tmp_proj"));

    fixture.storage->renameProjection(fixture.storage->getProjection("p_1.tmp_proj"), "p.proj", /*fsync=*/ false);
    EXPECT_FALSE(fixture.storage->hasProjection("p_1.tmp_proj"));
    ASSERT_TRUE(fixture.storage->hasProjection("p.proj"));
    EXPECT_FALSE(std::filesystem::exists(fixture.base_path / "all_1_1_0" / "p_1.tmp_proj"));
    ASSERT_TRUE(std::filesystem::exists(fixture.base_path / "all_1_1_0" / "p.proj"));

    fixture.storage->createProjection("q.tmp_proj", IDataPartStorage::ProjectionStorageFormat::LEGACY_NESTED);
    fixture.storage->removeProjection(fixture.storage->getProjection("q.tmp_proj"));
    EXPECT_FALSE(fixture.storage->hasProjection("q.tmp_proj"));
    EXPECT_FALSE(std::filesystem::exists(fixture.base_path / "all_1_1_0" / "q.tmp_proj"));

    /// The schema survives and matches disk truth.
    auto detected = fixture.storage->detectProjections();
    ASSERT_EQ(detected.size(), 1u);
    EXPECT_TRUE(detected.contains("p.proj"));
    EXPECT_EQ(detected.at("p.proj").format, IDataPartStorage::ProjectionStorageFormat::LEGACY_NESTED);
}

TEST(ProjectionStorageSchema, DetectProjections)
{
    PartStorageFixture fixture;
    std::filesystem::create_directories(fixture.base_path / fixture.part_dir / "nested.proj");
    std::filesystem::create_directories(fixture.base_path / fixture.part_dir / "tmp.tmp_proj");
    std::filesystem::create_directories(fixture.base_path / fixture.part_dir / "not_a_projection");

    auto detected = fixture.storage->detectProjections();
    ASSERT_EQ(detected.size(), 2u);
    EXPECT_EQ(detected.at("nested.proj").format, IDataPartStorage::ProjectionStorageFormat::LEGACY_NESTED);
    EXPECT_TRUE(detected.at("tmp.tmp_proj").is_temp);
}

TEST(ProjectionStorageSchema, RenameKeepsHistoricalFsync)
{
    /// The historical single sync on the moved dir (02361_fsync_profile_events pins the event count).
    PartStorageFixture plain;
    plain.storage->setProjections({});
    plain.disk->sync_guard_paths.clear();
    plain.storage->rename("", "all_2_2_0", nullptr, false, /*fsync_part_dir=*/ true);
    EXPECT_EQ(plain.disk->sync_guard_paths, (Strings{"all_2_2_0"}));

    /// Without the setting nothing is synced.
    PartStorageFixture fixture;
    fixture.storage->setProjections({});
    fixture.disk->sync_guard_paths.clear();
    fixture.storage->rename("", "all_1_1_1", nullptr, false, /*fsync_part_dir=*/ false);
    EXPECT_TRUE(fixture.disk->sync_guard_paths.empty());
}

TEST(ProjectionStorageSchema, RenameProjectionFsyncsEnclosingDir)
{
    PartStorageFixture fixture;
    fixture.storage->setProjections({});

    /// The rename entry lives in the part dir.
    fixture.storage->createProjection("q_1.tmp_proj", IDataPartStorage::ProjectionStorageFormat::LEGACY_NESTED);
    fixture.disk->sync_guard_paths.clear();
    fixture.storage->renameProjection(fixture.storage->getProjection("q_1.tmp_proj"), "q.proj", /*fsync=*/ true);
    EXPECT_EQ(fixture.disk->sync_guard_paths, (Strings{"all_1_1_0"}));

    /// fsync=false requests no guards.
    fixture.disk->sync_guard_paths.clear();
    fixture.storage->renameProjection(fixture.storage->getProjection("q.proj"), "q_1.tmp_proj", /*fsync=*/ false);
    EXPECT_TRUE(fixture.disk->sync_guard_paths.empty());
}

TEST(ProjectionStorageSchema, ProbeProjections)
{
    PartStorageFixture fixture;
    std::filesystem::create_directories(fixture.base_path / fixture.part_dir / "nested.proj");
    std::filesystem::create_directories(fixture.base_path / fixture.part_dir / "other.proj");

    auto probed = fixture.storage->detectProjections({.candidates = Strings{"nested.proj", "other.proj", "absent.proj"}});
    ASSERT_EQ(probed.size(), 2u);
    EXPECT_EQ(probed.at("nested.proj").format, IDataPartStorage::ProjectionStorageFormat::LEGACY_NESTED);
    EXPECT_EQ(probed.at("other.proj").format, IDataPartStorage::ProjectionStorageFormat::LEGACY_NESTED);
    EXPECT_FALSE(probed.contains("absent.proj"));
}
