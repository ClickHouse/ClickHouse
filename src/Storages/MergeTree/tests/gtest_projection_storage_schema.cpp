#include <gtest/gtest.h>

#include <base/defines.h>
#include <Common/Exception.h>
#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>

#include <filesystem>

using namespace DB;

/// A LOGICAL_ERROR aborts in debug/sanitizer builds but is thrown as an ordinary exception in
/// release builds - including the coverage build - because handle_error_code (Common/Exception.cpp)
/// only aborts when debug_or_sanitizer_build is set (nothing sets abort_on_logical_error in the unit
/// test binary). A single assertion type therefore cannot cover both, so branch on the build type
/// exactly as the runtime does. This keeps the check meaningful everywhere instead of passing in one
/// build family and failing in the other.
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
        sync_guard_paths.push_back(path);
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
    EXPECT_EQ(IDataPartStorage::Projection::dirNameType("all_1_1_0.p.proj"), IDataPartStorage::Projection::Status::Live);
    EXPECT_EQ(IDataPartStorage::Projection::dirNameType("all_1_1_0.p_1.tmp_proj"), IDataPartStorage::Projection::Status::Temp);
    EXPECT_EQ(IDataPartStorage::Projection::dirNameType("all_1_1_0"), IDataPartStorage::Projection::Status::None);
    EXPECT_EQ(IDataPartStorage::Projection::dirNameType("proj"), IDataPartStorage::Projection::Status::None);
    EXPECT_EQ(IDataPartStorage::Projection::dirNameType(""), IDataPartStorage::Projection::Status::None);
}

TEST(ProjectionDirNames, SiblingOwner)
{
    /// FLAT sibling: the owner is the prefix before the first dot.
    EXPECT_EQ(IDataPartStorage::Projection::owner("store/uuid", "all_1_1_0.p.proj"), "all_1_1_0");
    EXPECT_EQ(IDataPartStorage::Projection::owner("store/uuid", "all_1_1_0.p.tmp_proj"), "all_1_1_0");
    /// Projection names may contain dots; the owner is everything before the first one.
    EXPECT_EQ(IDataPartStorage::Projection::owner("store/uuid", "all_1_1_0.my.p.proj"), "all_1_1_0");
    /// NESTED child: the owner is the basename of the root the projection dir lives in.
    EXPECT_EQ(IDataPartStorage::Projection::owner("store/uuid/all_1_1_0", "p.proj"), "all_1_1_0");
    EXPECT_EQ(IDataPartStorage::Projection::owner("store/uuid/all_1_1_0/", "p.tmp_proj"), "all_1_1_0");
    /// Not a projection dir at all.
    EXPECT_EQ(IDataPartStorage::Projection::owner("store/uuid", "all_1_1_0"), "");
    /// Owner equality distinguishes "part_1" from "part_10".
    EXPECT_EQ(IDataPartStorage::Projection::owner("store/uuid", "part_10.p.proj"), "part_10");
    EXPECT_NE(IDataPartStorage::Projection::owner("store/uuid", "part_10.p.proj"), "part_1");
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
    fixture.storage->setProjectionStorageFormat(IDataPartStorage::ProjectionStorageFormat::FLAT);
    fixture.storage->setProjections({});
    fixture.storage->createProjection("p_1.tmp_proj");
    fixture.storage->renameProjection(fixture.storage->getProjection("p_1.tmp_proj"), "p.proj", /*fsync=*/ false);

    /// Only temporary projections may be removed individually; removing a live one is a logical error.
    EXPECT_LOGICAL_ERROR(fixture.storage->removeProjection(fixture.storage->getProjection("p.proj")));
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
    fixture.storage->setProjectionStorageFormat(IDataPartStorage::ProjectionStorageFormat::FLAT);
    fixture.storage->setProjections({});

    fixture.storage->createProjection("p_1.tmp_proj");
    ASSERT_TRUE(fixture.storage->hasProjection("p_1.tmp_proj"));
    ASSERT_TRUE(std::filesystem::exists(fixture.base_path / "all_1_1_0.p_1.tmp_proj"));

    fixture.storage->renameProjection(fixture.storage->getProjection("p_1.tmp_proj"), "p.proj", /*fsync=*/ false);
    EXPECT_FALSE(fixture.storage->hasProjection("p_1.tmp_proj"));
    ASSERT_TRUE(fixture.storage->hasProjection("p.proj"));
    EXPECT_FALSE(std::filesystem::exists(fixture.base_path / "all_1_1_0.p_1.tmp_proj"));
    ASSERT_TRUE(std::filesystem::exists(fixture.base_path / "all_1_1_0.p.proj"));

    fixture.storage->createProjection("q.tmp_proj");
    fixture.storage->removeProjection(fixture.storage->getProjection("q.tmp_proj"));
    EXPECT_FALSE(fixture.storage->hasProjection("q.tmp_proj"));
    EXPECT_FALSE(std::filesystem::exists(fixture.base_path / "all_1_1_0.q.tmp_proj"));

    /// The schema survives and matches disk truth.
    auto detected = fixture.storage->detectProjections();
    ASSERT_EQ(detected.size(), 1u);
    EXPECT_TRUE(detected.contains("p.proj"));
    EXPECT_EQ(detected.at("p.proj").format, IDataPartStorage::ProjectionStorageFormat::FLAT);
}

TEST(ProjectionStorageSchema, DetectProjectionsBothLayouts)
{
    PartStorageFixture fixture;
    std::filesystem::create_directories(fixture.base_path / fixture.part_dir / "nested.proj");
    std::filesystem::create_directories(fixture.base_path / "all_1_1_0.flat.proj");
    std::filesystem::create_directories(fixture.base_path / "all_1_1_0.tmp.tmp_proj");
    std::filesystem::create_directories(fixture.base_path / "all_1_1_1.other.proj");   /// different owner

    auto detected = fixture.storage->detectProjections();
    ASSERT_EQ(detected.size(), 3u);
    EXPECT_EQ(detected.at("nested.proj").format, IDataPartStorage::ProjectionStorageFormat::LEGACY_NESTED);
    EXPECT_EQ(detected.at("flat.proj").format, IDataPartStorage::ProjectionStorageFormat::FLAT);
    EXPECT_TRUE(detected.at("tmp.tmp_proj").is_temp);
}

TEST(ProjectionStorageSchema, RenameFsyncsSiblingNamespace)
{
    PartStorageFixture fixture;
    fixture.storage->setProjectionStorageFormat(IDataPartStorage::ProjectionStorageFormat::FLAT);
    fixture.storage->setProjections({});
    fixture.storage->createProjection("p.proj");

    /// Publish (parent moves last): siblings-before-commit sync on the root, the moved dir, the root again.
    fixture.disk->sync_guard_paths.clear();
    fixture.storage->rename(/*new_root_path=*/ "", /*new_part_dir=*/ "all_1_1_1", /*log=*/ nullptr,
                            /*remove_new_dir_if_exists=*/ false, /*fsync_part_dir=*/ true);
    EXPECT_EQ(fixture.disk->sync_guard_paths, (Strings{"", "all_1_1_1", ""}));

    /// Without the setting nothing is synced.
    fixture.disk->sync_guard_paths.clear();
    fixture.storage->rename("", "all_1_1_2", nullptr, false, /*fsync_part_dir=*/ false);
    EXPECT_TRUE(fixture.disk->sync_guard_paths.empty());

    /// A sibling-less rename keeps the historical single sync on the moved dir
    /// (02361_fsync_profile_events pins the event count).
    PartStorageFixture plain;
    plain.storage->setProjectionStorageFormat(IDataPartStorage::ProjectionStorageFormat::FLAT);
    plain.storage->setProjections({});
    plain.disk->sync_guard_paths.clear();
    plain.storage->rename("", "all_2_2_0", nullptr, false, /*fsync_part_dir=*/ true);
    EXPECT_EQ(plain.disk->sync_guard_paths, (Strings{"all_2_2_0"}));
}

TEST(ProjectionStorageSchema, RenameProjectionFsyncsEnclosingDir)
{
    PartStorageFixture fixture;
    fixture.storage->setProjectionStorageFormat(IDataPartStorage::ProjectionStorageFormat::FLAT);
    fixture.storage->setProjections({});

    /// FLAT: the rename entry lives in the parts root.
    fixture.storage->createProjection("p_1.tmp_proj");
    fixture.disk->sync_guard_paths.clear();
    fixture.storage->renameProjection(fixture.storage->getProjection("p_1.tmp_proj"), "p.proj", /*fsync=*/ true);
    EXPECT_EQ(fixture.disk->sync_guard_paths, (Strings{""}));

    /// NESTED: the rename entry lives in the part dir.
    fixture.storage->setProjectionStorageFormat(IDataPartStorage::ProjectionStorageFormat::LEGACY_NESTED);
    fixture.storage->createProjection("q_1.tmp_proj");
    fixture.disk->sync_guard_paths.clear();
    fixture.storage->renameProjection(fixture.storage->getProjection("q_1.tmp_proj"), "q.proj", /*fsync=*/ true);
    EXPECT_EQ(fixture.disk->sync_guard_paths, (Strings{"all_1_1_0"}));

    /// fsync=false requests no guards.
    fixture.disk->sync_guard_paths.clear();
    fixture.storage->renameProjection(fixture.storage->getProjection("q.proj"), "q_1.tmp_proj", /*fsync=*/ false);
    EXPECT_TRUE(fixture.disk->sync_guard_paths.empty());
}

TEST(ProjectionStorageSchema, ProbeProjectionsBothLayouts)
{
    PartStorageFixture fixture;
    std::filesystem::create_directories(fixture.base_path / fixture.part_dir / "nested.proj");
    std::filesystem::create_directories(fixture.base_path / "all_1_1_0.flat.proj");
    std::filesystem::create_directories(fixture.base_path / "all_1_1_0.shadowed.proj");
    std::filesystem::create_directories(fixture.base_path / fixture.part_dir / "shadowed.proj");

    auto probed = fixture.storage->probeProjections({"nested.proj", "flat.proj", "shadowed.proj", "absent.proj"});
    ASSERT_EQ(probed.size(), 3u);
    EXPECT_EQ(probed.at("nested.proj").format, IDataPartStorage::ProjectionStorageFormat::LEGACY_NESTED);
    EXPECT_EQ(probed.at("flat.proj").format, IDataPartStorage::ProjectionStorageFormat::FLAT);
    /// A nested child shadows a same-named flat sibling.
    EXPECT_EQ(probed.at("shadowed.proj").format, IDataPartStorage::ProjectionStorageFormat::LEGACY_NESTED);
    EXPECT_FALSE(probed.contains("absent.proj"));
}
