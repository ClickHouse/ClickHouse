#pragma once

#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>

#include <filesystem>
#include <memory>
#include <string>

namespace DB
{

/// A tempdir-backed part storage, for the unique-key gtests that need real sidecar files without a
/// part in any table's part set. The directory is unique per instance, so tests can run in parallel.
struct PartStorageFixture
{
    std::filesystem::path base_path;
    std::string part_dir;
    DiskPtr disk;
    VolumePtr volume;
    MutableDataPartStoragePtr storage;

    explicit PartStorageFixture(const std::string & name = "part_storage")
    {
        const auto unique_id
            = std::to_string(::getpid()) + "_" + std::to_string(reinterpret_cast<uintptr_t>(this));
        base_path = std::filesystem::temp_directory_path() / ("uk_gtest_" + name + "_" + unique_id);
        part_dir = "part";
        std::filesystem::create_directories(base_path / part_dir);

        disk = std::make_shared<DiskLocal>("test_disk_" + unique_id, base_path.string());
        volume = std::make_shared<SingleDiskVolume>("test_volume", disk);
        storage = std::make_shared<DataPartStorageOnDiskFull>(volume, /*root_path=*/"", part_dir);
    }

    ~PartStorageFixture()
    {
        std::error_code ec;
        std::filesystem::remove_all(base_path, ec);
    }

    std::filesystem::path partFile(const std::string & name) const { return base_path / part_dir / name; }
};

}
