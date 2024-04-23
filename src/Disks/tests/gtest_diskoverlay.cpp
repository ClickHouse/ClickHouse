#include <filesystem>
#include <Disks/DiskLocal.h>
#include <Disks/DiskOverlay.h>
#include <Disks/IDisk.h>
#include <Disks/ObjectStorages/MetadataStorageFromDisk.h>
#include <gtest/gtest.h>
#include <Disks/ObjectStorages/IObjectStorage_fwd.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include "Core/Defines.h"
#include "Disks/WriteMode.h"

using DB::DiskPtr, DB::MetadataStoragePtr;

DB::DiskPtr createLocalDisk(const std::string & path)
{
    fs::create_directory(path);
    return std::make_shared<DB::DiskLocal>("local_disk", path);
}

DB::MetadataStoragePtr createLocalDiskMetaData(const std::string & path)
{
    return std::make_shared<DB::MetadataStorageFromDisk>(createLocalDisk(path), "...");
}

class OverlayTest : public testing::Test {
public:
    DiskPtr base, diff, over;
    MetadataStoragePtr meta, tr_meta;

    void SetUp() override {
        fs::create_directory("/home/ubuntu/tmp/test_overlay");

        base = createLocalDisk("/home/ubuntu/tmp/test_overlay/base");
        diff = createLocalDisk("/home/ubuntu/tmp/test_overlay/over");

        meta = createLocalDiskMetaData("/home/ubuntu/tmp/test_overlay/meta");
        tr_meta = createLocalDiskMetaData("/home/ubuntu/tmp/test_overlay/tr_meta");

        over = std::make_shared<DB::DiskOverlay>("disk_overlay", base, diff, meta, tr_meta);
    }

    void TearDown() override {
        fs::remove_all("/home/ubuntu/tmp/test_overlay");
    }
};

TEST_F(OverlayTest, createRemoveFile)
{
    base->createFile("file.txt");
    EXPECT_EQ(over->exists("file.txt"), true);

    over->removeFile("file.txt");
    EXPECT_EQ(over->exists("file.txt"), false);

    over->createFile("file.txt");
    EXPECT_EQ(over->exists("file.txt"), true);

    over->removeFile("file.txt");
    EXPECT_EQ(over->exists("file.txt"), false);
}

TEST_F(OverlayTest, listFiles)
{
    base->createDirectory("folder");
    base->createFile("folder/file1.txt");
    
    over->createFile("folder/file2.txt");

    std::vector<String> paths, corr({"file1.txt", "file2.txt"});
    over->listFiles("folder", paths);

    std::sort(paths.begin(), paths.end());
    EXPECT_EQ(paths, corr);

    over->writeFile("folder/file1.txt", DB::DBMS_DEFAULT_BUFFER_SIZE, DB::WriteMode::Append);
    paths.clear();
    over->listFiles("folder", paths);

    std::sort(paths.begin(), paths.end());
    EXPECT_EQ(paths, corr);
}

TEST_F(OverlayTest, moveFile)
{
    base->createFile("file1.txt");
    over->moveFile("file1.txt", "file2.txt");

    std::vector<String> paths, corr({"file2.txt"});
    over->listFiles("", paths);

    std::sort(paths.begin(), paths.end());
    EXPECT_EQ(paths, corr);

    over->createFile("file1.txt");
    paths.clear();
    corr = {"file1.txt", "file2.txt"};
    over->listFiles("", paths);

    std::sort(paths.begin(), paths.end());
    EXPECT_EQ(paths, corr);
}
