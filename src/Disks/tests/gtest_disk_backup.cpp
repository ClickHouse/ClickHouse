#include <filesystem>
#include <fstream>
#include <memory>
#include <Disks/DiskLocal.h>
#include <Disks/DiskBackup.h>
#include <Disks/IDisk.h>
#include <Disks/ObjectStorages/MetadataStorageFromDisk.h>
#include <gtest/gtest.h>
#include <Disks/ObjectStorages/IObjectStorage_fwd.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <Core/Defines.h>
#include <Disks/WriteMode.h>
#include "Disks/MetadataStorageFromBackupFile.h"
#include "base/types.h"

#include <iostream>

using DB::DiskPtr, DB::MetadataStoragePtr;


DB::DiskPtr createLocalDisk(const std::string & path)
{
    fs::create_directory(path);
    return std::make_shared<DB::DiskLocal>("local_disk", path);
}

class DiskBackupTest : public testing::Test {
public:
    DiskPtr delegate, backup;
    MetadataStoragePtr meta;

    void SetUp() override {
        fs::create_directories("tmp/test_backup");

        delegate = createLocalDisk("tmp/test_backup/delegate");
    }

    void TearDown() override {
        fs::remove_all("tmp/test_backup");
    }

    void createBackupMetadataFile(const char* data) const {
        Coordination::WriteBufferFromFile write_buffer("tmp/test_backup/.backup");

        write_buffer.write(data, strlen(data));
        write_buffer.finalize();
    }

    void createDiskBackup() {
        meta = std::make_shared<DB::MetadataStorageFromBackupFile>("tmp/test_backup/.backup");
        backup = std::make_shared<DB::DiskBackup>("disk_backup", delegate, meta);
    }
};

TEST_F(DiskBackupTest, checkFileExists)
{
    const char* data = 
    "<config>"
    "  <version>1</version>"
    "  <deduplicate_files>1</deduplicate_files>"
    "  <timestamp>2023-08-28 10:07:20</timestamp>"
    "  <uuid>b70413e4-f421-4b81-9069-5c72018699f7</uuid>"
    "  <contents>"
    "    <file>"
    "      <name>tmp/test_backup/delegate/test.txt</name>"
    "      <size>210</size>"
    "      <checksum>f62085ffac70227a8f3e7f11b8448f8d</checksum>"
    "    </file>"
    "  </contents>"
    "</config>";
    createBackupMetadataFile(data);
    createDiskBackup();

    delegate->createFile("test.txt");
    
    EXPECT_EQ(backup->exists("test.txt"), true);
}

TEST_F(DiskBackupTest, getObjectType)
{
    const char* data = 
    "<config>"
    "  <version>1</version>"
    "  <deduplicate_files>1</deduplicate_files>"
    "  <timestamp>2023-08-28 10:07:20</timestamp>"
    "  <uuid>b70413e4-f421-4b81-9069-5c72018699f7</uuid>"
    "  <contents>"
    "    <file>"
    "      <name>tmp/test_backup/delegate/test_folder/test.txt</name>"
    "      <size>210</size>"
    "      <checksum>f62085ffac70227a8f3e7f11b8448f8d</checksum>"
    "    </file>"
    "  </contents>"
    "</config>";
    createBackupMetadataFile(data);
    createDiskBackup();

    delegate->createDirectory("test_folder");
    delegate->createFile("test_folder/test.txt");
    
    EXPECT_EQ(backup->isFile("test_folder/test.txt"), true);
    EXPECT_EQ(backup->isDirectory("test_folder"), true);
}
