#include <gtest/gtest.h>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <memory>
#include <mutex>
#include <Core/ServerUUID.h>
#include <Disks/DiskLocal.h>
#include <Disks/ObjectStorages/MetadataStorageFromDisk.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <Common/ObjectStorageKey.h>
#include <Common/ThreadStatus.h>
#include <Common/getRandomASCIIString.h>
#include <Common/Exception.h>
#include <Interpreters/Context.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <base/sleep.h>

namespace fs = std::filesystem;

class MetadataLocalDiskTest : public testing::Test
{
public:
    void SetUp() override
    {
        DB::ServerUUID::setRandomForUnitTests();
    }

    std::shared_ptr<DB::IMetadataStorage> getMetadataStorage(const std::string & path)
    {
        std::unique_lock<std::mutex> lock(active_metadatas_mutex);
        if (!active_metadatas[path])
            active_metadatas[path] = createMetadataStorage(path);
        return active_metadatas[path];
    }

    void TearDown() override
    {
        for (const auto & [path, metadata] : active_metadatas)
            metadata->shutdown();

        if (!local_disk_metadata_dir.empty())
            fs::remove_all(local_disk_metadata_dir);
    }

private:
    std::shared_ptr<DB::IMetadataStorage> createMetadataStorage(const std::string & path)
    {
        local_disk_metadata_dir = "./test-metadata-dir." + DB::getRandomASCIIString(6);
        fs::create_directories(local_disk_metadata_dir);
        auto metadata_disk = std::make_shared<DB::DiskLocal>("test-metadata", local_disk_metadata_dir);
        auto metadata = std::make_shared<DB::MetadataStorageFromDisk>(metadata_disk, path);
        return metadata;
    }

    using MetadataPtr = std::shared_ptr<DB::IMetadataStorage>;
    std::unordered_map<std::string, MetadataPtr> active_metadatas;
    std::mutex active_metadatas_mutex;
    std::string local_disk_metadata_dir;
};

TEST_F(MetadataLocalDiskTest, TestHardlinkRewrite)
{
    auto metadata = getMetadataStorage("/TestHardlinkRewrite");

    {
        auto transaction = metadata->createTransaction();
        transaction->createMetadataFile("original_file", DB::ObjectStorageKey::createAsAbsolute("key1"), 111);
        transaction->commit();
    }

    EXPECT_EQ(metadata->getHardlinkCount("original_file"), 0);
    EXPECT_EQ(metadata->getFileSize("original_file"), 111);

    {
        auto transaction = metadata->createTransaction();
        transaction->createHardLink("original_file", "hardlinked_file");
        transaction->commit();
    }

    EXPECT_EQ(metadata->getFileSize("hardlinked_file"), 111);

    EXPECT_EQ(metadata->getHardlinkCount("original_file"), 1);
    EXPECT_EQ(metadata->getHardlinkCount("hardlinked_file"), 1);

    {
        auto transaction = metadata->createTransaction();
        transaction->createMetadataFile("hardlinked_file", DB::ObjectStorageKey::createAsAbsolute("key2"), 222);
        transaction->commit();
    }

    EXPECT_EQ(metadata->getHardlinkCount("original_file"), 1);
    EXPECT_EQ(metadata->getHardlinkCount("hardlinked_file"), 1);

    EXPECT_EQ(metadata->getFileSize("original_file"), 222);
    EXPECT_EQ(metadata->getFileSize("hardlinked_file"), 222);

    auto original_blobs = metadata->getStorageObjects("original_file");
    auto hardlinked_blobs = metadata->getStorageObjects("hardlinked_file");

    EXPECT_EQ(original_blobs.size(), 1);
    EXPECT_EQ(hardlinked_blobs.size(), 1);

    EXPECT_EQ(original_blobs[0].remote_path, "key2");
    EXPECT_EQ(original_blobs[0].bytes_size, 222);

    EXPECT_EQ(original_blobs[0].remote_path, "key2");
    EXPECT_EQ(original_blobs[0].bytes_size, 222);
}

TEST_F(MetadataLocalDiskTest, TestValidSingleOperationsWrite)
{
    auto metadata = getMetadataStorage("/TestValidSingleOperationsWrite");

    auto transaction = metadata->createTransaction();

    transaction->writeInlineDataToFile("f.txt", "hello");
    transaction->commit();

    EXPECT_TRUE(metadata->existsFile("f.txt"));
    EXPECT_EQ(metadata->readInlineDataToString("f.txt"), "hello");
    EXPECT_FALSE(metadata->existsDirectory("f.txt"));
}

TEST_F(MetadataLocalDiskTest, TestValidAddBlob)
{
    auto metadata = getMetadataStorage("/TestValidAddBlob");

    {
        auto transaction = metadata->createTransaction();

        transaction->addBlobToMetadata("f.txt", DB::ObjectStorageKey::createAsRelative("/TestValidAddBlob", "hello"), 1000);
        transaction->addBlobToMetadata("f.txt", DB::ObjectStorageKey::createAsAbsolute("world"), 2000);
        transaction->commit();
    }

    EXPECT_TRUE(metadata->existsFile("f.txt"));
    EXPECT_FALSE(metadata->existsDirectory("f.txt"));
    auto blobs1 = metadata->getStorageObjects("f.txt");
    EXPECT_EQ(blobs1.size(), 2);
    EXPECT_EQ(blobs1[0].remote_path, "/TestValidAddBlob/hello");
    EXPECT_EQ(blobs1[0].bytes_size, 1000);
    EXPECT_EQ(blobs1[1].remote_path, "world");
    EXPECT_EQ(blobs1[1].bytes_size, 2000);

    {
        auto transaction = metadata->createTransaction();

        transaction->addBlobToMetadata("f.txt", DB::ObjectStorageKey::createAsRelative("/TestValidAddBlob", "goodbye"), 300);
        transaction->addBlobToMetadata("f.txt", DB::ObjectStorageKey::createAsAbsolute("everybody"), 400);
        transaction->commit();
    }

    EXPECT_TRUE(metadata->existsFile("f.txt"));
    EXPECT_FALSE(metadata->existsDirectory("f.txt"));
    auto blobs2 = metadata->getStorageObjects("f.txt");
    EXPECT_EQ(blobs2.size(), 4);
    EXPECT_EQ(blobs2[0].remote_path, "/TestValidAddBlob/hello");
    EXPECT_EQ(blobs2[0].bytes_size, 1000);
    EXPECT_EQ(blobs2[1].remote_path, "world");
    EXPECT_EQ(blobs2[1].bytes_size, 2000);
    EXPECT_EQ(blobs2[2].remote_path, "/TestValidAddBlob/goodbye");
    EXPECT_EQ(blobs2[2].bytes_size, 300);
    EXPECT_EQ(blobs2[3].remote_path, "everybody");
    EXPECT_EQ(blobs2[3].bytes_size, 400);

    {
        auto transaction = metadata->createTransaction();

        transaction->moveFile("f.txt", "fff.txt");

        transaction->addBlobToMetadata("f.txt", DB::ObjectStorageKey::createAsRelative("/TestValidAddBlob", "I've"), 500);
        transaction->addBlobToMetadata("f.txt", DB::ObjectStorageKey::createAsRelative("/TestValidAddBlob", "got"), 600);
        transaction->addBlobToMetadata("f.txt", DB::ObjectStorageKey::createAsRelative("/TestValidAddBlob", "to go"), 700);

        transaction->addBlobToMetadata("fff.txt", DB::ObjectStorageKey::createAsRelative("/TestValidAddBlob", "goodbye"), 700);
        transaction->addBlobToMetadata("fff.txt", DB::ObjectStorageKey::createAsRelative("/TestValidAddBlob", "everybody"), 800);

        transaction->commit();
    }

    EXPECT_TRUE(metadata->existsFile("f.txt"));
    EXPECT_TRUE(metadata->existsFile("fff.txt"));
    auto blobs_f = metadata->getStorageObjects("f.txt");
    auto blobs_fff = metadata->getStorageObjects("fff.txt");
    EXPECT_EQ(blobs_f.size(), 3);
    EXPECT_EQ(blobs_f[0].remote_path, "/TestValidAddBlob/I've");
    EXPECT_EQ(blobs_f[0].bytes_size, 500);
    EXPECT_EQ(blobs_f[2].remote_path, "/TestValidAddBlob/to go");
    EXPECT_EQ(blobs_f[2].bytes_size, 700);

    EXPECT_EQ(blobs_fff.size(), 6);
    EXPECT_EQ(blobs_fff[0].remote_path, "/TestValidAddBlob/hello");
    EXPECT_EQ(blobs_fff[0].bytes_size, 1000);
    EXPECT_EQ(blobs_fff[1].remote_path, "world");
    EXPECT_EQ(blobs_fff[1].bytes_size, 2000);
    EXPECT_EQ(blobs_fff[2].remote_path, "/TestValidAddBlob/goodbye");
    EXPECT_EQ(blobs_fff[2].bytes_size, 300);
    EXPECT_EQ(blobs_fff[3].remote_path, "everybody");
    EXPECT_EQ(blobs_fff[3].bytes_size, 400);
    EXPECT_EQ(blobs_fff[4].remote_path, "/TestValidAddBlob/goodbye");
    EXPECT_EQ(blobs_fff[4].bytes_size, 700);
    EXPECT_EQ(blobs_fff[5].remote_path, "/TestValidAddBlob/everybody");
    EXPECT_EQ(blobs_fff[5].bytes_size, 800);
}

TEST_F(MetadataLocalDiskTest, TestValidCreateMetadataAddBlob)
{
    auto metadata = getMetadataStorage("/TestValidCreateMetadataAddBlob");

    {
        auto transaction = metadata->createTransaction();
        transaction->createDirectory("tmp_part");
        transaction->createMetadataFile("tmp_part/f.txt.tmp", DB::ObjectStorageKey::createAsRelative("/TestValidCreateMetadataAddBlob", "hello"), 1000);
        transaction->moveFile("tmp_part/f.txt.tmp", "tmp_part/f.txt");
        transaction->moveDirectory("tmp_part", "part");
        transaction->addBlobToMetadata("part/f.txt", DB::ObjectStorageKey::createAsRelative("/TestValidCreateMetadataAddBlob", "world"), 2000);
        transaction->commit();
    }

    {
        auto transaction = metadata->createTransaction();

        transaction->addBlobToMetadata("part/f.txt", DB::ObjectStorageKey::createAsRelative("/TestValidCreateMetadataAddBlob", "goodbye"), 300);
        transaction->commit();
    }

    {
        auto transaction = metadata->createTransaction();

        transaction->addBlobToMetadata("part/f.txt", DB::ObjectStorageKey::createAsRelative("/TestValidCreateMetadataAddBlob", "everybody"), 400);
        transaction->commit();
    }

    EXPECT_TRUE(metadata->existsFile("part/f.txt"));
    EXPECT_FALSE(metadata->existsDirectory("f.txt"));
    auto blobs2 = metadata->getStorageObjects("part/f.txt");
    EXPECT_EQ(blobs2.size(), 4);
    EXPECT_EQ(blobs2[0].remote_path, "/TestValidCreateMetadataAddBlob/hello");
    EXPECT_EQ(blobs2[0].bytes_size, 1000);
    EXPECT_EQ(blobs2[1].remote_path, "/TestValidCreateMetadataAddBlob/world");
    EXPECT_EQ(blobs2[1].bytes_size, 2000);
    EXPECT_EQ(blobs2[2].remote_path, "/TestValidCreateMetadataAddBlob/goodbye");
    EXPECT_EQ(blobs2[2].bytes_size, 300);
    EXPECT_EQ(blobs2[3].remote_path, "/TestValidCreateMetadataAddBlob/everybody");
    EXPECT_EQ(blobs2[3].bytes_size, 400);
}


TEST_F(MetadataLocalDiskTest, TestValidSingleOperationsMove)
{
    auto metadata = getMetadataStorage("/TestValidSingleOperationsMove");

    {
        auto transaction = metadata->createTransaction();
        transaction->writeInlineDataToFile("f.txt", "hello");
        transaction->commit();
    }

    auto transaction = metadata->createTransaction();

    transaction->moveFile("f.txt", "g.txt");
    transaction->commit();

    EXPECT_TRUE(metadata->existsFile("g.txt"));
    EXPECT_FALSE(metadata->existsFile("f.txt"));
}

TEST_F(MetadataLocalDiskTest, TestValidSingleOperationsHardlink)
{

    auto metadata = getMetadataStorage("/TestValidSingleOperationsHardlink");
    {
        auto transaction = metadata->createTransaction();
        transaction->writeInlineDataToFile("g.txt", "hello");
        transaction->commit();
    }

    auto transaction = metadata->createTransaction();

    transaction->createHardLink("g.txt", "g1.txt");
    transaction->commit();

    EXPECT_TRUE(metadata->existsFile("g.txt"));
    EXPECT_TRUE(metadata->existsFile("g1.txt"));
    EXPECT_EQ(metadata->readInlineDataToString("g.txt"), "hello");
    EXPECT_EQ(metadata->readInlineDataToString("g1.txt"), "hello");
}

TEST_F(MetadataLocalDiskTest, TestValidSingleOperationsReplace)
{
    auto metadata = getMetadataStorage("/TestValidSingleOperationsReplace");
    {
        auto write = metadata->createTransaction();

        write->writeInlineDataToFile("f.txt", "world");
        write->commit();
    }

    auto transaction = metadata->createTransaction();

    transaction->replaceFile("f.txt", "g.txt");
    transaction->commit();

    EXPECT_TRUE(metadata->existsFile("g.txt"));
    EXPECT_FALSE(metadata->existsFile("f.txt"));
    EXPECT_EQ(metadata->readInlineDataToString("g.txt"), "world");
}

TEST_F(MetadataLocalDiskTest, TestValidSingleOperationsRemove)
{
    auto metadata = getMetadataStorage("/TestValidSingleOperationsRemove");
    {
        auto write = metadata->createTransaction();

        write->writeInlineDataToFile("g.txt", "world");
        write->commit();
    }

    auto transaction = metadata->createTransaction();
    transaction->unlinkFile("g.txt");
    transaction->commit();

    EXPECT_FALSE(metadata->existsFile("g.txt"));
}

TEST_F(MetadataLocalDiskTest, TestValidUnlinkMetadata)
{
    auto metadata = getMetadataStorage("/TestValidUnlinkMetadata");
    {
        auto tx = metadata->createTransaction();

        /// Create a file with 1 reference
        tx->createEmptyMetadataFile("a");

        /// Create another file and make a hardlink to it
        tx->createEmptyMetadataFile("b");
        tx->createHardLink("b", "bb");

        tx->commit();
    }

    {
        auto tx = metadata->createTransaction();

        auto unlink_outcome_a = tx->unlinkMetadata("a");
        auto unlink_outcome_b = tx->unlinkMetadata("b");

        /// Create a file and remove it in the same Tx
        tx->createEmptyMetadataFile("c");
        auto unlink_outcome_c = tx->unlinkMetadata("c");

        tx->commit();

        /// "a" was removed
        EXPECT_TRUE(unlink_outcome_a);
        EXPECT_EQ(unlink_outcome_a->num_hardlinks, 0);

        /// "b" was removed but "bb" still exists
        EXPECT_TRUE(unlink_outcome_b);
        EXPECT_NE(unlink_outcome_b->num_hardlinks, 0);

        /// "c" was created and removed
        EXPECT_TRUE(unlink_outcome_c);
        EXPECT_EQ(unlink_outcome_c->num_hardlinks, 0);
    }

    {
        auto tx = metadata->createTransaction();

        auto unlink_outcome_bb = tx->unlinkMetadata("bb");

        /// Create another file and make a hardlink to it
        tx->createEmptyMetadataFile("d");
        tx->createHardLink("d", "dd");
        auto unlink_outcome_d = tx->unlinkMetadata("d");
        auto unlink_outcome_dd = tx->unlinkMetadata("dd");

        tx->commit();

        /// "bb" was the last reference to this file
        EXPECT_TRUE(unlink_outcome_bb);
        EXPECT_EQ(unlink_outcome_bb->num_hardlinks, 0);

        /// "d" and "dd" were removed
        EXPECT_TRUE(unlink_outcome_d);
        EXPECT_EQ(unlink_outcome_d->num_hardlinks, 1);
        EXPECT_EQ(unlink_outcome_dd->num_hardlinks, 0);
    }
}

TEST_F(MetadataLocalDiskTest, TestValidSingleOperationsCreateDirectory)
{
    auto metadata = getMetadataStorage("/TestValidSingleOperationsCreateDirectory");
    auto transaction = metadata->createTransaction();

    transaction->createDirectory("testdir");
    transaction->commit();

    EXPECT_TRUE(metadata->existsDirectory("testdir"));
    EXPECT_FALSE(metadata->existsFile("testdir"));
}

TEST_F(MetadataLocalDiskTest, TestValidSingleOperationsMoveDirectory)
{
    auto metadata = getMetadataStorage("/TestValidSingleOperationsMoveDirectory");
    {
        auto transaction = metadata->createTransaction();
        transaction->createDirectory("testdir");
        transaction->commit();
    }

    auto transaction = metadata->createTransaction();

    transaction->moveDirectory("testdir", "testdir1");
    transaction->commit();

    EXPECT_TRUE(metadata->existsDirectory("testdir1"));
    EXPECT_FALSE(metadata->existsFile("testdir1"));
    EXPECT_FALSE(metadata->existsFileOrDirectory("testdir"));
}

TEST_F(MetadataLocalDiskTest, TestValidSingleOperationsRemoveDirectory)
{
    auto metadata = getMetadataStorage("/TestValidSingleOperationsRemoveDirectory");
    {
        auto transaction = metadata->createTransaction();
        transaction->createDirectory("testdir");
        transaction->commit();
    }

    auto transaction = metadata->createTransaction();

    transaction->removeDirectory("testdir");
    transaction->commit();
    EXPECT_FALSE(metadata->existsFileOrDirectory("testdir"));
}

TEST_F(MetadataLocalDiskTest, TestValidSingleOperationsCreateDirectories)
{
    auto metadata = getMetadataStorage("/TestValidSingleOperationsCreateDirectories");
    auto transaction = metadata->createTransaction();

    transaction->createDirectoryRecursive("dir1/dir2/dir3");
    transaction->commit();

    EXPECT_TRUE(metadata->existsDirectory("dir1/dir2/dir3"));
    EXPECT_FALSE(metadata->existsFile("dir1/dir2/dir3"));
}

TEST_F(MetadataLocalDiskTest, TestValidSingleOperationsRemoveRecursive)
{
    auto metadata = getMetadataStorage("/TestValidSingleOperationsRemoveRecursive");
    {
        auto transaction1 = metadata->createTransaction();
        transaction1->createDirectoryRecursive("dir1/dir2/dir3");
        transaction1->commit();
        auto transaction2 = metadata->createTransaction();
        transaction2->writeInlineDataToFile("dir1/dir2/dir3/file.txt", "temp");
        transaction2->commit();
    }

    auto transaction = metadata->createTransaction();

    transaction->removeRecursive("dir1");
    transaction->commit();

    EXPECT_FALSE(metadata->existsFileOrDirectory("dir1/dir2/dir3"));
    EXPECT_FALSE(metadata->existsFileOrDirectory("dir1/dir2"));
    EXPECT_FALSE(metadata->existsFileOrDirectory("dir1"));
}

TEST_F(MetadataLocalDiskTest, TestValidCreateRemoveInOneTx)
{
    auto metadata = getMetadataStorage("/TestValidCreateRemoveInOneTx");
    {
        auto transaction1 = metadata->createTransaction();
        transaction1->writeInlineDataToFile("file.txt", "temp");
        transaction1->unlinkFile("file.txt");
        transaction1->commit();
    }

    EXPECT_FALSE(metadata->existsFile("file.txt"));

    {
        auto transaction1 = metadata->createTransaction();
        transaction1->writeInlineDataToFile("file.txt", "perm");
        transaction1->commit();
    }

    EXPECT_TRUE(metadata->existsFile("file.txt"));
    EXPECT_EQ(metadata->readInlineDataToString("file.txt"), "perm");
}

TEST_F(MetadataLocalDiskTest, TestValidSingleOperationsModifyData)
{
    auto metadata = getMetadataStorage("/TestValidSingleOperationsWrite");

    {
        auto transaction = metadata->createTransaction();
        transaction->writeInlineDataToFile("f.txt", "hello");
        transaction->commit();
    }

    EXPECT_TRUE(metadata->existsFile("f.txt"));
    EXPECT_EQ(metadata->readInlineDataToString("f.txt"), "hello");

    {
        auto transaction = metadata->createTransaction();
        transaction->writeInlineDataToFile("f.txt", "bye");
        transaction->commit();
    }

    EXPECT_EQ(metadata->readInlineDataToString("f.txt"), "bye");
}

TEST_F(MetadataLocalDiskTest, TestValidMultipleModifications)
{
    auto metadata = getMetadataStorage("/TestValidSingleOperationsWrite");

    {
        auto transaction = metadata->createTransaction();
        transaction->writeInlineDataToFile("f.txt", "hello");
        transaction->commit();
    }

    EXPECT_TRUE(metadata->existsFile("f.txt"));
    EXPECT_EQ(metadata->readInlineDataToString("f.txt"), "hello");
    EXPECT_EQ(metadata->getHardlinkCount("f.txt"), 0);

    {
        auto transaction = metadata->createTransaction();
        transaction->writeInlineDataToFile("f.txt", "bye");
        transaction->writeInlineDataToFile("f.txt", "bye-bye");
        transaction->commit();
    }

    EXPECT_EQ(metadata->readInlineDataToString("f.txt"), "bye-bye");
    EXPECT_EQ(metadata->getHardlinkCount("f.txt"), 0);
}

TEST_F(MetadataLocalDiskTest, TestValidModifyDataAndMove)
{
    auto metadata = getMetadataStorage("/TestValidModifyDataAndMove");

    {
        auto transaction = metadata->createTransaction();
        transaction->writeInlineDataToFile("f.txt", "hello");
        transaction->commit();
    }

    EXPECT_TRUE(metadata->existsFile("f.txt"));
    EXPECT_EQ(metadata->readInlineDataToString("f.txt"), "hello");
    EXPECT_EQ(metadata->getHardlinkCount("f.txt"), 0);

    {
        auto transaction = metadata->createTransaction();
        transaction->writeInlineDataToFile("f.txt", "bye");
        transaction->moveFile("f.txt", "g.txt");
        transaction->commit();
    }

    EXPECT_FALSE(metadata->existsFileOrDirectory("f.txt"));
    EXPECT_TRUE(metadata->existsFile("g.txt"));
    EXPECT_EQ(metadata->readInlineDataToString("g.txt"), "bye");
    EXPECT_EQ(metadata->getHardlinkCount("g.txt"), 0);
}

TEST_F(MetadataLocalDiskTest, TestValidModifyDataAndHardlink)
{
    auto metadata = getMetadataStorage("/TestValidModifyDataAndHardlink");

    {
        auto transaction = metadata->createTransaction();
        transaction->writeInlineDataToFile("f.txt", "hello");
        transaction->commit();
    }

    EXPECT_TRUE(metadata->existsFile("f.txt"));
    EXPECT_EQ(metadata->readInlineDataToString("f.txt"), "hello");
    EXPECT_EQ(metadata->getHardlinkCount("f.txt"), 0);

    {
        auto transaction = metadata->createTransaction();
        transaction->writeInlineDataToFile("f.txt", "bye");
        transaction->createHardLink("f.txt", "g.txt");
        transaction->commit();
    }

    EXPECT_TRUE(metadata->existsFile("f.txt"));
    EXPECT_TRUE(metadata->existsFile("g.txt"));
    EXPECT_EQ(metadata->readInlineDataToString("f.txt"), "bye");
    EXPECT_EQ(metadata->readInlineDataToString("g.txt"), "bye");
    EXPECT_EQ(metadata->getHardlinkCount("f.txt"), 1);
    EXPECT_EQ(metadata->getHardlinkCount("g.txt"), 1);

    {
        auto transaction = metadata->createTransaction();
        transaction->createHardLink("g.txt", "h.txt");
        transaction->writeInlineDataToFile("h.txt", "bye-bye");
        transaction->unlinkMetadata("g.txt");
        transaction->commit();
    }

    EXPECT_TRUE(metadata->existsFile("f.txt"));
    EXPECT_FALSE(metadata->existsFileOrDirectory("g.txt"));
    EXPECT_TRUE(metadata->existsFile("h.txt"));
    EXPECT_EQ(metadata->readInlineDataToString("f.txt"), "bye-bye");
    EXPECT_EQ(metadata->readInlineDataToString("h.txt"), "bye-bye");
    EXPECT_EQ(metadata->getHardlinkCount("f.txt"), 1);
    EXPECT_EQ(metadata->getHardlinkCount("h.txt"), 1);
}

namespace {
void createDatabaseAndTableDirs(const std::shared_ptr<DB::IMetadataStorage> & metadata, const std::string & path)
{
    auto transaction = metadata->createTransaction();
    transaction->createDirectoryRecursive(path);
    transaction->commit();
}
}

TEST_F(MetadataLocalDiskTest, TestValidComplexOperationsInsertPartSimple)
{
    auto metadata = getMetadataStorage("/TestValidSingleOperationsInsertPartSimple");
    createDatabaseAndTableDirs(metadata, "data/database/table");

    auto transaction = metadata->createTransaction();
    transaction->createDirectory("data/database/table/all_0_0_0");
    transaction->writeInlineDataToFile("data/database/table/all_0_0_0/column.bin", "binarydata");
    transaction->writeInlineDataToFile("data/database/table/all_0_0_0/column.mrk2", "otherdata");

    transaction->writeInlineDataToFile("data/database/table/all_0_0_0/stolbets.bin", "binarydata");
    transaction->writeInlineDataToFile("data/database/table/all_0_0_0/stolbets.mrk2", "otherdata");

    transaction->writeInlineDataToFile("data/database/table/all_0_0_0/primary.idx", "moredata");
    transaction->writeInlineDataToFile("data/database/table/all_0_0_0/columns.txt", "hello there");
    transaction->writeInlineDataToFile("data/database/table/all_0_0_0/count.txt", "hello there");
    transaction->writeInlineDataToFile("data/database/table/all_0_0_0/ttl.json", "hello there");
    transaction->writeInlineDataToFile("data/database/table/all_0_0_0/serialization_info.json", "hello there");

    transaction->commit();

    EXPECT_TRUE(metadata->existsDirectory("data/database/table/all_0_0_0"));

    EXPECT_TRUE(metadata->existsFile("data/database/table/all_0_0_0/column.bin"));
    EXPECT_EQ(metadata->readInlineDataToString("data/database/table/all_0_0_0/column.bin"), "binarydata");

    EXPECT_TRUE(metadata->existsFile("data/database/table/all_0_0_0/serialization_info.json"));
    EXPECT_EQ(metadata->readInlineDataToString("data/database/table/all_0_0_0/serialization_info.json"), "hello there");
    EXPECT_EQ(metadata->listDirectory("data/database/table/all_0_0_0/").size(), 9);
}


TEST_F(MetadataLocalDiskTest, TestValidComplexOperationsInsertPartDuplicateDirCreate)
{
    auto metadata = getMetadataStorage("/TestValidComplexOperationsInsertPartDuplicateDirCreate");
    createDatabaseAndTableDirs(metadata, "data/database/table");

    auto transaction = metadata->createTransaction();
    transaction->createDirectory("data/database/table/all_0_0_0");
    transaction->createDirectory("data/database/table/all_0_0_0");
    transaction->createDirectory("data/database/table/all_0_0_0");

    transaction->writeInlineDataToFile("data/database/table/all_0_0_0/column.bin", "binarydata");
    transaction->writeInlineDataToFile("data/database/table/all_0_0_0/column.mrk2", "otherdata");

    transaction->writeInlineDataToFile("data/database/table/all_0_0_0/stolbets.bin", "binarydata");
    transaction->writeInlineDataToFile("data/database/table/all_0_0_0/stolbets.mrk2", "otherdata");

    transaction->writeInlineDataToFile("data/database/table/all_0_0_0/primary.idx", "moredata");
    transaction->writeInlineDataToFile("data/database/table/all_0_0_0/columns.txt", "hello there");
    transaction->writeInlineDataToFile("data/database/table/all_0_0_0/checksums.txt", "hello there");
    transaction->writeInlineDataToFile("data/database/table/all_0_0_0/count.txt", "hello there");
    transaction->writeInlineDataToFile("data/database/table/all_0_0_0/ttl.json", "hello there");
    transaction->writeInlineDataToFile("data/database/table/all_0_0_0/serialization_info.json", "hello there");

    transaction->commit();

    EXPECT_TRUE(metadata->existsDirectory("data/database/table/all_0_0_0"));

    EXPECT_TRUE(metadata->existsFile("data/database/table/all_0_0_0/column.bin"));
    EXPECT_EQ(metadata->readInlineDataToString("data/database/table/all_0_0_0/column.bin"), "binarydata");

    EXPECT_TRUE(metadata->existsFile("data/database/table/all_0_0_0/serialization_info.json"));
    EXPECT_EQ(metadata->readInlineDataToString("data/database/table/all_0_0_0/serialization_info.json"), "hello there");
    EXPECT_EQ(metadata->listDirectory("data/database/table/all_0_0_0/").size(), 10);
}


TEST_F(MetadataLocalDiskTest, TestValidComplexOperationsInsertPartProjections)
{
    auto metadata = getMetadataStorage("/TestValidComplexOperationsInsertPartProjections");
    createDatabaseAndTableDirs(metadata, "data/database/table");

    auto transaction = metadata->createTransaction();
    transaction->createDirectory("data/database/table/all_0_0_0");
    transaction->createDirectory("data/database/table/all_0_0_0");
    transaction->createDirectory("data/database/table/all_0_0_0");

    transaction->writeInlineDataToFile("data/database/table/all_0_0_0/column.bin", "binarydata");
    transaction->writeInlineDataToFile("data/database/table/all_0_0_0/column.mrk2", "otherdata");

    transaction->writeInlineDataToFile("data/database/table/all_0_0_0/stolbets.bin", "binarydata");
    transaction->writeInlineDataToFile("data/database/table/all_0_0_0/stolbets.mrk2", "otherdata");

    transaction->writeInlineDataToFile("data/database/table/all_0_0_0/primary.idx", "moredata");
    transaction->writeInlineDataToFile("data/database/table/all_0_0_0/columns.txt", "hello there");
    transaction->writeInlineDataToFile("data/database/table/all_0_0_0/checksums.txt", "hello there");
    transaction->writeInlineDataToFile("data/database/table/all_0_0_0/count.txt", "hello there");
    transaction->writeInlineDataToFile("data/database/table/all_0_0_0/ttl.json", "hello there");
    transaction->writeInlineDataToFile("data/database/table/all_0_0_0/serialization_info.json", "hello there");

    transaction->createDirectory("data/database/table/all_0_0_0/coolprojection.proj");
    transaction->writeInlineDataToFile("data/database/table/all_0_0_0/coolprojection.proj/column3.bin", "bbbb");
    transaction->writeInlineDataToFile("data/database/table/all_0_0_0/coolprojection.proj/column3.mrk2", "qqqq");
    transaction->writeInlineDataToFile("data/database/table/all_0_0_0/coolprojection.proj/primary.idx", "qqqq");

    transaction->commit();

    EXPECT_TRUE(metadata->existsDirectory("data/database/table/all_0_0_0"));

    EXPECT_TRUE(metadata->existsFile("data/database/table/all_0_0_0/column.bin"));
    EXPECT_EQ(metadata->readInlineDataToString("data/database/table/all_0_0_0/column.bin"), "binarydata");

    EXPECT_TRUE(metadata->existsFile("data/database/table/all_0_0_0/serialization_info.json"));
    EXPECT_EQ(metadata->readInlineDataToString("data/database/table/all_0_0_0/serialization_info.json"), "hello there");

    EXPECT_TRUE(metadata->existsDirectory("data/database/table/all_0_0_0/coolprojection.proj"));
    EXPECT_EQ(metadata->readInlineDataToString("data/database/table/all_0_0_0/coolprojection.proj/primary.idx"), "qqqq");

    EXPECT_EQ(metadata->listDirectory("data/database/table/all_0_0_0/").size(), 11);
    EXPECT_EQ(metadata->listDirectory("data/database/table/all_0_0_0/coolprojection.proj").size(), 3);
}


TEST_F(MetadataLocalDiskTest, TestValidComplexOperationsMutatePart)
{
    auto metadata = getMetadataStorage("/TestValidComplexOperationsMutatePart");
    createDatabaseAndTableDirs(metadata, "data/database/table");
    {
        auto source_part = metadata->createTransaction();
        source_part->createDirectory("data/database/table/all_0_0_0");
        source_part->writeInlineDataToFile("data/database/table/all_0_0_0/stolbets.bin", "binarydata_source");
        source_part->writeInlineDataToFile("data/database/table/all_0_0_0/stolbets.mrk2", "otherdata_source");
        source_part->commit();
    }

    auto transaction = metadata->createTransaction();
    transaction->createDirectory("data/database/table/all_0_0_0_5");

    transaction->writeInlineDataToFile("data/database/table/all_0_0_0_5/column.bin", "binarydata");
    transaction->writeInlineDataToFile("data/database/table/all_0_0_0_5/column.mrk2", "otherdata");

    transaction->createHardLink("data/database/table/all_0_0_0/stolbets.bin", "data/database/table/all_0_0_0_5/stolbets.bin");
    transaction->createHardLink("data/database/table/all_0_0_0/stolbets.mrk2", "data/database/table/all_0_0_0_5/stolbets.mrk2");

    transaction->writeInlineDataToFile("data/database/table/all_0_0_0_5/primary.idx", "moredata");
    transaction->writeInlineDataToFile("data/database/table/all_0_0_0_5/columns.txt", "hello there");
    transaction->writeInlineDataToFile("data/database/table/all_0_0_0_5/checksums.txt", "hello there");
    transaction->writeInlineDataToFile("data/database/table/all_0_0_0_5/count.txt", "hello there");
    transaction->writeInlineDataToFile("data/database/table/all_0_0_0_5/ttl.json", "hello there");

    transaction->commit();

    EXPECT_TRUE(metadata->existsDirectory("data/database/table/all_0_0_0_5"));

    EXPECT_TRUE(metadata->existsFile("data/database/table/all_0_0_0_5/column.bin"));
    EXPECT_EQ(metadata->readInlineDataToString("data/database/table/all_0_0_0_5/column.bin"), "binarydata");

    EXPECT_EQ(metadata->readInlineDataToString("data/database/table/all_0_0_0_5/stolbets.bin"), "binarydata_source");
    EXPECT_EQ(metadata->readInlineDataToString("data/database/table/all_0_0_0_5/stolbets.mrk2"), "otherdata_source");
}


TEST_F(MetadataLocalDiskTest, TestValidComplexOperationsMutatePartWithProjections)
{
    auto metadata = getMetadataStorage("/TestValidComplexOperationsMutatePartWithProjections");
    createDatabaseAndTableDirs(metadata, "data/database/table");
    {
        auto source_part = metadata->createTransaction();
        source_part->createDirectory("data/database/table/all_0_0_0");
        source_part->writeInlineDataToFile("data/database/table/all_0_0_0/stolbets.bin", "binarydata_source");
        source_part->writeInlineDataToFile("data/database/table/all_0_0_0/stolbets.mrk2", "otherdata_source");
        source_part->createDirectory("data/database/table/all_0_0_0/someprojection.proj");
        source_part->writeInlineDataToFile("data/database/table/all_0_0_0/someprojection.proj/primary.idx", "aaaaa");
        source_part->writeInlineDataToFile("data/database/table/all_0_0_0/someprojection.proj/columns.txt", "aaaaa");
        source_part->commit();
    }

    auto transaction = metadata->createTransaction();
    transaction->createDirectory("data/database/table/all_0_0_0_5");

    transaction->writeInlineDataToFile("data/database/table/all_0_0_0_5/column.bin", "binarydata");
    transaction->writeInlineDataToFile("data/database/table/all_0_0_0_5/column.mrk2", "otherdata");

    transaction->createHardLink("data/database/table/all_0_0_0/stolbets.bin", "data/database/table/all_0_0_0_5/stolbets.bin");
    transaction->createHardLink("data/database/table/all_0_0_0/stolbets.mrk2", "data/database/table/all_0_0_0_5/stolbets.mrk2");

    transaction->writeInlineDataToFile("data/database/table/all_0_0_0_5/primary.idx", "moredata");
    transaction->writeInlineDataToFile("data/database/table/all_0_0_0_5/columns.txt", "hello there");
    transaction->writeInlineDataToFile("data/database/table/all_0_0_0_5/checksums.txt", "hello there");
    transaction->writeInlineDataToFile("data/database/table/all_0_0_0_5/count.txt", "hello there");
    transaction->writeInlineDataToFile("data/database/table/all_0_0_0_5/ttl.json", "hello there");

    transaction->createDirectory("data/database/table/all_0_0_0_5/someprojection.proj");
    transaction->writeInlineDataToFile("data/database/table/all_0_0_0_5/someprojection.proj/staff.bin", "");
    transaction->createHardLink("data/database/table/all_0_0_0/someprojection.proj/columns.txt", "data/database/table/all_0_0_0_5/someprojection.proj/columns.txt");
    transaction->createHardLink("data/database/table/all_0_0_0/someprojection.proj/primary.idx", "data/database/table/all_0_0_0_5/someprojection.proj/primary.idx");

    transaction->commit();

    EXPECT_TRUE(metadata->existsDirectory("data/database/table/all_0_0_0_5"));

    EXPECT_TRUE(metadata->existsFile("data/database/table/all_0_0_0_5/column.bin"));
    EXPECT_EQ(metadata->readInlineDataToString("data/database/table/all_0_0_0_5/column.bin"), "binarydata");

    EXPECT_EQ(metadata->readInlineDataToString("data/database/table/all_0_0_0_5/stolbets.bin"), "binarydata_source");
    EXPECT_EQ(metadata->readInlineDataToString("data/database/table/all_0_0_0_5/stolbets.mrk2"), "otherdata_source");

    EXPECT_TRUE(metadata->existsDirectory("data/database/table/all_0_0_0_5/someprojection.proj"));
    EXPECT_TRUE(metadata->existsFile("data/database/table/all_0_0_0_5/someprojection.proj/staff.bin"));
    EXPECT_EQ(metadata->readInlineDataToString("data/database/table/all_0_0_0_5/someprojection.proj/staff.bin"), "");
    EXPECT_EQ(metadata->readInlineDataToString("data/database/table/all_0_0_0_5/someprojection.proj/primary.idx"), "aaaaa");
}

TEST_F(MetadataLocalDiskTest, TestValidComplexOperationsMutatePartWithProjectionsWithTmp)
{
    auto metadata = getMetadataStorage("/TestValidComplexOperationsMutatePartWithProjectionsWithTmp");
    createDatabaseAndTableDirs(metadata, "data/database/table");
    {
        auto source_part = metadata->createTransaction();
        source_part->createDirectory("data/database/table/tmp_all_0_0_0");
        source_part->writeInlineDataToFile("data/database/table/tmp_all_0_0_0/stolbets.bin", "binarydata_source");
        source_part->writeInlineDataToFile("data/database/table/tmp_all_0_0_0/stolbets.mrk2", "otherdata_source");
        source_part->createDirectory("data/database/table/tmp_all_0_0_0/someprojection.proj");
        source_part->writeInlineDataToFile("data/database/table/tmp_all_0_0_0/someprojection.proj/primary.idx", "aaaaa");
        source_part->writeInlineDataToFile("data/database/table/tmp_all_0_0_0/someprojection.proj/columns.txt", "aaaaa");
        source_part->moveDirectory("data/database/table/tmp_all_0_0_0", "data/database/table/all_0_0_0");
        source_part->commit();
    }

    auto transaction = metadata->createTransaction();
    transaction->createDirectory("data/database/table/tmp_all_0_0_0_5");

    transaction->writeInlineDataToFile("data/database/table/tmp_all_0_0_0_5/column.bin", "binarydata");
    transaction->writeInlineDataToFile("data/database/table/tmp_all_0_0_0_5/column.mrk2", "otherdata");

    transaction->createHardLink("data/database/table/all_0_0_0/stolbets.bin", "data/database/table/tmp_all_0_0_0_5/stolbets.bin");
    transaction->createHardLink("data/database/table/all_0_0_0/stolbets.mrk2", "data/database/table/tmp_all_0_0_0_5/stolbets.mrk2");

    transaction->writeInlineDataToFile("data/database/table/tmp_all_0_0_0_5/primary.idx", "moredata");
    transaction->writeInlineDataToFile("data/database/table/tmp_all_0_0_0_5/columns.txt", "hello there");
    transaction->writeInlineDataToFile("data/database/table/tmp_all_0_0_0_5/checksums.txt", "hello there");
    transaction->writeInlineDataToFile("data/database/table/tmp_all_0_0_0_5/count.txt", "hello there");
    transaction->writeInlineDataToFile("data/database/table/tmp_all_0_0_0_5/ttl.json", "hello there");

    transaction->createDirectory("data/database/table/tmp_all_0_0_0_5/someprojection.proj");
    transaction->writeInlineDataToFile("data/database/table/tmp_all_0_0_0_5/someprojection.proj/staff.bin", "");
    transaction->createHardLink("data/database/table/all_0_0_0/someprojection.proj/columns.txt", "data/database/table/tmp_all_0_0_0_5/someprojection.proj/columns.txt");
    transaction->createHardLink("data/database/table/all_0_0_0/someprojection.proj/primary.idx", "data/database/table/tmp_all_0_0_0_5/someprojection.proj/primary.idx");

    transaction->moveDirectory("data/database/table/tmp_all_0_0_0_5", "data/database/table/all_0_0_0_5");

    transaction->commit();

    EXPECT_TRUE(metadata->existsDirectory("data/database/table/all_0_0_0_5"));

    EXPECT_EQ(metadata->listDirectory("data/database/table/all_0_0_0_5").size(), 10);

    EXPECT_TRUE(metadata->existsFile("data/database/table/all_0_0_0_5/column.bin"));
    EXPECT_EQ(metadata->readInlineDataToString("data/database/table/all_0_0_0_5/column.bin"), "binarydata");

    EXPECT_EQ(metadata->readInlineDataToString("data/database/table/all_0_0_0_5/stolbets.bin"), "binarydata_source");
    EXPECT_EQ(metadata->readInlineDataToString("data/database/table/all_0_0_0_5/stolbets.mrk2"), "otherdata_source");

    EXPECT_TRUE(metadata->existsDirectory("data/database/table/all_0_0_0_5/someprojection.proj"));
    EXPECT_TRUE(metadata->existsFile("data/database/table/all_0_0_0_5/someprojection.proj/staff.bin"));
    EXPECT_EQ(metadata->readInlineDataToString("data/database/table/all_0_0_0_5/someprojection.proj/staff.bin"), "");
    EXPECT_EQ(metadata->readInlineDataToString("data/database/table/all_0_0_0_5/someprojection.proj/primary.idx"), "aaaaa");
}

TEST_F(MetadataLocalDiskTest, ManyMetadataTx)
{
    auto metadata = getMetadataStorage("/ManyMetadataTx");
    createDatabaseAndTableDirs(metadata, "data/database/table");
    {
        auto source_part = metadata->createTransaction();
        source_part->createDirectory("data/database/table/tmp_all_0_0_0");
        source_part->writeInlineDataToFile("data/database/table/tmp_all_0_0_0/stolbets.bin", "binarydata_source");
        source_part->writeInlineDataToFile("data/database/table/tmp_all_0_0_0/stolbets.mrk2", "otherdata_source");
        source_part->createDirectory("data/database/table/tmp_all_0_0_0/someprojection.proj");
        source_part->writeInlineDataToFile("data/database/table/tmp_all_0_0_0/someprojection.proj/primary.idx", "aaaaa");
        source_part->writeInlineDataToFile("data/database/table/tmp_all_0_0_0/someprojection.proj/columns.txt", "aaaaa");
        source_part->moveDirectory("data/database/table/tmp_all_0_0_0", "data/database/table/all_0_0_0");
        source_part->commit();
    }

    EXPECT_EQ(metadata->getHardlinkCount("data/database/table/all_0_0_0/someprojection.proj/columns.txt"), 0);

    std::vector<DB::MetadataTransactionPtr> transactions;

    for (int i = 0; i < 1000; ++i)
    {
        auto transaction = metadata->createTransaction();
        std::string tmp_dir = "data/database/table/tmp_all_0_0_0_" + std::to_string(i);
        transaction->createDirectory(tmp_dir);

        transaction->writeInlineDataToFile(tmp_dir + "/column.bin", "binarydata");
        transaction->writeInlineDataToFile(tmp_dir + "/column.mrk2", "otherdata");

        transaction->createHardLink("data/database/table/all_0_0_0/stolbets.bin", tmp_dir + "/stolbets.bin");
        transaction->createHardLink("data/database/table/all_0_0_0/stolbets.mrk2", tmp_dir + "/stolbets.mrk2");

        transaction->writeInlineDataToFile(tmp_dir + "/primary.idx", "moredata");
        transaction->writeInlineDataToFile(tmp_dir + "/columns.txt", "hello there");
        transaction->writeInlineDataToFile(tmp_dir + "/checksums.txt", "hello there");
        transaction->writeInlineDataToFile(tmp_dir + "/count.txt", "hello there");
        transaction->writeInlineDataToFile(tmp_dir + "/ttl.json", "hello there");

        transaction->createDirectory(tmp_dir + "/someprojection.proj");
        transaction->writeInlineDataToFile(tmp_dir + "/someprojection.proj/staff.bin", "");
        transaction->createHardLink("data/database/table/all_0_0_0/someprojection.proj/columns.txt", tmp_dir + "/someprojection.proj/columns.txt");
        transaction->createHardLink("data/database/table/all_0_0_0/someprojection.proj/primary.idx", tmp_dir + "/someprojection.proj/primary.idx");

        transaction->moveDirectory(tmp_dir, "data/database/table/all_0_0_0_" + std::to_string(i));

        transactions.push_back(transaction);
    }

    for (auto & transaction : transactions)
        transaction->commit();

    EXPECT_EQ(metadata->getHardlinkCount("data/database/table/all_0_0_0/someprojection.proj/columns.txt"), 1000);
}

TEST_F(MetadataLocalDiskTest, TestValidComplexOperationsRemovePartOrdinary)
{
    auto metadata = getMetadataStorage("/TestValidComplexOperationsRemovePartOrdinary");
    createDatabaseAndTableDirs(metadata, "data/database/table");
    {
        auto source_part = metadata->createTransaction();
        source_part->createDirectory("data/database/table/all_0_0_0");
        source_part->writeInlineDataToFile("data/database/table/all_0_0_0/stolbets.bin", "binarydata_source");
        source_part->writeInlineDataToFile("data/database/table/all_0_0_0/stolbets.mrk2", "otherdata_source");
        source_part->createDirectory("data/database/table/all_0_0_0/someprojection.proj");
        source_part->writeInlineDataToFile("data/database/table/all_0_0_0/someprojection.proj/primary.idx", "aaaaa");
        source_part->writeInlineDataToFile("data/database/table/all_0_0_0/someprojection.proj/columns.txt", "aaaaa");
        source_part->commit();
    }

    auto transaction = metadata->createTransaction();

    transaction->unlinkFile("data/database/table/all_0_0_0/someprojection.proj/primary.idx");
    transaction->unlinkFile("data/database/table/all_0_0_0/someprojection.proj/columns.txt");
    transaction->removeDirectory("data/database/table/all_0_0_0/someprojection.proj");
    transaction->unlinkFile("data/database/table/all_0_0_0/stolbets.bin");
    transaction->unlinkFile("data/database/table/all_0_0_0/stolbets.mrk2");
    transaction->removeDirectory("data/database/table/all_0_0_0");

    transaction->commit();

    EXPECT_FALSE(metadata->existsDirectory("data/database/table/all_0_0_0"));
}


TEST_F(MetadataLocalDiskTest, TestValidComplexOperationsRemovePartBadCase)
{
    auto metadata = getMetadataStorage("/TestValidComplexOperationsRemovePartBadCase");
    createDatabaseAndTableDirs(metadata, "data/database/table");
    {
        auto source_part = metadata->createTransaction();
        source_part->createDirectory("data/database/table/all_0_0_0");
        source_part->writeInlineDataToFile("data/database/table/all_0_0_0/stolbets.bin", "binarydata_source");
        source_part->writeInlineDataToFile("data/database/table/all_0_0_0/stolbets.mrk2", "otherdata_source");
        source_part->createDirectory("data/database/table/all_0_0_0/someprojection.proj");
        source_part->writeInlineDataToFile("data/database/table/all_0_0_0/someprojection.proj/primary.idx", "aaaaa");
        source_part->writeInlineDataToFile("data/database/table/all_0_0_0/someprojection.proj/columns.txt", "aaaaa");
        source_part->commit();
    }
    auto transaction = metadata->createTransaction();
    transaction->removeRecursive("data/database/table/all_0_0_0");
    transaction->commit();

    EXPECT_FALSE(metadata->existsDirectory("data/database/table/all_0_0_0"));
}


TEST_F(MetadataLocalDiskTest, TestValidComplexOperationsMovePartDetached)
{
    auto metadata = getMetadataStorage("/TestValidComplexOperationsMovePartDetached");
    createDatabaseAndTableDirs(metadata, "data/database/table");
    {
        {
            auto detached_dir = metadata->createTransaction();
            detached_dir->createDirectory("data/database/table/detached");
            detached_dir->commit();
        }
        auto source_part = metadata->createTransaction();
        source_part->createDirectory("data/database/table/all_0_0_0");
        source_part->writeInlineDataToFile("data/database/table/all_0_0_0/stolbets.bin", "binarydata_source");
        source_part->writeInlineDataToFile("data/database/table/all_0_0_0/stolbets.mrk2", "otherdata_source");
        source_part->createDirectory("data/database/table/all_0_0_0/someprojection.proj");
        source_part->writeInlineDataToFile("data/database/table/all_0_0_0/someprojection.proj/primary.idx", "aaaaa");
        source_part->writeInlineDataToFile("data/database/table/all_0_0_0/someprojection.proj/columns.txt", "aaaaa");
        source_part->commit();
    }
    auto transaction = metadata->createTransaction();
    transaction->moveDirectory("data/database/table/all_0_0_0", "data/database/table/detached/broken_all_0_0_0");
    transaction->commit();

    EXPECT_TRUE(metadata->existsDirectory("data/database/table/detached/broken_all_0_0_0"));
    EXPECT_EQ(metadata->listDirectory("data/database/table/detached/broken_all_0_0_0").size(), 3);
}


TEST_F(MetadataLocalDiskTest, TestValidConcurrentHardlinks)
{
    auto metadata = getMetadataStorage("/TestValidConcurrentHardlinks");
    createDatabaseAndTableDirs(metadata, "data/database/table");
    {
        auto source_part = metadata->createTransaction();
        source_part->createDirectory("data/database/table/all_0_0_0");
        source_part->writeInlineDataToFile("data/database/table/all_0_0_0/stolbets.bin", "binarydata_source");
        source_part->writeInlineDataToFile("data/database/table/all_0_0_0/stolbets.mrk2", "otherdata_source");
        source_part->createDirectory("data/database/table/all_0_0_0/someprojection.proj");
        source_part->writeInlineDataToFile("data/database/table/all_0_0_0/someprojection.proj/primary.idx", "aaaaa");
        source_part->writeInlineDataToFile("data/database/table/all_0_0_0/someprojection.proj/columns.txt", "aaaaa");
        source_part->commit();
    }

    auto update_transaction = metadata->createTransaction();
    auto backup_transaction = metadata->createTransaction();

    update_transaction->createDirectory("data/database/table/tmp_all_0_0_0_1");
    update_transaction->createHardLink("data/database/table/all_0_0_0/stolbets.bin", "data/database/table/tmp_all_0_0_0_1/stolbets.bin");
    update_transaction->createHardLink("data/database/table/all_0_0_0/stolbets.mrk2", "data/database/table/tmp_all_0_0_0_1/stolbets.mrk2");
    update_transaction->createDirectory("data/database/table/tmp_all_0_0_0_1/someprojection.proj");
    for (const auto & name : metadata->listDirectory("data/database/table/all_0_0_0/someprojection.proj"))
    {
        update_transaction->createHardLink("data/database/table/all_0_0_0/someprojection.proj/" + name,
            "data/database/table/tmp_all_0_0_0_1/someprojection.proj/" + name);
    }

    backup_transaction->createDirectory("data/database/table/backup_all_0_0_0");
    backup_transaction->createHardLink("data/database/table/all_0_0_0/stolbets.bin", "data/database/table/backup_all_0_0_0/stolbets.bin");
    backup_transaction->createHardLink("data/database/table/all_0_0_0/stolbets.mrk2", "data/database/table/backup_all_0_0_0/stolbets.mrk2");
    backup_transaction->createDirectory("data/database/table/backup_all_0_0_0/someprojection.proj");
    for (const auto & name : metadata->listDirectory("data/database/table/all_0_0_0/someprojection.proj"))
    {
        backup_transaction->createHardLink("data/database/table/all_0_0_0/someprojection.proj/" + name,
            "data/database/table/backup_all_0_0_0/someprojection.proj/" + name);
    }
    backup_transaction->commit();
    /// Check hardlinks count
    EXPECT_EQ(metadata->getHardlinkCount("data/database/table/backup_all_0_0_0/stolbets.bin"), 1);
    EXPECT_EQ(metadata->getHardlinkCount("data/database/table/backup_all_0_0_0/stolbets.mrk2"), 1);
    EXPECT_EQ(metadata->getHardlinkCount("data/database/table/backup_all_0_0_0/someprojection.proj/primary.idx"), 1);
    EXPECT_EQ(metadata->getHardlinkCount("data/database/table/backup_all_0_0_0/someprojection.proj/columns.txt"), 1);

    update_transaction->moveDirectory("data/database/table/tmp_all_0_0_0_1", "data/database/table/all_0_0_0_1");
    update_transaction->commit();

    EXPECT_TRUE(metadata->existsDirectory("data/database/table/backup_all_0_0_0"));
    EXPECT_TRUE(metadata->existsDirectory("data/database/table/all_0_0_0"));
    EXPECT_TRUE(metadata->existsDirectory("data/database/table/all_0_0_0_1"));
    EXPECT_FALSE(metadata->existsFileOrDirectory("data/database/table/tmp_all_0_0_0_1"));
    EXPECT_EQ(metadata->listDirectory("data/database/table/backup_all_0_0_0").size(), 3);
    EXPECT_EQ(metadata->listDirectory("data/database/table/all_0_0_0_1").size(), 3);

    /// Check hardlinks count
    EXPECT_EQ(metadata->getHardlinkCount("data/database/table/all_0_0_0/stolbets.bin"), 2);
    EXPECT_EQ(metadata->getHardlinkCount("data/database/table/all_0_0_0/stolbets.mrk2"), 2);
    EXPECT_EQ(metadata->getHardlinkCount("data/database/table/all_0_0_0/someprojection.proj/primary.idx"), 2);
    EXPECT_EQ(metadata->getHardlinkCount("data/database/table/all_0_0_0/someprojection.proj/columns.txt"), 2);
    EXPECT_EQ(metadata->getHardlinkCount("data/database/table/all_0_0_0_1/stolbets.bin"), 2);
    EXPECT_EQ(metadata->getHardlinkCount("data/database/table/all_0_0_0_1/stolbets.mrk2"), 2);
    EXPECT_EQ(metadata->getHardlinkCount("data/database/table/all_0_0_0_1/someprojection.proj/primary.idx"), 2);
    EXPECT_EQ(metadata->getHardlinkCount("data/database/table/all_0_0_0_1/someprojection.proj/columns.txt"), 2);
    EXPECT_EQ(metadata->getHardlinkCount("data/database/table/backup_all_0_0_0/stolbets.bin"), 2);
    EXPECT_EQ(metadata->getHardlinkCount("data/database/table/backup_all_0_0_0/stolbets.mrk2"), 2);
    EXPECT_EQ(metadata->getHardlinkCount("data/database/table/backup_all_0_0_0/someprojection.proj/primary.idx"), 2);
    EXPECT_EQ(metadata->getHardlinkCount("data/database/table/backup_all_0_0_0/someprojection.proj/columns.txt"), 2);

    /// MetadataStorageFromDiskTransaction::removeRecursive DOES NOT handle hardlinks count
    /// only DiskObjectStorageTransaction::removeSharedRecursive handles it right
    /// when MergeTreeDataPart is removed it uses DiskObjectStorageTransaction.

    /// Remove original dir and check hardlinks count again
    {
        auto rm_transaction = metadata->createTransaction();
        rm_transaction->removeRecursive("data/database/table/all_0_0_0");
        rm_transaction->commit();
    }

    EXPECT_TRUE(metadata->existsDirectory("data/database/table/backup_all_0_0_0"));
    EXPECT_FALSE(metadata->existsFileOrDirectory("data/database/table/all_0_0_0"));
    EXPECT_TRUE(metadata->existsDirectory("data/database/table/all_0_0_0_1"));
    EXPECT_EQ(metadata->getHardlinkCount("data/database/table/backup_all_0_0_0/stolbets.bin"), 2);
    EXPECT_EQ(metadata->getHardlinkCount("data/database/table/backup_all_0_0_0/stolbets.mrk2"), 2);
    EXPECT_EQ(metadata->getHardlinkCount("data/database/table/backup_all_0_0_0/someprojection.proj/primary.idx"), 2);
    EXPECT_EQ(metadata->getHardlinkCount("data/database/table/backup_all_0_0_0/someprojection.proj/columns.txt"), 2);

    /// Remove backup dir and check hardlinks count again
    {
        auto rm_backup_transaction = metadata->createTransaction();
        rm_backup_transaction->removeRecursive("data/database/table/backup_all_0_0_0");
        rm_backup_transaction->commit();
    }

    EXPECT_FALSE(metadata->existsFileOrDirectory("data/database/table/backup_all_0_0_0"));
    EXPECT_FALSE(metadata->existsFileOrDirectory("data/database/table/all_0_0_0"));
    EXPECT_TRUE(metadata->existsDirectory("data/database/table/all_0_0_0_1"));
    EXPECT_EQ(metadata->getHardlinkCount("data/database/table/all_0_0_0_1/stolbets.bin"), 2);
    EXPECT_EQ(metadata->getHardlinkCount("data/database/table/all_0_0_0_1/stolbets.mrk2"), 2);
    EXPECT_EQ(metadata->getHardlinkCount("data/database/table/all_0_0_0_1/someprojection.proj/primary.idx"), 2);
    EXPECT_EQ(metadata->getHardlinkCount("data/database/table/all_0_0_0_1/someprojection.proj/columns.txt"), 2);
}


TEST_F(MetadataLocalDiskTest, TestNonExistingObjects)
{
    auto metadata = getMetadataStorage("/TestNonExistingObjects");
    EXPECT_FALSE(metadata->existsFile("non-existing"));
    EXPECT_FALSE(metadata->existsDirectory("non-existing"));
    EXPECT_THROW(metadata->listDirectory("non-existing"), std::exception);
    EXPECT_THROW(metadata->readInlineDataToString("non-existing"), std::exception);
    EXPECT_THROW(metadata->getFileSize("non-existing"), std::exception);
    EXPECT_THROW(metadata->getLastModified("non-existing"), std::exception);
    EXPECT_THROW(metadata->getLastChanged("non-existing"), std::exception);
    EXPECT_THROW(metadata->getStorageObjects("non-existing"), std::exception);
    EXPECT_THROW(metadata->getHardlinkCount("non-existing"), std::exception);
}

TEST_F(MetadataLocalDiskTest, TestNonExistingObjectsInTransaction)
{
    auto metadata = getMetadataStorage("/TestNonExistingObjectsInTransaction");

    {
        auto transaction = metadata->createTransaction();
        transaction->createDirectory("dir");
        transaction->createEmptyMetadataFile("file.txt");
        transaction->commit();
        EXPECT_TRUE(metadata->existsDirectory("dir"));
        EXPECT_TRUE(metadata->existsFile("file.txt"));
    }

    {
        auto transaction = metadata->createTransaction();
        EXPECT_THROW({
                transaction->createEmptyMetadataFile("non-existing/file.txt");
                transaction->commit();
            }, std::exception);
    }

    {
        auto transaction = metadata->createTransaction();
        EXPECT_THROW({
                transaction->createMetadataFile("non-existing/file.txt", DB::ObjectStorageKey::createAsAbsolute("blob_name"), 42);
                transaction->commit();
            }, std::exception);
    }

    {
        auto transaction = metadata->createTransaction();
        EXPECT_THROW({
                transaction->createDirectory("non-existing/subdir");
                transaction->commit();
            }, std::exception);
    }

    {
        auto transaction = metadata->createTransaction();
        EXPECT_THROW({
                transaction->createHardLink("non-existing", "new-file");
                transaction->commit();
            }, std::exception);
    }

    {
        auto transaction = metadata->createTransaction();
        EXPECT_THROW({
                transaction->unlinkFile("file.txt/non-existing");
                transaction->commit();
            }, std::exception);
    }

    {
        auto transaction = metadata->createTransaction();
        EXPECT_THROW({
                transaction->unlinkFile("dir/non-existing");
                transaction->commit();
            }, std::exception);
    }

    {
        auto transaction = metadata->createTransaction();
        EXPECT_THROW({
                transaction->unlinkFile("non-existing/non-existing");
                transaction->commit();
            }, std::exception);
    }

    {
        auto transaction = metadata->createTransaction();
        EXPECT_THROW({
                transaction->removeDirectory("file.txt/non-existing");
                transaction->commit();
            }, std::exception);
    }

    {
        auto transaction = metadata->createTransaction();
        EXPECT_THROW({
                transaction->removeDirectory("dir/non-existing");
                transaction->commit();
            }, std::exception);
    }

    {
        auto transaction = metadata->createTransaction();
        EXPECT_THROW({
                transaction->removeDirectory("non-existing/non-existing");
                transaction->commit();
            }, std::exception);
    }

    {
        auto transaction = metadata->createTransaction();
        EXPECT_NO_THROW({
                transaction->removeRecursive("file.txt/non-existing");
                transaction->commit();
            });
    }

    {
        auto transaction = metadata->createTransaction();
        EXPECT_NO_THROW({
                transaction->removeRecursive("dir/non-existing");
                transaction->commit();
            });
    }

    {
        auto transaction = metadata->createTransaction();
        EXPECT_NO_THROW({
                transaction->removeRecursive("non-existing/non-existing");
                transaction->commit();
            });
    }
}
