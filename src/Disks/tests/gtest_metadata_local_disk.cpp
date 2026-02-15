#include <gtest/gtest.h>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <memory>
#include <mutex>
#include <Core/ServerUUID.h>
#include <Disks/DiskLocal.h>
#include <Disks/DiskObjectStorage/MetadataStorages/Local/MetadataStorageFromDisk.h>
#include <Common/ObjectStorageKeyGenerator.h>
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

    std::shared_ptr<DB::IDisk> getMetadataDisk(const std::string & path)
    {
        std::unique_lock<std::mutex> lock(active_metadatas_mutex);
        chassert(active_disks.contains(path));
        return active_disks[path];
    }

    void TearDown() override
    {
        for (const auto & [path, metadata] : active_metadatas)
            metadata->shutdown();

        for (const auto & [_, disk] : active_disks)
            fs::remove_all(disk->getPath());
    }

private:
    std::shared_ptr<DB::IMetadataStorage> createMetadataStorage(const std::string & path)
    {
        const auto local_disk_metadata_dir = "./test-metadata-dir." + DB::getRandomASCIIString(6);
        fs::create_directories(local_disk_metadata_dir);

        auto disk = active_disks[path] = std::make_shared<DB::DiskLocal>("test-metadata", local_disk_metadata_dir);
        auto key_generator = DB::createObjectStorageKeyGeneratorByTemplate("[a-z]{32}");
        auto metadata = active_metadatas[path] = std::make_shared<DB::MetadataStorageFromDisk>(disk, path, key_generator);

        return metadata;
    }

    std::mutex active_metadatas_mutex;
    std::unordered_map<std::string, std::shared_ptr<DB::IMetadataStorage>> active_metadatas;
    std::unordered_map<std::string, std::shared_ptr<DB::IDisk>> active_disks;
};

void verifyBlobsToRemove(const DB::MetadataStoragePtr & metadata, std::set<std::string> expected_blobs)
{
    std::unordered_map<DB::Location, DB::LocationInfo> cluster_registry = {{"main", {true, true, ""}}};
    DB::ClusterConfigurationPtr cluster = std::make_shared<DB::ClusterConfiguration>(std::move(cluster_registry));
    auto blobs_to_remove = metadata->getBlobsToRemove(cluster, 10000);

    std::set<std::string> remote_paths;
    for (const auto & [blob, locations] : blobs_to_remove)
    {
        EXPECT_EQ(locations, DB::LocationSet{"main"});
        remote_paths.insert(blob.remote_path);
    }

    EXPECT_EQ(remote_paths, expected_blobs);
}

TEST_F(MetadataLocalDiskTest, TestHardlinkRewrite)
{
    auto metadata = getMetadataStorage("/TestHardlinkRewrite");

    {
        auto transaction = metadata->createTransaction();
        transaction->createMetadataFile("original_file", {DB::StoredObject(DB::ObjectStorageKey::createAsAbsolute("key1").serialize(), "original_file", 111)});
        transaction->commit(DB::NoCommitOptions{});
    }

    EXPECT_EQ(metadata->getHardlinkCount("original_file"), 0);
    EXPECT_EQ(metadata->getFileSize("original_file"), 111);

    {
        auto transaction = metadata->createTransaction();
        transaction->createHardLink("original_file", "hardlinked_file");
        transaction->commit(DB::NoCommitOptions{});
    }

    EXPECT_EQ(metadata->getFileSize("hardlinked_file"), 111);

    EXPECT_EQ(metadata->getHardlinkCount("original_file"), 1);
    EXPECT_EQ(metadata->getHardlinkCount("hardlinked_file"), 1);

    {
        auto transaction = metadata->createTransaction();
        transaction->createMetadataFile("hardlinked_file", {DB::StoredObject(DB::ObjectStorageKey::createAsAbsolute("key2").serialize(), "hardlinked_file", 222)});
        transaction->commit(DB::NoCommitOptions{});
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
    transaction->commit(DB::NoCommitOptions{});

    EXPECT_TRUE(metadata->existsFile("f.txt"));
    EXPECT_EQ(metadata->readInlineDataToString("f.txt"), "hello");
    EXPECT_FALSE(metadata->existsDirectory("f.txt"));
}

TEST_F(MetadataLocalDiskTest, TestValidAddBlob)
{
    auto metadata = getMetadataStorage("/TestValidAddBlob");

    {
        auto transaction = metadata->createTransaction();

        transaction->addBlobToMetadata("f.txt", DB::StoredObject(DB::ObjectStorageKey::createAsRelative("/TestValidAddBlob", "hello").serialize(), "f.txt", 1000));
        transaction->addBlobToMetadata("f.txt", DB::StoredObject(DB::ObjectStorageKey::createAsAbsolute("world").serialize(), "f.txt", 2000));
        transaction->commit(DB::NoCommitOptions{});
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

        transaction->addBlobToMetadata("f.txt", DB::StoredObject(DB::ObjectStorageKey::createAsRelative("/TestValidAddBlob", "goodbye").serialize(), "f.txt", 300));
        transaction->addBlobToMetadata("f.txt", DB::StoredObject(DB::ObjectStorageKey::createAsAbsolute("everybody").serialize(), "f.txt", 400));
        transaction->commit(DB::NoCommitOptions{});
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

        transaction->addBlobToMetadata("f.txt", DB::StoredObject(DB::ObjectStorageKey::createAsRelative("/TestValidAddBlob", "I've").serialize(), "f.txt", 500));
        transaction->addBlobToMetadata("f.txt", DB::StoredObject(DB::ObjectStorageKey::createAsRelative("/TestValidAddBlob", "got").serialize(), "f.txt", 600));
        transaction->addBlobToMetadata("f.txt", DB::StoredObject(DB::ObjectStorageKey::createAsRelative("/TestValidAddBlob", "to go").serialize(), "f.txt", 700));

        transaction->addBlobToMetadata("fff.txt", DB::StoredObject(DB::ObjectStorageKey::createAsRelative("/TestValidAddBlob", "goodbye").serialize(), "fff.txt", 700));
        transaction->addBlobToMetadata("fff.txt", DB::StoredObject(DB::ObjectStorageKey::createAsRelative("/TestValidAddBlob", "everybody").serialize(), "fff.txt", 800));

        transaction->commit(DB::NoCommitOptions{});
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
        transaction->createMetadataFile("tmp_part/f.txt.tmp", {DB::StoredObject(DB::ObjectStorageKey::createAsRelative("/TestValidCreateMetadataAddBlob", "hello").serialize(), "tmp_part/f.txt.tmp", 1000)});
        transaction->moveFile("tmp_part/f.txt.tmp", "tmp_part/f.txt");
        transaction->moveDirectory("tmp_part", "part");
        transaction->addBlobToMetadata("part/f.txt", DB::StoredObject(DB::ObjectStorageKey::createAsRelative("/TestValidCreateMetadataAddBlob", "world").serialize(), "part/f.txt", 2000));
        transaction->commit(DB::NoCommitOptions{});
    }

    {
        auto transaction = metadata->createTransaction();

        transaction->addBlobToMetadata("part/f.txt", DB::StoredObject(DB::ObjectStorageKey::createAsRelative("/TestValidCreateMetadataAddBlob", "goodbye").serialize(), "part/f.txt", 300));
        transaction->commit(DB::NoCommitOptions{});
    }

    {
        auto transaction = metadata->createTransaction();

        transaction->addBlobToMetadata("part/f.txt", DB::StoredObject(DB::ObjectStorageKey::createAsRelative("/TestValidCreateMetadataAddBlob", "everybody").serialize(), "part/f.txt", 400));
        transaction->commit(DB::NoCommitOptions{});
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
        transaction->commit(DB::NoCommitOptions{});
    }

    auto transaction = metadata->createTransaction();

    transaction->moveFile("f.txt", "g.txt");
    transaction->commit(DB::NoCommitOptions{});

    EXPECT_TRUE(metadata->existsFile("g.txt"));
    EXPECT_FALSE(metadata->existsFile("f.txt"));
}

TEST_F(MetadataLocalDiskTest, TestValidSingleOperationsHardlink)
{

    auto metadata = getMetadataStorage("/TestValidSingleOperationsHardlink");
    {
        auto transaction = metadata->createTransaction();
        transaction->writeInlineDataToFile("g.txt", "hello");
        transaction->commit(DB::NoCommitOptions{});
    }

    auto transaction = metadata->createTransaction();

    transaction->createHardLink("g.txt", "g1.txt");
    transaction->commit(DB::NoCommitOptions{});

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
        write->commit(DB::NoCommitOptions{});
    }

    auto transaction = metadata->createTransaction();

    transaction->replaceFile("f.txt", "g.txt");
    transaction->commit(DB::NoCommitOptions{});

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
        write->commit(DB::NoCommitOptions{});
    }

    auto transaction = metadata->createTransaction();
    transaction->unlinkFile("g.txt", /*if_exists=*/false, /*should_remove_objects=*/true);
    transaction->commit(DB::NoCommitOptions{});

    EXPECT_FALSE(metadata->existsFile("g.txt"));
}

TEST_F(MetadataLocalDiskTest, TestValidUnlinkMetadata)
{
    auto metadata = getMetadataStorage("/TestValidUnlinkMetadata");
    {
        auto tx = metadata->createTransaction();

        /// Create a file with 1 reference
        tx->createMetadataFile("a", /*objects=*/{});

        /// Create another file and make a hardlink to it
        tx->createMetadataFile("b", /*objects=*/{});
        tx->createHardLink("b", "bb");

        tx->commit(DB::NoCommitOptions{});
    }

    {
        auto tx = metadata->createTransaction();

        tx->unlinkFile("a", /*if_exists=*/false, /*should_remove_objects=*/true);
        tx->unlinkFile("b", /*if_exists=*/false, /*should_remove_objects=*/true);

        /// Create a file and remove it in the same Tx
        tx->createMetadataFile("c", /*objects=*/{});
        tx->unlinkFile("c", /*if_exists=*/false, /*should_remove_objects=*/true);

        tx->commit(DB::NoCommitOptions{});

        /// "a" was removed
        /// "b" was removed but "bb" still exists
        /// "c" was created and removed
        EXPECT_FALSE(metadata->existsFile("a"));
        EXPECT_FALSE(metadata->existsFile("b"));
        EXPECT_FALSE(metadata->existsFile("c"));
        EXPECT_TRUE(metadata->existsFile("bb"));
    }

    {
        auto tx = metadata->createTransaction();

        tx->unlinkFile("bb", /*if_exists=*/false, /*should_remove_objects=*/true);

        /// Create another file and make a hardlink to it
        tx->createMetadataFile("d", /*objects=*/{});
        tx->createHardLink("d", "dd");
        tx->unlinkFile("d", /*if_exists=*/false, /*should_remove_objects=*/true);
        tx->unlinkFile("dd", /*if_exists=*/false, /*should_remove_objects=*/true);

        tx->commit(DB::NoCommitOptions{});

        /// "bb" was the last reference to this file
        /// "d" and "dd" were removed
        EXPECT_FALSE(metadata->existsFile("bb"));
        EXPECT_FALSE(metadata->existsFile("d"));
        EXPECT_FALSE(metadata->existsFile("dd"));
    }
}

TEST_F(MetadataLocalDiskTest, TestValidSingleOperationsCreateDirectory)
{
    auto metadata = getMetadataStorage("/TestValidSingleOperationsCreateDirectory");
    auto transaction = metadata->createTransaction();

    transaction->createDirectory("testdir");
    transaction->commit(DB::NoCommitOptions{});

    EXPECT_TRUE(metadata->existsDirectory("testdir"));
    EXPECT_FALSE(metadata->existsFile("testdir"));
}

TEST_F(MetadataLocalDiskTest, TestValidSingleOperationsMoveDirectory)
{
    auto metadata = getMetadataStorage("/TestValidSingleOperationsMoveDirectory");
    {
        auto transaction = metadata->createTransaction();
        transaction->createDirectory("testdir");
        transaction->commit(DB::NoCommitOptions{});
    }

    auto transaction = metadata->createTransaction();

    transaction->moveDirectory("testdir", "testdir1");
    transaction->commit(DB::NoCommitOptions{});

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
        transaction->commit(DB::NoCommitOptions{});
    }

    auto transaction = metadata->createTransaction();

    transaction->removeDirectory("testdir");
    transaction->commit(DB::NoCommitOptions{});
    EXPECT_FALSE(metadata->existsFileOrDirectory("testdir"));
}

TEST_F(MetadataLocalDiskTest, TestValidSingleOperationsCreateDirectories)
{
    auto metadata = getMetadataStorage("/TestValidSingleOperationsCreateDirectories");
    auto transaction = metadata->createTransaction();

    transaction->createDirectoryRecursive("dir1/dir2/dir3");
    transaction->commit(DB::NoCommitOptions{});

    EXPECT_TRUE(metadata->existsDirectory("dir1/dir2/dir3"));
    EXPECT_FALSE(metadata->existsFile("dir1/dir2/dir3"));
}

TEST_F(MetadataLocalDiskTest, TestValidSingleOperationsRemoveRecursive)
{
    auto metadata = getMetadataStorage("/TestValidSingleOperationsRemoveRecursive");
    {
        auto transaction1 = metadata->createTransaction();
        transaction1->createDirectoryRecursive("dir1/dir2/dir3");
        transaction1->commit(DB::NoCommitOptions{});
        auto transaction2 = metadata->createTransaction();
        transaction2->writeInlineDataToFile("dir1/dir2/dir3/file.txt", "temp");
        transaction2->commit(DB::NoCommitOptions{});
    }

    auto transaction = metadata->createTransaction();

    transaction->removeRecursive("dir1", /*should_remove_objects=*/nullptr);
    transaction->commit(DB::NoCommitOptions{});

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
        transaction1->unlinkFile("file.txt", /*if_exists=*/false, /*should_remove_objects=*/true);
        transaction1->commit(DB::NoCommitOptions{});
    }

    EXPECT_FALSE(metadata->existsFile("file.txt"));

    {
        auto transaction1 = metadata->createTransaction();
        transaction1->writeInlineDataToFile("file.txt", "perm");
        transaction1->commit(DB::NoCommitOptions{});
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
        transaction->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsFile("f.txt"));
    EXPECT_EQ(metadata->readInlineDataToString("f.txt"), "hello");

    {
        auto transaction = metadata->createTransaction();
        transaction->writeInlineDataToFile("f.txt", "bye");
        transaction->commit(DB::NoCommitOptions{});
    }

    EXPECT_EQ(metadata->readInlineDataToString("f.txt"), "bye");
}

TEST_F(MetadataLocalDiskTest, TestValidMultipleModifications)
{
    auto metadata = getMetadataStorage("/TestValidSingleOperationsWrite");

    {
        auto transaction = metadata->createTransaction();
        transaction->writeInlineDataToFile("f.txt", "hello");
        transaction->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsFile("f.txt"));
    EXPECT_EQ(metadata->readInlineDataToString("f.txt"), "hello");
    EXPECT_EQ(metadata->getHardlinkCount("f.txt"), 0);

    {
        auto transaction = metadata->createTransaction();
        transaction->writeInlineDataToFile("f.txt", "bye");
        transaction->writeInlineDataToFile("f.txt", "bye-bye");
        transaction->commit(DB::NoCommitOptions{});
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
        transaction->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsFile("f.txt"));
    EXPECT_EQ(metadata->readInlineDataToString("f.txt"), "hello");
    EXPECT_EQ(metadata->getHardlinkCount("f.txt"), 0);

    {
        auto transaction = metadata->createTransaction();
        transaction->writeInlineDataToFile("f.txt", "bye");
        transaction->moveFile("f.txt", "g.txt");
        transaction->commit(DB::NoCommitOptions{});
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
        transaction->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsFile("f.txt"));
    EXPECT_EQ(metadata->readInlineDataToString("f.txt"), "hello");
    EXPECT_EQ(metadata->getHardlinkCount("f.txt"), 0);

    {
        auto transaction = metadata->createTransaction();
        transaction->writeInlineDataToFile("f.txt", "bye");
        transaction->createHardLink("f.txt", "g.txt");
        transaction->commit(DB::NoCommitOptions{});
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
        transaction->unlinkFile("g.txt", /*if_exists=*/false, /*should_remove_objects=*/true);
        transaction->commit(DB::NoCommitOptions{});
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
    transaction->commit(DB::NoCommitOptions{});
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

    transaction->commit(DB::NoCommitOptions{});

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

    transaction->commit(DB::NoCommitOptions{});

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

    transaction->commit(DB::NoCommitOptions{});

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
        source_part->commit(DB::NoCommitOptions{});
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

    transaction->commit(DB::NoCommitOptions{});

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
        source_part->commit(DB::NoCommitOptions{});
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

    transaction->commit(DB::NoCommitOptions{});

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
        source_part->commit(DB::NoCommitOptions{});
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

    transaction->commit(DB::NoCommitOptions{});

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
        source_part->commit(DB::NoCommitOptions{});
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
        transaction->commit(DB::NoCommitOptions{});

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
        source_part->commit(DB::NoCommitOptions{});
    }

    auto transaction = metadata->createTransaction();

    transaction->unlinkFile("data/database/table/all_0_0_0/someprojection.proj/primary.idx", /*if_exists=*/false, /*should_remove_objects=*/true);
    transaction->unlinkFile("data/database/table/all_0_0_0/someprojection.proj/columns.txt", /*if_exists=*/false, /*should_remove_objects=*/true);
    transaction->removeDirectory("data/database/table/all_0_0_0/someprojection.proj");
    transaction->unlinkFile("data/database/table/all_0_0_0/stolbets.bin", /*if_exists=*/false, /*should_remove_objects=*/true);
    transaction->unlinkFile("data/database/table/all_0_0_0/stolbets.mrk2", /*if_exists=*/false, /*should_remove_objects=*/true);
    transaction->removeDirectory("data/database/table/all_0_0_0");

    transaction->commit(DB::NoCommitOptions{});

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
        source_part->commit(DB::NoCommitOptions{});
    }
    auto transaction = metadata->createTransaction();
    transaction->removeRecursive("data/database/table/all_0_0_0", /*should_remove_objects=*/nullptr);
    transaction->commit(DB::NoCommitOptions{});

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
            detached_dir->commit(DB::NoCommitOptions{});
        }
        auto source_part = metadata->createTransaction();
        source_part->createDirectory("data/database/table/all_0_0_0");
        source_part->writeInlineDataToFile("data/database/table/all_0_0_0/stolbets.bin", "binarydata_source");
        source_part->writeInlineDataToFile("data/database/table/all_0_0_0/stolbets.mrk2", "otherdata_source");
        source_part->createDirectory("data/database/table/all_0_0_0/someprojection.proj");
        source_part->writeInlineDataToFile("data/database/table/all_0_0_0/someprojection.proj/primary.idx", "aaaaa");
        source_part->writeInlineDataToFile("data/database/table/all_0_0_0/someprojection.proj/columns.txt", "aaaaa");
        source_part->commit(DB::NoCommitOptions{});
    }
    auto transaction = metadata->createTransaction();
    transaction->moveDirectory("data/database/table/all_0_0_0", "data/database/table/detached/broken_all_0_0_0");
    transaction->commit(DB::NoCommitOptions{});

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
        source_part->commit(DB::NoCommitOptions{});
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
    backup_transaction->commit(DB::NoCommitOptions{});
    /// Check hardlinks count
    EXPECT_EQ(metadata->getHardlinkCount("data/database/table/backup_all_0_0_0/stolbets.bin"), 1);
    EXPECT_EQ(metadata->getHardlinkCount("data/database/table/backup_all_0_0_0/stolbets.mrk2"), 1);
    EXPECT_EQ(metadata->getHardlinkCount("data/database/table/backup_all_0_0_0/someprojection.proj/primary.idx"), 1);
    EXPECT_EQ(metadata->getHardlinkCount("data/database/table/backup_all_0_0_0/someprojection.proj/columns.txt"), 1);

    update_transaction->moveDirectory("data/database/table/tmp_all_0_0_0_1", "data/database/table/all_0_0_0_1");
    update_transaction->commit(DB::NoCommitOptions{});

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

    /// Remove original dir and check hardlinks count again
    {
        auto rm_transaction = metadata->createTransaction();
        rm_transaction->removeRecursive("data/database/table/all_0_0_0", /*should_remove_objects=*/nullptr);
        rm_transaction->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsDirectory("data/database/table/backup_all_0_0_0"));
    EXPECT_FALSE(metadata->existsFileOrDirectory("data/database/table/all_0_0_0"));
    EXPECT_TRUE(metadata->existsDirectory("data/database/table/all_0_0_0_1"));
    EXPECT_EQ(metadata->getHardlinkCount("data/database/table/backup_all_0_0_0/stolbets.bin"), 1);
    EXPECT_EQ(metadata->getHardlinkCount("data/database/table/backup_all_0_0_0/stolbets.mrk2"), 1);
    EXPECT_EQ(metadata->getHardlinkCount("data/database/table/backup_all_0_0_0/someprojection.proj/primary.idx"), 1);
    EXPECT_EQ(metadata->getHardlinkCount("data/database/table/backup_all_0_0_0/someprojection.proj/columns.txt"), 1);

    /// Remove backup dir and check hardlinks count again
    {
        auto rm_backup_transaction = metadata->createTransaction();
        rm_backup_transaction->removeRecursive("data/database/table/backup_all_0_0_0", /*should_remove_objects=*/nullptr);
        rm_backup_transaction->commit(DB::NoCommitOptions{});
    }

    EXPECT_FALSE(metadata->existsFileOrDirectory("data/database/table/backup_all_0_0_0"));
    EXPECT_FALSE(metadata->existsFileOrDirectory("data/database/table/all_0_0_0"));
    EXPECT_TRUE(metadata->existsDirectory("data/database/table/all_0_0_0_1"));
    EXPECT_EQ(metadata->getHardlinkCount("data/database/table/all_0_0_0_1/stolbets.bin"), 0);
    EXPECT_EQ(metadata->getHardlinkCount("data/database/table/all_0_0_0_1/stolbets.mrk2"), 0);
    EXPECT_EQ(metadata->getHardlinkCount("data/database/table/all_0_0_0_1/someprojection.proj/primary.idx"), 0);
    EXPECT_EQ(metadata->getHardlinkCount("data/database/table/all_0_0_0_1/someprojection.proj/columns.txt"), 0);
}

TEST_F(MetadataLocalDiskTest, TestUnlinkRollbackHardlinks)
{
    auto metadata = getMetadataStorage("/TestUnlinkRollbackHardlinks");

    {
        auto tx = metadata->createTransaction();
        tx->createMetadataFile("file", /*objects=*/{});
        tx->createHardLink("file", "file-link-1");
        tx->createHardLink("file", "file-link-2");
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_EQ(metadata->getHardlinkCount("file-link-1"), 2);

    /// Check that rolled back undo will not break real hardlinks on files
    {
        auto tx = metadata->createTransaction();
        tx->unlinkFile("file-link-1", /*if_exists=*/false, /*should_remove_objects=*/true);
        tx->unlinkFile("file", /*if_exists=*/false, /*should_remove_objects=*/true);
        tx->createMetadataFile("non-existing/fail-tx", /*objects=*/{});
        EXPECT_THROW(tx->commit(DB::NoCommitOptions{}), std::exception);
    }

    EXPECT_EQ(metadata->getHardlinkCount("file"), 2);
    EXPECT_EQ(metadata->getHardlinkCount("file-link-1"), 2);
    EXPECT_EQ(metadata->getHardlinkCount("file-link-2"), 2);
}

TEST_F(MetadataLocalDiskTest, TestFoldedRemoveRecursiveRollback)
{
    auto metadata = getMetadataStorage("/TestUnlinkRollbackHardlinks");

    /// From committed state
    {
        auto tx_1 = metadata->createTransaction();
        tx_1->createDirectoryRecursive("a/b/c/d/e");
        tx_1->writeStringToFile("a/b/c/d/e/truth", "Reality is an illusion, the universe is a hologram");
        tx_1->commit(DB::NoCommitOptions{});

        EXPECT_EQ(metadata->readFileToString("a/b/c/d/e/truth"), "Reality is an illusion, the universe is a hologram");

        auto tx_2 = metadata->createTransaction();
        tx_2->removeRecursive("a/b/c/d/e", /*should_remove_objects=*/nullptr);
        tx_2->removeRecursive("a/b/c/d", /*should_remove_objects=*/nullptr);
        tx_2->removeRecursive("a/b/c", /*should_remove_objects=*/nullptr);
        tx_2->removeRecursive("a/b", /*should_remove_objects=*/nullptr);
        tx_2->createMetadataFile("non-existing/fail-tx", /*objects=*/{});
        EXPECT_THROW(tx_2->commit(DB::NoCommitOptions{}), std::exception);

        EXPECT_EQ(metadata->readFileToString("a/b/c/d/e/truth"), "Reality is an illusion, the universe is a hologram");

        auto tx_3 = metadata->createTransaction();
        tx_3->removeRecursive("a/b", /*should_remove_objects=*/nullptr);
        tx_3->removeDirectory("a");
        tx_3->commit(DB::NoCommitOptions{});

        EXPECT_FALSE(metadata->existsFile("a/b/c/d/e/truth"));
        EXPECT_FALSE(metadata->existsDirectory("a"));
    }

    /// From uncommitted state
    {
        auto tx = metadata->createTransaction();
        tx->createDirectoryRecursive("a/b/c/d/e");
        tx->writeStringToFile("a/b/c/d/e/truth", "Reality is an illusion, the universe is a hologram");
        tx->removeRecursive("a/b/c/d/e", /*should_remove_objects=*/nullptr);
        tx->removeRecursive("a/b/c/d", /*should_remove_objects=*/nullptr);
        tx->removeRecursive("a/b/c", /*should_remove_objects=*/nullptr);
        tx->removeDirectory("a/b");
        tx->commit(DB::NoCommitOptions{});

        EXPECT_FALSE(metadata->existsDirectory("a/b"));
        EXPECT_TRUE(metadata->existsDirectory("a"));
    }
}

TEST_F(MetadataLocalDiskTest, TestTruncate)
{
    auto metadata = getMetadataStorage("/TestTruncate");

    {
        auto tx = metadata->createTransaction();
        tx->addBlobToMetadata("file", DB::StoredObject(DB::ObjectStorageKey::createAsAbsolute("blob-1").serialize(), "file", 100));
        tx->addBlobToMetadata("file", DB::StoredObject(DB::ObjectStorageKey::createAsAbsolute("blob-2").serialize(), "file", 200));
        tx->addBlobToMetadata("file", DB::StoredObject(DB::ObjectStorageKey::createAsAbsolute("blob-3").serialize(), "file", 100));
        tx->addBlobToMetadata("file", DB::StoredObject(DB::ObjectStorageKey::createAsAbsolute("blob-4").serialize(), "file", 400));
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_EQ(metadata->getStorageObjects("file").size(), 4);

    /// Check that truncate combines
    {
        auto tx = metadata->createTransaction();
        tx->truncateFile("file", 300);  /// blob-1 + blob-2
        tx->truncateFile("file", 100);  /// blob-1
        tx->commit(DB::NoCommitOptions{});

        verifyBlobsToRemove(metadata, {"blob-2", "blob-3", "blob-4"});
    }

    EXPECT_EQ(metadata->getStorageObjects("file").size(), 1);
    EXPECT_EQ(metadata->getStorageObjects("file").front().remote_path, "blob-1");
}

TEST_F(MetadataLocalDiskTest, TestRecursiveCyclicRemove)
{
    auto metadata = getMetadataStorage("/TestRecursiveCyclicRemove");
    auto disk = getMetadataDisk("/TestRecursiveCyclicRemove");

    disk->createDirectory("root");
    disk->createDirectorySymlink("root", "root/root");

    /// Check that remove recursive will not hang
    {
        auto tx = metadata->createTransaction();
        tx->removeRecursive("root", /*should_remove_objects=*/nullptr);
        EXPECT_THROW(tx->commit(DB::NoCommitOptions{}), std::exception);
    }
}

TEST_F(MetadataLocalDiskTest, TestRecursiveRemoveHardlinks)
{
    auto metadata = getMetadataStorage("/TestRecursiveRemoveHardlinks");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("root");
        tx->writeInlineDataToFile("root/A", "hello");
        tx->createHardLink("root/A", "root/B");
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_EQ(metadata->readInlineDataToString("root/A"), "hello");
    EXPECT_EQ(metadata->readInlineDataToString("root/B"), "hello");

    /// Check that remove recursive will not hang
    {
        auto tx = metadata->createTransaction();
        tx->removeRecursive("root", /*should_remove_objects=*/nullptr);
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_FALSE(metadata->existsDirectory("root"));
    EXPECT_FALSE(metadata->existsFile("root/A"));
    EXPECT_FALSE(metadata->existsFile("root/B"));
}

TEST_F(MetadataLocalDiskTest, TestComplexUnlink)
{
    auto metadata = getMetadataStorage("/TestComplexUnlink");

    {
        auto tx = metadata->createTransaction();
        tx->unlinkFile("file", /*if_exists=*/true, /*should_remove_objects=*/true);
        tx->createMetadataFile("file", {DB::StoredObject("key", "file", 1)});
        tx->commit(DB::NoCommitOptions{});
        verifyBlobsToRemove(metadata, {});
    }

    {
        auto tx = metadata->createTransaction();
        tx->unlinkFile("file", /*if_exists=*/false, /*should_remove_objects=*/false);
        tx->commit(DB::NoCommitOptions{});
        verifyBlobsToRemove(metadata, {});
    }

    EXPECT_FALSE(metadata->existsFile("file"));
}

TEST_F(MetadataLocalDiskTest, TestComplexRemoveRecursive)
{
    auto metadata = getMetadataStorage("/TestComplexRemoveRecursive");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("A");
        tx->createDirectory("A/B");
        tx->createDirectory("A/B/C");
        tx->createDirectory("A/D");
        tx->createMetadataFile("A/file-1", {DB::StoredObject("key-1", "file-1", 1)});
        tx->createMetadataFile("A/B/file-2", {DB::StoredObject("key-2", "file-2", 1)});
        tx->createMetadataFile("A/B/C/file-3", {DB::StoredObject("key-3", "file-3", 1)});
        tx->createMetadataFile("A/D/file-4", {DB::StoredObject("key-4", "file-4", 1)});
        tx->commit(DB::NoCommitOptions{});
        verifyBlobsToRemove(metadata, {});
    }

    auto should_remove_objects = [](const std::string & relative_path)
    {
        return relative_path == "B/file-2" or relative_path == "B/C/file-3" or relative_path == "D/file-4";
    };

    {
        auto tx = metadata->createTransaction();
        tx->removeRecursive("A", should_remove_objects);
        tx->commit(DB::NoCommitOptions{});
        verifyBlobsToRemove(metadata, {"key-2", "key-3", "key-4"});
    }

    EXPECT_FALSE(metadata->existsFileOrDirectory("A"));
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
        transaction->createMetadataFile("file.txt", /*objects=*/{});
        transaction->commit(DB::NoCommitOptions{});
        EXPECT_TRUE(metadata->existsDirectory("dir"));
        EXPECT_TRUE(metadata->existsFile("file.txt"));
    }

    {
        auto transaction = metadata->createTransaction();
        EXPECT_THROW({
                transaction->createMetadataFile("non-existing/file.txt", /*objects=*/{});
                transaction->commit(DB::NoCommitOptions{});
            }, std::exception);
    }

    {
        auto transaction = metadata->createTransaction();
        EXPECT_THROW({
                transaction->createMetadataFile("non-existing/file.txt", {DB::StoredObject(DB::ObjectStorageKey::createAsAbsolute("blob_name").serialize(), "non-existing/file.txt", 42)});
                transaction->commit(DB::NoCommitOptions{});
            }, std::exception);
    }

    {
        auto transaction = metadata->createTransaction();
        EXPECT_THROW({
                transaction->createDirectory("non-existing/subdir");
                transaction->commit(DB::NoCommitOptions{});
            }, std::exception);
    }

    {
        auto transaction = metadata->createTransaction();
        EXPECT_THROW({
                transaction->createHardLink("non-existing", "new-file");
                transaction->commit(DB::NoCommitOptions{});
            }, std::exception);
    }

    {
        auto transaction = metadata->createTransaction();
        EXPECT_THROW({
                transaction->unlinkFile("file.txt/non-existing", /*if_exists=*/false, /*should_remove_objects=*/true);
                transaction->commit(DB::NoCommitOptions{});
            }, std::exception);
    }

    {
        auto transaction = metadata->createTransaction();
        EXPECT_THROW({
                transaction->unlinkFile("dir/non-existing", /*if_exists=*/false, /*should_remove_objects=*/true);
                transaction->commit(DB::NoCommitOptions{});
            }, std::exception);
    }

    {
        auto transaction = metadata->createTransaction();
        EXPECT_THROW({
                transaction->unlinkFile("non-existing/non-existing", /*if_exists=*/false, /*should_remove_objects=*/true);
                transaction->commit(DB::NoCommitOptions{});
            }, std::exception);
    }

    {
        auto transaction = metadata->createTransaction();
        EXPECT_THROW({
                transaction->removeDirectory("file.txt/non-existing");
                transaction->commit(DB::NoCommitOptions{});
            }, std::exception);
    }

    {
        auto transaction = metadata->createTransaction();
        EXPECT_THROW({
                transaction->removeDirectory("dir/non-existing");
                transaction->commit(DB::NoCommitOptions{});
            }, std::exception);
    }

    {
        auto transaction = metadata->createTransaction();
        EXPECT_THROW({
                transaction->removeDirectory("non-existing/non-existing");
                transaction->commit(DB::NoCommitOptions{});
            }, std::exception);
    }

    {
        auto transaction = metadata->createTransaction();
        EXPECT_NO_THROW({
                transaction->removeRecursive("file.txt/non-existing", /*should_remove_objects=*/nullptr);
                transaction->commit(DB::NoCommitOptions{});
            });
    }

    {
        auto transaction = metadata->createTransaction();
        EXPECT_NO_THROW({
                transaction->removeRecursive("dir/non-existing", /*should_remove_objects=*/nullptr);
                transaction->commit(DB::NoCommitOptions{});
            });
    }

    {
        auto transaction = metadata->createTransaction();
        EXPECT_NO_THROW({
                transaction->removeRecursive("non-existing/non-existing", /*should_remove_objects=*/nullptr);
                transaction->commit(DB::NoCommitOptions{});
            });
    }
}
