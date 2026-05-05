#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/Memory/MetadataStorageInMemory.h>

#include <Common/ObjectStorageKey.h>
#include <Common/ObjectStorageKeyGenerator.h>


class MetadataInMemoryTest : public testing::Test
{
public:
    std::shared_ptr<DB::MetadataStorageInMemory> getMetadataStorage()
    {
        auto key_generator = DB::createObjectStorageKeyGeneratorByTemplate("[a-z]{32}");
        return std::make_shared<DB::MetadataStorageInMemory>(/*compatible_key_prefix_=*/"", key_generator);
    }
};

/// Mirrors `MetadataLocalDiskTest::TestHardlinkRewrite`: rewriting a hardlinked file via
/// `createMetadataFile` must update the shared metadata so every link observes the new content.
TEST_F(MetadataInMemoryTest, TestHardlinkRewrite)
{
    auto metadata = getMetadataStorage();

    {
        auto transaction = metadata->createTransaction();
        transaction->createMetadataFile(
            "original_file",
            {DB::StoredObject(DB::ObjectStorageKey::createAsAbsolute("key1").serialize(), "original_file", 111)});
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
        transaction->createMetadataFile(
            "hardlinked_file",
            {DB::StoredObject(DB::ObjectStorageKey::createAsAbsolute("key2").serialize(), "hardlinked_file", 222)});
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

    EXPECT_EQ(hardlinked_blobs[0].remote_path, "key2");
    EXPECT_EQ(hardlinked_blobs[0].bytes_size, 222);
}
