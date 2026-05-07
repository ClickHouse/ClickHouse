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

/// Verifies that on a partial-failure exception during commit, the operation-level
/// rollback journal restores files, directories, and shared `BlobGroup` content
/// (including hardlink `ref_count`) to the pre-transaction state.
TEST_F(MetadataInMemoryTest, TestRollbackRestoresAllTouchedState)
{
    auto metadata = getMetadataStorage();

    /// Seed: two files (one with a hardlink) and a directory.
    {
        auto transaction = metadata->createTransaction();
        transaction->createDirectory("dir");
        transaction->createMetadataFile(
            "dir/a",
            {DB::StoredObject(DB::ObjectStorageKey::createAsAbsolute("k1").serialize(), "dir/a", 100)});
        transaction->createMetadataFile(
            "dir/b",
            {DB::StoredObject(DB::ObjectStorageKey::createAsAbsolute("k2").serialize(), "dir/b", 200)});
        transaction->createHardLink("dir/a", "dir/a_link");
        transaction->commit(DB::NoCommitOptions{});
    }

    EXPECT_EQ(metadata->getHardlinkCount("dir/a"), 1);
    EXPECT_EQ(metadata->getHardlinkCount("dir/a_link"), 1);
    EXPECT_EQ(metadata->getFileSize("dir/a"), 100);
    EXPECT_EQ(metadata->getFileSize("dir/b"), 200);

    /// Transaction with a successful early op + a failing later op:
    /// - createDirectory("new_dir")  -> succeeds
    /// - unlinkFile("dir/a", ...)    -> succeeds; decrements `BlobGroup::ref_count`,
    ///                                  erases metadata entry
    /// - createMetadataFile("dir/a_link", ...) -> would mutate the shared BlobGroup
    /// - createDirectory("dir")      -> FAILS: a file with the same name already exists
    /// All earlier mutations must be rolled back.
    {
        auto transaction = metadata->createTransaction();
        transaction->createDirectory("new_dir");
        transaction->unlinkFile("dir/a", /* if_exists= */ false, /* should_remove_objects= */ true);
        transaction->createMetadataFile(
            "dir/a_link",
            {DB::StoredObject(DB::ObjectStorageKey::createAsAbsolute("k3").serialize(), "dir/a_link", 333)});
        transaction->createDirectory("dir/b");

        EXPECT_THROW(transaction->commit(DB::NoCommitOptions{}), DB::Exception);
    }

    /// Everything from the failed transaction must be reverted:
    /// - "new_dir" should not exist
    /// - "dir/a" should still exist with original size and hardlink count
    /// - "dir/a_link" should still see original blob (k1, 100), not (k3, 333)
    /// - hardlink ref counts restored
    EXPECT_FALSE(metadata->existsDirectory("new_dir"));
    EXPECT_TRUE(metadata->existsFile("dir/a"));
    EXPECT_TRUE(metadata->existsFile("dir/a_link"));
    EXPECT_EQ(metadata->getHardlinkCount("dir/a"), 1);
    EXPECT_EQ(metadata->getHardlinkCount("dir/a_link"), 1);
    EXPECT_EQ(metadata->getFileSize("dir/a"), 100);
    EXPECT_EQ(metadata->getFileSize("dir/a_link"), 100);

    auto blobs = metadata->getStorageObjects("dir/a_link");
    ASSERT_EQ(blobs.size(), 1);
    EXPECT_EQ(blobs[0].remote_path, "k1");
    EXPECT_EQ(blobs[0].bytes_size, 100);

    /// `dir/b` is still untouched.
    EXPECT_EQ(metadata->getFileSize("dir/b"), 200);
}
