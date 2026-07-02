#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/Memory/MetadataStorageInMemory.h>

#include <Common/ObjectStorageKey.h>
#include <Common/ObjectStorageKeyGenerator.h>

#include <algorithm>


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

/// `MetadataStorageInMemory` stores entries with relative paths (e.g. `a/`, `a/file`).
/// Root-path queries must still match other backends: the root always exists and
/// `listDirectory("/")` must return top-level entries.
TEST_F(MetadataInMemoryTest, TestRootPathSemantics)
{
    auto metadata = getMetadataStorage();

    EXPECT_TRUE(metadata->existsDirectory("/"));
    EXPECT_TRUE(metadata->existsFileOrDirectory("/"));
    EXPECT_TRUE(metadata->listDirectory("/").empty());

    {
        auto transaction = metadata->createTransaction();
        transaction->createDirectory("a");
        transaction->createMetadataFile(
            "top_file",
            {DB::StoredObject(DB::ObjectStorageKey::createAsAbsolute("k1").serialize(), "top_file", 10)});
        transaction->createMetadataFile(
            "a/inner_file",
            {DB::StoredObject(DB::ObjectStorageKey::createAsAbsolute("k2").serialize(), "a/inner_file", 20)});
        transaction->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsDirectory("/"));
    EXPECT_TRUE(metadata->existsFileOrDirectory("/"));

    auto root_children = metadata->listDirectory("/");
    std::sort(root_children.begin(), root_children.end());
    EXPECT_EQ(root_children, std::vector<std::string>({"a", "top_file"}));

    /// Iterator output should match the relative form used by other lookups so values
    /// can be passed back into `existsFile` / `findFile` without mangling.
    auto iter = metadata->iterateDirectory("/");
    std::vector<std::string> iter_paths;
    for (; iter->isValid(); iter->next())
        iter_paths.push_back(iter->path());
    std::sort(iter_paths.begin(), iter_paths.end());
    EXPECT_EQ(iter_paths, std::vector<std::string>({"a", "top_file"}));
}

/// `MergeTree` reads a part directory's mtime as the part `modification_time`
/// (`DataPartStorageOnDiskBase::getLastModified`), and sets it on the temp part directory just
/// before renaming it into place. Verify directories carry timestamps: `getLastModified` does
/// not report the epoch for a directory, `setLastModified` updates it, `moveDirectory` preserves
/// it, and `setLastModified` on a non-existent path throws instead of silently dropping it.
TEST_F(MetadataInMemoryTest, TestDirectoryModificationTime)
{
    auto metadata = getMetadataStorage();

    {
        auto transaction = metadata->createTransaction();
        transaction->createDirectory("part_tmp");
        transaction->commit(DB::NoCommitOptions{});
    }

    /// A freshly created directory gets the current time as its mtime, never the epoch.
    EXPECT_GT(metadata->getLastModified("part_tmp").epochTime(), 0);

    const Poco::Timestamp ts = Poco::Timestamp::fromEpochTime(1234567890);
    {
        auto transaction = metadata->createTransaction();
        transaction->setLastModified("part_tmp", ts);
        transaction->commit(DB::NoCommitOptions{});
    }

    /// The set timestamp must be observable, whether or not the caller passes a trailing slash
    /// (part directory paths are passed without one).
    EXPECT_EQ(metadata->getLastModified("part_tmp").epochTime(), 1234567890);
    EXPECT_EQ(metadata->getLastModified("part_tmp/").epochTime(), 1234567890);

    /// Renaming the temp directory into its final place must preserve the timestamp, otherwise
    /// the part would report a 1970 `modification_time` after the move.
    {
        auto transaction = metadata->createTransaction();
        transaction->moveDirectory("part_tmp", "all_1_1_0");
        transaction->commit(DB::NoCommitOptions{});
    }

    EXPECT_FALSE(metadata->existsDirectory("part_tmp"));
    EXPECT_TRUE(metadata->existsDirectory("all_1_1_0"));
    EXPECT_EQ(metadata->getLastModified("all_1_1_0").epochTime(), 1234567890);

    /// `setLastModified` on a path that is neither a file nor a directory must throw.
    {
        auto transaction = metadata->createTransaction();
        transaction->setLastModified("does_not_exist", ts);
        EXPECT_THROW(transaction->commit(DB::NoCommitOptions{}), DB::Exception);
    }
}

/// The rollback journal must restore a directory's pre-transaction mtime when a later operation
/// in the same transaction fails, not just its existence.
TEST_F(MetadataInMemoryTest, TestDirectoryModificationTimeRollback)
{
    auto metadata = getMetadataStorage();

    const Poco::Timestamp original_ts = Poco::Timestamp::fromEpochTime(1000000000);
    {
        auto transaction = metadata->createTransaction();
        transaction->createDirectory("dir");
        transaction->setLastModified("dir", original_ts);
        transaction->commit(DB::NoCommitOptions{});
    }
    EXPECT_EQ(metadata->getLastModified("dir").epochTime(), 1000000000);

    /// Update the directory mtime, then fail later in the same transaction.
    {
        auto transaction = metadata->createTransaction();
        transaction->setLastModified("dir", Poco::Timestamp::fromEpochTime(2000000000));
        /// `createMetadataFile` under a non-existent parent throws, failing the commit.
        transaction->createMetadataFile(
            "missing_parent/file",
            {DB::StoredObject(DB::ObjectStorageKey::createAsAbsolute("k").serialize(), "missing_parent/file", 1)});
        EXPECT_THROW(transaction->commit(DB::NoCommitOptions{}), DB::Exception);
    }

    /// The original mtime must be restored.
    EXPECT_EQ(metadata->getLastModified("dir").epochTime(), 1000000000);
}

/// `truncateFile(path, 0)` must stay idempotent across metadata backends, matching the disk-backed
/// `MetadataStorageFromDisk::TruncateMetadataFileOperation` and the public `IDisk::truncateFile`
/// contract codified by `DiskObjectStorageTest.TruncateFileToZero`: truncating a missing file to
/// zero is a no-op, truncating an existing file to zero drops its blobs, and only a non-zero target
/// on a missing file is an error.
TEST_F(MetadataInMemoryTest, TestTruncateMissingFileToZero)
{
    auto metadata = getMetadataStorage();

    /// Truncating a missing file to zero is a no-op and must not create the file.
    {
        auto transaction = metadata->createTransaction();
        transaction->truncateFile("missing_file", 0);
        transaction->commit(DB::NoCommitOptions{});
    }
    EXPECT_FALSE(metadata->existsFile("missing_file"));

    /// Truncating a missing file to a non-zero size is still an error.
    {
        auto transaction = metadata->createTransaction();
        transaction->truncateFile("missing_file", 5);
        EXPECT_THROW(transaction->commit(DB::NoCommitOptions{}), DB::Exception);
    }
    EXPECT_FALSE(metadata->existsFile("missing_file"));

    /// Truncating an existing file to zero keeps the file but drops its blobs.
    {
        auto transaction = metadata->createTransaction();
        transaction->createMetadataFile(
            "existing_file",
            {DB::StoredObject(DB::ObjectStorageKey::createAsAbsolute("k1").serialize(), "existing_file", 111)});
        transaction->commit(DB::NoCommitOptions{});
    }
    EXPECT_EQ(metadata->getFileSize("existing_file"), 111);

    {
        auto transaction = metadata->createTransaction();
        transaction->truncateFile("existing_file", 0);
        transaction->commit(DB::NoCommitOptions{});
    }
    EXPECT_TRUE(metadata->existsFile("existing_file"));
    EXPECT_EQ(metadata->getFileSize("existing_file"), 0);
    EXPECT_TRUE(metadata->getStorageObjects("existing_file").empty());
}
