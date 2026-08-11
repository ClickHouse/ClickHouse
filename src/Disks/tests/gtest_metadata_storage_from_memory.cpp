#include <Disks/DiskObjectStorage/DiskObjectStorage.h>
#include <Disks/DiskObjectStorage/DiskObjectStorageTransaction.h>
#include <Disks/DiskObjectStorage/MetadataStorages/Local/MetadataStorageFromDisk.h>
#include <Disks/DiskObjectStorage/MetadataStorages/Memory/MetadataStorageFromMemory.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Disks/DiskObjectStorage/Replication/ClusterConfiguration.h>
#include <Disks/DiskObjectStorage/Replication/ObjectStorageRouter.h>
#include <Disks/DiskLocal.h>
#include <Common/Exception.h>
#include <Common/ObjectStorageKeyGenerator.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Poco/TemporaryFile.h>
#include <Poco/Util/XMLConfiguration.h>

#include <gtest/gtest.h>

#include <filesystem>

namespace fs = std::filesystem;
using namespace DB;

namespace
{

std::shared_ptr<MetadataStorageFromMemory> makeWritableStorage()
{
    return std::make_shared<MetadataStorageFromMemory>(
        "/", createObjectStorageKeyGeneratorByPrefix("test-prefix/"));
}

StoredObjects singleObject(const String & remote_path, const String & local_path, uint64_t bytes_size)
{
    return {StoredObject(remote_path, local_path, bytes_size)};
}

}

TEST(MetadataStorageFromMemory, OverwriteReleasesCreatedBlob)
{
    auto storage = makeWritableStorage();
    auto tx = storage->createTransaction();

    tx->createMetadataFile("a.bin", singleObject("blobs/first", "a.bin", 10));
    tx->createMetadataFile("a.bin", singleObject("blobs/second", "a.bin", 20));

    EXPECT_TRUE(storage->existsFile("a.bin"));
    EXPECT_EQ(storage->getFileSize("a.bin"), 20u);
    EXPECT_EQ(storage->getStorageObjects("a.bin").at(0).remote_path, "blobs/second");

    EXPECT_EQ(storage->takePendingOwnRemovals(), std::vector<String>{"blobs/first"});
    /// The accumulator is take-out.
    EXPECT_TRUE(storage->takePendingOwnRemovals().empty());
}

TEST(MetadataStorageFromMemory, OverwriteDropsSharedBlob)
{
    auto storage = makeWritableStorage();
    auto tx = storage->createTransaction();
    tx->createMetadataFile("a.bin", singleObject("blobs/shared", "a.bin", 10));
    tx->incrementBlobRefCount("blobs/shared");

    tx->createMetadataFile("a.bin", singleObject("blobs/fresh", "a.bin", 20));

    /// The shared blob belongs to another owner: it must not be scheduled for deletion.
    EXPECT_TRUE(storage->takePendingOwnRemovals().empty());
    EXPECT_EQ(storage->getStorageObjects("a.bin").at(0).remote_path, "blobs/fresh");
    /// The rewrite replaced the record, so the share mark is gone with it.
    EXPECT_EQ(storage->getHardlinkCount("a.bin"), 0u);
}

TEST(MetadataStorageFromMemory, InlineOverwriteReleasesCreatedBlob)
{
    auto storage = makeWritableStorage();
    auto tx = storage->createTransaction();

    /// A same-transaction rewrite of a blob-backed file by an inline one must reclaim the blob.
    tx->createMetadataFile("a.bin", singleObject("blobs/first", "a.bin", 10));
    tx->writeInlineDataToFile("a.bin", "tiny");

    EXPECT_EQ(storage->takePendingOwnRemovals(), std::vector<String>{"blobs/first"});
    EXPECT_EQ(storage->readInlineDataToString("a.bin"), "tiny");
    EXPECT_EQ(storage->getFileSize("a.bin"), 4u);
    EXPECT_TRUE(storage->getStorageObjects("a.bin").empty());
}

TEST(MetadataStorageFromMemory, UnlinkFileRouting)
{
    auto storage = makeWritableStorage();
    auto tx = storage->createTransaction();

    tx->createMetadataFile("created_removed.bin", singleObject("blobs/created_removed", "created_removed.bin", 1));
    tx->createMetadataFile("created_kept.bin", singleObject("blobs/created_kept", "created_kept.bin", 1));
    tx->createMetadataFile("shared.bin", singleObject("blobs/shared", "shared.bin", 1));
    tx->incrementBlobRefCount("blobs/shared");

    tx->unlinkFile("created_removed.bin", /*if_exists=*/false, /*should_remove_objects=*/true);
    tx->unlinkFile("created_kept.bin", /*if_exists=*/false, /*should_remove_objects=*/false);
    tx->unlinkFile("shared.bin", /*if_exists=*/false, /*should_remove_objects=*/true);
    tx->unlinkFile("no_such_file.bin", /*if_exists=*/true, /*should_remove_objects=*/true);

    EXPECT_THROW(tx->unlinkFile("no_such_file.bin", /*if_exists=*/false, /*should_remove_objects=*/true), Exception);

    /// Only the owned record removed with should_remove_objects=true releases its blob.
    EXPECT_EQ(storage->takePendingOwnRemovals(), std::vector<String>{"blobs/created_removed"});
    EXPECT_FALSE(storage->existsFile("created_removed.bin"));
    EXPECT_FALSE(storage->existsFile("created_kept.bin"));
    EXPECT_FALSE(storage->existsFile("shared.bin"));
}

TEST(MetadataStorageFromMemory, MoveDirectoryRekeysSubtree)
{
    auto storage = makeWritableStorage();
    auto tx = storage->createTransaction();

    tx->createDirectory("sub.tmp");
    tx->createMetadataFile("sub.tmp/a.bin", singleObject("blobs/a", "sub.tmp/a.bin", 1));
    tx->writeInlineDataToFile("sub.tmp/b.txt", "b");
    tx->createMetadataFile("other.bin", singleObject("blobs/other", "other.bin", 1));

    tx->moveDirectory("sub.tmp", "sub");

    EXPECT_FALSE(storage->existsDirectory("sub.tmp"));
    EXPECT_TRUE(storage->existsDirectory("sub"));
    EXPECT_TRUE(storage->existsFile("sub/a.bin"));
    EXPECT_TRUE(storage->existsFile("sub/b.txt"));
    EXPECT_FALSE(storage->existsFile("sub.tmp/a.bin"));
    EXPECT_TRUE(storage->existsFile("other.bin"));

    EXPECT_EQ(storage->listDirectory(""), (std::vector<String>{"other.bin", "sub"}));
    EXPECT_EQ(storage->getStorageObjects("sub/a.bin").at(0).local_path, "sub/a.bin");

    /// A rename is not a removal: nothing is scheduled for deletion.
    EXPECT_TRUE(storage->takePendingOwnRemovals().empty());

    EXPECT_THROW(tx->moveDirectory("no_such_dir", "somewhere"), Exception);
    tx->createDirectory("other.tmp");
    EXPECT_THROW(tx->moveDirectory("other.tmp", "sub"), Exception);
}

TEST(MetadataStorageFromMemory, MoveDirectoryDoesNotTouchSimilarlyNamedSibling)
{
    auto storage = makeWritableStorage();
    auto tx = storage->createTransaction();

    tx->createDirectory("sub");
    tx->createDirectory("sub2");
    tx->createMetadataFile("sub/a.bin", singleObject("blobs/a", "sub/a.bin", 1));
    tx->createMetadataFile("sub2/b.bin", singleObject("blobs/b", "sub2/b.bin", 1));

    tx->moveDirectory("sub", "renamed");

    EXPECT_TRUE(storage->existsFile("renamed/a.bin"));
    EXPECT_TRUE(storage->existsFile("sub2/b.bin"));
    EXPECT_FALSE(storage->existsFile("renamed2/b.bin"));
}

TEST(MetadataStorageFromMemory, EmptyFileHasNoBlobAndNoInlineData)
{
    auto storage = makeWritableStorage();
    ASSERT_TRUE(storage->supportsEmptyFilesWithoutBlobs());

    auto tx = storage->createTransaction();
    tx->createMetadataFile("empty.bin", /*objects=*/{});

    EXPECT_TRUE(storage->existsFile("empty.bin"));
    EXPECT_EQ(storage->getFileSize("empty.bin"), 0u);
    EXPECT_TRUE(storage->getStorageObjects("empty.bin").empty());
    EXPECT_TRUE(storage->readInlineDataToString("empty.bin").empty());
}

TEST(MetadataStorageFromMemory, MoveFileSemantics)
{
    auto storage = makeWritableStorage();
    auto tx = storage->createTransaction();

    tx->createMetadataFile("from.bin", singleObject("blobs/from", "from.bin", 5));
    tx->createMetadataFile("occupied.bin", singleObject("blobs/occupied", "occupied.bin", 5));

    EXPECT_THROW(tx->moveFile("no_such.bin", "to.bin"), Exception);
    EXPECT_THROW(tx->moveFile("from.bin", "occupied.bin"), Exception);

    tx->moveFile("from.bin", "to.bin");
    EXPECT_FALSE(storage->existsFile("from.bin"));
    EXPECT_TRUE(storage->existsFile("to.bin"));
    EXPECT_EQ(storage->getStorageObjects("to.bin").at(0).remote_path, "blobs/from");
    EXPECT_EQ(storage->getStorageObjects("to.bin").at(0).local_path, "to.bin");

    /// A move is not a removal.
    EXPECT_TRUE(storage->takePendingOwnRemovals().empty());
}

TEST(MetadataStorageFromMemory, ReplaceFileOverwritesDestination)
{
    auto storage = makeWritableStorage();
    auto tx = storage->createTransaction();

    tx->createMetadataFile("from.bin", singleObject("blobs/from", "from.bin", 5));
    tx->createMetadataFile("occupied.bin", singleObject("blobs/occupied", "occupied.bin", 5));

    tx->replaceFile("from.bin", "occupied.bin");

    EXPECT_FALSE(storage->existsFile("from.bin"));
    EXPECT_TRUE(storage->existsFile("occupied.bin"));
    EXPECT_EQ(storage->getStorageObjects("occupied.bin").at(0).remote_path, "blobs/from");
    EXPECT_EQ(storage->getStorageObjects("occupied.bin").at(0).local_path, "occupied.bin");
    /// The overwritten record's blob is queued for disposal.
    EXPECT_EQ(storage->takePendingOwnRemovals(), std::vector<String>{"blobs/occupied"});

    /// Replacing onto a free name is a plain move.
    tx->replaceFile("occupied.bin", "free.bin");
    EXPECT_TRUE(storage->existsFile("free.bin"));
    EXPECT_FALSE(storage->existsFile("occupied.bin"));

    EXPECT_THROW(tx->replaceFile("no_such.bin", "x.bin"), Exception);
}

TEST(MetadataStorageFromMemory, RemoveRecursiveHonorsPredicate)
{
    auto storage = makeWritableStorage();
    auto tx = storage->createTransaction();

    tx->createDirectory("sub");
    tx->createMetadataFile("sub/kept.bin", singleObject("blobs/kept", "sub/kept.bin", 1));
    tx->createMetadataFile("sub/removed.bin", singleObject("blobs/removed", "sub/removed.bin", 1));
    tx->createMetadataFile("outside.bin", singleObject("blobs/outside", "outside.bin", 1));

    /// The predicate receives paths relative to the removed root.
    tx->removeRecursive("sub", [](const std::string & path) { return path == "removed.bin"; });

    EXPECT_FALSE(storage->existsDirectory("sub"));
    EXPECT_FALSE(storage->existsFile("sub/kept.bin"));
    EXPECT_FALSE(storage->existsFile("sub/removed.bin"));
    EXPECT_TRUE(storage->existsFile("outside.bin"));
    EXPECT_EQ(storage->takePendingOwnRemovals(), std::vector<String>{"blobs/removed"});
}

TEST(MetadataStorageFromMemory, ReplicationRecordsAccumulate)
{
    auto storage = makeWritableStorage();
    auto tx = storage->createTransaction();

    tx->recordBlobsReplication(StoredObject("blobs/a", "a.bin", 1), {"eu-west-1"});
    tx->recordBlobsReplication(StoredObject("blobs/a", "a.bin", 1), {"us-east-1"});
    tx->recordBlobsReplication(StoredObject("blobs/b", "b.bin", 1), {"eu-west-1"});
    tx->recordBlobsReplication(StoredObject("blobs/c", "c.bin", 1), /*missing_locations=*/{});

    auto records = storage->takeReplicationRecords();
    ASSERT_EQ(records.size(), 2u);
    EXPECT_EQ(records.at("blobs/a"), (Locations{"eu-west-1", "us-east-1"}));
    EXPECT_EQ(records.at("blobs/b"), Locations{"eu-west-1"});

    EXPECT_TRUE(storage->takeReplicationRecords().empty());
}

TEST(MetadataStorageFromMemory, DirectoriesAndSubdirectoryListing)
{
    auto storage = makeWritableStorage();
    auto tx = storage->createTransaction();

    /// An explicitly created empty directory must exist.
    tx->createDirectory("empty.dir");
    EXPECT_TRUE(storage->existsDirectory("empty.dir"));
    EXPECT_TRUE(storage->existsFileOrDirectory("empty.dir"));
    EXPECT_FALSE(storage->existsFile("empty.dir"));

    tx->createDirectory("sub.dir");
    tx->createMetadataFile("sub.dir/a.bin", singleObject("blobs/a", "sub.dir/a.bin", 1));
    EXPECT_TRUE(storage->existsDirectory("sub.dir"));

    tx->createMetadataFile("root.bin", singleObject("blobs/root", "root.bin", 1));

    /// listDirectory returns direct children only, files and directories alike.
    EXPECT_EQ(storage->listDirectory(""), (std::vector<String>{"empty.dir", "root.bin", "sub.dir"}));
    EXPECT_EQ(storage->listDirectory("sub.dir"), std::vector<String>{"sub.dir/a.bin"});

    /// The iterator yields both files and subdirectories; `existsFile` distinguishes them.
    std::vector<String> root_files;
    std::vector<String> root_dirs;
    for (auto it = storage->iterateDirectory(""); it->isValid(); it->next())
        (storage->existsFile(it->path()) ? root_files : root_dirs).push_back(it->name());
    EXPECT_EQ(root_files, std::vector<String>{"root.bin"});
    EXPECT_EQ(root_dirs, (std::vector<String>{"empty.dir", "sub.dir"}));

    std::vector<String> subdir_files;
    for (auto it = storage->iterateDirectory("sub.dir"); it->isValid(); it->next())
        if (storage->existsFile(it->path()))
            subdir_files.push_back(it->name());
    EXPECT_EQ(subdir_files, std::vector<String>{"a.bin"});

    tx->removeDirectory("empty.dir");
    EXPECT_FALSE(storage->existsDirectory("empty.dir"));
}

TEST(MetadataStorageFromMemory, RemoveDirectoryValidatesTarget)
{
    auto storage = makeWritableStorage();
    auto tx = storage->createTransaction();

    tx->createMetadataFile("file.bin", singleObject("blobs/file", "file.bin", 1));
    tx->createDirectory("dir");

    EXPECT_THROW(tx->removeDirectory("no_such"), Exception);
    EXPECT_THROW(tx->removeDirectory("file.bin"), Exception);
    EXPECT_THROW(tx->removeDirectory("file.bin/no_such"), Exception);

    tx->removeDirectory("dir");
    EXPECT_FALSE(storage->existsDirectory("dir"));
}

TEST(MetadataStorageFromMemory, PathTopologyIsEnforced)
{
    auto storage = makeWritableStorage();
    auto tx = storage->createTransaction();

    tx->createMetadataFile("file.bin", singleObject("blobs/file", "file.bin", 1));

    /// A file or directory can only be created under an existing directory.
    EXPECT_THROW(tx->createMetadataFile("missing/a.bin", singleObject("blobs/a", "missing/a.bin", 1)), Exception);
    EXPECT_THROW(tx->writeInlineDataToFile("missing/b.txt", "b"), Exception);
    EXPECT_THROW(tx->createDirectory("missing/sub"), Exception);

    /// A file component cannot act as a directory.
    EXPECT_THROW(tx->createMetadataFile("file.bin/a.bin", singleObject("blobs/a", "file.bin/a.bin", 1)), Exception);
    EXPECT_THROW(tx->createDirectory("file.bin"), Exception);
    EXPECT_THROW(tx->createDirectoryRecursive("file.bin/sub"), Exception);

    /// Moves land under existing directories only.
    EXPECT_THROW(tx->moveFile("file.bin", "missing/file.bin"), Exception);
    EXPECT_THROW(tx->replaceFile("file.bin", "missing/file.bin"), Exception);

    tx->createDirectoryRecursive("a/b/c");
    EXPECT_TRUE(storage->existsDirectory("a/b/c"));

    /// A directory cannot move under itself.
    EXPECT_THROW(tx->moveDirectory("a", "a/b/d"), Exception);

    /// An existing directory is kept as is.
    tx->createDirectory("a");
    EXPECT_TRUE(storage->existsDirectory("a/b/c"));

    EXPECT_TRUE(storage->existsFile("file.bin"));
}

TEST(MetadataStorageFromMemory, IncrementBlobRefCountBumpsHardlinkCount)
{
    auto storage = makeWritableStorage();
    auto tx = storage->createTransaction();

    tx->createMetadataFile("shared.bin", singleObject("blobs/shared", "shared.bin", 1));
    tx->createMetadataFile("own.bin", singleObject("blobs/own", "own.bin", 1));

    tx->incrementBlobRefCount("blobs/shared");
    tx->incrementBlobRefCount("blobs/shared");
    EXPECT_EQ(storage->getHardlinkCount("shared.bin"), 2u);
    EXPECT_EQ(storage->getHardlinkCount("own.bin"), 0u);

    tx->decrementBlobRefCount("blobs/shared");
    EXPECT_EQ(storage->getHardlinkCount("shared.bin"), 1u);

    /// A blob no record references cannot be counted; a missing file cannot be queried.
    EXPECT_THROW(tx->incrementBlobRefCount("blobs/unknown"), Exception);
    EXPECT_THROW(storage->getHardlinkCount("no_such_file.bin"), Exception);
}

TEST(MetadataStorageFromMemory, TransientBuildStateDetection)
{
    auto storage = makeWritableStorage();
    auto tx = storage->createTransaction();
    EXPECT_FALSE(storage->hasTransientBuildState());

    /// Plain records are content, not transient state.
    tx->createMetadataFile("owned.bin", singleObject("blobs/owned", "owned.bin", 1));
    EXPECT_FALSE(storage->hasTransientBuildState());

    tx->recordBlobsReplication(StoredObject("blobs/owned", "owned.bin", 1), {"eu-west-1"});
    EXPECT_TRUE(storage->hasTransientBuildState());
    storage->takeReplicationRecords();
    EXPECT_FALSE(storage->hasTransientBuildState());

    tx->createMetadataFile("shared.bin", singleObject("blobs/shared", "shared.bin", 1));
    tx->incrementBlobRefCount("blobs/shared");
    EXPECT_TRUE(storage->hasTransientBuildState());
    tx->decrementBlobRefCount("blobs/shared");
    EXPECT_FALSE(storage->hasTransientBuildState());

    tx->createMetadataFile("dropped.bin", singleObject("blobs/dropped", "dropped.bin", 1));
    tx->unlinkFile("dropped.bin", /*if_exists=*/false, /*should_remove_objects=*/true);
    EXPECT_TRUE(storage->hasTransientBuildState());
    storage->takePendingOwnRemovals();
    EXPECT_FALSE(storage->hasTransientBuildState());
}

/// A real disk transaction bound to a memory metadata storage: uploads hit the real object
/// storage, metadata applies immediately.
class DiskObjectStorageOverMemoryMetadataTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        temp_dir = std::make_unique<Poco::TemporaryFile>();
        temp_dir->createDirectories();

        const String dir = temp_dir->path() + "/";
        object_storage = std::make_shared<LocalObjectStorage>(
            LocalObjectStorageSettings("test", dir + "blobs", /*read_only_=*/false));
        DiskPtr metadata_disk = std::make_shared<DiskLocal>("metadata_disk", dir + "metadata");
        MetadataStoragePtr metadata_storage = std::make_shared<MetadataStorageFromDisk>(
            metadata_disk, "/", object_storage->createKeyGenerator(),
            /*persist_removal_queue_=*/false, /*removal_log_compaction_threshold_=*/1000);

        std::unordered_map<Location, LocationInfo> cluster_registry = {{"main", {true, true, ""}}};
        std::unordered_map<Location, ObjectStoragePtr> object_storage_registry = {{"main", object_storage}};

        ClusterConfigurationPtr cluster = std::make_shared<ClusterConfiguration>("local_blobs", std::move(cluster_registry));
        ObjectStorageRouterPtr object_storages = std::make_shared<ObjectStorageRouter>(std::move(object_storage_registry));

        Poco::AutoPtr<Poco::Util::XMLConfiguration> config(new Poco::Util::XMLConfiguration());
        disk = std::make_shared<DiskObjectStorage>(
            "local_blobs", std::move(cluster), std::move(metadata_storage), std::move(object_storages), nullptr, *config, "");
    }

    size_t countBlobs() const
    {
        size_t count = 0;
        const String prefix = object_storage->getCommonKeyPrefix();
        if (!fs::exists(prefix))
            return 0;
        for (const auto & entry : fs::recursive_directory_iterator(prefix))
            if (entry.is_regular_file())
                ++count;
        return count;
    }

    std::unique_ptr<Poco::TemporaryFile> temp_dir;
    std::shared_ptr<LocalObjectStorage> object_storage;
    std::shared_ptr<DiskObjectStorage> disk;
};

TEST_F(DiskObjectStorageOverMemoryMetadataTest, WritesApplyEagerly)
{
    auto wrapped = disk->wrapWithMemoryMetadata();
    auto memory = std::dynamic_pointer_cast<MetadataStorageFromMemory>(wrapped->getMetadataStorage());
    ASSERT_TRUE(memory);
    ASSERT_FALSE(memory->isReadOnly());
    ASSERT_TRUE(memory->appliesOperationsEagerly());

    auto tx = wrapped->createTransaction();

    {
        auto buf = tx->writeFile("data.bin", 4096, WriteMode::Rewrite, {});
        writeString("hello", *buf);
        buf->finalize();
    }

    /// The write is visible in the tree while the transaction is still open...
    EXPECT_TRUE(memory->existsFile("data.bin"));
    EXPECT_EQ(memory->getFileSize("data.bin"), 5u);
    auto objects = memory->getStorageObjects("data.bin");
    ASSERT_EQ(objects.size(), 1u);
    /// ...and the blob physically exists in the object storage.
    EXPECT_TRUE(fs::exists(objects.at(0).remote_path));

    EXPECT_EQ(memory->getHardlinkCount("data.bin"), 0u);

    /// An eagerly applied removal releases the created blob for pre-commit disposal.
    tx->removeFile("data.bin");
    EXPECT_FALSE(memory->existsFile("data.bin"));
    EXPECT_EQ(memory->takePendingOwnRemovals(), std::vector<String>{objects.at(0).remote_path});

    /// The blob is still tracked by the transaction and reclaimed by undo.
    tx->undo();
    EXPECT_FALSE(fs::exists(objects.at(0).remote_path));
}

TEST_F(DiskObjectStorageOverMemoryMetadataTest, SmallWritesAreInlinedByTheTransaction)
{
    auto wrapped = disk->wrapWithMemoryMetadata();
    auto memory = wrapped->getMetadataStorage();
    ASSERT_TRUE(memory->supportsInlineData());
    auto tx = wrapped->createTransaction();

    WriteSettings settings;
    settings.inline_file_max_bytes = 16;

    const size_t blobs_before = countBlobs();

    /// Content within the threshold is stored inline at finalize; no blob is written.
    {
        auto buf = tx->writeFile("small.txt", 4096, WriteMode::Rewrite, settings);
        writeString("tiny", *buf);
        buf->finalize();
    }
    EXPECT_EQ(memory->readInlineDataToString("small.txt"), "tiny");
    EXPECT_TRUE(memory->getStorageObjects("small.txt").empty());
    EXPECT_EQ(countBlobs(), blobs_before);

    /// The first byte past the threshold spills into the ordinary blob pipeline.
    {
        auto buf = tx->writeFile("big.bin", 4096, WriteMode::Rewrite, settings);
        writeString("0123456789abcdef-and-then-some", *buf);
        buf->finalize();
    }
    EXPECT_TRUE(memory->readInlineDataToString("big.bin").empty());
    ASSERT_EQ(memory->getStorageObjects("big.bin").size(), 1u);
    EXPECT_EQ(memory->getFileSize("big.bin"), 30u);
    EXPECT_EQ(countBlobs(), blobs_before + 1);

    tx->undo();
}

TEST_F(DiskObjectStorageOverMemoryMetadataTest, CopyFilePreservesInlineContent)
{
    auto wrapped = disk->wrapWithMemoryMetadata();
    auto memory = wrapped->getMetadataStorage();
    auto tx = wrapped->createTransaction();

    WriteSettings settings;
    settings.inline_file_max_bytes = 16;

    const size_t blobs_before = countBlobs();

    {
        auto buf = tx->writeFile("small.txt", 4096, WriteMode::Rewrite, settings);
        writeString("tiny", *buf);
        buf->finalize();
    }

    /// The copy carries the inline content over; no blob appears on either side.
    tx->copyFile("small.txt", "copy.txt", /*read_settings=*/{}, /*write_settings=*/{});
    EXPECT_EQ(memory->readInlineDataToString("copy.txt"), "tiny");
    EXPECT_TRUE(memory->getStorageObjects("copy.txt").empty());
    EXPECT_EQ(countBlobs(), blobs_before);

    tx->undo();
}

TEST_F(DiskObjectStorageOverMemoryMetadataTest, EmptyFileUploadsNoBlob)
{
    auto wrapped = disk->wrapWithMemoryMetadata();
    auto memory = wrapped->getMetadataStorage();
    auto tx = wrapped->createTransaction();

    const size_t blobs_before = countBlobs();
    tx->createFile("empty.bin");

    EXPECT_TRUE(memory->existsFile("empty.bin"));
    EXPECT_EQ(memory->getFileSize("empty.bin"), 0u);
    EXPECT_TRUE(memory->getStorageObjects("empty.bin").empty());
    EXPECT_EQ(countBlobs(), blobs_before);
}

TEST_F(DiskObjectStorageOverMemoryMetadataTest, PendingOwnRemovalsAwaitCommitOrUndo)
{
    auto wrapped = disk->wrapWithMemoryMetadata();
    auto memory = std::dynamic_pointer_cast<MetadataStorageFromMemory>(wrapped->getMetadataStorage());
    ASSERT_TRUE(memory);
    auto tx = wrapped->createTransaction();

    for (const auto * name : {"kept.bin", "removed.bin"})
    {
        auto buf = tx->writeFile(name, 4096, WriteMode::Rewrite, {});
        writeString("content", *buf);
        buf->finalize();
    }

    const auto kept_blob = memory->getStorageObjects("kept.bin").at(0).remote_path;
    tx->removeFile("removed.bin");

    /// The dropped blob is only recorded; physical deletion is the caller's job.
    auto pending = memory->takePendingOwnRemovals();
    ASSERT_EQ(pending.size(), 1u);
    EXPECT_TRUE(fs::exists(pending.at(0)));
    EXPECT_TRUE(fs::exists(kept_blob));

    tx->undo();
    EXPECT_FALSE(fs::exists(pending.at(0)));
    EXPECT_FALSE(fs::exists(kept_blob));
}

TEST_F(DiskObjectStorageOverMemoryMetadataTest, UndoRemovesRewrittenBlobs)
{
    auto wrapped = disk->wrapWithMemoryMetadata();
    auto memory = std::dynamic_pointer_cast<MetadataStorageFromMemory>(wrapped->getMetadataStorage());
    ASSERT_TRUE(memory);
    auto tx = wrapped->createTransaction();

    for (const auto * content : {"first", "second"})
    {
        auto buf = tx->writeFile("rewritten.bin", 4096, WriteMode::Rewrite, {});
        writeString(content, *buf);
        buf->finalize();
    }

    /// The overwritten first blob awaits pre-commit disposal, the live one backs the file.
    EXPECT_EQ(memory->takePendingOwnRemovals().size(), 1u);
    EXPECT_EQ(countBlobs(), 2u);

    /// undo drops everything the transaction uploaded, including the overwritten blob.
    tx->undo();
    EXPECT_EQ(countBlobs(), 0u);
}
