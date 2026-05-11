#include <gtest/gtest.h>

#include <Storages/MergeTree/ANNIndex/ANNGroupStorageDiskFull.h>
#include <Storages/MergeTree/ANNIndex/IANNGroupStorage.h>

#include <Disks/DiskLocal.h>
#include <Disks/IDisk.h>
#include <Disks/IDiskTransaction.h>
#include <Disks/SingleDiskVolume.h>

#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>

#include <Poco/TemporaryFile.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>

using namespace DB;

namespace
{
namespace fs = std::filesystem;

fs::path makeUniqueTestDir(const std::string & name)
{
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    auto dir = fs::temp_directory_path() / ("clickhouse-ann-group-" + name + "-" + std::to_string(now));
    fs::create_directories(dir);
    return dir;
}

class TempDirScope
{
public:
    explicit TempDirScope(const std::string & name)
        : path(makeUniqueTestDir(name))
    {
    }

    ~TempDirScope()
    {
        std::error_code ec;
        fs::remove_all(path, ec);
    }

    fs::path path;
};

std::string readAll(IANNGroupStorage & storage, const std::string & name)
{
    const size_t size = storage.getFileSize(name);
    auto in = storage.readFile(name, ReadSettings{}, size);
    std::string out;
    out.resize(size);
    if (size > 0)
        in->readStrict(out.data(), size);
    return out;
}

void writeAll(IANNGroupStorage & storage, const std::string & name, std::string_view payload)
{
    auto buf = storage.writeFile(name, 4096, WriteMode::Rewrite, WriteSettings{});
    buf->write(payload.data(), payload.size());
    buf->finalize();
}

VolumePtr makeLocalVolume(const fs::path & root)
{
    auto disk = std::make_shared<DiskLocal>("ann_local_disk", root.string() + "/");
    return std::make_shared<SingleDiskVolume>("ann_vol", disk);
}

}

TEST(ANNGroupStorageTest, PathsAndBasicFileOps)
{
    TempDirScope scope("paths");
    auto volume = makeLocalVolume(scope.path);
    const std::string rel_group = "ann_ABCDEF";
    volume->getDisk(0)->createDirectory(rel_group);

    ANNGroupStorageDiskFull storage(volume, rel_group);
    EXPECT_TRUE(storage.exists());
    EXPECT_EQ(storage.getRelativePath(), rel_group);
    EXPECT_EQ(storage.getGroupDir(), rel_group);

    const fs::path expected_full = fs::path(volume->getDisk(0)->getPath()) / rel_group;
    EXPECT_EQ(fs::path(storage.getFullPath()), expected_full);

    writeAll(storage, "a.bin", "alpha");
    writeAll(storage, "b.bin", "bravo-bravo");
    writeAll(storage, "c.bin", "charlie!");

    EXPECT_TRUE(storage.existsFile("a.bin"));
    EXPECT_TRUE(storage.existsFile("b.bin"));
    EXPECT_TRUE(storage.existsFile("c.bin"));
    EXPECT_FALSE(storage.existsFile("missing.bin"));

    EXPECT_EQ(storage.getFileSize("a.bin"), 5u);
    EXPECT_EQ(storage.getFileSize("b.bin"), 11u);
    EXPECT_EQ(storage.getFileSize("c.bin"), 8u);

    EXPECT_EQ(readAll(storage, "a.bin"), "alpha");
    EXPECT_EQ(readAll(storage, "b.bin"), "bravo-bravo");
    EXPECT_EQ(readAll(storage, "c.bin"), "charlie!");

    /// Last-modified should be a recent, non-zero timestamp.
    Poco::Timestamp ts = storage.getFileLastModified("a.bin");
    EXPECT_GT(ts.epochTime(), 0);
}

TEST(ANNGroupStorageTest, RenameFileAndRemove)
{
    TempDirScope scope("rename");
    auto volume = makeLocalVolume(scope.path);
    const std::string rel_group = "ann_rename";
    volume->getDisk(0)->createDirectory(rel_group);

    ANNGroupStorageDiskFull storage(volume, rel_group);
    writeAll(storage, "old.bin", "payload");

    storage.renameFile("old.bin", "new.bin");
    EXPECT_FALSE(storage.existsFile("old.bin"));
    EXPECT_TRUE(storage.existsFile("new.bin"));
    EXPECT_EQ(readAll(storage, "new.bin"), "payload");

    storage.removeFileIfExists("new.bin");
    EXPECT_FALSE(storage.existsFile("new.bin"));
    /// Idempotent: must not throw when the file is already gone.
    storage.removeFileIfExists("new.bin");
}

TEST(ANNGroupStorageTest, RenameDirectoryTracksNewPath)
{
    TempDirScope scope("renamedir");
    auto volume = makeLocalVolume(scope.path);
    const std::string rel_from = "tmp_ann_X";
    const std::string rel_to = "ann_X";
    volume->getDisk(0)->createDirectory(rel_from);

    ANNGroupStorageDiskFull storage(volume, rel_from);
    writeAll(storage, "id_map.bin", "hello");
    EXPECT_EQ(storage.getRelativePath(), rel_from);

    storage.renameDirectoryTo(rel_to);
    EXPECT_EQ(storage.getRelativePath(), rel_to);

    const fs::path expected_full = fs::path(volume->getDisk(0)->getPath()) / rel_to;
    EXPECT_EQ(fs::path(storage.getFullPath()), expected_full);
    EXPECT_TRUE(storage.exists());
    EXPECT_EQ(readAll(storage, "id_map.bin"), "hello");
}

TEST(ANNGroupStorageTest, TransactionWritePathIsObservableAfterCommit)
{
    /// Note: `DiskLocal::createTransaction` returns a `FakeDiskTransaction`, which is
    /// not atomic — writes are visible immediately. We verify that the transactional
    /// code path compiles, runs, and yields the expected file contents after `commit`,
    /// and that `undo` is a safe no-op on this disk implementation.
    TempDirScope scope("txn");
    auto volume = makeLocalVolume(scope.path);
    const std::string rel_group = "ann_txn";
    volume->getDisk(0)->createDirectory(rel_group);

    auto disk = volume->getDisk(0);
    auto txn = disk->createTransaction();

    ANNGroupStorageDiskFull storage(volume, rel_group, txn);

    writeAll(storage, "payload.bin", "transactional content");

    txn->commit();

    EXPECT_TRUE(storage.existsFile("payload.bin"));
    EXPECT_EQ(readAll(storage, "payload.bin"), "transactional content");

    /// A second no-op transaction: undo must be safe.
    auto txn2 = disk->createTransaction();
    ANNGroupStorageDiskFull storage2(volume, rel_group, txn2);
    writeAll(storage2, "extra.bin", "x");
    EXPECT_NO_THROW(txn2->undo());
}
