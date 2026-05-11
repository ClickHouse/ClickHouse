#include <gtest/gtest.h>

#include <Storages/MergeTree/ANNIndex/ANNIndexTableMeta.h>

#include <Disks/DiskLocal.h>
#include <Disks/IDisk.h>
#include <Disks/IDiskTransaction.h>
#include <Disks/SingleDiskVolume.h>
#include <IO/WriteBufferFromFileBase.h>

#include <Common/Exception.h>

#include <chrono>
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
    auto dir = fs::temp_directory_path() / ("clickhouse-ann-table-meta-" + name + "-" + std::to_string(now));
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

VolumePtr makeLocalVolume(const fs::path & root)
{
    auto disk = std::make_shared<DiskLocal>("ann_table_meta_disk", root.string() + "/");
    return std::make_shared<SingleDiskVolume>("ann_table_meta_vol", disk);
}

ANNIndexTableMeta makeSample()
{
    ANNIndexTableMeta m;
    m.version = 1;
    m.shape.dim = 128;
    m.shape.metric = 1;
    m.shape.algorithm = "diskann";
    m.shape.params_hash = 0x0123456789abcdefull;
    m.hash_algo = "sipHash64";
    m.hash_seed = 0xcafebabedeadbeefull;
    return m;
}

}

TEST(ANNIndexTableMetaTest, LoadOrEmptyWhenMissing)
{
    TempDirScope scope("missing");
    auto volume = makeLocalVolume(scope.path);
    const std::string rel_root = "root";
    volume->getDisk(0)->createDirectory(rel_root);

    auto loaded = ANNIndexTableMeta::loadOrEmpty(volume, rel_root);
    EXPECT_EQ(loaded.version, 1u);
    EXPECT_EQ(loaded.shape.dim, 0u);
    EXPECT_EQ(loaded.shape.metric, 0u);
    EXPECT_TRUE(loaded.shape.algorithm.empty());
    EXPECT_EQ(loaded.shape.params_hash, 0u);
    EXPECT_EQ(loaded.hash_seed, 0u);
}

TEST(ANNIndexTableMetaTest, WriteAndLoadRoundTripNonTransactional)
{
    TempDirScope scope("roundtrip");
    auto volume = makeLocalVolume(scope.path);
    const std::string rel_root = "root";
    volume->getDisk(0)->createDirectory(rel_root);

    auto m = makeSample();
    m.writeTo(volume, rel_root);

    auto loaded = ANNIndexTableMeta::loadOrEmpty(volume, rel_root);
    EXPECT_EQ(loaded.version, m.version);
    EXPECT_EQ(loaded.shape, m.shape);
    EXPECT_EQ(loaded.hash_algo, m.hash_algo);
    EXPECT_EQ(loaded.hash_seed, m.hash_seed);
}

TEST(ANNIndexTableMetaTest, CorruptedJsonRejected)
{
    TempDirScope scope("corrupt");
    auto volume = makeLocalVolume(scope.path);
    const std::string rel_root = "root";
    auto disk = volume->getDisk(0);
    disk->createDirectory(rel_root);

    /// Write garbage directly to meta.json.
    {
        auto out = disk->writeFile(fs::path(rel_root) / "meta.json",
                                   4096, WriteMode::Rewrite, WriteSettings{});
        const std::string junk = "{this is : not valid json,,,";
        out->write(junk.data(), junk.size());
        out->finalize();
    }

    EXPECT_THROW(ANNIndexTableMeta::loadOrEmpty(volume, rel_root), DB::Exception);
}

TEST(ANNIndexTableMetaTest, MissingShapeRejected)
{
    TempDirScope scope("noshape");
    auto volume = makeLocalVolume(scope.path);
    const std::string rel_root = "root";
    auto disk = volume->getDisk(0);
    disk->createDirectory(rel_root);

    {
        auto out = disk->writeFile(fs::path(rel_root) / "meta.json",
                                   4096, WriteMode::Rewrite, WriteSettings{});
        const std::string payload = "{\"version\": 1}";
        out->write(payload.data(), payload.size());
        out->finalize();
    }

    EXPECT_THROW(ANNIndexTableMeta::loadOrEmpty(volume, rel_root), DB::Exception);
}

TEST(ANNIndexTableMetaTest, TransactionalWriteObservableAfterCommit)
{
    /// With `DiskLocal`, `createTransaction` returns a `FakeDiskTransaction`, so writes
    /// are not rolled back by `undo`. This test covers the code path that `writeTo`
    /// takes when a transaction is provided, and verifies the end-state after commit.
    TempDirScope scope("txn");
    auto volume = makeLocalVolume(scope.path);
    const std::string rel_root = "root";
    auto disk = volume->getDisk(0);
    disk->createDirectory(rel_root);

    auto txn = disk->createTransaction();

    auto m = makeSample();
    m.writeTo(volume, rel_root, txn);

    txn->commit();

    auto loaded = ANNIndexTableMeta::loadOrEmpty(volume, rel_root);
    EXPECT_EQ(loaded.shape, m.shape);
    EXPECT_EQ(loaded.hash_seed, m.hash_seed);
    EXPECT_EQ(loaded.hash_algo, m.hash_algo);
}
