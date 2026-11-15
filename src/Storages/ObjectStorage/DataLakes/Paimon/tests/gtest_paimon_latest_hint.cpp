#include <gtest/gtest.h>

#include <config.h>

#if USE_AVRO

#include <Core/Field.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonClient.h>
#include <Common/tests/gtest_global_context.h>

#include <base/scope_guard.h>

#include <unistd.h>

#include <atomic>
#include <exception>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;
using namespace DB;

namespace
{

struct ScopedTempDir
{
    fs::path path;

    explicit ScopedTempDir(const std::string & name)
        : path(fs::temp_directory_path() / (name + "_" + std::to_string(::getpid())))
    {
        std::error_code ec;
        fs::remove_all(path, ec);
        fs::create_directories(path);
    }

    ~ScopedTempDir()
    {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

void writeFile(const fs::path & path, const std::string & contents)
{
    fs::create_directories(path.parent_path());
    std::ofstream out;
    out.exceptions(std::ios::failbit | std::ios::badbit);
    out.open(path, std::ios::binary | std::ios::trunc);
    out << contents;
    out.close();
}

void replaceFileAtomically(const fs::path & path, const std::string & contents, size_t sequence)
{
    fs::path temporary_path(path.string() + ".tmp." + std::to_string(sequence));
    writeFile(temporary_path, contents);
    fs::rename(temporary_path, path);
}

fs::path makePaimonTable(const fs::path & root, const std::vector<Int64> & snapshot_ids, const std::string & latest_hint)
{
    auto table = root / "test.db" / "test_table";
    for (Int64 snapshot_id : snapshot_ids)
    {
        writeFile(table / Paimon::PAIMON_SNAPSHOT_DIR / (std::string(Paimon::PAIMON_SNAPSHOT_PREFIX) + std::to_string(snapshot_id)), "{}");
    }
    writeFile(table / Paimon::PAIMON_SNAPSHOT_DIR / Paimon::PAIMON_SNAPSHOT_LATEST_HINT, latest_hint);
    return table;
}

class CountingLocalObjectStorage : public LocalObjectStorage
{
public:
    using LocalObjectStorage::LocalObjectStorage;

    SmallObjectDataWithMetadata readSmallObjectAndGetObjectMetadata(
        const StoredObject & object,
        const ReadSettings & read_settings,
        size_t max_size_bytes,
        std::optional<size_t> read_hint) const override
    {
        small_object_reads.fetch_add(1, std::memory_order_relaxed);
        last_local_buffer_size.store(read_settings.local_fs_settings.buffer_size, std::memory_order_relaxed);
        last_remote_buffer_size.store(read_settings.remote_fs_settings.buffer_size, std::memory_order_relaxed);
        last_max_size_bytes.store(max_size_bytes, std::memory_order_relaxed);
        return LocalObjectStorage::readSmallObjectAndGetObjectMetadata(object, read_settings, max_size_bytes, read_hint);
    }

    size_t getSmallObjectReads() const { return small_object_reads.load(std::memory_order_relaxed); }
    size_t getLastLocalBufferSize() const { return last_local_buffer_size.load(std::memory_order_relaxed); }
    size_t getLastRemoteBufferSize() const { return last_remote_buffer_size.load(std::memory_order_relaxed); }
    size_t getLastMaxSizeBytes() const { return last_max_size_bytes.load(std::memory_order_relaxed); }

private:
    mutable std::atomic<size_t> small_object_reads{0};
    mutable std::atomic<size_t> last_local_buffer_size{0};
    mutable std::atomic<size_t> last_remote_buffer_size{0};
    mutable std::atomic<size_t> last_max_size_bytes{0};
};

std::shared_ptr<CountingLocalObjectStorage> makeLocalObjectStorage(const fs::path & root)
{
    return std::make_shared<CountingLocalObjectStorage>(
        LocalObjectStorageSettings("test_paimon_latest_hint", root.string(), /*read_only_=*/false));
}

}

TEST(PaimonLatestHint, ReadsConcurrentlyReplacedHintAsSmallObject)
{
    ScopedTempDir temporary_directory("ch_gtest_paimon_latest_hint");
    auto table = makePaimonTable(temporary_directory.path, {9, 10}, "9");
    auto hint_path = table / Paimon::PAIMON_SNAPSHOT_DIR / Paimon::PAIMON_SNAPSHOT_LATEST_HINT;

    auto context = Context::createCopy(getContext().context);
    context->setSetting("remote_filesystem_read_method", String("threadpool"));
    context->setSetting("remote_filesystem_read_prefetch", Field(true));

    auto object_storage = makeLocalObjectStorage(temporary_directory.path);
    PaimonTableClient client(object_storage, table.string(), context);

    std::atomic<bool> stop{false};
    std::exception_ptr writer_exception;
    {
        std::thread writer(
            [&]
            {
                try
                {
                    size_t sequence = 0;
                    while (!stop.load(std::memory_order_relaxed))
                    {
                        replaceFileAtomically(hint_path, (sequence % 2 == 0) ? "10" : "9", sequence);
                        ++sequence;
                    }
                }
                catch (...)
                {
                    writer_exception = std::current_exception();
                    stop.store(true, std::memory_order_relaxed);
                }
            });

        SCOPE_EXIT({
            stop.store(true, std::memory_order_relaxed);
            writer.join();
        });

        for (size_t iteration = 0; iteration < 2000; ++iteration)
        {
            auto snapshot_info = client.getLatestTableSnapshotInfo();
            ASSERT_TRUE(snapshot_info.has_value());
            EXPECT_EQ(snapshot_info->first, 10);
            EXPECT_TRUE(fs::exists(snapshot_info->second));
        }
    }

    if (writer_exception)
    {
        try
        {
            std::rethrow_exception(writer_exception);
        }
        catch (const std::exception & exception)
        {
            FAIL() << "Hint writer failed: " << exception.what();
        }
    }

    EXPECT_GT(object_storage->getSmallObjectReads(), 0);
    EXPECT_EQ(object_storage->getLastMaxSizeBytes(), 64);
    EXPECT_GE(object_storage->getLastLocalBufferSize(), 64);
    EXPECT_GE(object_storage->getLastRemoteBufferSize(), 64);
}

TEST(PaimonLatestHint, FallsBackToListingForInvalidHint)
{
    ScopedTempDir temporary_directory("ch_gtest_paimon_invalid_latest_hint");
    auto table = makePaimonTable(temporary_directory.path, {1, 2}, "3trailing");

    auto object_storage = makeLocalObjectStorage(temporary_directory.path);
    PaimonTableClient client(object_storage, table.string(), getContext().context);

    auto snapshot_info = client.getLatestTableSnapshotInfo();
    ASSERT_TRUE(snapshot_info.has_value());
    EXPECT_EQ(snapshot_info->first, 2);
    EXPECT_TRUE(fs::exists(snapshot_info->second));
    EXPECT_EQ(object_storage->getSmallObjectReads(), 1);
}

#endif
