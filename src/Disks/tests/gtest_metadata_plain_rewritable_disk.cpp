#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Disks/WriteMode.h>

#include <IO/ReadSettings.h>
#include <IO/SharedThreadPools.h>
#include <IO/Operators.h>

#include <Core/ServerUUID.h>

#include <Common/thread_local_rng.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>

#include <base/scope_guard.h>

#include <Poco/AutoPtr.h>
#include <Poco/Channel.h>
#include <Poco/Logger.h>
#include <Poco/Message.h>

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <ranges>
#include <thread>

namespace ProfileEvents
{
    extern const Event DiskPlainRewritableUndoStageRetries;
}

namespace DB::ErrorCodes
{
    extern const int FAULT_INJECTED;
}

using namespace DB;

/// A local object storage that can be told to reject every request that changes something, the way object storage
/// behaves during an outage. Reads keep working, so a test can still look at what is stored.
class FailingLocalObjectStorage : public LocalObjectStorage
{
public:
    using LocalObjectStorage::LocalObjectStorage;

    void failRequests(bool fail) { failing = fail; }

    std::unique_ptr<WriteBufferFromFileBase> writeObject(
        const StoredObject & object,
        WriteMode mode,
        std::optional<ObjectAttributes> attributes = {},
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        const WriteSettings & write_settings = {}) override
    {
        throwIfFailing(object);
        return LocalObjectStorage::writeObject(object, mode, attributes, buf_size, write_settings);
    }

    void copyObject(
        const StoredObject & object_from,
        const StoredObject & object_to,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        std::optional<ObjectAttributes> object_to_attributes = {}) override
    {
        throwIfFailing(object_to);
        LocalObjectStorage::copyObject(object_from, object_to, read_settings, write_settings, object_to_attributes);
    }

    void removeObjectIfExists(const StoredObject & object) override
    {
        throwIfFailing(object);
        LocalObjectStorage::removeObjectIfExists(object);
    }

private:
    std::atomic<bool> failing = false;

    void throwIfFailing(const StoredObject & object) const
    {
        if (failing)
            throw Exception(ErrorCodes::FAULT_INJECTED, "Object storage is unavailable, cannot change '{}'", object.remote_path);
    }
};

/// Collects log messages, so that a test can assert on what the code reported rather than only on its return values.
class CapturingChannel : public Poco::Channel
{
public:
    void log(const Poco::Message & message) override
    {
        std::lock_guard lock(mutex);
        messages.push_back(message.getText());
    }

    size_t count(const std::string & substring) const
    {
        std::lock_guard lock(mutex);
        return std::ranges::count_if(messages, [&](const auto & message) { return message.find(substring) != std::string::npos; });
    }

private:
    mutable std::mutex mutex;
    std::vector<std::string> messages;
};

/// Sends everything a logger reports to a `CapturingChannel` for as long as it is alive.
class LogCapture
{
public:
    explicit LogCapture(const std::string & logger_name)
        : logger(Poco::Logger::get(logger_name))
        , previous_channel(logger.getChannel())
        , previous_level(logger.getLevel())
        , channel(new CapturingChannel)
    {
        logger.setChannel(channel);
        logger.setLevel("error");
    }

    ~LogCapture()
    {
        logger.setChannel(previous_channel);
        logger.setLevel(previous_level);
    }

    size_t count(const std::string & substring) const { return channel->count(substring); }

private:
    Poco::Logger & logger;
    Poco::AutoPtr<Poco::Channel> previous_channel;
    int previous_level;
    Poco::AutoPtr<CapturingChannel> channel;
};

class MetadataPlainRewritableDiskTest : public testing::Test
{
public:
    /// Whether the object storage of the disk can be told to reject requests. Set it before the first
    /// `getMetadataStorage` call of a test, and reach it with `getFailingObjectStorage`.
    bool object_storage_can_fail = false;

    void SetUp() override
    {
        if (!initialized)
        {
            ServerUUID::setRandomForUnitTests();
            getIOThreadPool().initializeWithDefaultSettingsIfNotInitialized();
            initialized = true;
        }
    }

    std::shared_ptr<IMetadataStorage> getMetadataStorage(const std::string & key_prefix)
    {
        std::unique_lock<std::mutex> lock(active_metadatas_mutex);

        if (!active_metadatas[key_prefix])
            active_metadatas[key_prefix] = createMetadataStorage(key_prefix);

        return active_metadatas[key_prefix];
    }

    std::shared_ptr<IMetadataStorage> restartMetadataStorage(const std::string & key_prefix)
    {
        std::unique_lock<std::mutex> lock(active_metadatas_mutex);
        auto object_storage = active_object_storages.at(key_prefix);
        active_metadatas[key_prefix] = std::make_shared<MetadataStorageFromPlainRewritableObjectStorage>(object_storage, "");
        return active_metadatas.at(key_prefix);
    }

    std::shared_ptr<IObjectStorage> getObjectStorage(const std::string & key_prefix)
    {
        std::unique_lock<std::mutex> lock(active_metadatas_mutex);
        return active_object_storages.at(key_prefix);
    }

    std::shared_ptr<FailingLocalObjectStorage> getFailingObjectStorage(const std::string & key_prefix)
    {
        std::unique_lock<std::mutex> lock(active_metadatas_mutex);
        return std::dynamic_pointer_cast<FailingLocalObjectStorage>(active_object_storages.at(key_prefix));
    }

    void TearDown() override
    {
        for (const auto & [_, metadata] : active_metadatas)
            metadata->shutdown();

        for (const auto & [_, object_storage] : active_object_storages)
        {
            object_storage->shutdown();
            fs::remove_all(object_storage->getCommonKeyPrefix());
        }
    }

private:
    std::shared_ptr<IMetadataStorage> createMetadataStorage(const std::string & key_prefix)
    {
        fs::remove_all("./" + key_prefix);
        LocalObjectStorageSettings settings("test", "./" + key_prefix, /*read_only_=*/false);
        std::shared_ptr<LocalObjectStorage> object_storage = object_storage_can_fail
            ? std::make_shared<FailingLocalObjectStorage>(std::move(settings))
            : std::make_shared<LocalObjectStorage>(std::move(settings));
        auto metadata_storage = std::make_shared<MetadataStorageFromPlainRewritableObjectStorage>(object_storage, "");

        active_metadatas.emplace(key_prefix, metadata_storage);
        active_object_storages.emplace(key_prefix, object_storage);

        return metadata_storage;
    }

    static inline bool initialized = false;

    std::mutex active_metadatas_mutex;
    std::unordered_map<std::string, std::shared_ptr<IMetadataStorage>> active_metadatas;
    std::unordered_map<std::string, std::shared_ptr<IObjectStorage>> active_object_storages;
};

static size_t writeObject(const std::shared_ptr<IObjectStorage> & object_storage, const std::string & remote_path, const std::string & data)
{
    StoredObject object(remote_path);
    auto buffer = object_storage->writeObject(object, WriteMode::Rewrite);
    buffer->write(data.data(), data.size());
    buffer->preFinalize();
    size_t written_bytes = buffer->count();
    buffer->finalize();
    return written_bytes;
}

static std::string readObject(const std::shared_ptr<IObjectStorage> & object_storage, const std::string & remote_path)
{
    StoredObject object(remote_path);
    auto buffer = object_storage->readObject(object, getReadSettings(), /*read_hint=*/std::nullopt);

    String content;
    readStringUntilEOF(content, *buffer);
    return content;
}

static std::string generateObjectKeyPrefixForDirectoryPath(const std::shared_ptr<IMetadataStorage> & metadata, const std::string & directory)
{
    auto tx = metadata->createTransaction();
    auto file_remote_path = tx->generateObjectKeyForPath(fs::path(directory) / "file.txt").serialize();
    return fs::path(file_remote_path).parent_path().filename();
}

static std::string generateObjectKeyForPath(const std::shared_ptr<IMetadataStorage> & metadata, const std::string & path)
{
    auto tx = metadata->createTransaction();
    return tx->generateObjectKeyForPath(path).serialize();
}

static std::string createMetadataObjectPath(const std::shared_ptr<IMetadataStorage> & metadata, const std::string & directory)
{
    auto tx = metadata->createTransaction();
    auto file_remote_path = tx->generateObjectKeyForPath(fs::path(directory) / "file.txt").serialize();
    auto object_key_prefix = fs::path(file_remote_path).parent_path().filename();
    auto common_key_prefix = fs::path(file_remote_path).parent_path().parent_path();
    return fs::path(common_key_prefix) / "__meta" / object_key_prefix / "prefix.path";
}

static std::vector<std::string> sorted(std::vector<std::string> array)
{
    std::sort(array.begin(), array.end());
    return array;
}

static std::vector<std::string> listAllBlobs(std::string test)
{
    if (!std::filesystem::exists(fmt::format("./{}", test)))
        return {};

    return sorted(std::filesystem::recursive_directory_iterator(fmt::format("./{}", test))
                    | std::views::filter([](const auto & inode) { return inode.is_regular_file(); })
                    | std::views::transform([](const auto & file) { return file.path(); })
                    | std::ranges::to<std::vector<std::string>>());
}

TEST_F(MetadataPlainRewritableDiskTest, JustWorking)
{
    auto metadata = getMetadataStorage("JustWorking");
    auto object_storage = getObjectStorage("JustWorking");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("A");
        tx->createDirectory("A/B");
        tx->createDirectory("A/B/C");
        tx->createDirectory("A/D");
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsDirectory("A"));
    EXPECT_TRUE(metadata->existsDirectory("A/B"));
    EXPECT_TRUE(metadata->existsDirectory("A/B/C"));
    EXPECT_TRUE(metadata->existsDirectory("A/D"));
    EXPECT_FALSE(metadata->existsDirectory("OTHER"));

    EXPECT_EQ(readObject(object_storage, createMetadataObjectPath(metadata, "A")), "A/");
    EXPECT_EQ(readObject(object_storage, createMetadataObjectPath(metadata, "A/B/C")), "A/B/C/");
}

TEST_F(MetadataPlainRewritableDiskTest, Ls)
{
    auto metadata = getMetadataStorage("Ls");
    auto object_storage = getObjectStorage("Ls");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("A");
        tx->createDirectory("B");
        tx->createDirectory("C");
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_EQ(sorted(metadata->listDirectory("/")), std::vector<std::string>({"A", "B", "C"}));
    EXPECT_EQ(sorted(metadata->listDirectory("")), std::vector<std::string>({"A", "B", "C"}));

    {
        auto tx = metadata->createTransaction();
        tx->createDirectoryRecursive("D/E/F/G/H");
        tx->createDirectoryRecursive("/D/E/F/K");
        tx->commit(DB::NoCommitOptions{});
    }

    /// For now we can not create file under the directory created in the same tx.
    {
        auto tx = metadata->createTransaction();
        size_t file_size = writeObject(object_storage, tx->generateObjectKeyForPath("D/E/F/G/H/file").serialize(), "file");
        tx->createMetadataFile("D/E/F/G/H/file", {StoredObject("file", "file", file_size)});
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_EQ(sorted(metadata->listDirectory("/D/E/F")), std::vector<std::string>({"G", "K"}));
    EXPECT_EQ(sorted(metadata->listDirectory("D/E/F/G/H")), std::vector<std::string>({"file"}));

    metadata = restartMetadataStorage("Ls");
    EXPECT_EQ(sorted(metadata->listDirectory("/")), std::vector<std::string>({"A", "B", "C", "D"}));
    EXPECT_EQ(sorted(metadata->listDirectory("")), std::vector<std::string>({"A", "B", "C", "D"}));
    EXPECT_EQ(sorted(metadata->listDirectory("/D/E/F")), std::vector<std::string>({"G", "K"}));
    EXPECT_EQ(sorted(metadata->listDirectory("D/E/F/G/H")), std::vector<std::string>({"file"}));
}

TEST_F(MetadataPlainRewritableDiskTest, MoveTree)
{
    auto metadata = getMetadataStorage("MoveTree");
    auto object_storage = getObjectStorage("MoveTree");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("A");
        tx->createDirectory("A/B");
        tx->createDirectoryRecursive("A/B/C/D");
        tx->commit(DB::NoCommitOptions{});
    }

    auto a_path = createMetadataObjectPath(metadata, "A");
    auto ab_path = createMetadataObjectPath(metadata, "A/B");
    auto abcd_path = createMetadataObjectPath(metadata, "A/B/C/D");

    /// Move tree starting from the root
    {
        auto tx = metadata->createTransaction();
        tx->moveDirectory("A", "MOVED");
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_EQ(readObject(object_storage, a_path), "MOVED/");
    EXPECT_EQ(readObject(object_storage, ab_path), "MOVED/B/");
    EXPECT_EQ(readObject(object_storage, abcd_path), "MOVED/B/C/D/");

    EXPECT_FALSE(metadata->existsDirectory("A"));
    EXPECT_FALSE(metadata->existsDirectory("A/B"));
    EXPECT_FALSE(metadata->existsDirectory("A/B/C"));
    EXPECT_FALSE(metadata->existsDirectory("A/B/C/D"));
    EXPECT_TRUE(metadata->existsDirectory("MOVED"));
    EXPECT_TRUE(metadata->existsDirectory("MOVED/B"));
    EXPECT_TRUE(metadata->existsDirectory("MOVED/B/C"));
    EXPECT_TRUE(metadata->existsDirectory("MOVED/B/C/D"));

    metadata = restartMetadataStorage("MoveTree");
    EXPECT_FALSE(metadata->existsDirectory("A"));
    EXPECT_FALSE(metadata->existsDirectory("A/B"));
    EXPECT_FALSE(metadata->existsDirectory("A/B/C"));
    EXPECT_FALSE(metadata->existsDirectory("A/B/C/D"));
    EXPECT_TRUE(metadata->existsDirectory("MOVED"));
    EXPECT_TRUE(metadata->existsDirectory("MOVED/B"));
    EXPECT_TRUE(metadata->existsDirectory("MOVED/B/C"));
    EXPECT_TRUE(metadata->existsDirectory("MOVED/B/C/D"));
}

TEST_F(MetadataPlainRewritableDiskTest, MoveUndo)
{
    auto metadata = getMetadataStorage("MoveUndo");
    auto object_storage = getObjectStorage("MoveUndo");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("A");
        tx->createDirectory("A/B");
        tx->createDirectory("A/B/C");
        tx->commit(DB::NoCommitOptions{});
    }

    auto a_path = createMetadataObjectPath(metadata, "A");
    auto ab_path = createMetadataObjectPath(metadata, "A/B");
    auto abc_path = createMetadataObjectPath(metadata, "A/B/C");

    /// Move tree starting from the root
    {
        auto tx = metadata->createTransaction();
        tx->moveDirectory("A", "MOVED");
        tx->moveFile("non-existing", "other-place");
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }

    EXPECT_EQ(readObject(object_storage, a_path), "A/");
    EXPECT_EQ(readObject(object_storage, ab_path), "A/B/");
    EXPECT_EQ(readObject(object_storage, abc_path), "A/B/C/");
    EXPECT_FALSE(metadata->existsFile("non-existing"));
    EXPECT_FALSE(metadata->existsFile("/non-existing"));
    EXPECT_FALSE(metadata->existsFile("other-place"));
    EXPECT_FALSE(metadata->existsFile("/other-place"));
    EXPECT_FALSE(metadata->existsDirectory("other-place"));
    EXPECT_FALSE(metadata->existsDirectory("/other-place"));
    EXPECT_TRUE(metadata->existsDirectory("/A"));
    EXPECT_TRUE(metadata->existsDirectory("/A/B"));
    EXPECT_TRUE(metadata->existsDirectory("/A/B/C/"));

    metadata = restartMetadataStorage("MoveUndo");
    EXPECT_FALSE(metadata->existsFile("non-existing"));
    EXPECT_FALSE(metadata->existsFile("/non-existing"));
    EXPECT_FALSE(metadata->existsFile("other-place"));
    EXPECT_FALSE(metadata->existsFile("/other-place"));
    EXPECT_FALSE(metadata->existsDirectory("other-place"));
    EXPECT_FALSE(metadata->existsDirectory("/other-place"));
    EXPECT_TRUE(metadata->existsDirectory("/A"));
    EXPECT_TRUE(metadata->existsDirectory("/A/B"));
    EXPECT_TRUE(metadata->existsDirectory("/A/B/C/"));
}

TEST_F(MetadataPlainRewritableDiskTest, CreateNotFromRoot)
{
    auto metadata = getMetadataStorage("CreateNotFromRoot");
    auto object_storage = getObjectStorage("CreateNotFromRoot");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("A/B/C");
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }

    EXPECT_FALSE(metadata->existsDirectory("A"));
    EXPECT_FALSE(metadata->existsDirectory("A/B"));
    EXPECT_FALSE(metadata->existsDirectory("A/B/C"));
}

TEST_F(MetadataPlainRewritableDiskTest, CreateRecursive)
{
    auto metadata = getMetadataStorage("CreateRecursive");
    auto object_storage = getObjectStorage("CreateRecursive");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectoryRecursive("A/B/C");
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsDirectory("A"));
    EXPECT_TRUE(metadata->existsDirectory("A/B"));
    EXPECT_TRUE(metadata->existsDirectory("A/B/C"));
}

TEST_F(MetadataPlainRewritableDiskTest, RemoveDirectory)
{
    auto metadata = getMetadataStorage("RemoveDirectory");
    auto object_storage = getObjectStorage("RemoveDirectory");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("A");
        tx->createDirectory("A/B");
        tx->createDirectory("A/B/C");
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsDirectory("A"));
    EXPECT_TRUE(metadata->existsDirectory("A/B"));
    EXPECT_TRUE(metadata->existsDirectory("A/B/C"));

    {
        auto tx = metadata->createTransaction();
        tx->removeDirectory("A/B/C");
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsDirectory("A"));
    EXPECT_TRUE(metadata->existsDirectory("A/B"));
    EXPECT_FALSE(metadata->existsDirectory("A/B/C"));

    {
        auto tx = metadata->createTransaction();
        tx->removeDirectory("A");
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }

    EXPECT_TRUE(metadata->existsDirectory("A"));
    EXPECT_TRUE(metadata->existsDirectory("A/B"));
    EXPECT_FALSE(metadata->existsDirectory("A/B/C"));

    metadata = restartMetadataStorage("RemoveDirectory");
    EXPECT_TRUE(metadata->existsDirectory("A"));
    EXPECT_TRUE(metadata->existsDirectory("A/B"));
    EXPECT_FALSE(metadata->existsDirectory("A/B/C"));

    {
        auto tx = metadata->createTransaction();
        tx->removeDirectory("A/B");
        tx->removeDirectory("A");
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_FALSE(metadata->existsDirectory("A"));
    EXPECT_FALSE(metadata->existsDirectory("A/B"));
    EXPECT_FALSE(metadata->existsDirectory("A/B/C"));

    metadata = restartMetadataStorage("RemoveDirectory");
    EXPECT_FALSE(metadata->existsDirectory("A"));
    EXPECT_FALSE(metadata->existsDirectory("A/B"));
    EXPECT_FALSE(metadata->existsDirectory("A/B/C"));
}

TEST_F(MetadataPlainRewritableDiskTest, RemoveDirectoryUndo)
{
    auto metadata = getMetadataStorage("RemoveDirectoryUndo");
    auto object_storage = getObjectStorage("RemoveDirectoryUndo");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("A");
        tx->createDirectory("A/B");
        tx->createDirectory("A/B/C");
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsDirectory("A"));
    EXPECT_TRUE(metadata->existsDirectory("A/B"));
    EXPECT_TRUE(metadata->existsDirectory("A/B/C"));

    {
        auto tx = metadata->createTransaction();
        tx->removeDirectory("A/B/C");
        tx->removeDirectory("A");
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }

    EXPECT_TRUE(metadata->existsDirectory("A"));
    EXPECT_TRUE(metadata->existsDirectory("A/B"));
    EXPECT_TRUE(metadata->existsDirectory("A/B/C"));

    {
        auto tx = metadata->createTransaction();
        tx->removeDirectory("X");
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }

    EXPECT_TRUE(metadata->existsDirectory("A"));
    EXPECT_TRUE(metadata->existsDirectory("A/B"));
    EXPECT_TRUE(metadata->existsDirectory("A/B/C"));
    EXPECT_FALSE(metadata->existsDirectory("X"));

    metadata = restartMetadataStorage("RemoveDirectoryUndo");
    EXPECT_TRUE(metadata->existsDirectory("A"));
    EXPECT_TRUE(metadata->existsDirectory("A/B"));
    EXPECT_TRUE(metadata->existsDirectory("A/B/C"));

    {
        auto tx = metadata->createTransaction();
        tx->removeDirectory("A/B/C");
        tx->removeDirectory("A/B");
        tx->removeDirectory("A");
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_FALSE(metadata->existsDirectory("A"));
    EXPECT_FALSE(metadata->existsDirectory("A/B"));
    EXPECT_FALSE(metadata->existsDirectory("A/B/C"));

    metadata = restartMetadataStorage("RemoveDirectoryUndo");
    EXPECT_FALSE(metadata->existsDirectory("A"));
    EXPECT_FALSE(metadata->existsDirectory("A/B"));
    EXPECT_FALSE(metadata->existsDirectory("A/B/C"));
}

TEST_F(MetadataPlainRewritableDiskTest, RemoveDirectoryRecursive)
{
    thread_local_rng.seed(42);

    auto metadata = getMetadataStorage("RemoveDirectoryRecursive");
    auto object_storage = getObjectStorage("RemoveDirectoryRecursive");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("root");
        tx->createDirectory("root/A");
        tx->createDirectory("root/A/B");
        tx->createDirectory("root/A/C");
        tx->createDirectory("root/A/B/D");
        tx->createDirectory("root/A/B/E");
        tx->createDirectory("root/A/B/E/F");
        tx->commit(DB::NoCommitOptions{});
    }

    {
        auto tx = metadata->createTransaction();
        size_t file_1_size = writeObject(object_storage, tx->generateObjectKeyForPath("root/A/file_1").serialize(), "1");
        size_t file_2_size = writeObject(object_storage, tx->generateObjectKeyForPath("root/A/B/file_2").serialize(), "2");
        size_t file_3_size = writeObject(object_storage, tx->generateObjectKeyForPath("root/A/C/file_3").serialize(), "3");
        size_t file_4_size = writeObject(object_storage, tx->generateObjectKeyForPath("root/A/B/E/F/file_4").serialize(), "4");
        tx->createMetadataFile("root/A/file_1", {StoredObject("root/A/file_1", "file_1", file_1_size)});
        tx->createMetadataFile("root/A/B/file_2", {StoredObject("root/A/B/file_2", "file_2", file_2_size)});
        tx->createMetadataFile("root/A/C/file_3", {StoredObject("root/A/C/file_3", "file_3", file_3_size)});
        tx->createMetadataFile("root/A/B/E/F/file_4", {StoredObject("root/A/B/E/F/file_4", "file_4", file_4_size)});
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsDirectory("root/A/B"));
    EXPECT_TRUE(metadata->existsDirectory("root/A/B/E/F"));
    EXPECT_EQ(readObject(object_storage, metadata->getStorageObjects("root/A/file_1").front().remote_path), "1");
    EXPECT_EQ(readObject(object_storage, metadata->getStorageObjects("root/A/B/file_2").front().remote_path), "2");
    EXPECT_EQ(readObject(object_storage, metadata->getStorageObjects("root/A/C/file_3").front().remote_path), "3");
    EXPECT_EQ(readObject(object_storage, metadata->getStorageObjects("root/A/B/E/F/file_4").front().remote_path), "4");

    auto inodes_start = listAllBlobs("RemoveDirectoryRecursive");
    EXPECT_EQ(inodes_start.size(), 11);  /// 7 directories + 4 files

    /// Check undo
    {
        auto tx = metadata->createTransaction();
        tx->removeRecursive("root/A", /*should_remove_blob=*/nullptr);
        tx->moveFile("non-existing", "other-place");
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }

    EXPECT_EQ(listAllBlobs("RemoveDirectoryRecursive"), inodes_start);

    /// Remove fs tree
    {
        auto tx = metadata->createTransaction();
        tx->removeRecursive("root/A", /*should_remove_blob=*/nullptr);
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_FALSE(metadata->existsDirectory("root/A"));
    EXPECT_FALSE(metadata->existsDirectory("root/A/B"));
    EXPECT_FALSE(metadata->existsDirectory("root/A/C"));
    EXPECT_FALSE(metadata->existsDirectory("root/A/B/D"));
    EXPECT_FALSE(metadata->existsDirectory("root/A/B/E"));
    EXPECT_FALSE(metadata->existsDirectory("root/A/B/E/F"));
    EXPECT_EQ(listAllBlobs("RemoveDirectoryRecursive"), std::vector<std::string>({
        "./RemoveDirectoryRecursive/__meta/faefxnlkbtfqgxcbfqfjtztsocaqrnqn/prefix.path",  /// /root
    }));
}

TEST_F(MetadataPlainRewritableDiskTest, RemoveDirectoryRecursiveVirtualNodes)
{
    thread_local_rng.seed(42);

    auto metadata = getMetadataStorage("RemoveDirectoryRecursiveVirtualNodes");
    auto object_storage = getObjectStorage("RemoveDirectoryRecursiveVirtualNodes");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("root");
        tx->createDirectory("root/A");
        tx->createDirectoryRecursive("root/A/B/C/D");
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsDirectory("root/A"));
    EXPECT_TRUE(metadata->existsDirectory("root/A/B"));
    EXPECT_TRUE(metadata->existsDirectory("root/A/B/C"));
    EXPECT_TRUE(metadata->existsDirectory("root/A/B/C/D"));

    {
        auto tx = metadata->createTransaction();
        tx->removeRecursive("root/A", /*should_remove_blob=*/nullptr);
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsDirectory("root"));
    EXPECT_FALSE(metadata->existsDirectory("root/A"));
    EXPECT_FALSE(metadata->existsDirectory("root/A/B"));
    EXPECT_FALSE(metadata->existsDirectory("root/A/B/C"));
    EXPECT_FALSE(metadata->existsDirectory("root/A/B/C/D"));

    metadata = restartMetadataStorage("RemoveDirectoryRecursiveVirtualNodes");
    EXPECT_TRUE(metadata->existsDirectory("root"));
    EXPECT_FALSE(metadata->existsDirectory("root/A"));
    EXPECT_FALSE(metadata->existsDirectory("root/A/B"));
    EXPECT_FALSE(metadata->existsDirectory("root/A/B/C"));
    EXPECT_FALSE(metadata->existsDirectory("root/A/B/C/D"));
    EXPECT_EQ(listAllBlobs("RemoveDirectoryRecursiveVirtualNodes"), std::vector<std::string>({
        "./RemoveDirectoryRecursiveVirtualNodes/__meta/faefxnlkbtfqgxcbfqfjtztsocaqrnqn/prefix.path",  /// /root
    }));
}

TEST_F(MetadataPlainRewritableDiskTest, MoveFile)
{
    auto metadata = getMetadataStorage("MoveFile");
    auto object_storage = getObjectStorage("MoveFile");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("A");
        tx->createDirectory("B");
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsDirectory("A"));
    EXPECT_TRUE(metadata->existsDirectory("B"));

    {
        auto tx = metadata->createTransaction();
        size_t file_size = writeObject(object_storage, tx->generateObjectKeyForPath("A/file").serialize(), "Hello world!");
        tx->createMetadataFile("A/file", {StoredObject("A/file", "file", file_size)});
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsFile("A/file"));

    auto a_file_path = metadata->getStorageObjects("A/file").front().remote_path;
    EXPECT_EQ(readObject(object_storage, a_file_path), "Hello world!");

    {
        auto tx = metadata->createTransaction();
        tx->moveFile("A/file", "B/file");
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_FALSE(metadata->existsFile("A/file"));
    EXPECT_TRUE(metadata->existsFile("B/file"));

    auto b_file_path = metadata->getStorageObjects("B/file").front().remote_path;
    EXPECT_EQ(readObject(object_storage, b_file_path), "Hello world!");

    EXPECT_NE(a_file_path, b_file_path);
}

TEST_F(MetadataPlainRewritableDiskTest, RewriteFileUpdatesSize)
{
    auto metadata = getMetadataStorage("RewriteFileUpdatesSize");
    auto object_storage = getObjectStorage("RewriteFileUpdatesSize");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("A");
        size_t file_size = writeObject(object_storage, tx->generateObjectKeyForPath("A/file").serialize(), "test");
        tx->createMetadataFile("A/file", {StoredObject("A/file", "file", file_size)});
        size_t root_file_size = writeObject(object_storage, tx->generateObjectKeyForPath("root_file").serialize(), "test");
        tx->createMetadataFile("root_file", {StoredObject("root_file", "root_file", root_file_size)});
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_EQ(metadata->getFileSize("A/file"), 4u);
    EXPECT_EQ(metadata->getFileSize("root_file"), 4u);

    {
        auto tx = metadata->createTransaction();
        size_t file_size = writeObject(object_storage, tx->generateObjectKeyForPath("A/file").serialize(), "Hello world!");
        tx->createMetadataFile("A/file", {StoredObject("A/file", "file", file_size)});
        size_t root_file_size = writeObject(object_storage, tx->generateObjectKeyForPath("root_file").serialize(), "Hello world!");
        tx->createMetadataFile("root_file", {StoredObject("root_file", "root_file", root_file_size)});
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_EQ(metadata->getFileSize("A/file"), 12u);
    EXPECT_EQ(metadata->getFileSize("root_file"), 12u);

    EXPECT_EQ(readObject(object_storage, metadata->getStorageObjects("A/file").front().remote_path), "Hello world!");
    EXPECT_EQ(readObject(object_storage, metadata->getStorageObjects("root_file").front().remote_path), "Hello world!");
}

TEST_F(MetadataPlainRewritableDiskTest, MoveFileUndo)
{
    auto metadata = getMetadataStorage("MoveFileUndo");
    auto object_storage = getObjectStorage("MoveFileUndo");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("A");
        tx->createDirectory("B");
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsDirectory("A"));
    EXPECT_TRUE(metadata->existsDirectory("B"));

    {
        auto tx = metadata->createTransaction();
        size_t file_size = writeObject(object_storage, tx->generateObjectKeyForPath("A/file").serialize(), "Hello world!");
        tx->createMetadataFile("A/file", {StoredObject("A/file", "file", file_size)});
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsFile("A/file"));

    auto path_1 = metadata->getStorageObjects("A/file").front().remote_path;
    EXPECT_EQ(readObject(object_storage, path_1), "Hello world!");

    {
        auto tx = metadata->createTransaction();
        tx->moveFile("A/file", "B/file");
        tx->moveFile("non-existing", "other-place");
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }

    EXPECT_TRUE(metadata->existsFile("A/file"));

    auto path_2 = metadata->getStorageObjects("A/file").front().remote_path;
    EXPECT_EQ(readObject(object_storage, path_2), "Hello world!");

    EXPECT_EQ(path_1, path_2);
}

TEST_F(MetadataPlainRewritableDiskTest, DirectoryFileNameCollision)
{
    auto metadata = getMetadataStorage("DirectoryFileNameCollision");
    auto object_storage = getObjectStorage("DirectoryFileNameCollision");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("A");
        tx->commit(DB::NoCommitOptions{});
    }

    {
        auto tx = metadata->createTransaction();
        size_t b_size = writeObject(object_storage, tx->generateObjectKeyForPath("A/B").serialize(), "Hello world!");
        tx->createMetadataFile("A/B", {StoredObject("A/B", "B", b_size)});
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_FALSE(metadata->existsDirectory("A/B"));
    EXPECT_TRUE(metadata->existsFile("A/B"));

    {
        auto tx = metadata->createTransaction();
        EXPECT_ANY_THROW(tx->createDirectory("A/B"));
    }

    EXPECT_FALSE(metadata->existsDirectory("A/B"));
    EXPECT_TRUE(metadata->existsFile("A/B"));

    metadata = restartMetadataStorage("DirectoryFileNameCollision");
    EXPECT_FALSE(metadata->existsDirectory("A/B"));
    EXPECT_TRUE(metadata->existsFile("A/B"));
}

TEST_F(MetadataPlainRewritableDiskTest, RemoveRecursiveEmpty)
{
    auto metadata = getMetadataStorage("RemoveRecursiveEmpty");
    auto object_storage = getObjectStorage("RemoveRecursiveEmpty");

    {
        auto tx = metadata->createTransaction();
        tx->removeRecursive("non-existing", /*should_remove_blob=*/nullptr);
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_FALSE(metadata->existsDirectory("non-existing"));
}

TEST_F(MetadataPlainRewritableDiskTest, RemoteLayout)
{
    thread_local_rng.seed(42);

    auto metadata = getMetadataStorage("RemoteLayout");
    auto object_storage = getObjectStorage("RemoteLayout");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("A");
        tx->createDirectory("A/B");
        tx->commit(DB::NoCommitOptions{});
    }

    std::string a_remote = generateObjectKeyPrefixForDirectoryPath(metadata, "A/");
    EXPECT_EQ(a_remote, "faefxnlkbtfqgxcbfqfjtztsocaqrnqn");
    EXPECT_EQ(generateObjectKeyPrefixForDirectoryPath(metadata, "A/"), a_remote);
    EXPECT_EQ(generateObjectKeyPrefixForDirectoryPath(metadata, "A/"), a_remote);
    EXPECT_EQ(generateObjectKeyPrefixForDirectoryPath(metadata, "A/"), a_remote);

    std::string ab_remote = generateObjectKeyPrefixForDirectoryPath(metadata, "A/B/");
    EXPECT_EQ(ab_remote, "ykwvvchguqasvfnkikaqtiebknfzafwv");
    EXPECT_EQ(generateObjectKeyPrefixForDirectoryPath(metadata, "A/B/"), ab_remote);
    EXPECT_EQ(generateObjectKeyPrefixForDirectoryPath(metadata, "A/B/"), ab_remote);
    EXPECT_EQ(generateObjectKeyPrefixForDirectoryPath(metadata, "A/B/"), ab_remote);

    std::string file_1_remote = generateObjectKeyForPath(metadata, "/A/file_1");
    EXPECT_EQ(file_1_remote, "./RemoteLayout/faefxnlkbtfqgxcbfqfjtztsocaqrnqn/file_1");
    EXPECT_EQ(file_1_remote, fmt::format("./RemoteLayout/{}/file_1", a_remote));

    std::string file_2_remote = generateObjectKeyForPath(metadata, "/A/B/file_2");
    EXPECT_EQ(file_2_remote, "./RemoteLayout/ykwvvchguqasvfnkikaqtiebknfzafwv/file_2");
    EXPECT_EQ(file_2_remote, fmt::format("./RemoteLayout/{}/file_2", ab_remote));

    /// Root files
    EXPECT_EQ(generateObjectKeyForPath(metadata, "root_file"), "./RemoteLayout/__root/root_file");
}

TEST_F(MetadataPlainRewritableDiskTest, RootFiles)
{
    thread_local_rng.seed(42);

    auto metadata = getMetadataStorage("RootFiles");
    auto object_storage = getObjectStorage("RootFiles");

    {
        auto tx = metadata->createTransaction();
        size_t a_size = writeObject(object_storage, tx->generateObjectKeyForPath("/A").serialize(), "A");
        size_t b_size = writeObject(object_storage, tx->generateObjectKeyForPath("/B").serialize(), "B");
        tx->createMetadataFile("/A", {StoredObject("A", "A", a_size)});
        tx->createMetadataFile("/B", {StoredObject("B", "B", b_size)});
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsDirectory(""));
    EXPECT_TRUE(metadata->existsDirectory("/"));
    EXPECT_TRUE(metadata->existsFile("A"));
    EXPECT_TRUE(metadata->existsFile("/A"));
    EXPECT_TRUE(metadata->existsFile("B"));
    EXPECT_TRUE(metadata->existsFile("/B"));

    {
        auto tx = metadata->createTransaction();
        tx->moveFile("/A", "/C");
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_FALSE(metadata->existsFile("A"));
    EXPECT_FALSE(metadata->existsFile("/A"));
    EXPECT_TRUE(metadata->existsFile("C"));
    EXPECT_TRUE(metadata->existsFile("/C"));

    metadata = restartMetadataStorage("RootFiles");

    EXPECT_FALSE(metadata->existsFile("A"));
    EXPECT_FALSE(metadata->existsFile("/A"));
    EXPECT_TRUE(metadata->existsFile("C"));
    EXPECT_TRUE(metadata->existsFile("/C"));

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("X");
        tx->moveFile("/C", "/X/C");
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_FALSE(metadata->existsFile("A"));
    EXPECT_FALSE(metadata->existsFile("/A"));
    EXPECT_TRUE(metadata->existsFile("B"));
    EXPECT_TRUE(metadata->existsFile("/B"));
    EXPECT_FALSE(metadata->existsFile("C"));
    EXPECT_FALSE(metadata->existsFile("/C"));
    EXPECT_TRUE(metadata->existsFile("X/C"));
    EXPECT_TRUE(metadata->existsFile("/X/C"));
    EXPECT_EQ(listAllBlobs("RootFiles"), std::vector<std::string>({
        "./RootFiles/__meta/ykwvvchguqasvfnkikaqtiebknfzafwv/prefix.path",  /// X
        "./RootFiles/__root/B",                                             /// /B
        "./RootFiles/ykwvvchguqasvfnkikaqtiebknfzafwv/C"                    /// X/C
    }));
}

TEST_F(MetadataPlainRewritableDiskTest, RemoveRoot)
{
    auto metadata = getMetadataStorage("RemoveRecursiveRoot");
    auto object_storage = getObjectStorage("RemoveRecursiveRoot");

    {
        auto tx = metadata->createTransaction();
        size_t a_size = writeObject(object_storage, tx->generateObjectKeyForPath("/A").serialize(), "A");
        size_t b_size = writeObject(object_storage, tx->generateObjectKeyForPath("/B").serialize(), "B");
        tx->createMetadataFile("/A", {StoredObject("A", "A", a_size)});
        tx->createMetadataFile("/B", {StoredObject("B", "B", b_size)});
        tx->commit(DB::NoCommitOptions{});
    }

    {
        auto tx = metadata->createTransaction();
        tx->removeDirectory("/");
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }

    {
        auto tx = metadata->createTransaction();
        tx->removeRecursive("/", /*should_remove_blob=*/nullptr);
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_EQ(listAllBlobs("RemoveRecursiveRoot"), std::vector<std::string>({
        "./RemoveRecursiveRoot/__root/A",
        "./RemoveRecursiveRoot/__root/B"
    }));

    {
        StoredObjects files_objects;
        files_objects.append_range(metadata->getStorageObjects("/A"));
        files_objects.append_range(metadata->getStorageObjects("/B"));

        auto tx = metadata->createTransaction();
        tx->unlinkFile("/A", /*if_exists=*/false, /*should_remove_objects=*/true);
        tx->unlinkFile("/B", /*if_exists=*/false, /*should_remove_objects=*/true);
        tx->commit(DB::NoCommitOptions{});

        object_storage->removeObjectsIfExist(files_objects);
    }

    EXPECT_EQ(listAllBlobs("RemoveRecursiveRoot"), std::vector<std::string>({}));

    {
        auto tx = metadata->createTransaction();
        tx->removeDirectory("/");
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsDirectory("/"));
    metadata = restartMetadataStorage("RemoveRecursiveRoot");

    {
        auto tx = metadata->createTransaction();
        tx->removeDirectory("/");
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsDirectory("/"));
}

TEST_F(MetadataPlainRewritableDiskTest, UnlinkNonExisting)
{
    auto metadata = getMetadataStorage("UnlinkNonExisting");
    auto object_storage = getObjectStorage("UnlinkNonExisting");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectoryRecursive("A/B/C");
        tx->commit(DB::NoCommitOptions{});
    }

    {
        auto tx = metadata->createTransaction();
        tx->unlinkFile("non-existing", /*if_exists=*/false, /*should_remove_objects=*/true);
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }

    {
        auto tx = metadata->createTransaction();
        tx->unlinkFile("non-existing/A", /*if_exists=*/false, /*should_remove_objects=*/true);
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }

    {
        auto tx = metadata->createTransaction();
        tx->unlinkFile("A/non-existing", /*if_exists=*/false, /*should_remove_objects=*/true);
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }
}

TEST_F(MetadataPlainRewritableDiskTest, MoveReplaceNonExisting)
{
    auto metadata = getMetadataStorage("MoveNonExisting");
    auto object_storage = getObjectStorage("MoveNonExisting");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectoryRecursive("A/B/C");
        tx->commit(DB::NoCommitOptions{});
    }

    {
        auto tx = metadata->createTransaction();
        tx->moveDirectory("non-existing", "other-place");
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }

    {
        auto tx = metadata->createTransaction();
        tx->moveDirectory("non-existing/A", "other-place");
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }

    {
        auto tx = metadata->createTransaction();
        tx->moveDirectory("A/non-existing", "other-place");
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }

    {
        auto tx = metadata->createTransaction();
        tx->moveFile("non-existing", "other-place");
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }

    {
        auto tx = metadata->createTransaction();
        tx->moveFile("non-existing/A", "other-place");
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }

    {
        auto tx = metadata->createTransaction();
        tx->moveFile("A/non-existing", "other-place");
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }

    {
        auto tx = metadata->createTransaction();
        tx->replaceFile("non-existing", "other-place");
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }

    {
        auto tx = metadata->createTransaction();
        tx->replaceFile("non-existing/A", "other-place");
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }

    {
        auto tx = metadata->createTransaction();
        tx->replaceFile("A/non-existing", "other-place");
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }
}

TEST_F(MetadataPlainRewritableDiskTest, RemoveNonExisting)
{
    auto metadata = getMetadataStorage("RemoveNonExisting");
    auto object_storage = getObjectStorage("RemoveNonExisting");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectoryRecursive("A/B/C");
        tx->commit(DB::NoCommitOptions{});
    }

    {
        auto tx = metadata->createTransaction();
        tx->removeDirectory("non-existing");
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }

    {
        auto tx = metadata->createTransaction();
        tx->removeDirectory("non-existing/A");
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }

    {
        auto tx = metadata->createTransaction();
        tx->removeDirectory("A/non-existing");
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }

    {
        auto tx = metadata->createTransaction();
        tx->removeRecursive("non-existing", /*should_remove_blob=*/nullptr);
        tx->commit(DB::NoCommitOptions{});
    }

    {
        auto tx = metadata->createTransaction();
        tx->removeRecursive("non-existing/A", /*should_remove_blob=*/nullptr);
        tx->commit(DB::NoCommitOptions{});
    }

    {
        auto tx = metadata->createTransaction();
        tx->removeRecursive("A/non-existing", /*should_remove_blob=*/nullptr);
        tx->commit(DB::NoCommitOptions{});
    }
}

TEST_F(MetadataPlainRewritableDiskTest, HardLinkNonExisting)
{
    auto metadata = getMetadataStorage("HardLinkNonExisting");
    auto object_storage = getObjectStorage("HardLinkNonExisting");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectoryRecursive("A/B/C");
        tx->commit(DB::NoCommitOptions{});
    }

    {
        auto tx = metadata->createTransaction();
        tx->createHardLink("non-existing", "other-place");
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }

    {
        auto tx = metadata->createTransaction();
        tx->createHardLink("non-existing/A", "other-place");
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }

    {
        auto tx = metadata->createTransaction();
        tx->createHardLink("A/non-existing", "other-place");
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }
}

TEST_F(MetadataPlainRewritableDiskTest, OperationsNonExisting)
{
    auto metadata = getMetadataStorage("OperationsNonExisting");
    auto object_storage = getObjectStorage("OperationsNonExisting");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectoryRecursive("A/B/C");
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_FALSE(metadata->existsFile("non-existing"));
    EXPECT_FALSE(metadata->existsDirectory("non-existing"));
    EXPECT_FALSE(metadata->existsFileOrDirectory("non-existing"));
    EXPECT_FALSE(metadata->existsFile("non-existing/A"));
    EXPECT_FALSE(metadata->existsDirectory("non-existing/A"));
    EXPECT_FALSE(metadata->existsFileOrDirectory("non-existing/A"));
    EXPECT_FALSE(metadata->existsFile("A/non-existing"));
    EXPECT_FALSE(metadata->existsDirectory("A/non-existing"));
    EXPECT_FALSE(metadata->existsFileOrDirectory("A/non-existing"));

    EXPECT_ANY_THROW(metadata->getFileSize("non-existing"));
    EXPECT_EQ(metadata->getFileSizeIfExists("non-existing"), std::nullopt);
    EXPECT_ANY_THROW(metadata->getFileSize("non-existing/A"));
    EXPECT_EQ(metadata->getFileSizeIfExists("non-existing/A"), std::nullopt);
    EXPECT_ANY_THROW(metadata->getFileSize("A/non-existing"));
    EXPECT_EQ(metadata->getFileSizeIfExists("A/non-existing"), std::nullopt);

    EXPECT_EQ(metadata->listDirectory("non-existing"), std::vector<std::string>());
    EXPECT_FALSE(metadata->iterateDirectory("non-existing")->isValid());
    EXPECT_EQ(metadata->listDirectory("non-existing/A"), std::vector<std::string>());
    EXPECT_FALSE(metadata->iterateDirectory("non-existing/A")->isValid());
    EXPECT_EQ(metadata->listDirectory("A/non-existing"), std::vector<std::string>());
    EXPECT_FALSE(metadata->iterateDirectory("A/non-existing")->isValid());

    EXPECT_ANY_THROW(metadata->getStorageObjects("non-existing"));
    EXPECT_EQ(metadata->getStorageObjectsIfExist("non-existing"), std::nullopt);
    EXPECT_ANY_THROW(metadata->getStorageObjects("non-existing/A"));
    EXPECT_EQ(metadata->getStorageObjectsIfExist("non-existing/A"), std::nullopt);
    EXPECT_ANY_THROW(metadata->getStorageObjects("A/non-existing"));
    EXPECT_EQ(metadata->getStorageObjectsIfExist("A/non-existing"), std::nullopt);

    EXPECT_ANY_THROW(metadata->getLastModified("non-existing"));
    EXPECT_EQ(metadata->getLastModifiedIfExists("non-existing"), std::nullopt);
    EXPECT_ANY_THROW(metadata->getLastModified("non-existing/A"));
    EXPECT_EQ(metadata->getLastModifiedIfExists("non-existing/A"), std::nullopt);
    EXPECT_ANY_THROW(metadata->getLastModified("A/non-existing"));
    EXPECT_EQ(metadata->getLastModifiedIfExists("A/non-existing"), std::nullopt);
}

TEST_F(MetadataPlainRewritableDiskTest, CreateFiles)
{
    auto metadata = getMetadataStorage("CreateFiles");
    auto object_storage = getObjectStorage("CreateFiles");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("/A");
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsDirectory("/A"));
    EXPECT_FALSE(metadata->existsFile("/A/f1"));

    {
        auto tx = metadata->createTransaction();
        size_t f1_size = writeObject(object_storage, tx->generateObjectKeyForPath("/A/f1").serialize(), "f1");
        tx->createMetadataFile("/A/f1", {StoredObject("A", "f1", f1_size)});
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsFile("/A/f1"));

    metadata = restartMetadataStorage("CreateFiles");
    EXPECT_TRUE(metadata->existsDirectory("/A"));
    EXPECT_TRUE(metadata->existsFile("/A/f1"));

    /// Some rewrites
    {
        auto tx = metadata->createTransaction();
        size_t size_1 = writeObject(object_storage, tx->generateObjectKeyForPath("/A/f1").serialize(), "Do the impossible, see the invisible");
        tx->createMetadataFile("/A/f1", {StoredObject("B", "f1", size_1)});
        size_t size_2 = writeObject(object_storage, tx->generateObjectKeyForPath("/A/f1").serialize(), "Touch the untouchable, break the unbreakable");
        tx->createMetadataFile("/A/f1", {StoredObject("C", "f1", size_2)});
        size_t size_3 = writeObject(object_storage, tx->generateObjectKeyForPath("/A/f1").serialize(), "Just break the rule, then you see the truth");
        tx->createMetadataFile("/A/f1", {StoredObject("G", "f1", size_3)});
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsFile("/A/f1"));

    metadata = restartMetadataStorage("CreateFiles");
    EXPECT_TRUE(metadata->existsDirectory("/A"));
    EXPECT_TRUE(metadata->existsFile("/A/f1"));

    {
        auto tx = metadata->createTransaction();
        tx->unlinkFile("/A/f1", /*if_exists=*/false, /*should_remove_objects=*/true);
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsDirectory("/A"));
    EXPECT_FALSE(metadata->existsFile("/A/f1"));

    {
        auto tx = metadata->createTransaction();
        tx->unlinkFile("/A/f1", /*if_exists=*/false, /*should_remove_objects=*/true);
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }
}

TEST_F(MetadataPlainRewritableDiskTest, MoveToExisting)
{
    auto metadata = getMetadataStorage("MoveToExisting");
    auto object_storage = getObjectStorage("MoveToExisting");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("/A");
        tx->createDirectory("/B");
        tx->createDirectory("/B/A");
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsDirectory("/A"));
    EXPECT_TRUE(metadata->existsDirectory("/B"));
    EXPECT_TRUE(metadata->existsDirectory("/B/A"));

    {
        auto tx = metadata->createTransaction();
        tx->moveDirectory("/A", "/B/A");
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }

    EXPECT_TRUE(metadata->existsDirectory("/A"));
    EXPECT_TRUE(metadata->existsDirectory("/B"));
    EXPECT_TRUE(metadata->existsDirectory("/B/A"));
}

TEST_F(MetadataPlainRewritableDiskTest, CreateDirectoryUndo)
{
    auto metadata = getMetadataStorage("CreateDirectoryUndo");
    auto object_storage = getObjectStorage("CreateDirectoryUndo");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("/A");
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsDirectory("/A"));

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("/A/B");
        tx->createDirectory("/A/B");
        tx->createDirectory("/A/B");
        tx->createDirectory("/A/B");
        tx->moveDirectory("non-existing", "other-place");
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }

    EXPECT_TRUE(metadata->existsDirectory("/A"));
    EXPECT_FALSE(metadata->existsDirectory("/A/B"));

    metadata = restartMetadataStorage("CreateDirectoryUndo");
    EXPECT_TRUE(metadata->existsDirectory("/A"));
    EXPECT_FALSE(metadata->existsDirectory("/A/B"));
}

TEST_F(MetadataPlainRewritableDiskTest, CreateHardLink)
{
    thread_local_rng.seed(42);

    auto metadata = getMetadataStorage("CreateHardLink");
    auto object_storage = getObjectStorage("CreateHardLink");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("/A");
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsDirectory("/A"));

    {
        auto tx = metadata->createTransaction();
        size_t f1_size = writeObject(object_storage, tx->generateObjectKeyForPath("/A/f1").serialize(), "f1");
        tx->createMetadataFile("/A/f1", {StoredObject("f1", "f1", f1_size)});
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsFile("/A/f1"));
    EXPECT_FALSE(metadata->existsFile("/A/f2"));

    {
        auto tx = metadata->createTransaction();
        tx->createHardLink("/A/f1", "A/f2");
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsFile("/A/f1"));
    EXPECT_TRUE(metadata->existsFile("/A/f2"));

    metadata = restartMetadataStorage("CreateHardLink");
    EXPECT_TRUE(metadata->existsDirectory("/A"));
    EXPECT_TRUE(metadata->existsFile("/A/f1"));
    EXPECT_TRUE(metadata->existsFile("/A/f2"));
    EXPECT_EQ(listAllBlobs("CreateHardLink"), std::vector<std::string>({
        "./CreateHardLink/__meta/faefxnlkbtfqgxcbfqfjtztsocaqrnqn/prefix.path",  /// /A
        "./CreateHardLink/faefxnlkbtfqgxcbfqfjtztsocaqrnqn/f1",                  /// /A/f1
        "./CreateHardLink/faefxnlkbtfqgxcbfqfjtztsocaqrnqn/f2"                   /// /A/f2
    }));
}

TEST_F(MetadataPlainRewritableDiskTest, CreateHardLinkUndo)
{
    thread_local_rng.seed(42);

    auto metadata = getMetadataStorage("CreateHardLinkUndo");
    auto object_storage = getObjectStorage("CreateHardLinkUndo");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("/A");
        tx->commit(DB::NoCommitOptions{});
    }

    {
        auto tx = metadata->createTransaction();
        size_t f1_size = writeObject(object_storage, tx->generateObjectKeyForPath("/A/f1").serialize(), "f1");
        tx->createMetadataFile("/A/f1", {StoredObject("f1", "f1", f1_size)});
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsDirectory("/A"));
    EXPECT_TRUE(metadata->existsFile("/A/f1"));
    EXPECT_FALSE(metadata->existsFile("/A/f2"));

    {
        auto tx = metadata->createTransaction();
        tx->createHardLink("/A/f1", "A/f2");
        tx->createHardLink("/B/f1", "A/f2");
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }

    EXPECT_EQ(listAllBlobs("CreateHardLinkUndo").size(), 2);
    EXPECT_TRUE(metadata->existsDirectory("/A"));
    EXPECT_TRUE(metadata->existsFile("/A/f1"));
    EXPECT_FALSE(metadata->existsFile("/A/f2"));

    {
        auto tx = metadata->createTransaction();
        tx->createHardLink("/A/f1", "A/f2");
        tx->createHardLink("f1", "f2");
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }

    EXPECT_EQ(listAllBlobs("CreateHardLinkUndo").size(), 2);
    EXPECT_TRUE(metadata->existsDirectory("/A"));
    EXPECT_TRUE(metadata->existsFile("/A/f1"));
    EXPECT_FALSE(metadata->existsFile("/A/f2"));

    {
        auto tx = metadata->createTransaction();
        tx->createHardLink("/A/f1", "A/f2");
        tx->createHardLink("/A/f1", "A/f2");
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }

    metadata = restartMetadataStorage("CreateHardLinkUndo");
    EXPECT_TRUE(metadata->existsDirectory("/A"));
    EXPECT_TRUE(metadata->existsFile("/A/f1"));
    EXPECT_FALSE(metadata->existsFile("/A/f2"));
    EXPECT_EQ(listAllBlobs("CreateHardLinkUndo"), std::vector<std::string>({
        "./CreateHardLinkUndo/__meta/faefxnlkbtfqgxcbfqfjtztsocaqrnqn/prefix.path",  /// /A
        "./CreateHardLinkUndo/faefxnlkbtfqgxcbfqfjtztsocaqrnqn/f1",                  /// /A/f1
    }));
}

TEST_F(MetadataPlainRewritableDiskTest, CreateHardLinkRootFiles)
{
    thread_local_rng.seed(42);

    auto metadata = getMetadataStorage("CreateHardLinkRootFiles");
    auto object_storage = getObjectStorage("CreateHardLinkRootFiles");

    {
        auto tx = metadata->createTransaction();
        size_t f1_size = writeObject(object_storage, tx->generateObjectKeyForPath("f1").serialize(), "f1");
        tx->createMetadataFile("/f1", {StoredObject("f1", "f1", f1_size)});
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsFile("/f1"));

    {
        auto tx = metadata->createTransaction();
        tx->createHardLink("/f1", "/f2");
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsFile("/f1"));
    EXPECT_TRUE(metadata->existsFile("/f2"));

    {
        auto tx = metadata->createTransaction();
        tx->createHardLink("/f2", "/f3");
        tx->createHardLink("/f1", "/f2");
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }

    metadata = restartMetadataStorage("CreateHardLinkRootFiles");
    EXPECT_TRUE(metadata->existsFile("/f1"));
    EXPECT_TRUE(metadata->existsFile("/f2"));
    EXPECT_FALSE(metadata->existsFile("/f3"));
    EXPECT_EQ(listAllBlobs("CreateHardLinkRootFiles"), std::vector<std::string>({
        "./CreateHardLinkRootFiles/__root/f1",
        "./CreateHardLinkRootFiles/__root/f2",
    }));
}

TEST_F(MetadataPlainRewritableDiskTest, MoveVirtual)
{
    thread_local_rng.seed(42);

    auto metadata = getMetadataStorage("MoveVirtual");
    auto object_storage = getObjectStorage("MoveVirtual");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectoryRecursive("/A/B/C/D/E");
        tx->createDirectoryRecursive("/A/B/C/X/Y");
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsDirectory("/A/B/C"));
    EXPECT_TRUE(metadata->existsDirectory("/A/B/C/D"));
    EXPECT_TRUE(metadata->existsDirectory("/A/B/C/X"));

    EXPECT_EQ(listAllBlobs("MoveVirtual"), std::vector<std::string>({
        "./MoveVirtual/__meta/faefxnlkbtfqgxcbfqfjtztsocaqrnqn/prefix.path",
        "./MoveVirtual/__meta/ykwvvchguqasvfnkikaqtiebknfzafwv/prefix.path",
    }));

    {
        auto tx = metadata->createTransaction();
        tx->moveDirectory("/A/B/C", "/A/B/H");
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_FALSE(metadata->existsDirectory("/A/B/C"));
    EXPECT_FALSE(metadata->existsDirectory("/A/B/C/D"));
    EXPECT_FALSE(metadata->existsDirectory("/A/B/C/X"));
    EXPECT_TRUE(metadata->existsDirectory("/A/B/H"));
    EXPECT_TRUE(metadata->existsDirectory("/A/B/H/D"));
    EXPECT_TRUE(metadata->existsDirectory("/A/B/H/X"));

    metadata = restartMetadataStorage("MoveVirtual");
    EXPECT_FALSE(metadata->existsDirectory("/A/B/C"));
    EXPECT_FALSE(metadata->existsDirectory("/A/B/C/D"));
    EXPECT_FALSE(metadata->existsDirectory("/A/B/C/X"));
    EXPECT_TRUE(metadata->existsDirectory("/A/B/H"));
    EXPECT_TRUE(metadata->existsDirectory("/A/B/H/D"));
    EXPECT_TRUE(metadata->existsDirectory("/A/B/H/X"));

    EXPECT_EQ(listAllBlobs("MoveVirtual"), std::vector<std::string>({
        "./MoveVirtual/__meta/faefxnlkbtfqgxcbfqfjtztsocaqrnqn/prefix.path",
        "./MoveVirtual/__meta/ykwvvchguqasvfnkikaqtiebknfzafwv/prefix.path",
    }));
}

TEST_F(MetadataPlainRewritableDiskTest, RemoveRecursiveVirtual)
{
    thread_local_rng.seed(42);

    auto metadata = getMetadataStorage("RemoveRecursiveVirtual");
    auto object_storage = getObjectStorage("RemoveRecursiveVirtual");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectoryRecursive("/A/B/C/D/E");
        tx->createDirectoryRecursive("/A/B/C/X/Y");
        tx->createDirectoryRecursive("/A/B/C/K/L");
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsDirectory("/A/B/C"));
    EXPECT_TRUE(metadata->existsDirectory("/A/B/C/D"));
    EXPECT_TRUE(metadata->existsDirectory("/A/B/C/X"));
    EXPECT_TRUE(metadata->existsDirectory("/A/B/C/K"));

    EXPECT_EQ(listAllBlobs("RemoveRecursiveVirtual"), std::vector<std::string>({
        "./RemoveRecursiveVirtual/__meta/faefxnlkbtfqgxcbfqfjtztsocaqrnqn/prefix.path",
        "./RemoveRecursiveVirtual/__meta/wcageakzukwtfkvkwibqrfhzrrlubsbg/prefix.path",
        "./RemoveRecursiveVirtual/__meta/ykwvvchguqasvfnkikaqtiebknfzafwv/prefix.path",
    }));

    {
        auto tx = metadata->createTransaction();
        tx->removeRecursive("/A/B/C/D", /*should_remove_blob=*/nullptr);
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsDirectory("/A/B/C"));
    EXPECT_FALSE(metadata->existsDirectory("/A/B/C/D"));
    EXPECT_FALSE(metadata->existsDirectory("/A/B/C/D/E"));
    EXPECT_TRUE(metadata->existsDirectory("/A/B/C/X"));
    EXPECT_TRUE(metadata->existsDirectory("/A/B/C/K"));

    metadata = restartMetadataStorage("RemoveRecursiveVirtual");
    EXPECT_TRUE(metadata->existsDirectory("/A/B/C"));
    EXPECT_FALSE(metadata->existsDirectory("/A/B/C/D"));
    EXPECT_FALSE(metadata->existsDirectory("/A/B/C/D/E"));
    EXPECT_TRUE(metadata->existsDirectory("/A/B/C/X"));
    EXPECT_TRUE(metadata->existsDirectory("/A/B/C/K"));

    EXPECT_EQ(listAllBlobs("RemoveRecursiveVirtual"), std::vector<std::string>({
        "./RemoveRecursiveVirtual/__meta/wcageakzukwtfkvkwibqrfhzrrlubsbg/prefix.path",
        "./RemoveRecursiveVirtual/__meta/ykwvvchguqasvfnkikaqtiebknfzafwv/prefix.path",
    }));

    {
        auto tx = metadata->createTransaction();
        tx->removeRecursive("/A/B/C", /*should_remove_blob=*/nullptr);
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_FALSE(metadata->existsDirectory("/A/B/C"));
    EXPECT_FALSE(metadata->existsDirectory("/A/B"));

    metadata = restartMetadataStorage("RemoveRecursiveVirtual");
    EXPECT_FALSE(metadata->existsDirectory("/A/B/C"));
    EXPECT_FALSE(metadata->existsDirectory("/A/B"));

    EXPECT_EQ(listAllBlobs("RemoveRecursiveVirtual"), std::vector<std::string>({}));
}

TEST_F(MetadataPlainRewritableDiskTest, VirtualSubpathTrim)
{
    thread_local_rng.seed(42);

    auto metadata = getMetadataStorage("VirtualSubpathTrim");
    auto object_storage = getObjectStorage("VirtualSubpathTrim");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectoryRecursive("/A/B/C");
        tx->createDirectoryRecursive("/A/B/C/D/E");
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsDirectory("/A/B/C"));
    EXPECT_TRUE(metadata->existsDirectory("/A/B/C/D"));
    EXPECT_TRUE(metadata->existsDirectory("/A/B/C/D/E"));

    EXPECT_EQ(listAllBlobs("VirtualSubpathTrim"), std::vector<std::string>({
        "./VirtualSubpathTrim/__meta/faefxnlkbtfqgxcbfqfjtztsocaqrnqn/prefix.path",
        "./VirtualSubpathTrim/__meta/ykwvvchguqasvfnkikaqtiebknfzafwv/prefix.path",
    }));

    {
        auto tx = metadata->createTransaction();
        tx->removeDirectory("/A/B/C/D/E");
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsDirectory("/A/B/C"));
    EXPECT_FALSE(metadata->existsDirectory("/A/B/C/D"));
    EXPECT_FALSE(metadata->existsDirectory("/A/B/C/D/E"));

    metadata = restartMetadataStorage("VirtualSubpathTrim");
    EXPECT_TRUE(metadata->existsDirectory("/A/B/C"));
    EXPECT_FALSE(metadata->existsDirectory("/A/B/C/D"));
    EXPECT_FALSE(metadata->existsDirectory("/A/B/C/D/E"));

    EXPECT_EQ(listAllBlobs("VirtualSubpathTrim"), std::vector<std::string>({
        "./VirtualSubpathTrim/__meta/faefxnlkbtfqgxcbfqfjtztsocaqrnqn/prefix.path",
    }));
}

TEST_F(MetadataPlainRewritableDiskTest, FileRemoteInfo)
{
    thread_local_rng.seed(42);

    auto metadata = getMetadataStorage("FileRemoteInfo");
    auto object_storage = getObjectStorage("FileRemoteInfo");

    size_t written_bytes = 0;
    time_t now = std::time(nullptr);
    {
        auto tx = metadata->createTransaction();
        tx->createDirectoryRecursive("/A/B/C");
        tx->commit(DB::NoCommitOptions{});

        tx = metadata->createTransaction();
        written_bytes = writeObject(object_storage, tx->generateObjectKeyForPath("/A/B/C/file").serialize(), "don't stop! don't stop!");
        tx->createMetadataFile("/A/B/C/file", {StoredObject("file", "file", written_bytes)});
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsDirectory("/A/B/C"));
    EXPECT_TRUE(metadata->existsFile("/A/B/C/file"));
    EXPECT_EQ(metadata->getFileSizeIfExists("/A/B/C/file"), written_bytes);
    EXPECT_EQ(metadata->getFileSize("/A/B/C/file"), written_bytes);
    EXPECT_THAT(metadata->getLastModifiedIfExists("/A/B/C/file")->epochTime(), testing::AllOf(testing::Ge(now - 1), testing::Le(now + 1)));
    EXPECT_THAT(metadata->getLastModifiedIfExists("/A/B/C/file")->epochTime(), testing::AllOf(testing::Ge(now - 1), testing::Le(now + 1)));

    EXPECT_EQ(listAllBlobs("FileRemoteInfo"), std::vector<std::string>({
        "./FileRemoteInfo/__meta/faefxnlkbtfqgxcbfqfjtztsocaqrnqn/prefix.path",
        "./FileRemoteInfo/faefxnlkbtfqgxcbfqfjtztsocaqrnqn/file",
    }));

    std::this_thread::sleep_for(std::chrono::seconds(5));

    metadata = restartMetadataStorage("FileRemoteInfo");
    EXPECT_TRUE(metadata->existsDirectory("/A/B/C"));
    EXPECT_TRUE(metadata->existsFile("/A/B/C/file"));
    EXPECT_EQ(metadata->getFileSizeIfExists("/A/B/C/file"), written_bytes);
    EXPECT_EQ(metadata->getFileSize("/A/B/C/file"), written_bytes);
    EXPECT_THAT(metadata->getLastModifiedIfExists("/A/B/C/file")->epochTime(), testing::AllOf(testing::Ge(now - 1), testing::Le(now + 1)));
    EXPECT_THAT(metadata->getLastModifiedIfExists("/A/B/C/file")->epochTime(), testing::AllOf(testing::Ge(now - 1), testing::Le(now + 1)));
}

TEST_F(MetadataPlainRewritableDiskTest, FileRemoteInfoAfterMove)
{
    thread_local_rng.seed(42);

    auto metadata = getMetadataStorage("FileRemoteInfoAfterMove");
    auto object_storage = getObjectStorage("FileRemoteInfoAfterMove");

    size_t written_bytes_file = 0;
    size_t written_bytes_tmp = 0;
    {
        auto tx = metadata->createTransaction();
        tx->createDirectoryRecursive("/A/B/C");
        tx->commit(DB::NoCommitOptions{});

        tx = metadata->createTransaction();
        written_bytes_file = writeObject(object_storage, tx->generateObjectKeyForPath("/A/B/C/file").serialize(), "don't stop! don't stop!");
        tx->createMetadataFile("/A/B/C/file", {StoredObject("file", "file", written_bytes_file)});
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_EQ(metadata->getFileSize("/A/B/C/file"), written_bytes_file);

    {
        auto tx = metadata->createTransaction();
        written_bytes_tmp = writeObject(object_storage, tx->generateObjectKeyForPath("/A/B/C/tmp").serialize(), "stop!");
        tx->createMetadataFile("/A/B/C/tmp", {StoredObject("tmp", "tmp", written_bytes_tmp)});
        tx->replaceFile("/A/B/C/tmp", "/A/B/C/file");
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsFile("/A/B/C/file"));
    EXPECT_FALSE(metadata->existsFile("/A/B/C/tmp"));
    EXPECT_NE(written_bytes_file, written_bytes_tmp);
    EXPECT_EQ(metadata->getFileSize("/A/B/C/file"), written_bytes_tmp);
}

TEST_F(MetadataPlainRewritableDiskTest, FileRemoteInfoMoveUndo)
{
    thread_local_rng.seed(42);

    auto metadata = getMetadataStorage("FileRemoteInfoMoveUndo");
    auto object_storage = getObjectStorage("FileRemoteInfoMoveUndo");

    size_t written_bytes_file = 0;
    size_t written_bytes_tmp = 0;
    {
        auto tx = metadata->createTransaction();
        tx->createDirectoryRecursive("/A/B/C");
        tx->commit(DB::NoCommitOptions{});

        tx = metadata->createTransaction();
        written_bytes_file = writeObject(object_storage, tx->generateObjectKeyForPath("/A/B/C/file").serialize(), "don't stop! don't stop!");
        tx->createMetadataFile("/A/B/C/file", {StoredObject("file", "file", written_bytes_file)});
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_EQ(metadata->getFileSize("/A/B/C/file"), written_bytes_file);

    {
        auto tx = metadata->createTransaction();
        written_bytes_tmp = writeObject(object_storage, tx->generateObjectKeyForPath("/A/B/C/tmp").serialize(), "stop!");
        tx->createMetadataFile("/A/B/C/tmp", {StoredObject("tmp", "tmp", written_bytes_tmp)});
        tx->replaceFile("/A/B/C/tmp", "/A/B/C/file");
        tx->moveFile("non-existing", "non-existing");
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }

    EXPECT_TRUE(metadata->existsFile("/A/B/C/file"));
    EXPECT_FALSE(metadata->existsFile("/A/B/C/tmp"));
    EXPECT_EQ(metadata->getFileSize("/A/B/C/file"), written_bytes_file);

    auto content = readObject(object_storage, metadata->getStorageObjects("/A/B/C/file").front().remote_path);
    EXPECT_EQ(content, "don't stop! don't stop!");

    EXPECT_EQ(listAllBlobs("FileRemoteInfoMoveUndo"), std::vector<std::string>({
        "./FileRemoteInfoMoveUndo/__meta/faefxnlkbtfqgxcbfqfjtztsocaqrnqn/prefix.path",
        "./FileRemoteInfoMoveUndo/faefxnlkbtfqgxcbfqfjtztsocaqrnqn/file",
        "./FileRemoteInfoMoveUndo/faefxnlkbtfqgxcbfqfjtztsocaqrnqn/tmp",
    }));
}

TEST_F(MetadataPlainRewritableDiskTest, OwnChangesVisibility)
{
    thread_local_rng.seed(42);

    auto metadata = getMetadataStorage("OwnChangesVisibility");
    auto object_storage = getObjectStorage("OwnChangesVisibility");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectoryRecursive("/A/B/C");
        size_t written_bytes = writeObject(object_storage, tx->generateObjectKeyForPath("/A/B/C/file").serialize(), "finally!");
        tx->createMetadataFile("/A/B/C/file", {StoredObject("file", "file", written_bytes)});
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsDirectory("/A/B/C"));
    EXPECT_TRUE(metadata->existsFile("/A/B/C/file"));
    EXPECT_EQ(readObject(object_storage, metadata->getStorageObjects("/A/B/C/file").front().remote_path), "finally!");

    EXPECT_EQ(listAllBlobs("OwnChangesVisibility"), std::vector<std::string>({
        "./OwnChangesVisibility/__meta/faefxnlkbtfqgxcbfqfjtztsocaqrnqn/prefix.path",
        "./OwnChangesVisibility/faefxnlkbtfqgxcbfqfjtztsocaqrnqn/file",
    }));

    metadata = restartMetadataStorage("OwnChangesVisibility");
    EXPECT_TRUE(metadata->existsDirectory("/A/B/C"));
    EXPECT_TRUE(metadata->existsFile("/A/B/C/file"));
    EXPECT_EQ(readObject(object_storage, metadata->getStorageObjects("/A/B/C/file").front().remote_path), "finally!");
}

TEST_F(MetadataPlainRewritableDiskTest, UncommittedMove)
{
    thread_local_rng.seed(42);

    auto metadata = getMetadataStorage("UncommittedMove");
    auto object_storage = getObjectStorage("UncommittedMove");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectoryRecursive("/A/B/C");
        tx->createDirectoryRecursive("/X/Y/Z");
        size_t written_bytes = writeObject(object_storage, tx->generateObjectKeyForPath("/A/B/C/file").serialize(), "finally!");
        tx->createMetadataFile("/A/B/C/file", {StoredObject("file", "file", written_bytes)});
        tx->moveFile("/A/B/C/file", "/X/Y/Z/file");
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsDirectory("/A/B/C"));
    EXPECT_TRUE(metadata->existsDirectory("/X/Y/Z"));
    EXPECT_FALSE(metadata->existsFile("/A/B/C/file"));
    EXPECT_TRUE(metadata->existsFile("/X/Y/Z/file"));
    EXPECT_EQ(readObject(object_storage, metadata->getStorageObjects("/X/Y/Z/file").front().remote_path), "finally!");

    EXPECT_EQ(listAllBlobs("UncommittedMove"), std::vector<std::string>({
        "./UncommittedMove/__meta/faefxnlkbtfqgxcbfqfjtztsocaqrnqn/prefix.path",
        "./UncommittedMove/__meta/ykwvvchguqasvfnkikaqtiebknfzafwv/prefix.path",
        "./UncommittedMove/ykwvvchguqasvfnkikaqtiebknfzafwv/file"
    }));

    metadata = restartMetadataStorage("UncommittedMove");
    EXPECT_TRUE(metadata->existsDirectory("/A/B/C"));
    EXPECT_TRUE(metadata->existsDirectory("/X/Y/Z"));
    EXPECT_FALSE(metadata->existsFile("/A/B/C/file"));
    EXPECT_TRUE(metadata->existsFile("/X/Y/Z/file"));
    EXPECT_EQ(readObject(object_storage, metadata->getStorageObjects("/X/Y/Z/file").front().remote_path), "finally!");
}

TEST_F(MetadataPlainRewritableDiskTest, UncommittedHardlink)
{
    thread_local_rng.seed(42);

    auto metadata = getMetadataStorage("UncommittedHardlink");
    auto object_storage = getObjectStorage("UncommittedHardlink");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectoryRecursive("/A/B/C");
        tx->createDirectoryRecursive("/X/Y/Z");
        size_t written_bytes = writeObject(object_storage, tx->generateObjectKeyForPath("/A/B/C/file").serialize(), "finally!");
        tx->createMetadataFile("/A/B/C/file", {StoredObject("file", "file", written_bytes)});
        tx->createHardLink("/A/B/C/file", "/X/Y/Z/file");
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsDirectory("/A/B/C"));
    EXPECT_TRUE(metadata->existsDirectory("/X/Y/Z"));
    EXPECT_TRUE(metadata->existsFile("/A/B/C/file"));
    EXPECT_TRUE(metadata->existsFile("/X/Y/Z/file"));
    EXPECT_EQ(readObject(object_storage, metadata->getStorageObjects("/A/B/C/file").front().remote_path), "finally!");
    EXPECT_EQ(readObject(object_storage, metadata->getStorageObjects("/X/Y/Z/file").front().remote_path), "finally!");

    EXPECT_EQ(listAllBlobs("UncommittedHardlink"), std::vector<std::string>({
        "./UncommittedHardlink/__meta/faefxnlkbtfqgxcbfqfjtztsocaqrnqn/prefix.path",
        "./UncommittedHardlink/__meta/ykwvvchguqasvfnkikaqtiebknfzafwv/prefix.path",
        "./UncommittedHardlink/faefxnlkbtfqgxcbfqfjtztsocaqrnqn/file",
        "./UncommittedHardlink/ykwvvchguqasvfnkikaqtiebknfzafwv/file"
    }));

    metadata = restartMetadataStorage("UncommittedHardlink");
    EXPECT_TRUE(metadata->existsDirectory("/A/B/C"));
    EXPECT_TRUE(metadata->existsDirectory("/X/Y/Z"));
    EXPECT_TRUE(metadata->existsFile("/A/B/C/file"));
    EXPECT_TRUE(metadata->existsFile("/X/Y/Z/file"));
    EXPECT_EQ(readObject(object_storage, metadata->getStorageObjects("/A/B/C/file").front().remote_path), "finally!");
    EXPECT_EQ(readObject(object_storage, metadata->getStorageObjects("/X/Y/Z/file").front().remote_path), "finally!");
}

TEST_F(MetadataPlainRewritableDiskTest, UncommittedHardlinkUndo)
{
    thread_local_rng.seed(42);

    auto metadata = getMetadataStorage("UncommittedHardlinkUndo");
    auto object_storage = getObjectStorage("UncommittedHardlinkUndo");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectoryRecursive("/A/B/C");
        tx->createDirectoryRecursive("/X/Y/Z");
        size_t written_bytes = writeObject(object_storage, tx->generateObjectKeyForPath("/A/B/C/file").serialize(), "finally!");
        tx->createMetadataFile("/A/B/C/file", {StoredObject("file", "file", written_bytes)});
        tx->createHardLink("/A/B/C/file", "/X/Y/Z/file");
        tx->moveFile("non-existing", "other-place");
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }

    EXPECT_FALSE(metadata->existsDirectory("/A/B/C"));
    EXPECT_FALSE(metadata->existsDirectory("/X/Y/Z"));
    EXPECT_EQ(listAllBlobs("UncommittedHardlinkUndo"), std::vector<std::string>({
        "./UncommittedHardlinkUndo/faefxnlkbtfqgxcbfqfjtztsocaqrnqn/file",
    }));

    metadata = restartMetadataStorage("UncommittedHardlinkUndo");
    EXPECT_FALSE(metadata->existsDirectory("/A/B/C"));
    EXPECT_FALSE(metadata->existsDirectory("/X/Y/Z"));
}

TEST_F(MetadataPlainRewritableDiskTest, UncommittedDirectoryMoves)
{
    thread_local_rng.seed(42);

    auto metadata = getMetadataStorage("UncommittedDirectoryMoves");
    auto object_storage = getObjectStorage("UncommittedDirectoryMoves");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectoryRecursive("/A/B/C");

        writeObject(object_storage, tx->generateObjectKeyForPath("/A/B/C/file").serialize(), "1");
        tx->createMetadataFile("/A/B/C/file", {StoredObject("file", "file", 1)});

        tx->moveDirectory("/A/B/C", "/A/B/D");
        tx->createDirectory("/A/B/C");

        writeObject(object_storage, tx->generateObjectKeyForPath("/A/B/C/file").serialize(), "2");
        tx->createMetadataFile("/A/B/C/file", {StoredObject("file", "file", 1)});

        writeObject(object_storage, tx->generateObjectKeyForPath("/A/B/D/file_2").serialize(), "3");
        tx->createMetadataFile("/A/B/D/file_2", {StoredObject("file_2", "file_2", 1)});

        tx->moveDirectory("/A/B/D", "/A/B/C/X");
        tx->createDirectory("/A/B/C/X/Y");

        writeObject(object_storage, tx->generateObjectKeyForPath("/A/B/C/X/Y/file").serialize(), "4");
        tx->createMetadataFile("/A/B/C/X/Y/file", {StoredObject("file", "file", 1)});

        writeObject(object_storage, tx->generateObjectKeyForPath("/A/B/C/X/file_3").serialize(), "5");
        tx->createMetadataFile("/A/B/C/X/file_3", {StoredObject("file_3", "file_3", 1)});

        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_EQ(listAllBlobs("UncommittedDirectoryMoves"), std::vector<std::string>({
        "./UncommittedDirectoryMoves/__meta/faefxnlkbtfqgxcbfqfjtztsocaqrnqn/prefix.path",  /// /A/B/C/X
        "./UncommittedDirectoryMoves/__meta/wcageakzukwtfkvkwibqrfhzrrlubsbg/prefix.path",  /// /A/B/C
        "./UncommittedDirectoryMoves/__meta/ykwvvchguqasvfnkikaqtiebknfzafwv/prefix.path",  /// /A/B/C/X/Y
        "./UncommittedDirectoryMoves/faefxnlkbtfqgxcbfqfjtztsocaqrnqn/file",                /// /A/B/C/X/file
        "./UncommittedDirectoryMoves/faefxnlkbtfqgxcbfqfjtztsocaqrnqn/file_2",              /// /A/B/C/X/file_2
        "./UncommittedDirectoryMoves/faefxnlkbtfqgxcbfqfjtztsocaqrnqn/file_3",              /// /A/B/C/X/file_3
        "./UncommittedDirectoryMoves/wcageakzukwtfkvkwibqrfhzrrlubsbg/file",                /// /A/B/C/file
        "./UncommittedDirectoryMoves/ykwvvchguqasvfnkikaqtiebknfzafwv/file",                /// /A/B/C/X/Y/file
    }));

    EXPECT_EQ(sorted(metadata->listDirectory("/")), std::vector<std::string>({"A"}));
    EXPECT_EQ(sorted(metadata->listDirectory("/A")), std::vector<std::string>({"B"}));
    EXPECT_EQ(sorted(metadata->listDirectory("/A/B")), std::vector<std::string>({"C"}));
    EXPECT_EQ(sorted(metadata->listDirectory("/A/B/C")), std::vector<std::string>({"X", "file"}));
    EXPECT_EQ(sorted(metadata->listDirectory("/A/B/C/X")), std::vector<std::string>({"Y", "file", "file_2", "file_3"}));
    EXPECT_EQ(sorted(metadata->listDirectory("/A/B/C/X/Y")), std::vector<std::string>({"file"}));
    EXPECT_EQ(readObject(object_storage, metadata->getStorageObjects("/A/B/C/X/file").front().remote_path), "1");
    EXPECT_EQ(readObject(object_storage, metadata->getStorageObjects("/A/B/C/file").front().remote_path), "2");
    EXPECT_EQ(readObject(object_storage, metadata->getStorageObjects("/A/B/C/X/file_2").front().remote_path), "3");
    EXPECT_EQ(readObject(object_storage, metadata->getStorageObjects("/A/B/C/X/Y/file").front().remote_path), "4");
    EXPECT_EQ(readObject(object_storage, metadata->getStorageObjects("/A/B/C/X/file_3").front().remote_path), "5");

    metadata = restartMetadataStorage("UncommittedDirectoryMoves");
    EXPECT_EQ(sorted(metadata->listDirectory("/")), std::vector<std::string>({"A"}));
    EXPECT_EQ(sorted(metadata->listDirectory("/A")), std::vector<std::string>({"B"}));
    EXPECT_EQ(sorted(metadata->listDirectory("/A/B")), std::vector<std::string>({"C"}));
    EXPECT_EQ(sorted(metadata->listDirectory("/A/B/C")), std::vector<std::string>({"X", "file"}));
    EXPECT_EQ(sorted(metadata->listDirectory("/A/B/C/X")), std::vector<std::string>({"Y", "file", "file_2", "file_3"}));
    EXPECT_EQ(sorted(metadata->listDirectory("/A/B/C/X/Y")), std::vector<std::string>({"file"}));
    EXPECT_EQ(readObject(object_storage, metadata->getStorageObjects("/A/B/C/X/file").front().remote_path), "1");
    EXPECT_EQ(readObject(object_storage, metadata->getStorageObjects("/A/B/C/file").front().remote_path), "2");
    EXPECT_EQ(readObject(object_storage, metadata->getStorageObjects("/A/B/C/X/file_2").front().remote_path), "3");
    EXPECT_EQ(readObject(object_storage, metadata->getStorageObjects("/A/B/C/X/Y/file").front().remote_path), "4");
    EXPECT_EQ(readObject(object_storage, metadata->getStorageObjects("/A/B/C/X/file_3").front().remote_path), "5");
}

TEST_F(MetadataPlainRewritableDiskTest, CreateDirectoryFromVirtualNode)
{
    thread_local_rng.seed(42);

    auto metadata = getMetadataStorage("CreateDirectoryFromVirtualNode");
    auto object_storage = getObjectStorage("CreateDirectoryFromVirtualNode");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectoryRecursive("/A/B/C");
        tx->commit(DB::NoCommitOptions{});

        tx = metadata->createTransaction();
        auto size_bytes = writeObject(object_storage, tx->generateObjectKeyForPath("/A/B/file").serialize(), "I'm real");
        tx->createMetadataFile("/A/B/file", {StoredObject("/A/B/file", "file", size_bytes)});
        tx->commit(DB::NoCommitOptions{});
    }
    EXPECT_EQ(readObject(object_storage, metadata->getStorageObjects("/A/B/file").front().remote_path), "I'm real");

    EXPECT_EQ(
        listAllBlobs("CreateDirectoryFromVirtualNode"),
        std::vector<std::string>({
            "./CreateDirectoryFromVirtualNode/__meta/faefxnlkbtfqgxcbfqfjtztsocaqrnqn/prefix.path",
            "./CreateDirectoryFromVirtualNode/__meta/ykwvvchguqasvfnkikaqtiebknfzafwv/prefix.path",
            "./CreateDirectoryFromVirtualNode/ykwvvchguqasvfnkikaqtiebknfzafwv/file",
        }));
}

TEST_F(MetadataPlainRewritableDiskTest, UnlinkUndoInCaseOfNetworkError)
{
    thread_local_rng.seed(42);

    auto metadata = getMetadataStorage("UnlinkUndoInCaseOfNetworkError");
    auto object_storage = getObjectStorage("UnlinkUndoInCaseOfNetworkError");

    {
        auto tx = metadata->createTransaction();

        tx->createDirectory("/A");
        auto size_bytes = writeObject(object_storage, tx->generateObjectKeyForPath("/A/file").serialize(), "This is San Francisco, city of stile disco");
        tx->createMetadataFile("/A/file", {StoredObject("/A/file", "file", size_bytes)});

        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_EQ(readObject(object_storage, metadata->getStorageObjects("/A/file").front().remote_path), "This is San Francisco, city of stile disco");

    {
        FailPointInjection::enableFailPoint("local_object_storage_network_error_during_remove");

        auto tx = metadata->createTransaction();
        tx->unlinkFile("/A/file", /*if_exists=*/false, /*should_remove_objects=*/true);
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }

    EXPECT_EQ(readObject(object_storage, metadata->getStorageObjects("/A/file").front().remote_path), "This is San Francisco, city of stile disco");
}

TEST_F(MetadataPlainRewritableDiskTest, TestComplexUnlink)
{
    thread_local_rng.seed(42);

    auto metadata = getMetadataStorage("TestComplexUnlink");
    auto object_storage = getObjectStorage("TestComplexUnlink");

    {
        auto tx = metadata->createTransaction();
        tx->unlinkFile("file", /*if_exists=*/true, /*should_remove_objects=*/true);
        tx->createMetadataFile("file", {DB::StoredObject("key", "file", 1)});
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsFile("file"));
}

TEST_F(MetadataPlainRewritableDiskTest, CreateExistingDirectory)
{
    auto metadata = getMetadataStorage("CreateExistingDirectory");
    auto object_storage = getObjectStorage("CreateExistingDirectory");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectoryRecursive("A/B/C");
        tx->commit(DB::NoCommitOptions{});
    }

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("A/B/C");
        size_t file_size = writeObject(object_storage, tx->generateObjectKeyForPath("A/B/C/file").serialize(), "1");
        tx->createMetadataFile("A/B/C/file", {StoredObject("A/B/C/file", "file", file_size)});
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsFile("A/B/C/file"));
    EXPECT_EQ(readObject(object_storage, metadata->getStorageObjects("A/B/C/file").front().remote_path), "1");

    metadata = restartMetadataStorage("CreateExistingDirectory");
    EXPECT_TRUE(metadata->existsFile("A/B/C/file"));
    EXPECT_EQ(readObject(object_storage, metadata->getStorageObjects("A/B/C/file").front().remote_path), "1");
}

TEST_F(MetadataPlainRewritableDiskTest, RecreateDirectoryInSameTransaction)
{
    auto metadata = getMetadataStorage("RecreateDirectoryInSameTransaction");
    auto object_storage = getObjectStorage("RecreateDirectoryInSameTransaction");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectoryRecursive("A/B/C");
        tx->commit(DB::NoCommitOptions{});
    }

    auto old_prefix = generateObjectKeyPrefixForDirectoryPath(metadata, "A/B/C/");

    {
        auto tx = metadata->createTransaction();
        tx->moveDirectory("A/B/C", "A/B/D");
        tx->createDirectory("A/B/C");
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_TRUE(metadata->existsDirectory("A/B/C"));
    EXPECT_TRUE(metadata->existsDirectory("A/B/D"));
    EXPECT_EQ(generateObjectKeyPrefixForDirectoryPath(metadata, "A/B/D/"), old_prefix);
    EXPECT_NE(generateObjectKeyPrefixForDirectoryPath(metadata, "A/B/C/"), old_prefix);
}

TEST_F(MetadataPlainRewritableDiskTest, TransactionViewAfterMove)
{
    auto metadata = getMetadataStorage("TransactionViewAfterMove");
    auto object_storage = getObjectStorage("TransactionViewAfterMove");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("A");
        tx->commit(DB::NoCommitOptions{});
    }

    auto a_prefix = generateObjectKeyPrefixForDirectoryPath(metadata, "A/");

    /// Creating the destination of an earlier move must be an idempotent no-op.
    {
        auto tx = metadata->createTransaction();
        tx->moveDirectory("A", "B");
        tx->createDirectory("B");
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_FALSE(metadata->existsDirectory("A"));
    EXPECT_TRUE(metadata->existsDirectory("B"));
    EXPECT_EQ(generateObjectKeyPrefixForDirectoryPath(metadata, "B/"), a_prefix);

    /// A path restored within the same transaction must be usable again.
    {
        auto tx = metadata->createTransaction();
        tx->moveDirectory("B", "A");
        tx->moveDirectory("A", "B");
        tx->createDirectory("B");
        tx->commit(DB::NoCommitOptions{});
    }

    EXPECT_FALSE(metadata->existsDirectory("A"));
    EXPECT_TRUE(metadata->existsDirectory("B"));
    EXPECT_EQ(generateObjectKeyPrefixForDirectoryPath(metadata, "B/"), a_prefix);

    metadata = restartMetadataStorage("TransactionViewAfterMove");
    EXPECT_FALSE(metadata->existsDirectory("A"));
    EXPECT_TRUE(metadata->existsDirectory("B"));
    EXPECT_EQ(generateObjectKeyPrefixForDirectoryPath(metadata, "B/"), a_prefix);
}

TEST_F(MetadataPlainRewritableDiskTest, ConcurrentCreateUnderMovedDirectory)
{
    auto metadata = getMetadataStorage("ConcurrentCreateUnderMovedDirectory");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("A");
        tx->commit(DB::NoCommitOptions{});
    }

    /// The move relocates the subtree as of the commit time: a directory committed
    /// concurrently under the source serializes before the move and travels with it.
    auto tx1 = metadata->createTransaction();
    tx1->moveDirectory("A", "B");

    {
        auto tx2 = metadata->createTransaction();
        tx2->createDirectory("A/C");
        tx2->commit(DB::NoCommitOptions{});
    }

    tx1->commit(DB::NoCommitOptions{});

    EXPECT_FALSE(metadata->existsDirectory("A"));
    EXPECT_TRUE(metadata->existsDirectory("B"));
    EXPECT_TRUE(metadata->existsDirectory("B/C"));

    metadata = restartMetadataStorage("ConcurrentCreateUnderMovedDirectory");
    EXPECT_FALSE(metadata->existsDirectory("A"));
    EXPECT_TRUE(metadata->existsDirectory("B/C"));
}

TEST_F(MetadataPlainRewritableDiskTest, ConcurrentRecreateUnderUnlinkFile)
{
    auto metadata = getMetadataStorage("ConcurrentRecreateUnderUnlinkFile");
    auto object_storage = getObjectStorage("ConcurrentRecreateUnderUnlinkFile");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("A");
        size_t size = writeObject(object_storage, tx->generateObjectKeyForPath("A/file").serialize(), "Old content");
        tx->createMetadataFile("A/file", {StoredObject("A/file", "file", size)});
        tx->commit(DB::NoCommitOptions{});
    }

    /// The unlink must not remove a file this transaction never observed.
    auto tx1 = metadata->createTransaction();
    tx1->unlinkFile("A/file", /*if_exists=*/true, /*should_remove_objects=*/true);

    {
        auto tx2 = metadata->createTransaction();
        tx2->removeRecursive("A", /*should_remove_blob=*/nullptr);
        tx2->createDirectory("A");
        size_t size = writeObject(object_storage, tx2->generateObjectKeyForPath("A/file").serialize(), "New content");
        tx2->createMetadataFile("A/file", {StoredObject("A/file", "file", size)});
        tx2->commit(DB::NoCommitOptions{});
    }

    EXPECT_ANY_THROW(tx1->commit(DB::NoCommitOptions{}));

    EXPECT_TRUE(metadata->existsFile("A/file"));
    EXPECT_EQ(readObject(object_storage, metadata->getStorageObjects("A/file").front().remote_path), "New content");
}

TEST_F(MetadataPlainRewritableDiskTest, SnapshotDecisionPreservedOnConcurrentChange)
{
    auto metadata = getMetadataStorage("SnapshotDecisionPreservedOnConcurrentChange");

    auto tx1 = metadata->createTransaction();
    tx1->removeRecursive("X", /*should_remove_blob=*/nullptr);

    {
        auto tx2 = metadata->createTransaction();
        tx2->createDirectory("X");
        tx2->commit(DB::NoCommitOptions{});
    }

    tx1->commit(DB::NoCommitOptions{});
    EXPECT_FALSE(metadata->existsDirectory("X"));
}

TEST_F(MetadataPlainRewritableDiskTest, ConcurrentCreateUnderUnlinkFile)
{
    auto metadata = getMetadataStorage("ConcurrentCreateUnderUnlinkFile");
    auto object_storage = getObjectStorage("ConcurrentCreateUnderUnlinkFile");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("A");
        tx->commit(DB::NoCommitOptions{});
    }

    /// The unlink removes the file as of the commit time: a file committed
    /// concurrently serializes before the unlink and gets removed by it.
    auto tx1 = metadata->createTransaction();
    tx1->unlinkFile("A/file", /*if_exists=*/false, /*should_remove_objects=*/true);

    {
        auto tx2 = metadata->createTransaction();
        size_t size = writeObject(object_storage, tx2->generateObjectKeyForPath("A/file").serialize(), "New content");
        tx2->createMetadataFile("A/file", {StoredObject("A/file", "file", size)});
        tx2->commit(DB::NoCommitOptions{});
    }

    tx1->commit(DB::NoCommitOptions{});

    EXPECT_FALSE(metadata->existsFile("A/file"));
    EXPECT_TRUE(metadata->existsDirectory("A"));
}

TEST_F(MetadataPlainRewritableDiskTest, ConcurrentRemoveOfMoveTarget)
{
    auto metadata = getMetadataStorage("ConcurrentRemoveOfMoveTarget");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("A");
        tx->createDirectory("B");
        tx->commit(DB::NoCommitOptions{});
    }

    auto tx1 = metadata->createTransaction();
    tx1->moveDirectory("A", "B");

    {
        auto tx2 = metadata->createTransaction();
        tx2->removeDirectory("B");
        tx2->commit(DB::NoCommitOptions{});
    }

    tx1->commit(DB::NoCommitOptions{});

    EXPECT_FALSE(metadata->existsDirectory("A"));
    EXPECT_TRUE(metadata->existsDirectory("B"));
}

TEST_F(MetadataPlainRewritableDiskTest, ConcurrentCreateDirectory)
{
    auto metadata = getMetadataStorage("ConcurrentCreateDirectory");

    /// Regression test for https://github.com/ClickHouse/ClickHouse/issues/111289
    auto tx1 = metadata->createTransaction();
    tx1->createDirectoryRecursive("A");

    {
        auto tx2 = metadata->createTransaction();
        tx2->createDirectoryRecursive("A");
        tx2->commit(DB::NoCommitOptions{});
    }

    auto remote_prefix = generateObjectKeyPrefixForDirectoryPath(metadata, "A/");
    tx1->commit(DB::NoCommitOptions{});

    EXPECT_TRUE(metadata->existsDirectory("A"));
    EXPECT_EQ(generateObjectKeyPrefixForDirectoryPath(metadata, "A/"), remote_prefix);
    EXPECT_EQ(listAllBlobs("ConcurrentCreateDirectory").size(), 1);

    metadata = restartMetadataStorage("ConcurrentCreateDirectory");
    EXPECT_TRUE(metadata->existsDirectory("A"));
    EXPECT_EQ(generateObjectKeyPrefixForDirectoryPath(metadata, "A/"), remote_prefix);
}

/// Reversing a directory move rewrites one `prefix.path` marker per directory, and each of those is a separate object
/// storage write. When one of them fails, object storage keeps describing the move while the in-memory filesystem
/// still describes the state before it, because a failed transaction never publishes its snapshot. The state of
/// object storage is unknown at that point, so the reversal repeats the write that failed instead of giving up.
TEST_F(MetadataPlainRewritableDiskTest, MoveDirectoryUndoRetriesUntilItSucceeds)
{
    auto metadata = getMetadataStorage("MoveDirectoryUndoRetries");
    auto object_storage = getObjectStorage("MoveDirectoryUndoRetries");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("A");
        tx->createDirectory("A/B");
        tx->commit(DB::NoCommitOptions{});
    }

    const auto a_path = createMetadataObjectPath(metadata, "A");
    const auto ab_path = createMetadataObjectPath(metadata, "A/B");

    {
        FailPointInjection::enableFailPoint("plain_object_storage_fail_on_directory_move_undo");
        SCOPE_EXIT(FailPointInjection::disableFailPoint("plain_object_storage_fail_on_directory_move_undo"));

        /// The move rewrites both markers; the failing file move is what makes the transaction roll back afterwards.
        auto tx = metadata->createTransaction();
        tx->moveDirectory("A", "MOVED");
        tx->moveFile("non-existing", "other-place");
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }

    /// The first attempt to restore a marker failed, and the retry put it back.
    EXPECT_EQ(readObject(object_storage, a_path), "A/");
    EXPECT_EQ(readObject(object_storage, ab_path), "A/B/");

    EXPECT_TRUE(metadata->existsDirectory("A"));
    EXPECT_TRUE(metadata->existsDirectory("A/B"));
    EXPECT_FALSE(metadata->existsDirectory("MOVED"));

    metadata = restartMetadataStorage("MoveDirectoryUndoRetries");
    EXPECT_TRUE(metadata->existsDirectory("A"));
    EXPECT_TRUE(metadata->existsDirectory("A/B"));
    EXPECT_FALSE(metadata->existsDirectory("MOVED"));
}

/// Reversing a file move restores several objects, and it removes the temporary copy of an object once that object is
/// back under its own key. A retry that started the reversal over would copy from a temporary object that an earlier
/// stage has already removed, so every stage is retried where it failed.
TEST_F(MetadataPlainRewritableDiskTest, MoveFileUndoRetriesTheFailedStageOnly)
{
    thread_local_rng.seed(42);

    auto metadata = getMetadataStorage("MoveFileUndoRetries");
    auto object_storage = getObjectStorage("MoveFileUndoRetries");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("/A");

        auto source_size = writeObject(object_storage, tx->generateObjectKeyForPath("/A/source").serialize(), "the source file");
        tx->createMetadataFile("/A/source", {StoredObject("/A/source", "source", source_size)});

        auto target_size = writeObject(object_storage, tx->generateObjectKeyForPath("/A/target").serialize(), "the target file");
        tx->createMetadataFile("/A/target", {StoredObject("/A/target", "target", target_size)});

        tx->commit(DB::NoCommitOptions{});
    }

    const auto source_blob = metadata->getStorageObjects("/A/source").front().remote_path;
    const auto target_blob = metadata->getStorageObjects("/A/target").front().remote_path;
    const auto objects_before = listAllBlobs("MoveFileUndoRetries");

    {
        /// The move copies both blobs aside and removes the target, and then fails before it can put the source in
        /// place. The reversal restores the source, drops its temporary copy, and only then restores the target.
        FailPointInjection::enableFailPoint("plain_object_storage_copy_fail_on_file_move");
        SCOPE_EXIT(FailPointInjection::disableFailPoint("plain_object_storage_copy_fail_on_file_move"));

        FailPointInjection::enableFailPoint("plain_object_storage_fail_on_file_move_undo");
        SCOPE_EXIT(FailPointInjection::disableFailPoint("plain_object_storage_fail_on_file_move_undo"));

        auto tx = metadata->createTransaction();
        tx->replaceFile("/A/source", "/A/target");
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }

    EXPECT_EQ(readObject(object_storage, source_blob), "the source file");
    EXPECT_EQ(readObject(object_storage, target_blob), "the target file");

    /// Nothing else is left behind: the temporary copies the reversal used are gone.
    EXPECT_EQ(listAllBlobs("MoveFileUndoRetries"), objects_before);

    metadata = restartMetadataStorage("MoveFileUndoRetries");
    EXPECT_TRUE(metadata->existsFile("/A/source"));
    EXPECT_TRUE(metadata->existsFile("/A/target"));
}

/// An object storage call can write and then report a failure, so `execute` cannot know from its own return values
/// what it has already changed. Here the copy that publishes the blob under its new key succeeds and the call fails
/// afterwards; without the reversal converging on the state the transaction started from, the blob would stay, and a
/// directory reports whatever blobs sit under its prefix - so a restart would show a file the transaction never
/// committed.
TEST_F(MetadataPlainRewritableDiskTest, MoveFileUndoRemovesABlobPublishedByAFailedCall)
{
    thread_local_rng.seed(42);

    auto metadata = getMetadataStorage("MoveFilePublishedBlob");
    auto object_storage = getObjectStorage("MoveFilePublishedBlob");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("/A");
        auto size_bytes = writeObject(object_storage, tx->generateObjectKeyForPath("/A/source").serialize(), "the source file");
        tx->createMetadataFile("/A/source", {StoredObject("/A/source", "source", size_bytes)});
        tx->commit(DB::NoCommitOptions{});
    }

    const auto source_blob = metadata->getStorageObjects("/A/source").front().remote_path;
    const auto objects_before = listAllBlobs("MoveFilePublishedBlob");

    {
        FailPointInjection::enableFailPoint("plain_object_storage_fail_after_copy_on_file_move");
        SCOPE_EXIT(FailPointInjection::disableFailPoint("plain_object_storage_fail_after_copy_on_file_move"));

        auto tx = metadata->createTransaction();
        tx->moveFile("/A/source", "/A/moved");
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }

    EXPECT_EQ(readObject(object_storage, source_blob), "the source file");
    EXPECT_EQ(listAllBlobs("MoveFilePublishedBlob"), objects_before);

    metadata = restartMetadataStorage("MoveFilePublishedBlob");
    EXPECT_TRUE(metadata->existsFile("/A/source"));
    EXPECT_FALSE(metadata->existsFile("/A/moved"));
}

/// A shutdown is the only exit that does not require object storage to accept the reversal. Here it happens before
/// the transaction, so the single attempt is not retried, and object storage is left holding a part of a transaction
/// that is reported as failed - which is exactly what the next start loads.
TEST_F(MetadataPlainRewritableDiskTest, MoveDirectoryUndoStopsRetryingOnShutdown)
{
    auto metadata = getMetadataStorage("MoveDirectoryUndoShutdown");
    auto object_storage = getObjectStorage("MoveDirectoryUndoShutdown");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("A");
        tx->commit(DB::NoCommitOptions{});
    }

    const auto a_path = createMetadataObjectPath(metadata, "A");

    metadata->shutdown();

    {
        FailPointInjection::enableFailPoint("plain_object_storage_fail_on_directory_move_undo");
        SCOPE_EXIT(FailPointInjection::disableFailPoint("plain_object_storage_fail_on_directory_move_undo"));

        auto tx = metadata->createTransaction();
        tx->moveDirectory("A", "MOVED");
        tx->moveFile("non-existing", "other-place");
        EXPECT_ANY_THROW(tx->commit(DB::NoCommitOptions{}));
    }

    /// The single attempt failed and nothing retried it, so the marker still describes the move.
    EXPECT_EQ(readObject(object_storage, a_path), "MOVED/");

    metadata = restartMetadataStorage("MoveDirectoryUndoShutdown");
    EXPECT_FALSE(metadata->existsDirectory("A"));
    EXPECT_TRUE(metadata->existsDirectory("MOVED"));
}

/// A reversal that object storage keeps rejecting never finishes on its own, and a shutdown is the only thing that
/// ends it. The move is held once one marker carries its new path, object storage is then told to reject every change,
/// and what the reversal reports is read back from the log: it repeats the same step, and it stops on the shutdown
/// rather than by succeeding or by giving up on its own.
TEST_F(MetadataPlainRewritableDiskTest, UndoStopsRetryingWhenTheDiskShutsDownWhileRetrying)
{
    object_storage_can_fail = true;

    auto metadata = getMetadataStorage("UndoShutdownWhileRetrying");
    auto object_storage = getFailingObjectStorage("UndoShutdownWhileRetrying");
    ASSERT_TRUE(object_storage);

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("A");
        tx->createDirectory("A/B");
        tx->commit(DB::NoCommitOptions{});
    }

    LogCapture log_capture("MetadataStorageFromPlainObjectStorageMoveDirectoryOperation");
    const auto retries_before = ProfileEvents::global_counters[ProfileEvents::DiskPlainRewritableUndoStageRetries];

    FailPointInjection::enableFailPoint("plain_object_storage_pause_on_directory_move");
    SCOPE_EXIT(FailPointInjection::disableFailPoint("plain_object_storage_pause_on_directory_move"));

    std::atomic<bool> commit_threw = false;
    std::thread committing([&]
    {
        auto tx = metadata->createTransaction();
        tx->moveDirectory("A", "MOVED");

        try
        {
            tx->commit(DB::NoCommitOptions{});
        }
        catch (...)
        {
            commit_threw = true;
        }
    });

    /// One marker now carries its new path, and the move is waiting.
    FailPointInjection::waitForPause("plain_object_storage_pause_on_directory_move");

    /// From here every change is rejected, so the rest of the move fails and the reversal of the marker above cannot
    /// succeed either.
    object_storage->failRequests(true);
    FailPointInjection::notifyFailPoint("plain_object_storage_pause_on_directory_move");

    /// Wait until the same step has been repeated, which is the state a shutdown has to be able to end.
    while (log_capture.count("failed") < 2)
        std::this_thread::yield();

    metadata->shutdown();
    committing.join();

    EXPECT_TRUE(commit_threw);
    EXPECT_EQ(log_capture.count("because the disk is shutting down"), 1u);
    EXPECT_GT(ProfileEvents::global_counters[ProfileEvents::DiskPlainRewritableUndoStageRetries], retries_before);

    object_storage->failRequests(false);
}
