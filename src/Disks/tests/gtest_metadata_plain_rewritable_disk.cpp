#include <Disks/ObjectStorages/IMetadataStorage.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Disks/ObjectStorages/Local/LocalObjectStorage.h>
#include <Disks/ObjectStorages/MetadataStorageFromPlainRewritableObjectStorage.h>
#include <Disks/ObjectStorages/createMetadataStorageMetrics.h>
#include <Disks/ObjectStorages/PlainRewritableObjectStorage.h>
#include <Disks/ObjectStorages/StoredObject.h>
#include <Disks/WriteMode.h>

#include <IO/ReadSettings.h>
#include <IO/SharedThreadPools.h>
#include <IO/Operators.h>

#include <Core/ServerUUID.h>

#include <Common/thread_local_rng.h>

#include <gtest/gtest.h>

#include <filesystem>
#include <ranges>

using namespace DB;

class MetadataPlainRewritableDiskTest : public testing::Test
{
public:
    void SetUp() override
    {
        if (!initialized)
        {
            ServerUUID::setRandomForUnitTests();
            getIOThreadPool().initialize(1, 1, 0);
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
        active_metadatas[key_prefix] = std::make_shared<MetadataStorageFromPlainRewritableObjectStorage>(object_storage, "", 0);
        return active_metadatas.at(key_prefix);
    }

    std::shared_ptr<IObjectStorage> getObjectStorage(const std::string & key_prefix)
    {
        std::unique_lock<std::mutex> lock(active_metadatas_mutex);
        return active_object_storages.at(key_prefix);
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
        MetadataStorageMetrics metadata_storage_metrics = MetadataStorageMetrics::create<LocalObjectStorage, MetadataStorageType::PlainRewritable>();
        EXPECT_EQ(metadata_storage_metrics.directory_created, ProfileEvents::DiskPlainRewritableLocalDirectoryCreated);
        EXPECT_EQ(metadata_storage_metrics.directory_removed, ProfileEvents::DiskPlainRewritableLocalDirectoryRemoved);
        EXPECT_EQ(metadata_storage_metrics.directory_map_size, CurrentMetrics::DiskPlainRewritableLocalDirectoryMapSize);
        EXPECT_EQ(metadata_storage_metrics.file_count, CurrentMetrics::DiskPlainRewritableLocalFileCount);

        fs::remove_all("./" + key_prefix);
        LocalObjectStorageSettings settings("./" + key_prefix, /*read_only_=*/false);
        auto object_storage = std::make_shared<PlainRewritableObjectStorage<LocalObjectStorage>>(std::move(metadata_storage_metrics), std::move(settings));
        auto metadata_storage = std::make_shared<MetadataStorageFromPlainRewritableObjectStorage>(object_storage, "", 0);

        active_metadatas.emplace(key_prefix, metadata_storage);
        active_object_storages.emplace(key_prefix, object_storage);

        return metadata_storage;
    }

    static inline bool initialized = false;

    std::mutex active_metadatas_mutex;
    std::unordered_map<std::string, std::shared_ptr<IMetadataStorage>> active_metadatas;
    std::unordered_map<std::string, std::shared_ptr<IObjectStorage>> active_object_storages;
};

void writeObject(const std::shared_ptr<IObjectStorage> & object_storage, const std::string & remote_path, const std::string & data)
{
    StoredObject object(remote_path);
    auto buffer = object_storage->writeObject(object, WriteMode::Rewrite);
    buffer->write(data.data(), data.size());
    buffer->finalize();
}

std::string readObject(const std::shared_ptr<IObjectStorage> & object_storage, const std::string & remote_path)
{
    StoredObject object(remote_path);
    auto buffer = object_storage->readObject(object, getReadSettings(), /*read_hint=*/std::nullopt);

    String content;
    readStringUntilEOF(content, *buffer);
    return content;
}

std::string createMetadataObjectPath(const std::shared_ptr<IObjectStorage> & object_storage, const std::string & directory)
{
    auto mid = object_storage->generateObjectKeyPrefixForDirectoryPath(fs::path(directory) / "", "").serialize();
    return fs::path(object_storage->getCommonKeyPrefix()) / "__meta" / mid / "prefix.path";
}

std::vector<std::string> sorted(std::vector<std::string> array)
{
    std::sort(array.begin(), array.end());
    return array;
}

std::vector<std::string> listAllBlobs(std::string test)
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
        tx->commit();
    }

    EXPECT_TRUE(metadata->existsDirectory("A"));
    EXPECT_TRUE(metadata->existsDirectory("A/B"));
    EXPECT_TRUE(metadata->existsDirectory("A/B/C"));
    EXPECT_TRUE(metadata->existsDirectory("A/D"));
    EXPECT_FALSE(metadata->existsDirectory("OTHER"));

    EXPECT_EQ(readObject(object_storage, createMetadataObjectPath(object_storage, "A")), "A/");
    EXPECT_EQ(readObject(object_storage, createMetadataObjectPath(object_storage, "A/B/C")), "A/B/C/");
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
        tx->commit();
    }

    EXPECT_EQ(sorted(metadata->listDirectory("/")), std::vector<std::string>({"A", "B", "C"}));
    EXPECT_EQ(sorted(metadata->listDirectory("")), std::vector<std::string>({"A", "B", "C"}));

    {
        auto tx = metadata->createTransaction();
        tx->createDirectoryRecursive("D/E/F/G/H");
        tx->createDirectoryRecursive("/D/E/F/K");
        tx->commit();
    }

    /// For now we can not create file under the directory created in the same tx.
    {
        auto tx = metadata->createTransaction();
        writeObject(object_storage, object_storage->generateObjectKeyForPath("D/E/F/G/H/file", std::nullopt).serialize(), "file");
        tx->createMetadataFile("D/E/F/G/H/file", {StoredObject()});
        tx->commit();
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
        tx->commit();
    }

    auto a_path = createMetadataObjectPath(object_storage, "A");
    auto ab_path = createMetadataObjectPath(object_storage, "A/B");
    auto abcd_path = createMetadataObjectPath(object_storage, "A/B/C/D");

    /// Move tree starting from the root
    {
        auto tx = metadata->createTransaction();
        tx->moveDirectory("A", "MOVED");
        tx->commit();
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
        tx->commit();
    }

    auto a_path = createMetadataObjectPath(object_storage, "A");
    auto ab_path = createMetadataObjectPath(object_storage, "A/B");
    auto abc_path = createMetadataObjectPath(object_storage, "A/B/C");

    /// Move tree starting from the root
    {
        auto tx = metadata->createTransaction();
        tx->moveDirectory("A", "MOVED");
        tx->moveFile("non-existing", "other-place");
        EXPECT_ANY_THROW(tx->commit());
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
        EXPECT_ANY_THROW(tx->commit());
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
        tx->commit();
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
        tx->commit();
    }

    EXPECT_TRUE(metadata->existsDirectory("A"));
    EXPECT_TRUE(metadata->existsDirectory("A/B"));
    EXPECT_TRUE(metadata->existsDirectory("A/B/C"));

    {
        auto tx = metadata->createTransaction();
        tx->removeDirectory("A/B/C");
        tx->commit();
    }

    EXPECT_TRUE(metadata->existsDirectory("A"));
    EXPECT_TRUE(metadata->existsDirectory("A/B"));
    EXPECT_FALSE(metadata->existsDirectory("A/B/C"));

    {
        auto tx = metadata->createTransaction();
        tx->removeDirectory("A");
        EXPECT_ANY_THROW(tx->commit());
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
        tx->commit();
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
        tx->commit();
    }

    EXPECT_TRUE(metadata->existsDirectory("A"));
    EXPECT_TRUE(metadata->existsDirectory("A/B"));
    EXPECT_TRUE(metadata->existsDirectory("A/B/C"));

    {
        auto tx = metadata->createTransaction();
        tx->removeDirectory("A/B/C");
        tx->removeDirectory("A");
        EXPECT_ANY_THROW(tx->commit());
    }

    EXPECT_TRUE(metadata->existsDirectory("A"));
    EXPECT_TRUE(metadata->existsDirectory("A/B"));
    EXPECT_TRUE(metadata->existsDirectory("A/B/C"));

    {
        auto tx = metadata->createTransaction();
        tx->removeDirectory("X");
        EXPECT_ANY_THROW(tx->commit());
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
        tx->commit();
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
        tx->commit();
    }

    {
        auto tx = metadata->createTransaction();
        writeObject(object_storage, object_storage->generateObjectKeyForPath("root/A/file_1", std::nullopt).serialize(), "1");
        writeObject(object_storage, object_storage->generateObjectKeyForPath("root/A/B/file_2", std::nullopt).serialize(), "2");
        writeObject(object_storage, object_storage->generateObjectKeyForPath("root/A/C/file_3", std::nullopt).serialize(), "3");
        writeObject(object_storage, object_storage->generateObjectKeyForPath("root/A/B/E/F/file_4", std::nullopt).serialize(), "4");
        tx->createMetadataFile("root/A/file_1", {StoredObject("root/A/file_1")});
        tx->createMetadataFile("root/A/B/file_2", {StoredObject("root/A/B/file_2")});
        tx->createMetadataFile("root/A/C/file_3", {StoredObject("root/A/C/file_3")});
        tx->createMetadataFile("root/A/B/E/F/file_4", {StoredObject("root/A/B/E/F/file_4")});
        tx->commit();
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
        tx->removeRecursive("root/A");
        tx->moveFile("non-existing", "other-place");
        EXPECT_ANY_THROW(tx->commit());
    }

    EXPECT_EQ(listAllBlobs("RemoveDirectoryRecursive"), inodes_start);

    /// Remove fs tree
    {
        auto tx = metadata->createTransaction();
        tx->removeRecursive("root/A");
        tx->commit();
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
        tx->commit();
    }

    EXPECT_TRUE(metadata->existsDirectory("root/A"));
    EXPECT_TRUE(metadata->existsDirectory("root/A/B"));
    EXPECT_TRUE(metadata->existsDirectory("root/A/B/C"));
    EXPECT_TRUE(metadata->existsDirectory("root/A/B/C/D"));

    {
        auto tx = metadata->createTransaction();
        tx->removeRecursive("root/A");
        tx->commit();
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
        tx->commit();
    }

    EXPECT_TRUE(metadata->existsDirectory("A"));
    EXPECT_TRUE(metadata->existsDirectory("B"));

    {
        auto tx = metadata->createTransaction();
        writeObject(object_storage, object_storage->generateObjectKeyForPath("A/file", std::nullopt).serialize(), "Hello world!");
        tx->createMetadataFile("A/file", {StoredObject("A/file")});
        tx->commit();
    }

    EXPECT_TRUE(metadata->existsFile("A/file"));

    auto a_file_path = metadata->getStorageObjects("A/file").front().remote_path;
    EXPECT_EQ(readObject(object_storage, a_file_path), "Hello world!");

    {
        auto tx = metadata->createTransaction();
        tx->moveFile("A/file", "B/file");
        tx->commit();
    }

    EXPECT_FALSE(metadata->existsFile("A/file"));
    EXPECT_TRUE(metadata->existsFile("B/file"));

    auto b_file_path = metadata->getStorageObjects("B/file").front().remote_path;
    EXPECT_EQ(readObject(object_storage, b_file_path), "Hello world!");

    EXPECT_NE(a_file_path, b_file_path);
}

TEST_F(MetadataPlainRewritableDiskTest, MoveFileUndo)
{
    auto metadata = getMetadataStorage("MoveFileUndo");
    auto object_storage = getObjectStorage("MoveFileUndo");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("A");
        tx->createDirectory("B");
        tx->commit();
    }

    EXPECT_TRUE(metadata->existsDirectory("A"));
    EXPECT_TRUE(metadata->existsDirectory("B"));

    {
        auto tx = metadata->createTransaction();
        writeObject(object_storage, object_storage->generateObjectKeyForPath("A/file", std::nullopt).serialize(), "Hello world!");
        tx->createMetadataFile("A/file", {StoredObject("A/file")});
        tx->commit();
    }

    EXPECT_TRUE(metadata->existsFile("A/file"));

    auto path_1 = metadata->getStorageObjects("A/file").front().remote_path;
    EXPECT_EQ(readObject(object_storage, path_1), "Hello world!");

    {
        auto tx = metadata->createTransaction();
        tx->moveFile("A/file", "B/file");
        tx->moveFile("non-existing", "other-place");
        EXPECT_ANY_THROW(tx->commit());
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
        tx->commit();
    }

    {
        auto tx = metadata->createTransaction();
        writeObject(object_storage, object_storage->generateObjectKeyForPath("A/B", std::nullopt).serialize(), "Hello world!");
        tx->createMetadataFile("A/B", {StoredObject("A/B")});
        tx->commit();
    }

    EXPECT_FALSE(metadata->existsDirectory("A/B"));
    EXPECT_TRUE(metadata->existsFile("A/B"));

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("A/B");
        EXPECT_ANY_THROW(tx->commit());
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
        tx->removeRecursive("non-existing");
        tx->commit();
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
        tx->commit();
    }

    std::string a_remote = object_storage->generateObjectKeyPrefixForDirectoryPath("A/", "").serialize();
    EXPECT_EQ(a_remote, "faefxnlkbtfqgxcbfqfjtztsocaqrnqn");
    EXPECT_EQ(object_storage->generateObjectKeyPrefixForDirectoryPath("A/", "").serialize(), a_remote);
    EXPECT_EQ(object_storage->generateObjectKeyPrefixForDirectoryPath("A/", "").serialize(), a_remote);
    EXPECT_EQ(object_storage->generateObjectKeyPrefixForDirectoryPath("A/", "").serialize(), a_remote);

    std::string ab_remote = object_storage->generateObjectKeyPrefixForDirectoryPath("A/B/", "").serialize();
    EXPECT_EQ(ab_remote, "ykwvvchguqasvfnkikaqtiebknfzafwv");
    EXPECT_EQ(object_storage->generateObjectKeyPrefixForDirectoryPath("A/B/", "").serialize(), ab_remote);
    EXPECT_EQ(object_storage->generateObjectKeyPrefixForDirectoryPath("A/B/", "").serialize(), ab_remote);
    EXPECT_EQ(object_storage->generateObjectKeyPrefixForDirectoryPath("A/B/", "").serialize(), ab_remote);

    std::string file_1_remote = object_storage->generateObjectKeyForPath("/A/file_1", std::nullopt).serialize();
    EXPECT_EQ(file_1_remote, "./RemoteLayout/faefxnlkbtfqgxcbfqfjtztsocaqrnqn/file_1");
    EXPECT_EQ(file_1_remote, fmt::format("./RemoteLayout/{}/file_1", a_remote));

    std::string file_2_remote = object_storage->generateObjectKeyForPath("/A/B/file_2", std::nullopt).serialize();
    EXPECT_EQ(file_2_remote, "./RemoteLayout/ykwvvchguqasvfnkikaqtiebknfzafwv/file_2");
    EXPECT_EQ(file_2_remote, fmt::format("./RemoteLayout/{}/file_2", ab_remote));

    /// Root files
    EXPECT_EQ(object_storage->generateObjectKeyForPath("root_file", std::nullopt).serialize(), "./RemoteLayout/__root/root_file");
}

TEST_F(MetadataPlainRewritableDiskTest, RootFiles)
{
    thread_local_rng.seed(42);

    auto metadata = getMetadataStorage("RootFiles");
    auto object_storage = getObjectStorage("RootFiles");

    {
        auto tx = metadata->createTransaction();
        writeObject(object_storage, object_storage->generateObjectKeyForPath("/A", std::nullopt).serialize(), "A");
        writeObject(object_storage, object_storage->generateObjectKeyForPath("/B", std::nullopt).serialize(), "B");
        tx->createMetadataFile("/A", {StoredObject("A")});
        tx->createMetadataFile("/B", {StoredObject("B")});
        tx->commit();
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
        tx->commit();
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
        tx->commit();
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
        writeObject(object_storage, object_storage->generateObjectKeyForPath("/A", std::nullopt).serialize(), "A");
        writeObject(object_storage, object_storage->generateObjectKeyForPath("/B", std::nullopt).serialize(), "B");
        tx->createMetadataFile("/A", {StoredObject("A")});
        tx->createMetadataFile("/B", {StoredObject("B")});
        tx->commit();
    }

    {
        auto tx = metadata->createTransaction();
        tx->removeDirectory("/");
        EXPECT_ANY_THROW(tx->commit());
    }

    {
        auto tx = metadata->createTransaction();
        tx->removeRecursive("/");
        tx->commit();
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
        tx->unlinkMetadata("/A");
        tx->unlinkMetadata("/B");
        tx->commit();

        object_storage->removeObjectsIfExist(files_objects);
    }

    EXPECT_EQ(listAllBlobs("RemoveRecursiveRoot"), std::vector<std::string>({}));

    {
        auto tx = metadata->createTransaction();
        tx->removeDirectory("/");
        tx->commit();
    }

    EXPECT_TRUE(metadata->existsDirectory("/"));
    metadata = restartMetadataStorage("RemoveRecursiveRoot");

    {
        auto tx = metadata->createTransaction();
        tx->removeDirectory("/");
        tx->commit();
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
        tx->commit();
    }

    {
        auto tx = metadata->createTransaction();
        tx->unlinkMetadata("non-existing");
        EXPECT_ANY_THROW(tx->commit());
    }

    {
        auto tx = metadata->createTransaction();
        tx->unlinkMetadata("non-existing/A");
        EXPECT_ANY_THROW(tx->commit());
    }

    {
        auto tx = metadata->createTransaction();
        tx->unlinkMetadata("A/non-existing");
        EXPECT_ANY_THROW(tx->commit());
    }

    {
        auto tx = metadata->createTransaction();
        tx->unlinkFile("non-existing");
        tx->commit();
    }

    {
        auto tx = metadata->createTransaction();
        tx->unlinkFile("non-existing/A");
        tx->commit();
    }

    {
        auto tx = metadata->createTransaction();
        tx->unlinkFile("A/non-existing");
        tx->commit();
    }
}

TEST_F(MetadataPlainRewritableDiskTest, MoveReplaceNonExisting)
{
    auto metadata = getMetadataStorage("MoveNonExisting");
    auto object_storage = getObjectStorage("MoveNonExisting");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectoryRecursive("A/B/C");
        tx->commit();
    }

    {
        auto tx = metadata->createTransaction();
        tx->moveDirectory("non-existing", "other-place");
        EXPECT_ANY_THROW(tx->commit());
    }

    {
        auto tx = metadata->createTransaction();
        tx->moveDirectory("non-existing/A", "other-place");
        EXPECT_ANY_THROW(tx->commit());
    }

    {
        auto tx = metadata->createTransaction();
        tx->moveDirectory("A/non-existing", "other-place");
        EXPECT_ANY_THROW(tx->commit());
    }

    {
        auto tx = metadata->createTransaction();
        tx->moveFile("non-existing", "other-place");
        EXPECT_ANY_THROW(tx->commit());
    }

    {
        auto tx = metadata->createTransaction();
        tx->moveFile("non-existing/A", "other-place");
        EXPECT_ANY_THROW(tx->commit());
    }

    {
        auto tx = metadata->createTransaction();
        tx->moveFile("A/non-existing", "other-place");
        EXPECT_ANY_THROW(tx->commit());
    }

    {
        auto tx = metadata->createTransaction();
        tx->replaceFile("non-existing", "other-place");
        EXPECT_ANY_THROW(tx->commit());
    }

    {
        auto tx = metadata->createTransaction();
        tx->replaceFile("non-existing/A", "other-place");
        EXPECT_ANY_THROW(tx->commit());
    }

    {
        auto tx = metadata->createTransaction();
        tx->replaceFile("A/non-existing", "other-place");
        EXPECT_ANY_THROW(tx->commit());
    }
}

TEST_F(MetadataPlainRewritableDiskTest, RemoveNonExisting)
{
    auto metadata = getMetadataStorage("RemoveNonExisting");
    auto object_storage = getObjectStorage("RemoveNonExisting");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectoryRecursive("A/B/C");
        tx->commit();
    }

    {
        auto tx = metadata->createTransaction();
        tx->removeDirectory("non-existing");
        EXPECT_ANY_THROW(tx->commit());
    }

    {
        auto tx = metadata->createTransaction();
        tx->removeDirectory("non-existing/A");
        EXPECT_ANY_THROW(tx->commit());
    }

    {
        auto tx = metadata->createTransaction();
        tx->removeDirectory("A/non-existing");
        EXPECT_ANY_THROW(tx->commit());
    }

    {
        auto tx = metadata->createTransaction();
        tx->removeRecursive("non-existing");
        tx->commit();
    }

    {
        auto tx = metadata->createTransaction();
        tx->removeRecursive("non-existing/A");
        tx->commit();
    }

    {
        auto tx = metadata->createTransaction();
        tx->removeRecursive("A/non-existing");
        tx->commit();
    }
}

TEST_F(MetadataPlainRewritableDiskTest, HardLinkNonExisting)
{
    auto metadata = getMetadataStorage("HardLinkNonExisting");
    auto object_storage = getObjectStorage("HardLinkNonExisting");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectoryRecursive("A/B/C");
        tx->commit();
    }

    {
        auto tx = metadata->createTransaction();
        tx->createHardLink("non-existing", "other-place");
        EXPECT_ANY_THROW(tx->commit());
    }

    {
        auto tx = metadata->createTransaction();
        tx->createHardLink("non-existing/A", "other-place");
        EXPECT_ANY_THROW(tx->commit());
    }

    {
        auto tx = metadata->createTransaction();
        tx->createHardLink("A/non-existing", "other-place");
        EXPECT_ANY_THROW(tx->commit());
    }
}

TEST_F(MetadataPlainRewritableDiskTest, LookupBlobs)
{
    auto metadata = getMetadataStorage("LookupBlobs");
    auto object_storage = getObjectStorage("LookupBlobs");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectoryRecursive("A/B/C");
        tx->commit();
    }

    {
        auto tx = metadata->createTransaction();
        EXPECT_EQ(tx->tryGetBlobsFromTransactionIfExists("non-existing"), std::nullopt);
        tx->commit();
    }

    {
        auto tx = metadata->createTransaction();
        EXPECT_EQ(tx->tryGetBlobsFromTransactionIfExists("non-existing/A"), std::nullopt);
        tx->commit();
    }

    {
        auto tx = metadata->createTransaction();
        EXPECT_EQ(tx->tryGetBlobsFromTransactionIfExists("A/B"), std::nullopt);
        tx->commit();
    }

    {
        auto tx = metadata->createTransaction();
        EXPECT_EQ(tx->tryGetBlobsFromTransactionIfExists("A/X"), std::nullopt);
        tx->commit();
    }
}

TEST_F(MetadataPlainRewritableDiskTest, OperationsNonExisting)
{
    auto metadata = getMetadataStorage("OperationsNonExisting");
    auto object_storage = getObjectStorage("OperationsNonExisting");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectoryRecursive("A/B/C");
        tx->commit();
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
        tx->commit();
    }

    EXPECT_TRUE(metadata->existsDirectory("/A"));
    EXPECT_FALSE(metadata->existsFile("/A/f1"));

    {
        auto tx = metadata->createTransaction();
        writeObject(object_storage, object_storage->generateObjectKeyForPath("/A/f1", std::nullopt).serialize(), "f1");
        tx->createMetadataFile("/A/f1", {StoredObject("A")});
        tx->commit();
    }

    EXPECT_TRUE(metadata->existsFile("/A/f1"));

    metadata = restartMetadataStorage("CreateFiles");
    EXPECT_TRUE(metadata->existsDirectory("/A"));
    EXPECT_TRUE(metadata->existsFile("/A/f1"));

    /// Some rewrites
    {
        auto tx = metadata->createTransaction();
        tx->createMetadataFile("/A/f1", {StoredObject("B")});
        tx->createMetadataFile("/A/f1", {StoredObject("C")});
        tx->createMetadataFile("/A/f1", {StoredObject("D")});
        tx->createMetadataFile("/A/f1", {StoredObject("E")});
        tx->createMetadataFile("/A/f1", {StoredObject("F")});
        tx->createMetadataFile("/A/f1", {StoredObject("G")});
        tx->commit();
    }

    EXPECT_TRUE(metadata->existsFile("/A/f1"));

    metadata = restartMetadataStorage("CreateFiles");
    EXPECT_TRUE(metadata->existsDirectory("/A"));
    EXPECT_TRUE(metadata->existsFile("/A/f1"));

    {
        auto tx = metadata->createTransaction();
        tx->unlinkMetadata("/A/f1");
        tx->commit();
    }

    EXPECT_TRUE(metadata->existsDirectory("/A"));
    EXPECT_FALSE(metadata->existsFile("/A/f1"));

    {
        auto tx = metadata->createTransaction();
        tx->unlinkMetadata("/A/f1");
        EXPECT_ANY_THROW(tx->commit());
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
        tx->commit();
    }

    EXPECT_TRUE(metadata->existsDirectory("/A"));
    EXPECT_TRUE(metadata->existsDirectory("/B"));
    EXPECT_TRUE(metadata->existsDirectory("/B/A"));

    {
        auto tx = metadata->createTransaction();
        tx->moveDirectory("/A", "/B/A");
        EXPECT_ANY_THROW(tx->commit());
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
        tx->commit();
    }

    EXPECT_TRUE(metadata->existsDirectory("/A"));

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("/A/B");
        tx->createDirectory("/A/B");
        tx->createDirectory("/A/B");
        tx->createDirectory("/A/B");
        tx->moveDirectory("non-existing", "other-place");
        EXPECT_ANY_THROW(tx->commit());
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
        tx->commit();
    }

    EXPECT_TRUE(metadata->existsDirectory("/A"));

    {
        auto tx = metadata->createTransaction();
        writeObject(object_storage, object_storage->generateObjectKeyForPath("/A/f1", std::nullopt).serialize(), "f1");
        tx->createMetadataFile("/A/f1", {StoredObject("f1")});
        tx->commit();
    }

    EXPECT_TRUE(metadata->existsFile("/A/f1"));
    EXPECT_FALSE(metadata->existsFile("/A/f2"));

    {
        auto tx = metadata->createTransaction();
        tx->createHardLink("/A/f1", "A/f2");
        tx->commit();
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
        tx->commit();
    }

    {
        auto tx = metadata->createTransaction();
        writeObject(object_storage, object_storage->generateObjectKeyForPath("/A/f1", std::nullopt).serialize(), "f1");
        tx->createMetadataFile("/A/f1", {StoredObject("f1")});
        tx->commit();
    }

    EXPECT_TRUE(metadata->existsDirectory("/A"));
    EXPECT_TRUE(metadata->existsFile("/A/f1"));
    EXPECT_FALSE(metadata->existsFile("/A/f2"));

    {
        auto tx = metadata->createTransaction();
        tx->createHardLink("/A/f1", "A/f2");
        tx->createHardLink("/B/f1", "A/f2");
        EXPECT_ANY_THROW(tx->commit());
    }

    EXPECT_EQ(listAllBlobs("CreateHardLinkUndo").size(), 2);
    EXPECT_TRUE(metadata->existsDirectory("/A"));
    EXPECT_TRUE(metadata->existsFile("/A/f1"));
    EXPECT_FALSE(metadata->existsFile("/A/f2"));

    {
        auto tx = metadata->createTransaction();
        tx->createHardLink("/A/f1", "A/f2");
        tx->createHardLink("f1", "f2");
        EXPECT_ANY_THROW(tx->commit());
    }

    EXPECT_EQ(listAllBlobs("CreateHardLinkUndo").size(), 2);
    EXPECT_TRUE(metadata->existsDirectory("/A"));
    EXPECT_TRUE(metadata->existsFile("/A/f1"));
    EXPECT_FALSE(metadata->existsFile("/A/f2"));

    {
        auto tx = metadata->createTransaction();
        tx->createHardLink("/A/f1", "A/f2");
        tx->createHardLink("/A/f1", "A/f2");
        EXPECT_ANY_THROW(tx->commit());
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
        writeObject(object_storage, object_storage->generateObjectKeyForPath("f1", std::nullopt).serialize(), "f1");
        tx->createMetadataFile("/f1", {StoredObject("f1")});
        tx->commit();
    }

    EXPECT_TRUE(metadata->existsFile("/f1"));

    {
        auto tx = metadata->createTransaction();
        tx->createHardLink("/f1", "/f2");
        tx->commit();
    }

    EXPECT_TRUE(metadata->existsFile("/f1"));
    EXPECT_TRUE(metadata->existsFile("/f2"));

    {
        auto tx = metadata->createTransaction();
        tx->createHardLink("/f2", "/f3");
        tx->createHardLink("/f1", "/f2");
        EXPECT_ANY_THROW(tx->commit());
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
        tx->commit();
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
        tx->commit();
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
        tx->commit();
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
        tx->removeRecursive("/A/B/C/D");
        tx->commit();
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
        tx->removeRecursive("/A/B/C");
        tx->commit();
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
        tx->commit();
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
        tx->commit();
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
