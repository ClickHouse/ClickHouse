#include <Disks/ObjectStorages/IMetadataStorage.h>
#include <Disks/ObjectStorages/Local/LocalObjectStorage.h>
#include <Disks/ObjectStorages/MetadataStorageFromPlainRewritableObjectStorage.h>
#include <Disks/ObjectStorages/createMetadataStorageMetrics.h>
#include <Disks/ObjectStorages/PlainRewritableObjectStorage.h>
#include <Disks/ObjectStorages/StoredObject.h>
#include <Disks/WriteMode.h>

#include <Core/ServerUUID.h>

#include <IO/ReadSettings.h>
#include <IO/SharedThreadPools.h>
#include <IO/Operators.h>

#include <gtest/gtest.h>

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

    /// This is a bug. Should be {A, B, C}
    EXPECT_EQ(sorted(metadata->listDirectory("/")), std::vector<std::string>{});
    EXPECT_EQ(sorted(metadata->listDirectory("")), std::vector<std::string>({"A/", "B/", "C/"}));
}

TEST_F(MetadataPlainRewritableDiskTest, MoveTree)
{
    auto metadata = getMetadataStorage("MoveTree");
    auto object_storage = getObjectStorage("MoveTree");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("A");
        tx->createDirectory("A/B");
        tx->createDirectory("A/B/C");
        tx->createDirectory("A/B/C/D");
        tx->commit();
    }

    auto a_path = createMetadataObjectPath(object_storage, "A");
    auto ab_path = createMetadataObjectPath(object_storage, "A/B");
    auto abc_path = createMetadataObjectPath(object_storage, "A/B/C");
    auto abcd_path = createMetadataObjectPath(object_storage, "A/B/C/D");

    /// Move tree starting from the root
    {
        auto tx = metadata->createTransaction();
        tx->moveDirectory("A", "MOVED");
        tx->commit();
    }

    EXPECT_EQ(readObject(object_storage, a_path), "MOVED/");
    EXPECT_EQ(readObject(object_storage, ab_path), "MOVED/B/");
    EXPECT_EQ(readObject(object_storage, abc_path), "MOVED/B/C/");
    EXPECT_EQ(readObject(object_storage, abcd_path), "MOVED/B/C/D/");

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
}

TEST_F(MetadataPlainRewritableDiskTest, CreateNotFromRoot)
{
    auto metadata = getMetadataStorage("CreateNotFromRoot");
    auto object_storage = getObjectStorage("CreateNotFromRoot");

    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("A/B/C");
        tx->commit();
    }

    /// It is a bug. It should not be possible to create folder unlinked from root.
    EXPECT_TRUE(metadata->existsDirectory("A/B/C"));
    EXPECT_FALSE(metadata->existsDirectory("A"));
    EXPECT_FALSE(metadata->existsDirectory("A/B"));
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

    /// Remove fs tree
    {
        auto tx = metadata->createTransaction();
        tx->removeDirectory("A");
        tx->commit();
    }

    /// This is a bug. Logical tree is broken.
    EXPECT_FALSE(metadata->existsDirectory("A"));
    EXPECT_TRUE(metadata->existsDirectory("A/B"));
    EXPECT_TRUE(metadata->existsDirectory("A/B/C"));
}

TEST_F(MetadataPlainRewritableDiskTest, RemoveDirectoryRecursive)
{
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

    auto inodes_start = std::filesystem::recursive_directory_iterator("./RemoveDirectoryRecursive")
                            | std::views::transform([](const auto & dir) { return dir.path(); })
                            | std::ranges::to<std::vector<std::string>>();
    EXPECT_EQ(inodes_start.size(), 23);

    /// Check undo
    {
        auto tx = metadata->createTransaction();
        tx->removeRecursive("root/A");
        tx->moveFile("non-existing", "other-place");
        EXPECT_ANY_THROW(tx->commit());
    }

    {
        auto inodes = std::filesystem::recursive_directory_iterator("./RemoveDirectoryRecursive")
                        | std::views::transform([](const auto & dir) { return dir.path(); })
                        | std::ranges::to<std::vector<std::string>>();
        EXPECT_EQ(inodes, inodes_start);
    }

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

    {
        auto inodes = std::filesystem::recursive_directory_iterator("./RemoveDirectoryRecursive")
                        | std::views::transform([](const auto & dir) { return dir.path(); })
                        | std::ranges::to<std::vector<std::string>>();

        /// Left nodes example: '/__meta', '/__meta/cixdezesimoamhzozymbalencsyqaakx', '/__meta/cixdezesimoamhzozymbalencsyqaakx/prefix.path'
        /// It is the directory 'root/'
        EXPECT_EQ(inodes.size(), 3);
    }
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

    /// This is a bug. Directory should not be created in this case.
    {
        auto tx = metadata->createTransaction();
        tx->createDirectory("A/B");
        tx->commit();
    }

    EXPECT_TRUE(metadata->existsDirectory("A/B"));
    EXPECT_FALSE(metadata->existsFile("A/B"));
}
