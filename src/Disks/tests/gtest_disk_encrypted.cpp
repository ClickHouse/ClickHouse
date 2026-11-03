#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/Replication/ClusterConfiguration.h>
#include <Disks/DiskObjectStorage/Replication/ObjectStorageRouter.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Core/Field.h>
#include <Disks/IDisk.h>
#include <Disks/DiskLocal.h>
#include <Disks/DiskEncrypted.h>
#include <Disks/DiskFactory.h>
#include <Disks/DiskSelector.h>
#include <Disks/ReadOnlyDiskWrapper.h>
#include <Disks/SingleDiskVolume.h>
#include <Disks/StoragePolicy.h>
#include <Disks/registerDisks.h>
#include <IO/FileEncryptionCommon.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <Disks/DiskObjectStorage/DiskObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/Local/MetadataStorageFromDisk.h>
#include <Poco/TemporaryFile.h>
#include <Poco/Util/XMLConfiguration.h>
#include <boost/algorithm/string/join.hpp>

using namespace DB;

constexpr auto kHeaderSize = FileEncryption::Header::kSize;


class DiskEncryptedTest : public ::testing::Test
{
protected:
    static void SetUpTestSuite()
    {
        registerDisks(/*global_skip_access_check*/ true);
    }

    static void TearDownTestSuite()
    {
        clearDiskRegistry();
    }

    void SetUp() override
    {
        /// Make local disk.
        temp_dir = std::make_unique<Poco::TemporaryFile>();
        temp_dir->createDirectories();
        local_disk = std::make_shared<DiskLocal>("local_disk", getDirectory());
    }

    void TearDown() override
    {
        encrypted_disk.reset();
        local_disk.reset();
    }

    DiskPtr makeEncryptedDisk(FileEncryption::Algorithm algorithm, const String & key, DiskPtr non_encrypted_disk, const String & path = "")
    {
        auto settings = std::make_unique<DiskEncryptedSettings>();
        settings->wrapped_disk = non_encrypted_disk;
        settings->current_algorithm = algorithm;
        auto fingerprint = FileEncryption::calculateKeyFingerprint(key);
        settings->all_keys[fingerprint] = key;
        settings->current_key = key;
        settings->current_key_fingerprint = fingerprint;
        settings->disk_path = path;
        encrypted_disk = std::make_shared<DiskEncrypted>("encrypted_disk", std::move(settings));
        return encrypted_disk;
    }

    static void configureEncryptedDisk(
        Poco::Util::XMLConfiguration & config,
        const String & prefix,
        const String & wrapped_disk,
        const std::optional<String> & path)
    {
        config.setString(prefix + ".key", "1234567890123456");
        config.setString(prefix + ".disk", wrapped_disk);
        if (path)
            config.setString(prefix + ".path", *path);
    }

    static std::shared_ptr<DiskEncrypted> makeConfiguredEncryptedDisk(
        const String & name,
        const Poco::Util::XMLConfiguration & config,
        const DisksMap & disks)
    {
        return std::make_shared<DiskEncrypted>(name, config, name, disks);
    }

    /// An object storage disk with local metadata in `metadata_root` and local objects in `objects_root`.
    /// It is created from configuration, as the server does, so the test does not depend on how the disk is assembled.
    static DiskPtr makeLocalObjectStorageDisk(const String & name, const String & metadata_root, const String & objects_root)
    {
        Poco::AutoPtr<Poco::Util::XMLConfiguration> config(new Poco::Util::XMLConfiguration());
        const String prefix = "disks." + name;
        config->setString(prefix + ".type", "object_storage");
        config->setString(prefix + ".object_storage_type", "local_blob_storage");
        config->setString(prefix + ".path", objects_root);
        config->setString(prefix + ".metadata_type", "local");
        config->setString(prefix + ".metadata_path", metadata_root);
        config->setBool(prefix + ".skip_access_check", true);

        return DiskFactory::instance().create(
            name, *config, prefix, getContext().context, DisksMap{}, /*attach*/ false, /*custom_disk*/ false, /*skip_types*/ {});
    }

    String getFileNames()
    {
        Strings file_names;
        encrypted_disk->listFiles("", file_names);
        return boost::algorithm::join(file_names, ", ");
    }

    String getDirectory()
    {
        return temp_dir->path() + "/";
    }

    String getFileContents(const String & file_name)
    {
        auto buf = encrypted_disk->readFile(file_name, /* settings= */ {}, /* read_hint= */ {});
        String str;
        readStringUntilEOF(str, *buf);
        return str;
    }

    static String getBinaryRepresentation(const String & abs_path)
    {
        auto buf = createReadBufferFromFileBase(abs_path, /* settings= */ {});
        String str;
        readStringUntilEOF(str, *buf);
        return str;
    }

    static void checkBinaryRepresentation(const String & abs_path, size_t size)
    {
        String str = getBinaryRepresentation(abs_path);
        EXPECT_EQ(str.size(), size);
        if (str.size() >= 3)
        {
            EXPECT_EQ(str.substr(0, 3), "ENC");
        }
    }

    static String getAlphabetWithDigits()
    {
        String contents;
        for (char c = 'a'; c <= 'z'; ++c)
            contents += c;
        for (char c = '0'; c <= '9'; ++c)
            contents += c;
        return contents;
    }

    void testSeekAndReadUntilPosition(DiskPtr disk, const String & filename, const ReadSettings & read_settings);

    std::unique_ptr<Poco::TemporaryFile> temp_dir;
    std::shared_ptr<DiskLocal> local_disk;
    std::shared_ptr<DiskEncrypted> encrypted_disk;
};


TEST_F(DiskEncryptedTest, WriteAndRead)
{
    makeEncryptedDisk(FileEncryption::Algorithm::AES_128_CTR, "1234567890123456", local_disk);

    /// No files
    EXPECT_EQ(getFileNames(), "");

    /// Write a file.
    {
        auto buf = encrypted_disk->writeFile("a.txt", DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, {});
        writeString(std::string_view{"Some text"}, *buf);
        buf->finalize();
    }

    /// Now we have one file.
    EXPECT_EQ(getFileNames(), "a.txt");
    EXPECT_EQ(encrypted_disk->getFileSize("a.txt"), 9);

    /// Read the file.
    EXPECT_EQ(getFileContents("a.txt"), "Some text");
    checkBinaryRepresentation(getDirectory() + "a.txt", kHeaderSize + 9);

    /// Remove the file.
    encrypted_disk->removeFile("a.txt");

    /// No files again.
    EXPECT_EQ(getFileNames(), "");
}


TEST_F(DiskEncryptedTest, Append)
{
    makeEncryptedDisk(FileEncryption::Algorithm::AES_128_CTR, "1234567890123456", local_disk);

    /// Write a file (we use the append mode).
    {
        auto buf = encrypted_disk->writeFile("a.txt", DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append, {});
        writeString(std::string_view{"Some text"}, *buf);
        buf->finalize();
    }

    EXPECT_EQ(encrypted_disk->getFileSize("a.txt"), 9);
    EXPECT_EQ(getFileContents("a.txt"), "Some text");
    checkBinaryRepresentation(getDirectory() + "a.txt", kHeaderSize + 9);

    /// Append the file.
    {
        auto buf = encrypted_disk->writeFile("a.txt", DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append, {});
        writeString(std::string_view{" Another text"}, *buf);
        buf->finalize();
    }

    EXPECT_EQ(encrypted_disk->getFileSize("a.txt"), 22);
    EXPECT_EQ(getFileContents("a.txt"), "Some text Another text");
    checkBinaryRepresentation(getDirectory() + "a.txt", kHeaderSize + 22);
}


TEST_F(DiskEncryptedTest, Truncate)
{
    makeEncryptedDisk(FileEncryption::Algorithm::AES_128_CTR, "1234567890123456", local_disk);

    /// Write a file (we use the append mode).
    {
        auto buf = encrypted_disk->writeFile("a.txt", DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append, {});
        writeString(std::string_view{"Some text"}, *buf);
        buf->finalize();
    }

    EXPECT_EQ(encrypted_disk->getFileSize("a.txt"), 9);
    EXPECT_EQ(getFileContents("a.txt"), "Some text");
    checkBinaryRepresentation(getDirectory() + "a.txt", kHeaderSize + 9);

    /// Truncate the file.
    encrypted_disk->truncateFile("a.txt", 4);

    EXPECT_EQ(encrypted_disk->getFileSize("a.txt"), 4);
    EXPECT_EQ(getFileContents("a.txt"), "Some");
    checkBinaryRepresentation(getDirectory() + "a.txt", kHeaderSize + 4);

    /// Truncate the file to zero size.
    encrypted_disk->truncateFile("a.txt", 0);

    EXPECT_EQ(encrypted_disk->getFileSize("a.txt"), 0);
    EXPECT_EQ(getFileContents("a.txt"), "");
    checkBinaryRepresentation(getDirectory() + "a.txt", 0);
}


TEST_F(DiskEncryptedTest, ZeroFileSize)
{
    makeEncryptedDisk(FileEncryption::Algorithm::AES_128_CTR, "1234567890123456", local_disk);

    /// Write nothing to a file.
    {
        auto buf = encrypted_disk->writeFile("a.txt", DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, {});
        buf->finalize();
    }

    EXPECT_EQ(encrypted_disk->getFileSize("a.txt"), 0);
    EXPECT_EQ(getFileContents("a.txt"), "");
    checkBinaryRepresentation(getDirectory() + "a.txt", 0);

    /// Append the file with nothing.
    {
        auto buf = encrypted_disk->writeFile("a.txt", DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append, {});
        buf->finalize();
    }

    EXPECT_EQ(encrypted_disk->getFileSize("a.txt"), 0);
    EXPECT_EQ(getFileContents("a.txt"), "");
    checkBinaryRepresentation(getDirectory() + "a.txt", 0);

    /// Truncate the file to zero size.
    encrypted_disk->truncateFile("a.txt", 0);

    EXPECT_EQ(encrypted_disk->getFileSize("a.txt"), 0);
    EXPECT_EQ(getFileContents("a.txt"), "");
    checkBinaryRepresentation(getDirectory() + "a.txt", 0);
}


TEST_F(DiskEncryptedTest, AnotherFolder)
{
    /// Encrypted disk will store its files at the path "folder1/folder2/".
    local_disk->createDirectories("folder1/folder2");
    makeEncryptedDisk(FileEncryption::Algorithm::AES_128_CTR, "1234567890123456", local_disk, "folder1/folder2/");

    /// Write a file.
    {
        auto buf = encrypted_disk->writeFile("a.txt", DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, {});
        writeString(std::string_view{"Some text"}, *buf);
        buf->finalize();
    }

    /// Now we have one file.
    EXPECT_EQ(getFileNames(), "a.txt");
    EXPECT_EQ(encrypted_disk->getFileSize("a.txt"), 9);

    /// Read the file.
    EXPECT_EQ(getFileContents("a.txt"), "Some text");
    checkBinaryRepresentation(getDirectory() + "folder1/folder2/a.txt", kHeaderSize + 9);
}


TEST_F(DiskEncryptedTest, RandomIV)
{
    makeEncryptedDisk(FileEncryption::Algorithm::AES_128_CTR, "1234567890123456", local_disk);

    /// Write two files with the same contents.
    {
        auto buf = encrypted_disk->writeFile("a.txt", DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, {});
        writeString(std::string_view{"Some text"}, *buf);
        buf->finalize();
    }

    {
        auto buf = encrypted_disk->writeFile("b.txt", DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, {});
        writeString(std::string_view{"Some text"}, *buf);
        buf->finalize();
    }

    /// Now we have two files.
    EXPECT_EQ(encrypted_disk->getFileSize("a.txt"), 9);
    EXPECT_EQ(encrypted_disk->getFileSize("b.txt"), 9);

    /// Read the files.
    EXPECT_EQ(getFileContents("a.txt"), "Some text");
    EXPECT_EQ(getFileContents("b.txt"), "Some text");
    checkBinaryRepresentation(getDirectory() + "a.txt", kHeaderSize + 9);
    checkBinaryRepresentation(getDirectory() + "b.txt", kHeaderSize + 9);

    String bina = getBinaryRepresentation(getDirectory() + "a.txt");
    String binb = getBinaryRepresentation(getDirectory() + "b.txt");
    constexpr size_t iv_offset = 23; /// See the description of the format in the comment for FileEncryption::Header.
    constexpr size_t iv_size = FileEncryption::InitVector::kSize;
    EXPECT_EQ(bina.substr(0, iv_offset), binb.substr(0, iv_offset)); /// Part of the header before IV is the same.
    EXPECT_NE(bina.substr(iv_offset, iv_size), binb.substr(iv_offset, iv_size)); /// IV differs.
    EXPECT_EQ(bina.substr(iv_offset + iv_size, kHeaderSize - iv_offset - iv_size),
              binb.substr(iv_offset + iv_size, kHeaderSize - iv_offset - iv_size)); /// Part of the header after IV is the same.
    EXPECT_NE(bina.substr(kHeaderSize), binb.substr(kHeaderSize)); /// Encrypted data differs.
}


#if 0
/// TODO: Try to change DiskEncrypted::writeFile() to fix this test.
/// It fails sometimes with quite an unexpected error:
/// libc++abi: terminating with uncaught exception of type std::__1::__fs::filesystem::filesystem_error:
///           filesystem error: in file_size: No such file or directory [/tmp/tmp14608aaaaaa/a.txt]
/// Aborted (core dumped)
/// It happens because for encrypted disks file appending is not atomic (see DiskEncrypted::writeFile())
/// and a file could be removed after checking its existence but before getting its size.
TEST_F(DiskEncryptedTest, RemoveFileDuringWriting)
{
    makeEncryptedDisk(FileEncryption::Algorithm::AES_128_CTR, "1234567890123456", local_disk);

    size_t n = 100000;
    std::thread t1{[&]
    {
        for (size_t i = 0; i != n; ++i)
            encrypted_disk->writeFile("a.txt", DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append, {});
    }};

    std::thread t2{[&]
    {
        for (size_t i = 0; i != n; ++i)
            encrypted_disk->removeFileIfExists("a.txt");
    }};

    t1.join();
    t2.join();
}
#endif

void DiskEncryptedTest::testSeekAndReadUntilPosition(DiskPtr disk, const String & filename, const ReadSettings & read_settings)
{
    /// Write a file.
    {
        auto buf = disk->writeFile(filename, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, {});
        writeString(getAlphabetWithDigits(), *buf);
        buf->finalize();
    }

    {
        /// Read the whole file.
        EXPECT_EQ(getFileContents(filename), getAlphabetWithDigits());
    }

    {
        /// Read the whole file in two portions.
        auto buf = disk->readFile(filename, read_settings, {});

        String str;
        readString(str, *buf, 5);
        EXPECT_EQ(str, "abcde");

        readStringUntilEOF(str, *buf);
        EXPECT_EQ(str, "fghijklmnopqrstuvwxyz0123456789");
    }

    {
        /// Read until specified position (setReadUntilPosition).
        auto buf = disk->readFile(filename, read_settings, {});

        buf->setReadUntilPosition(10);

        String str;
        readStringUntilEOF(str, *buf);
        EXPECT_EQ(str, "abcdefghij");
    }

    {
        /// Read until specified position (setReadUntilPosition), then move that position forward.
        auto buf = disk->readFile(filename, read_settings, {});
        buf->setReadUntilPosition(10);

        String str;
        readStringUntilEOF(str, *buf);
        EXPECT_EQ(str, "abcdefghij");

        buf->setReadUntilPosition(15);
        readStringUntilEOF(str, *buf);
        EXPECT_EQ(str, "klmno");

        buf->setReadUntilPosition(20);
        readString(str, *buf, 2);
        EXPECT_EQ(str, "pq");

        readStringUntilEOF(str, *buf);
        EXPECT_EQ(str, "rst");

        buf->setReadUntilEnd();
        readStringUntilEOF(str, *buf);
        EXPECT_EQ(str, "uvwxyz0123456789");
    }

    {
        /// Read until specified position (setReadUntilPosition), then move that position backward.
        auto buf = disk->readFile(filename, read_settings, {});
        buf->setReadUntilPosition(10);

        String str;
        readString(str, *buf, 4);
        EXPECT_EQ(str, "abcd");

        buf->setReadUntilPosition(8);
        readStringUntilEOF(str, *buf);
        EXPECT_EQ(str, "efgh");
    }

    {
        /// Read until specified position (setReadUntilPosition), then move that position backward and seek.
        auto buf = disk->readFile("a.txt", read_settings, {});
        buf->setReadUntilPosition(10);

        String str;
        readString(str, *buf, 4);
        EXPECT_EQ(str, "abcd");

        buf->setReadUntilPosition(8);
        buf->seek(0, SEEK_SET);
        readStringUntilEOF(str, *buf);
        EXPECT_EQ(str, "abcdefgh");
    }

    {
        /// Seek and then read until a specified position.
        auto buf = disk->readFile(filename, read_settings, {});

        String str;
        buf->seek(0, SEEK_SET);
        buf->setReadUntilPosition(6);
        readStringUntilEOF(str, *buf);
        EXPECT_EQ(str, "abcdef");

        buf->seek(3, SEEK_SET);
        buf->setReadUntilPosition(5);
        readStringUntilEOF(str, *buf);
        EXPECT_EQ(str, "de");

        buf->setReadUntilPosition(15);
        buf->seek(10, SEEK_SET);
        readStringUntilEOF(str, *buf);
        EXPECT_EQ(str, "klmno");

        buf->seek(0, SEEK_SET);
        buf->setReadUntilPosition(5);
        readStringUntilEOF(str, *buf);
        EXPECT_EQ(str, "abcde");

        buf->seek(-1, SEEK_CUR);
        buf->setReadUntilEnd();
        readStringUntilEOF(str, *buf);
        EXPECT_EQ(str, "efghijklmnopqrstuvwxyz0123456789");
    }
}

TEST_F(DiskEncryptedTest, LocalBlobs)
{
    ObjectStoragePtr object_storage = std::make_shared<LocalObjectStorage>(LocalObjectStorageSettings("test", fs::path{getDirectory()} / "local_blobs", false));
    DiskPtr metadata_disk = std::make_shared<DiskLocal>("metadata_disk", fs::path{getDirectory()} / "metadata");
    MetadataStoragePtr metadata_storage = std::make_shared<MetadataStorageFromDisk>(metadata_disk, "/", object_storage->createKeyGenerator(), /*persist_removal_queue_=*/true, /*removal_log_compaction_threshold_=*/1000);

    std::unordered_map<Location, LocationInfo> cluster_registry = {{"main", {true, true, ""}}};
    std::unordered_map<Location, ObjectStoragePtr> object_storage_registry = {{"main", object_storage}};

    ClusterConfigurationPtr cluster = std::make_shared<ClusterConfiguration>("local_blobs", std::move(cluster_registry));
    ObjectStorageRouterPtr object_storages = std::make_shared<ObjectStorageRouter>(std::move(object_storage_registry));

    Poco::AutoPtr<Poco::Util::XMLConfiguration> config(new Poco::Util::XMLConfiguration());
    auto local_blobs = std::make_shared<DiskObjectStorage>("local_blobs", std::move(cluster), std::move(metadata_storage), std::move(object_storages), nullptr, *config, "");

    makeEncryptedDisk(FileEncryption::Algorithm::AES_128_CTR, "1234567890123456", local_blobs);

    testSeekAndReadUntilPosition(encrypted_disk, "a.txt", {});

    {
        ReadSettings read_settings;
        read_settings.local_fs_settings.buffer_size = 1;
        testSeekAndReadUntilPosition(encrypted_disk, "b.txt", read_settings);
    }
}

TEST_F(DiskEncryptedTest, DoubleEncrypted)
{
    auto single_encrypted_disk = makeEncryptedDisk(FileEncryption::Algorithm::AES_128_CTR, "1234567890123456", local_disk);
    auto double_encrypted_disk = makeEncryptedDisk(FileEncryption::Algorithm::AES_128_CTR, "1234567890123456", single_encrypted_disk);

    testSeekAndReadUntilPosition(encrypted_disk, "a.txt", {});
}

TEST_F(DiskEncryptedTest, ConfigurationRejectsAbsolutePath)
{
    /// An absolute `path` would escape the wrapped disk: `DiskLocal` joins paths with `fs::path(root) / path`,
    /// and `fs::path::operator/` discards the root when the right-hand side is absolute. Such a path must be rejected
    /// before storage-policy namespace validation because it violates the delegate boundary.
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config(new Poco::Util::XMLConfiguration());
    configureEncryptedDisk(*config, "encrypted1", "local_disk", "/data/");

    DisksMap disks{{"local_disk", local_disk}};
    EXPECT_THROW(makeConfiguredEncryptedDisk("encrypted1", *config, disks), DB::Exception);
}

TEST_F(DiskEncryptedTest, ConfigurationRejectsPathEscapingDelegateRoot)
{
    /// A relative `path` that escapes the delegate's root via `..` lands outside the wrapped disk, just like an
    /// absolute path and breaks isolation. It must be rejected.
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config(new Poco::Util::XMLConfiguration());
    configureEncryptedDisk(*config, "encrypted1", "local_disk", "../escape/");

    DisksMap disks{{"local_disk", local_disk}};
    EXPECT_THROW(makeConfiguredEncryptedDisk("encrypted1", *config, disks), DB::Exception);
}

TEST_F(DiskEncryptedTest, ConfigurationRejectsSymlinkEscapingDelegateRoot)
{
    const fs::path delegate_root = fs::path(getDirectory()) / "delegate";
    const fs::path outside_root = fs::path(getDirectory()) / "outside";
    fs::create_directories(delegate_root);
    fs::create_directories(outside_root);
    fs::create_directory_symlink(outside_root, delegate_root / "link");

    /// The root ends with a separator, so that only the symlink, not a missing separator, puts the path outside.
    auto delegate_disk = std::make_shared<DiskLocal>("delegate_disk", delegate_root.string() + "/");
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config(new Poco::Util::XMLConfiguration());
    configureEncryptedDisk(*config, "encrypted1", "delegate_disk", "link/encrypted/");

    DisksMap disks{{"delegate_disk", delegate_disk}};
    EXPECT_THROW(makeConfiguredEncryptedDisk("encrypted1", *config, disks), DB::Exception);
}

TEST_F(DiskEncryptedTest, ConfigurationRejectsSymlinkEscapingObjectStorageDelegateRoot)
{
    /// The path of an object storage disk with local metadata is the local metadata directory.
    const fs::path metadata_root = fs::path(getDirectory()) / "metadata";
    const fs::path outside_root = fs::path(getDirectory()) / "outside";
    fs::create_directories(metadata_root);
    fs::create_directories(outside_root);
    fs::create_directory_symlink(outside_root, metadata_root / "link");

    /// The root ends with a separator, so that only the symlink, not a missing separator, puts the path outside.
    auto delegate_disk = makeLocalObjectStorageDisk("delegate_disk", metadata_root.string() + "/", getDirectory() + "local_blobs/");
    ASSERT_TRUE(delegate_disk->getDataSourceDescription().isPathLocal());

    Poco::AutoPtr<Poco::Util::XMLConfiguration> config(new Poco::Util::XMLConfiguration());
    configureEncryptedDisk(*config, "encrypted1", "delegate_disk", "link/encrypted/");

    DisksMap disks{{"delegate_disk", delegate_disk}};
    EXPECT_THROW(makeConfiguredEncryptedDisk("encrypted1", *config, disks), DB::Exception);
}

TEST_F(DiskEncryptedTest, SameDelegateAndExplicitPathShareStorageNamespace)
{
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config(new Poco::Util::XMLConfiguration());
    configureEncryptedDisk(*config, "encrypted1", "local_disk", "data/");
    configureEncryptedDisk(*config, "encrypted2", "local_disk", "data/");

    DisksMap disks{{"local_disk", local_disk}};
    auto encrypted1 = makeConfiguredEncryptedDisk("encrypted1", *config, disks);
    disks.emplace("encrypted1", encrypted1);
    auto encrypted2 = makeConfiguredEncryptedDisk("encrypted2", *config, disks);

    EXPECT_TRUE(haveSameStorageNamespace(encrypted1, encrypted2));
}

TEST_F(DiskEncryptedTest, SameNamespaceOverDifferentDelegatesIsDetected)
{
    auto nested_local_disk = std::make_shared<DiskLocal>("nested_local_disk", getDirectory() + "data/");

    Poco::AutoPtr<Poco::Util::XMLConfiguration> config(new Poco::Util::XMLConfiguration());
    configureEncryptedDisk(*config, "encrypted1", "nested_local_disk", "encrypted/");
    configureEncryptedDisk(*config, "encrypted2", "local_disk", "data/encrypted/");

    DisksMap disks{{"local_disk", local_disk}, {"nested_local_disk", nested_local_disk}};
    auto encrypted1 = makeConfiguredEncryptedDisk("encrypted1", *config, disks);
    disks.emplace("encrypted1", encrypted1);
    auto encrypted2 = makeConfiguredEncryptedDisk("encrypted2", *config, disks);

    EXPECT_TRUE(haveSameStorageNamespace(encrypted1, encrypted2));
}

TEST_F(DiskEncryptedTest, TraversalAliasOfSamePathIsDetected)
{
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config(new Poco::Util::XMLConfiguration());
    configureEncryptedDisk(*config, "encrypted1", "local_disk", "a/../b/");
    configureEncryptedDisk(*config, "encrypted2", "local_disk", "b/");

    DisksMap disks{{"local_disk", local_disk}};
    auto encrypted1 = makeConfiguredEncryptedDisk("encrypted1", *config, disks);
    disks.emplace("encrypted1", encrypted1);
    auto encrypted2 = makeConfiguredEncryptedDisk("encrypted2", *config, disks);

    EXPECT_TRUE(haveSameStorageNamespace(encrypted1, encrypted2));
}

TEST_F(DiskEncryptedTest, DifferentPathsOrDelegatesHaveDistinctStorageNamespaces)
{
    auto other_local_disk = std::make_shared<DiskLocal>("other_local_disk", getDirectory() + "other/");

    Poco::AutoPtr<Poco::Util::XMLConfiguration> config(new Poco::Util::XMLConfiguration());
    configureEncryptedDisk(*config, "encrypted1", "local_disk", "data/");
    configureEncryptedDisk(*config, "encrypted2", "local_disk", "other_data/");
    configureEncryptedDisk(*config, "encrypted3", "other_local_disk", "data/");

    DisksMap disks{{"local_disk", local_disk}, {"other_local_disk", other_local_disk}};
    auto encrypted1 = makeConfiguredEncryptedDisk("encrypted1", *config, disks);
    disks.emplace("encrypted1", encrypted1);
    auto encrypted2 = makeConfiguredEncryptedDisk("encrypted2", *config, disks);
    disks.emplace("encrypted2", encrypted2);
    auto encrypted3 = makeConfiguredEncryptedDisk("encrypted3", *config, disks);

    EXPECT_FALSE(haveSameStorageNamespace(encrypted1, encrypted2));
    EXPECT_FALSE(haveSameStorageNamespace(encrypted1, encrypted3));
    EXPECT_FALSE(haveSameStorageNamespace(encrypted2, encrypted3));
}

TEST_F(DiskEncryptedTest, EncryptedWithoutPathSharesNamespaceWithItsDelegate)
{
    /// Without an explicit `path` the encrypted disk writes into the root of the wrapped disk, so both disks
    /// read and write the same files.
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config(new Poco::Util::XMLConfiguration());
    configureEncryptedDisk(*config, "encrypted1", "local_disk", std::nullopt);

    DisksMap disks{{"local_disk", local_disk}};
    auto encrypted1 = makeConfiguredEncryptedDisk("encrypted1", *config, disks);

    EXPECT_TRUE(haveSameStorageNamespace(encrypted1, local_disk));
}

TEST_F(DiskEncryptedTest, LocalDiskAliasesShareStorageNamespace)
{
    auto local_disk_alias = std::make_shared<DiskLocal>("local_disk_alias", getDirectory());

    EXPECT_TRUE(haveSameStorageNamespace(local_disk, local_disk_alias));
}

TEST_F(DiskEncryptedTest, LocalDiskSymlinkAliasesShareStorageNamespace)
{
    const fs::path real_path = fs::path(getDirectory()) / "real";
    const fs::path alias_path = fs::path(getDirectory()) / "alias";
    fs::create_directories(real_path);
    fs::create_directory_symlink(real_path, alias_path);

    auto real_disk = std::make_shared<DiskLocal>("real_disk", real_path);
    auto alias_disk = std::make_shared<DiskLocal>("alias_disk", alias_path);

    EXPECT_TRUE(haveSameStorageNamespace(real_disk, alias_disk));
}

TEST_F(DiskEncryptedTest, ConfigurationAcceptsInRootDirectoryNamedLikeASymlink)
{
    /// The same shape as the symlink tests, but `link` is an ordinary directory inside the root, so the path stays
    /// inside the wrapped disk.
    const fs::path delegate_root = fs::path(getDirectory()) / "delegate";
    fs::create_directories(delegate_root / "link");

    auto delegate_disk = std::make_shared<DiskLocal>("delegate_disk", delegate_root.string() + "/");
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config(new Poco::Util::XMLConfiguration());
    configureEncryptedDisk(*config, "encrypted1", "delegate_disk", "link/encrypted/");

    DisksMap disks{{"delegate_disk", delegate_disk}};
    EXPECT_NO_THROW(makeConfiguredEncryptedDisk("encrypted1", *config, disks));
}

TEST_F(DiskEncryptedTest, ConfigurationRejectsSymlinkFollowedByParentEscapingDelegateRoot)
{
    /// `link/..` is the parent of the directory `link` points to, not the root of the wrapped disk, so the path below
    /// is outside the root although collapsing `..` lexically would put it inside.
    const fs::path delegate_root = fs::path(getDirectory()) / "delegate";
    const fs::path outside_root = fs::path(getDirectory()) / "outside";
    fs::create_directories(delegate_root);
    fs::create_directories(outside_root / "sub");
    fs::create_directory_symlink(outside_root / "sub", delegate_root / "link");

    auto delegate_disk = std::make_shared<DiskLocal>("delegate_disk", delegate_root.string() + "/");
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config(new Poco::Util::XMLConfiguration());
    configureEncryptedDisk(*config, "encrypted1", "delegate_disk", "link/../encrypted/");

    DisksMap disks{{"delegate_disk", delegate_disk}};
    EXPECT_THROW(makeConfiguredEncryptedDisk("encrypted1", *config, disks), DB::Exception);
}

TEST_F(DiskEncryptedTest, ConfigurationAcceptsDelegateRootWithoutTrailingSeparator)
{
    /// `metadata_path` of an object storage disk does not have to end with a separator, and the disk uses it as is.
    const fs::path metadata_root = fs::path(getDirectory()) / "metadata";
    fs::create_directories(metadata_root);

    auto delegate_disk = makeLocalObjectStorageDisk("delegate_disk", metadata_root.string(), getDirectory() + "local_blobs/");
    ASSERT_EQ(delegate_disk->getPath(), metadata_root.string());

    Poco::AutoPtr<Poco::Util::XMLConfiguration> config(new Poco::Util::XMLConfiguration());
    configureEncryptedDisk(*config, "encrypted1", "delegate_disk", "encrypted/");
    configureEncryptedDisk(*config, "encrypted2", "delegate_disk", std::nullopt);

    DisksMap disks{{"delegate_disk", delegate_disk}};
    std::shared_ptr<DiskEncrypted> encrypted1;
    ASSERT_NO_THROW(encrypted1 = makeConfiguredEncryptedDisk("encrypted1", *config, disks));
    EXPECT_EQ(fs::path(encrypted1->getPath()), metadata_root / "encrypted/");

    /// Without a `path`, the encrypted disk is the root of the wrapped disk, with or without the separator.
    disks.emplace("encrypted1", encrypted1);
    auto encrypted2 = makeConfiguredEncryptedDisk("encrypted2", *config, disks);
    EXPECT_TRUE(haveSameStorageNamespace(encrypted2, delegate_disk));
    EXPECT_FALSE(haveSameStorageNamespace(encrypted1, delegate_disk));
}

TEST_F(DiskEncryptedTest, EncryptedSiblingsOverObjectStorageAreRejectedInOnePolicy)
{
    /// Two `encrypted` disks over one writable object storage disk with one `path` are handed one metadata storage,
    /// but neither wraps the other. A policy holding both sees every part twice.
    auto delegate_disk = makeLocalObjectStorageDisk("delegate_disk", getDirectory() + "metadata/", getDirectory() + "local_blobs/");

    Poco::AutoPtr<Poco::Util::XMLConfiguration> config(new Poco::Util::XMLConfiguration());
    configureEncryptedDisk(*config, "encrypted1", "delegate_disk", "encrypted/");
    configureEncryptedDisk(*config, "encrypted2", "delegate_disk", "encrypted/");

    DisksMap disks{{"delegate_disk", delegate_disk}};
    auto encrypted1 = makeConfiguredEncryptedDisk("encrypted1", *config, disks);
    disks.emplace("encrypted1", encrypted1);
    auto encrypted2 = makeConfiguredEncryptedDisk("encrypted2", *config, disks);

    EXPECT_TRUE(areDisksDuplicateStorage(encrypted1, encrypted2));

    Volumes volumes{std::make_shared<SingleDiskVolume>("first", encrypted1), std::make_shared<SingleDiskVolume>("second", encrypted2)};
    EXPECT_THROW((void)std::make_shared<StoragePolicy>("siblings", volumes, 0.1), DB::Exception);

    /// Each of them in a policy of its own is a supported setup.
    EXPECT_NO_THROW((void)std::make_shared<StoragePolicy>("first", Volumes{volumes[0]}, 0.1));
    EXPECT_NO_THROW((void)std::make_shared<StoragePolicy>("second", Volumes{volumes[1]}, 0.1));
}

TEST_F(DiskEncryptedTest, LayerBesideItsDelegateIsRejectedInOnePolicy)
{
    /// An `encrypted` disk without a `path` lists the files of the disk it wraps, so a policy holding both
    /// enumerates them twice.
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config(new Poco::Util::XMLConfiguration());
    configureEncryptedDisk(*config, "encrypted1", "local_disk", std::nullopt);

    DisksMap disks{{"local_disk", local_disk}};
    auto encrypted1 = makeConfiguredEncryptedDisk("encrypted1", *config, disks);

    EXPECT_TRUE(areDisksDuplicateStorage(encrypted1, local_disk));

    Volumes volumes{std::make_shared<SingleDiskVolume>("plain", local_disk), std::make_shared<SingleDiskVolume>("encrypted", encrypted1)};
    EXPECT_THROW((void)std::make_shared<StoragePolicy>("layered", volumes, 0.1), DB::Exception);
}

TEST_F(DiskEncryptedTest, ReadOnlyAliasBesideWriterIsRejectedInOnePolicy)
{
    /// If the reader's copy of a part wins, the writer's copy is loaded as outdated and removed through the writable
    /// disk, which deletes the files the reader uses. A read-only disk does not make the pair safe.
    auto reader = std::make_shared<ReadOnlyDiskWrapper>(std::make_shared<DiskLocal>("local_disk_alias", getDirectory()));
    ASSERT_TRUE(reader->isReadOnly());

    EXPECT_TRUE(areDisksDuplicateStorage(local_disk, reader));

    Volumes volumes{std::make_shared<SingleDiskVolume>("writer", local_disk), std::make_shared<SingleDiskVolume>("reader", reader)};
    EXPECT_THROW((void)std::make_shared<StoragePolicy>("mixed", volumes, 0.1), DB::Exception);
}

TEST_F(DiskEncryptedTest, SharedLocalMetadataOverDifferentObjectStoragesIsDetected)
{
    /// `DiskObjectStorage::iterateDirectory` lists the local metadata directory whatever the objects are, so two disks
    /// with one `metadata_path` see the same parts even with their objects in different places.
    auto first = makeLocalObjectStorageDisk("first", getDirectory() + "metadata/", getDirectory() + "blobs_first/");
    auto second = makeLocalObjectStorageDisk("second", getDirectory() + "metadata/", getDirectory() + "blobs_second/");

    EXPECT_TRUE(areDisksDuplicateStorage(first, second));

    Volumes volumes{std::make_shared<SingleDiskVolume>("first", first), std::make_shared<SingleDiskVolume>("second", second)};
    EXPECT_THROW((void)std::make_shared<StoragePolicy>("shared_metadata", volumes, 0.1), DB::Exception);

    /// Different metadata directories over different objects are different storages.
    auto third = makeLocalObjectStorageDisk("third", getDirectory() + "metadata_third/", getDirectory() + "blobs_third/");
    EXPECT_FALSE(areDisksDuplicateStorage(first, third));
}
