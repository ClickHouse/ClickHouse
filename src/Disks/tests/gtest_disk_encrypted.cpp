#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Disks/IDisk.h>
#include <Disks/DiskLocal.h>
#include <Disks/DiskEncrypted.h>
#include <IO/FileEncryptionCommon.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>
#include <Disks/ObjectStorages/Local/LocalObjectStorage.h>
#include <Disks/ObjectStorages/MetadataStorageFromDisk.h>
#include <Poco/TemporaryFile.h>
#include <Poco/Util/XMLConfiguration.h>
#include <boost/algorithm/string/join.hpp>

using namespace DB;

constexpr auto kHeaderSize = FileEncryption::Header::kSize;


class DiskEncryptedTest : public ::testing::Test
{
protected:
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

    String getFileContents(const String & file_name, std::optional<size_t> file_size = {})
    {
        auto buf = encrypted_disk->readFile(file_name, /* settings= */ {}, /* read_hint= */ {}, file_size);
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

    /// Read the file with specified file size.
    EXPECT_EQ(getFileContents("a.txt", 9), "Some text");
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
        auto buf = disk->readFile(filename, read_settings, {}, {});

        String str;
        readString(str, *buf, 5);
        EXPECT_EQ(str, "abcde");

        readStringUntilEOF(str, *buf);
        EXPECT_EQ(str, "fghijklmnopqrstuvwxyz0123456789");
    }

    {
        /// Read until specified position (setReadUntilPosition).
        auto buf = disk->readFile(filename, read_settings, {}, {});

        buf->setReadUntilPosition(10);

        String str;
        readStringUntilEOF(str, *buf);
        EXPECT_EQ(str, "abcdefghij");
    }

    {
        /// Read until specified position (setReadUntilPosition), then move that position forward.
        auto buf = disk->readFile(filename, read_settings, {}, {});
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
        auto buf = disk->readFile(filename, read_settings, {}, {});
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
        auto buf = disk->readFile("a.txt", read_settings, {}, {});
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
        auto buf = disk->readFile(filename, read_settings, {}, {});

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
    getContext().context->setServerSetting("storage_metadata_write_full_object_key", true);

    auto object_storage = std::make_shared<LocalObjectStorage>(fs::path{getDirectory()} / "local_blobs");
    auto metadata_disk = std::make_shared<DiskLocal>("metadata_disk", fs::path{getDirectory()} / "metadata");
    auto metadata_storage = std::make_shared<MetadataStorageFromDisk>(metadata_disk, "/");
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config(new Poco::Util::XMLConfiguration());

    auto local_blobs = std::make_shared<DiskObjectStorage>("local_blobs", "/", metadata_storage, object_storage, *config, "");

    makeEncryptedDisk(FileEncryption::Algorithm::AES_128_CTR, "1234567890123456", local_blobs);

    testSeekAndReadUntilPosition(encrypted_disk, "a.txt", {});
    
    {
        ReadSettings read_settings;
        read_settings.local_fs_buffer_size = 1;
        testSeekAndReadUntilPosition(encrypted_disk, "b.txt", read_settings);
    }
}

TEST_F(DiskEncryptedTest, DoubleEncrypted)
{
    auto single_encrypted_disk = makeEncryptedDisk(FileEncryption::Algorithm::AES_128_CTR, "1234567890123456", local_disk);
    auto double_encrypted_disk = makeEncryptedDisk(FileEncryption::Algorithm::AES_128_CTR, "1234567890123456", single_encrypted_disk);

    testSeekAndReadUntilPosition(encrypted_disk, "a.txt", {});
}
