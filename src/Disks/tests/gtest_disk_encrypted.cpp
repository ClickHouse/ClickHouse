#include <gtest/gtest.h>

#include <Disks/IDisk.h>
#include <Disks/DiskLocal.h>
#include <Disks/DiskEncrypted.h>
#include <IO/FileEncryptionCommon.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <Poco/TemporaryFile.h>
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

    void makeEncryptedDisk(FileEncryption::Algorithm algorithm, const String & key, const String & path = "")
    {
        auto settings = std::make_unique<DiskEncryptedSettings>();
        settings->wrapped_disk = local_disk;
        settings->current_algorithm = algorithm;
        auto fingerprint = FileEncryption::calculateKeyFingerprint(key);
        settings->all_keys[fingerprint] = key;
        settings->current_key = key;
        settings->current_key_fingerprint = fingerprint;
        settings->disk_path = path;
        encrypted_disk = std::make_shared<DiskEncrypted>("encrypted_disk", std::move(settings));
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

    std::unique_ptr<Poco::TemporaryFile> temp_dir;
    std::shared_ptr<DiskLocal> local_disk;
    std::shared_ptr<DiskEncrypted> encrypted_disk;
};


TEST_F(DiskEncryptedTest, WriteAndRead)
{
    makeEncryptedDisk(FileEncryption::Algorithm::AES_128_CTR, "1234567890123456");

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
    makeEncryptedDisk(FileEncryption::Algorithm::AES_128_CTR, "1234567890123456");

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
    makeEncryptedDisk(FileEncryption::Algorithm::AES_128_CTR, "1234567890123456");

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
    makeEncryptedDisk(FileEncryption::Algorithm::AES_128_CTR, "1234567890123456");

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
    makeEncryptedDisk(FileEncryption::Algorithm::AES_128_CTR, "1234567890123456", "folder1/folder2/");

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
    makeEncryptedDisk(FileEncryption::Algorithm::AES_128_CTR, "1234567890123456");

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
    makeEncryptedDisk(FileEncryption::Algorithm::AES_128_CTR, "1234567890123456");

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
