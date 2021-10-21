#include <gtest/gtest.h>

#include <Disks/IDisk.h>
#include <Disks/DiskLocal.h>
#include <Disks/DiskEncrypted.h>
#include <IO/FileEncryptionCommon.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/createReadBufferFromFileBase.h>
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
        local_disk = std::make_shared<DiskLocal>("local_disk", getDirectory(), 0);
    }

    void TearDown() override
    {
        encrypted_disk.reset();
        local_disk.reset();
    }

    void makeEncryptedDisk(FileEncryption::Algorithm algorithm, const String & key)
    {
        auto settings = std::make_unique<DiskEncryptedSettings>();
        settings->wrapped_disk = local_disk;
        settings->current_algorithm = algorithm;
        settings->keys[0] = key;
        settings->current_key_id = 0;
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

    String getFileContents(const String & file_name)
    {
        auto buf = encrypted_disk->readFile(file_name, {}, 0);
        String str;
        readStringUntilEOF(str, *buf);
        return str;
    }

    static void checkBinaryRepresentation(const String & abs_path, size_t size)
    {
        auto buf = createReadBufferFromFileBase(abs_path, {}, 0);
        String str;
        readStringUntilEOF(str, *buf);
        EXPECT_EQ(str.size(), size);
        if (str.size() >= 3)
            EXPECT_EQ(str.substr(0, 3), "ENC");
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
        auto buf = encrypted_disk->writeFile("a.txt", DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite);
        writeString(std::string_view{"Some text"}, *buf);
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
    makeEncryptedDisk(FileEncryption::Algorithm::AES_128_CTR, "1234567890123456");

    /// Write a file (we use the append mode).
    {
        auto buf = encrypted_disk->writeFile("a.txt", DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append);
        writeString(std::string_view{"Some text"}, *buf);
    }

    EXPECT_EQ(encrypted_disk->getFileSize("a.txt"), 9);
    EXPECT_EQ(getFileContents("a.txt"), "Some text");
    checkBinaryRepresentation(getDirectory() + "a.txt", kHeaderSize + 9);

    /// Append the file.
    {
        auto buf = encrypted_disk->writeFile("a.txt", DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append);
        writeString(std::string_view{" Another text"}, *buf);
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
        auto buf = encrypted_disk->writeFile("a.txt", DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append);
        writeString(std::string_view{"Some text"}, *buf);
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
        auto buf = encrypted_disk->writeFile("a.txt", DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite);
    }

    EXPECT_EQ(encrypted_disk->getFileSize("a.txt"), 0);
    EXPECT_EQ(getFileContents("a.txt"), "");
    checkBinaryRepresentation(getDirectory() + "a.txt", 0);

    /// Append the file with nothing.
    {
        auto buf = encrypted_disk->writeFile("a.txt", DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append);
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
