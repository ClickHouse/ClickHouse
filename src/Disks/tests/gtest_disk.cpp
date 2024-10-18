#include <gtest/gtest.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include "gtest_disk.h"
#include <filesystem>

namespace fs = std::filesystem;


DB::DiskPtr createDisk(const std::string & path)
{
    fs::create_directory(path);
    return std::make_shared<DB::DiskLocal>("local_disk", path);
}

void destroyDisk(DB::DiskPtr & disk)
{
    const auto path = disk->getPath();
    disk.reset();
    fs::remove_all(path);
}

class DiskTest : public testing::Test
{
public:
    void SetUp() override { disk = createDisk(); }
    void TearDown() override { destroyDisk(disk); }

    DB::DiskPtr disk;
};


TEST_F(DiskTest, createDirectories)
{
    disk->createDirectories("test_dir1/");
    EXPECT_TRUE(disk->existsDirectory("test_dir1/"));

    disk->createDirectories("test_dir2/nested_dir/");
    EXPECT_TRUE(disk->existsDirectory("test_dir2/nested_dir/"));
}


TEST_F(DiskTest, writeFile)
{
    {
        std::unique_ptr<DB::WriteBuffer> out = disk->writeFile("test_file");
        writeString("test data", *out);
    }

    String data;
    {
        std::unique_ptr<DB::ReadBuffer> in = disk->readFile("test_file", DB::getReadSettings());
        readString(data, *in);
    }

    EXPECT_EQ("test data", data);
    EXPECT_EQ(data.size(), disk->getFileSize("test_file"));
}


TEST_F(DiskTest, readFile)
{
    {
        std::unique_ptr<DB::WriteBuffer> out = disk->writeFile("test_file");
        writeString("test data", *out);
    }

    auto read_settings = DB::getReadSettings();

    // Test SEEK_SET
    {
        String buf(4, '0');
        std::unique_ptr<DB::SeekableReadBuffer> in = disk->readFile("test_file", read_settings);

        in->seek(5, SEEK_SET);

        in->readStrict(buf.data(), 4);
        EXPECT_EQ("data", buf);
    }

    // Test SEEK_CUR
    {
        std::unique_ptr<DB::SeekableReadBuffer> in = disk->readFile("test_file", read_settings);
        String buf(4, '0');

        in->readStrict(buf.data(), 4);
        EXPECT_EQ("test", buf);

        // Skip whitespace
        in->seek(1, SEEK_CUR);

        in->readStrict(buf.data(), 4);
        EXPECT_EQ("data", buf);
    }
}


TEST_F(DiskTest, iterateDirectory)
{
    disk->createDirectories("test_dir/nested_dir/");

    {
        auto iter = disk->iterateDirectory("");
        EXPECT_TRUE(iter->isValid());
        EXPECT_EQ("test_dir/", iter->path());
        iter->next();
        EXPECT_FALSE(iter->isValid());
    }

    {
        auto iter = disk->iterateDirectory("test_dir/");
        EXPECT_TRUE(iter->isValid());
        EXPECT_EQ("test_dir/nested_dir/", iter->path());
        iter->next();
        EXPECT_FALSE(iter->isValid());
    }
}
