#include <gtest/gtest.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include "gtest_disk.h"

#if !__clang__
#    pragma GCC diagnostic push
#    pragma GCC diagnostic ignored "-Wsuggest-override"
#endif

template <typename T>
DB::DiskPtr createDisk();

template <>
DB::DiskPtr createDisk<DB::DiskMemory>()
{
    return std::make_shared<DB::DiskMemory>("memory_disk");
}

template <>
DB::DiskPtr createDisk<DB::DiskLocal>()
{
    Poco::File("tmp/").createDirectory();
    return std::make_shared<DB::DiskLocal>("local_disk", "tmp/", 0);
}


template <typename T>
void destroyDisk(DB::DiskPtr & disk)
{
    disk.reset();
}

template <>
void destroyDisk<DB::DiskMemory>(DB::DiskPtr & disk)
{
    disk.reset();
}

template <>
void destroyDisk<DB::DiskLocal>(DB::DiskPtr & disk)
{
    disk.reset();
    Poco::File("tmp/").remove(true);
}


template <typename T>
class DiskTest : public testing::Test
{
public:
    void SetUp() override { disk_ = createDisk<T>(); }

    void TearDown() override { destroyDisk<T>(disk_); }

    const DB::DiskPtr & getDisk() { return disk_; }

private:
    DB::DiskPtr disk_;
};


using DiskImplementations = testing::Types<DB::DiskMemory, DB::DiskLocal>;
TYPED_TEST_SUITE(DiskTest, DiskImplementations);


TYPED_TEST(DiskTest, createDirectories)
{
    const auto & disk = this->getDisk();

    disk->createDirectories("test_dir1/");
    EXPECT_TRUE(disk->isDirectory("test_dir1/"));

    disk->createDirectories("test_dir2/nested_dir/");
    EXPECT_TRUE(disk->isDirectory("test_dir2/nested_dir/"));
}


TYPED_TEST(DiskTest, writeFile)
{
    const auto & disk = this->getDisk();

    {
        std::unique_ptr<DB::WriteBuffer> out = disk->writeFile("test_file");
        writeString("test data", *out);
    }

    DB::String data;
    {
        std::unique_ptr<DB::ReadBuffer> in = disk->readFile("test_file");
        readString(data, *in);
    }

    EXPECT_EQ("test data", data);
    EXPECT_EQ(data.size(), disk->getFileSize("test_file"));
}


TYPED_TEST(DiskTest, readFile)
{
    const auto & disk = this->getDisk();

    {
        std::unique_ptr<DB::WriteBuffer> out = disk->writeFile("test_file");
        writeString("test data", *out);
    }

    // Test SEEK_SET
    {
        String buf(4, '0');
        std::unique_ptr<DB::SeekableReadBuffer> in = disk->readFile("test_file");

        in->seek(5, SEEK_SET);

        in->readStrict(buf.data(), 4);
        EXPECT_EQ("data", buf);
    }

    // Test SEEK_CUR
    {
        std::unique_ptr<DB::SeekableReadBuffer> in = disk->readFile("test_file");
        String buf(4, '0');

        in->readStrict(buf.data(), 4);
        EXPECT_EQ("test", buf);

        // Skip whitespace
        in->seek(1, SEEK_CUR);

        in->readStrict(buf.data(), 4);
        EXPECT_EQ("data", buf);
    }
}


TYPED_TEST(DiskTest, iterateDirectory)
{
    const auto & disk = this->getDisk();

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
