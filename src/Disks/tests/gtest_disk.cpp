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
    void SetUp() override { disk = createDisk<T>(); }
    void TearDown() override { destroyDisk<T>(disk); }

    DB::DiskPtr disk;
};


using DiskImplementations = testing::Types<DB::DiskMemory, DB::DiskLocal>;
TYPED_TEST_SUITE(DiskTest, DiskImplementations);


TYPED_TEST(DiskTest, createDirectories)
{
    this->disk->createDirectories("test_dir1/");
    EXPECT_TRUE(this->disk->isDirectory("test_dir1/"));

    this->disk->createDirectories("test_dir2/nested_dir/");
    EXPECT_TRUE(this->disk->isDirectory("test_dir2/nested_dir/"));
}


TYPED_TEST(DiskTest, writeFile)
{
    {
        std::unique_ptr<DB::WriteBuffer> out = this->disk->writeFile("test_file");
        writeString("test data", *out);
    }

    DB::String data;
    {
        std::unique_ptr<DB::ReadBuffer> in = this->disk->readFile("test_file");
        readString(data, *in);
    }

    EXPECT_EQ("test data", data);
    EXPECT_EQ(data.size(), this->disk->getFileSize("test_file"));
}


TYPED_TEST(DiskTest, readFile)
{
    {
        std::unique_ptr<DB::WriteBuffer> out = this->disk->writeFile("test_file");
        writeString("test data", *out);
    }

    // Test SEEK_SET
    {
        String buf(4, '0');
        std::unique_ptr<DB::SeekableReadBuffer> in = this->disk->readFile("test_file");

        in->seek(5, SEEK_SET);

        in->readStrict(buf.data(), 4);
        EXPECT_EQ("data", buf);
    }

    // Test SEEK_CUR
    {
        std::unique_ptr<DB::SeekableReadBuffer> in = this->disk->readFile("test_file");
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
    this->disk->createDirectories("test_dir/nested_dir/");

    {
        auto iter = this->disk->iterateDirectory("");
        EXPECT_TRUE(iter->isValid());
        EXPECT_EQ("test_dir/", iter->path());
        iter->next();
        EXPECT_FALSE(iter->isValid());
    }

    {
        auto iter = this->disk->iterateDirectory("test_dir/");
        EXPECT_TRUE(iter->isValid());
        EXPECT_EQ("test_dir/nested_dir/", iter->path());
        iter->next();
        EXPECT_FALSE(iter->isValid());
    }
}
