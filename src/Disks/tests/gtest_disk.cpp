#include <gtest/gtest.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include "gtest_disk.h"
#include <Disks/HDFS/DiskHDFS.cpp>
#include <Poco/Util/XMLConfiguration.h>


#if !defined(__clang__)
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


const String hdfs_uri = "hdfs://192.168.112.2:9000/disk_test/";
const String metadata_path = "/home/kssenii/metadata/";
const String config_path = "/home/kssenii/ClickHouse/programs/server/config.xml";
const String file_name = "test.txt";


TEST(DiskTest, RemoveFileHDFS)
{
    Poco::Util::AbstractConfiguration *config = new Poco::Util::XMLConfiguration(config_path);
    auto disk = DB::DiskHDFS("disk_hdfs", hdfs_uri, metadata_path, *config);

    DB::HDFSBuilderWrapper builder = DB::createHDFSBuilder(hdfs_uri, *config);
    DB::HDFSFSPtr fs = DB::createHDFSFS(builder.get());

    disk.writeFile(file_name, 1024, DB::WriteMode::Rewrite);
    auto metadata = disk.readMeta(file_name);

    const String hdfs_file_name = metadata.remote_fs_objects[0].first;
    const String hdfs_file_path = "/disk_test/" + hdfs_file_name;

    auto ret = hdfsExists(fs.get(), hdfs_file_path.data());
    EXPECT_EQ(0, ret);

    disk.removeFile(file_name);
    ret = hdfsExists(fs.get(), hdfs_file_path.data());
    EXPECT_EQ(-1, ret);
}


TEST(DiskTest, WriteReadHDFS)
{
    Poco::Util::AbstractConfiguration *config = new Poco::Util::XMLConfiguration(config_path);
    auto disk = DB::DiskHDFS("disk_hdfs", hdfs_uri, metadata_path, *config);

    {
        auto out = disk.writeFile(file_name, 1024, DB::WriteMode::Rewrite);
        writeString("Test write to file", *out);
    }

    {
        DB::String result;
        auto in = disk.readFile(file_name, 1024, 1024, 1024, 1024, nullptr);
        readString(result, *in);
        EXPECT_EQ("Test write to file", result);
    }

    disk.removeFileIfExists(file_name);
}


TEST(DiskTest, RewriteFileHDFS)
{
    Poco::Util::AbstractConfiguration *config = new Poco::Util::XMLConfiguration(config_path);
    auto disk = DB::DiskHDFS("disk_hdfs", hdfs_uri, metadata_path, *config);

    for (size_t i = 1; i <= 10; ++i)
    {
        std::unique_ptr<DB::WriteBuffer> out = disk.writeFile(file_name, 1024, DB::WriteMode::Rewrite);
        writeString("Text" + DB::toString(i), *out);
    }

    {
        String result;
        auto in = disk.readFile(file_name, 1024, 1024, 1024, 1024, nullptr);
        readString(result, *in);
        EXPECT_EQ("Text10", result);
        readString(result, *in);
        EXPECT_EQ("", result);
    }

    disk.removeFileIfExists(file_name);
}


TEST(DiskTest, AppendFileHDFS)
{
    Poco::Util::AbstractConfiguration *config = new Poco::Util::XMLConfiguration(config_path);
    auto disk = DB::DiskHDFS("disk_hdfs", hdfs_uri, metadata_path, *config);

    {
        std::unique_ptr<DB::WriteBuffer> out = disk.writeFile(file_name, 1024, DB::WriteMode::Append);
        writeString("Text", *out);
        for (size_t i = 0; i < 10; ++i)
        {
            writeIntText(i, *out);
        }
    }

    {
        String result, expected;
        auto in = disk.readFile(file_name, 1024, 1024, 1024, 1024, nullptr);

        readString(result, *in);
        EXPECT_EQ("Text0123456789", result);

        readString(result, *in);
        EXPECT_EQ("", result);
    }

    disk.removeFileIfExists(file_name);
}


TEST(DiskTest, SeekHDFS)
{
    Poco::Util::AbstractConfiguration *config = new Poco::Util::XMLConfiguration(config_path);
    auto disk = DB::DiskHDFS("disk_hdfs", hdfs_uri, metadata_path, *config);

    {
        std::unique_ptr<DB::WriteBuffer> out = disk.writeFile(file_name, 1024, DB::WriteMode::Rewrite);
        writeString("test data", *out);
    }

    /// Test SEEK_SET
    {
        String buf(4, '0');
        std::unique_ptr<DB::SeekableReadBuffer> in = disk.readFile(file_name, 1024, 1024, 1024, 1024, nullptr);

        in->seek(5, SEEK_SET);

        in->readStrict(buf.data(), 4);
        EXPECT_EQ("data", buf);
    }

    /// Test SEEK_CUR
    {
        std::unique_ptr<DB::SeekableReadBuffer> in = disk.readFile(file_name, 1024, 1024, 1024, 1024, nullptr);
        String buf(4, '0');

        in->readStrict(buf.data(), 4);
        EXPECT_EQ("test", buf);

        // Skip whitespace
        in->seek(1, SEEK_CUR);

        in->readStrict(buf.data(), 4);
        EXPECT_EQ("data", buf);
    }

    disk.removeFileIfExists(file_name);
}
