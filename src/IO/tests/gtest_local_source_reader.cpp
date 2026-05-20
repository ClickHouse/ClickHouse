#include <IO/ISourceReader.h>
#include <IO/LocalSourceReader.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>

#include <gtest/gtest.h>
#include <fstream>
#include <filesystem>

using namespace DB;

class LocalSourceReaderTest : public ::testing::Test
{
protected:
    std::filesystem::path tmp_dir;
    std::filesystem::path test_file;

    void SetUp() override
    {
        tmp_dir = std::filesystem::temp_directory_path() / "test_local_source_reader";
        std::filesystem::create_directories(tmp_dir);
        test_file = tmp_dir / "test_data.bin";

        std::ofstream f(test_file, std::ios::binary);
        for (int rep = 0; rep < 4; ++rep)
            for (int b = 0; b < 256; ++b)
                f.put(static_cast<char>(b));
    }

    void TearDown() override
    {
        std::filesystem::remove_all(tmp_dir);
    }
};

TEST_F(LocalSourceReaderTest, OpenReadFullRange)
{
    LocalSourceReader reader;
    StoredObject obj;
    obj.remote_path = test_file.string();
    obj.bytes_size = 1024;

    auto buf = reader.open(obj, /*use_external_buffer=*/false);
    ASSERT_TRUE(buf);

    std::vector<char> data(1024);
    size_t total = 0;
    while (total < data.size())
    {
        size_t bytes = buf->read(data.data() + total, data.size() - total);
        if (bytes == 0)
            break;
        total += bytes;
    }
    EXPECT_EQ(total, 1024u);
    EXPECT_EQ(static_cast<unsigned char>(data[0]), 0);
    EXPECT_EQ(static_cast<unsigned char>(data[255]), 255);
}

TEST_F(LocalSourceReaderTest, OpenSeekAndRead)
{
    LocalSourceReader reader;
    StoredObject obj;
    obj.remote_path = test_file.string();
    obj.bytes_size = 1024;

    auto buf = reader.open(obj, /*use_external_buffer=*/false);
    ASSERT_TRUE(buf);
    buf->seek(256, SEEK_SET);

    std::vector<char> data(100);
    size_t total = 0;
    while (total < data.size())
    {
        size_t bytes = buf->read(data.data() + total, data.size() - total);
        if (bytes == 0)
            break;
        total += bytes;
    }
    EXPECT_EQ(total, 100u);
    EXPECT_EQ(static_cast<unsigned char>(data[0]), 0);
    EXPECT_EQ(static_cast<unsigned char>(data[99]), 99);
}

TEST_F(LocalSourceReaderTest, OpenReadPastEOF)
{
    LocalSourceReader reader;
    StoredObject obj;
    obj.remote_path = test_file.string();
    obj.bytes_size = 1024;

    auto buf = reader.open(obj, /*use_external_buffer=*/false);
    ASSERT_TRUE(buf);
    buf->seek(900, SEEK_SET);

    std::vector<char> data(200);
    size_t total = 0;
    while (total < data.size())
    {
        size_t bytes = buf->read(data.data() + total, data.size() - total);
        if (bytes == 0)
            break;
        total += bytes;
    }
    EXPECT_EQ(total, 124u);
}

TEST_F(LocalSourceReaderTest, Name)
{
    LocalSourceReader reader;
    EXPECT_EQ(reader.name(), "LocalSourceReader");
}
