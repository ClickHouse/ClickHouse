#include <IO/ISourceReader.h>
#include <IO/LocalSourceReader.h>
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

TEST_F(LocalSourceReaderTest, ReadFullRange)
{
    LocalSourceReader reader;
    StoredObject obj;
    obj.remote_path = test_file.string();
    obj.bytes_size = 1024;

    std::vector<char> buf(1024);
    size_t bytes_read = reader.read(obj, 0, 1024, buf.data());
    EXPECT_EQ(bytes_read, 1024);
    EXPECT_EQ(static_cast<unsigned char>(buf[0]), 0);
    EXPECT_EQ(static_cast<unsigned char>(buf[255]), 255);
}

TEST_F(LocalSourceReaderTest, ReadSubRange)
{
    LocalSourceReader reader;
    StoredObject obj;
    obj.remote_path = test_file.string();
    obj.bytes_size = 1024;

    std::vector<char> buf(100);
    size_t bytes_read = reader.read(obj, 256, 100, buf.data());
    EXPECT_EQ(bytes_read, 100);
    EXPECT_EQ(static_cast<unsigned char>(buf[0]), 0);
    EXPECT_EQ(static_cast<unsigned char>(buf[99]), 99);
}

TEST_F(LocalSourceReaderTest, ReadPastEOF)
{
    LocalSourceReader reader;
    StoredObject obj;
    obj.remote_path = test_file.string();
    obj.bytes_size = 1024;

    std::vector<char> buf(200);
    size_t bytes_read = reader.read(obj, 900, 200, buf.data());
    EXPECT_EQ(bytes_read, 124);
}

TEST_F(LocalSourceReaderTest, Name)
{
    LocalSourceReader reader;
    EXPECT_EQ(reader.name(), "LocalSourceReader");
}
