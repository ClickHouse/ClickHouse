#if defined(__linux__) || defined(__FreeBSD__)

#include <gtest/gtest.h>

#include <Core/Defines.h>
#include <unistd.h>
#include <IO/ReadBufferAIO.h>
#include <Common/randomSeed.h>
#include <fstream>
#include <string>


namespace
{
std::string createTmpFileForEOFtest()
{
    char pattern[] = "/tmp/fileXXXXXX";
    if (char * dir = ::mkdtemp(pattern); dir)
    {
        return std::string(dir) + "/foo";
    }
    else
    {
        /// We have no tmp in docker
        /// So we have to use root
        std::string almost_rand_dir = std::string{"/"} + std::to_string(randomSeed()) + "foo";
        return almost_rand_dir;
    }

}

void prepareForEOF(std::string & filename, std::string & buf)
{
    static const std::string symbols = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    filename = createTmpFileForEOFtest();

    size_t n = 10 * DEFAULT_AIO_FILE_BLOCK_SIZE;
    buf.reserve(n);

    for (size_t i = 0; i < n; ++i)
        buf += symbols[i % symbols.length()];

    std::ofstream out(filename);
    out << buf;
}


}
TEST(ReadBufferAIOTest, TestReadAfterAIO)
{
    using namespace DB;
    std::string data;
    std::string file_path;
    prepareForEOF(file_path, data);
    ReadBufferAIO testbuf(file_path);

    std::string newdata;
    newdata.resize(data.length());

    size_t total_read = testbuf.read(newdata.data(), newdata.length());
    EXPECT_EQ(total_read, data.length());
    EXPECT_TRUE(testbuf.eof());


    testbuf.seek(data.length() - 100, SEEK_SET);

    std::string smalldata;
    smalldata.resize(100);
    size_t read_after_eof = testbuf.read(smalldata.data(), smalldata.size());
    EXPECT_EQ(read_after_eof, 100);
    EXPECT_TRUE(testbuf.eof());


    testbuf.seek(0, SEEK_SET);
    std::string repeatdata;
    repeatdata.resize(data.length());
    size_t read_after_eof_big = testbuf.read(repeatdata.data(), repeatdata.size());
    EXPECT_EQ(read_after_eof_big, data.length());
    EXPECT_TRUE(testbuf.eof());
}

#endif
