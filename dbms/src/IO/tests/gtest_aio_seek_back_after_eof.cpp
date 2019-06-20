#if defined(__linux__) || defined(__FreeBSD__)

#pragma GCC diagnostic ignored "-Wsign-compare"
#ifdef __clang__
#pragma clang diagnostic ignored "-Wzero-as-null-pointer-constant"
#pragma clang diagnostic ignored "-Wundef"
#endif
#include <gtest/gtest.h>

#include <Core/Defines.h>
#include <port/unistd.h>
#include <IO/ReadBufferAIO.h>
#include <fstream>

namespace
{
std::string createTmpFileForEOFtest()
{
    char pattern[] = "/tmp/fileXXXXXX";
    char * dir = ::mkdtemp(pattern);
    return std::string(dir) + "/foo";
}

void prepare_for_eof(std::string & filename, std::string & buf)
{
    static const std::string symbols = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    filename = createTmpFileForEOFtest();

    size_t n = 10 * DEFAULT_AIO_FILE_BLOCK_SIZE;
    buf.reserve(n);

    for (size_t i = 0; i < n; ++i)
        buf += symbols[i % symbols.length()];

    std::ofstream out(filename.c_str());
    out << buf;
}


}
TEST(ReadBufferAIOTest, TestReadAfterAIO)
{
    using namespace DB;
    std::string data;
    std::string file_path;
    prepare_for_eof(file_path, data);
    ReadBufferAIO testbuf(file_path);

    std::string newdata;
    newdata.resize(data.length());

    size_t total_read = testbuf.read(newdata.data(), newdata.length());
    EXPECT_EQ(total_read, data.length());
    EXPECT_TRUE(testbuf.eof());


    testbuf.seek(data.length() - 100);

    std::string smalldata;
    smalldata.resize(100);
    size_t read_after_eof = testbuf.read(smalldata.data(), smalldata.size());
    EXPECT_EQ(read_after_eof, 100);
    EXPECT_TRUE(testbuf.eof());


    testbuf.seek(0);
    std::string repeatdata;
    repeatdata.resize(data.length());
    size_t read_after_eof_big = testbuf.read(repeatdata.data(), repeatdata.size());
    EXPECT_EQ(read_after_eof_big, data.length());
    EXPECT_TRUE(testbuf.eof());
}

#endif
