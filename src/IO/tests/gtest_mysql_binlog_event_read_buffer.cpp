#include <gtest/gtest.h>
#include <Common/Exception.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/MySQLBinlogEventReadBuffer.h>

using namespace DB;

TEST(MySQLBinlogEventReadBuffer, CheckBoundary)
{
    for (size_t index = 1; index < 4; ++index)
    {
        std::vector<char> memory_data(index, 0x01);
        ReadBufferFromMemory nested_in(memory_data.data(), index);

        MySQLBinlogEventReadBuffer binlog_in(nested_in);
        EXPECT_THROW(binlog_in.ignore(), Exception);
    }
}


