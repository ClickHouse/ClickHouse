#include <gtest/gtest.h>
#include <Common/Exception.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/MySQLBinlogEventReadBuffer.h>

using namespace DB;

TEST(MySQLBinlogEventReadBuffer, CheckBoundary)
{
    for (size_t index = 1; index < 4; ++index)
    {
        std::vector<char> memory_data(index, 0x01);
        ReadBufferFromMemory nested_in(memory_data.data(), index);

        EXPECT_THROW({ MySQLBinlogEventReadBuffer binlog_in(nested_in, 4); }, Exception);
    }
}

TEST(MySQLBinlogEventReadBuffer, NiceBufferSize)
{
    char res[2];
    std::vector<char> memory_data(6, 0x01);
    ReadBufferFromMemory nested_in(memory_data.data(), 6);

    MySQLBinlogEventReadBuffer binlog_in(nested_in, 4);
    binlog_in.readStrict(res, 2);
    ASSERT_EQ(res[0], 0x01);
    ASSERT_EQ(res[1], 0x01);
    ASSERT_TRUE(binlog_in.eof());
}

TEST(MySQLBinlogEventReadBuffer, BadBufferSizes)
{
    char res[4];
    ConcatReadBuffer concat_buffer;
    std::vector<std::shared_ptr<std::vector<char>>> memory_buffers_data;
    std::vector<size_t> bad_buffers_size = {2, 1, 2, 3};

    for (const auto & bad_buffer_size : bad_buffers_size)
    {
        memory_buffers_data.emplace_back(std::make_shared<std::vector<char>>(bad_buffer_size, 0x01));
        concat_buffer.appendBuffer(std::make_unique<ReadBufferFromMemory>(memory_buffers_data.back()->data(), bad_buffer_size));
    }

    MySQLBinlogEventReadBuffer binlog_in(concat_buffer, 4);
    binlog_in.readStrict(res, 4);

    for (const auto & res_byte : res)
        ASSERT_EQ(res_byte, 0x01);

    ASSERT_TRUE(binlog_in.eof());
}

TEST(MySQLBinlogEventReadBuffer, NiceAndBadBufferSizes)
{
    char res[12];
    ConcatReadBuffer::Buffers nested_buffers;
    std::vector<std::shared_ptr<std::vector<char>>> memory_buffers_data;
    std::vector<size_t> buffers_size = {6, 1, 3, 6};

    for (const auto & bad_buffer_size : buffers_size)
    {
        memory_buffers_data.emplace_back(std::make_shared<std::vector<char>>(bad_buffer_size, 0x01));
        nested_buffers.emplace_back(std::make_unique<ReadBufferFromMemory>(memory_buffers_data.back()->data(), bad_buffer_size));
    }

    ConcatReadBuffer concat_buffer(std::move(nested_buffers));
    MySQLBinlogEventReadBuffer binlog_in(concat_buffer, 4);
    binlog_in.readStrict(res, 12);

    for (const auto & res_byte : res)
        ASSERT_EQ(res_byte, 0x01);

    ASSERT_TRUE(binlog_in.eof());
}

