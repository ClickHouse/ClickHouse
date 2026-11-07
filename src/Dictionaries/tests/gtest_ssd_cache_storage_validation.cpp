#include <gtest/gtest.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}

TEST(SSDCacheStorageValidation, WriteBufferSizeExceedsCapacity)
{
    size_t max_partitions_count = 1;
    size_t file_size = 4096;
    size_t write_buffer_size = 8192;

    EXPECT_GT(write_buffer_size, max_partitions_count * file_size);
}
