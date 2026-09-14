#include <Common/StringUtils.h>

#include <gtest/gtest.h>

#include <vector>


TEST(StringUtils, IsAllASCII)
{
    const std::vector<size_t> sizes = {0, 1, 15, 16, 17, 31, 32, 33, 63, 64, 65, 256};

    for (const size_t size : sizes)
    {
        std::vector<UInt8> data(size, 'a');
        ASSERT_TRUE(isAllASCII(data.data(), data.size())) << "size: " << size;

        if (size == 0)
            continue;

        data[0] = 0;
        ASSERT_TRUE(isAllASCII(data.data(), data.size())) << "size: " << size;

        data[size - 1] = 0x7F;
        ASSERT_TRUE(isAllASCII(data.data(), data.size())) << "size: " << size;

        for (const size_t position : {size_t(0), size / 2, size - 1})
        {
            data[position] = 0x80;
            ASSERT_FALSE(isAllASCII(data.data(), data.size())) << "size: " << size << ", position: " << position;

            data[position] = 0xFF;
            ASSERT_FALSE(isAllASCII(data.data(), data.size())) << "size: " << size << ", position: " << position;

            data[position] = 'a';
        }
    }
}
