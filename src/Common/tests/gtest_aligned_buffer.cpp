#include <gtest/gtest.h>

#include <Common/AlignedBuffer.h>
#include <Common/CacheLine.h>

#include <array>
#include <cstdint>
#include <vector>

using namespace DB;

TEST(AlignedBuffer, OccupiesWholeCacheLines)
{
    constexpr std::array<size_t, 6> sizes{1, 8, 16, 24, 64, 65};
    constexpr std::array<size_t, 3> alignments{1, 8, 16};
    std::vector<AlignedBuffer> buffers;
    buffers.reserve(sizes.size() * alignments.size());
    for (size_t size : sizes)
        for (size_t alignment : alignments)
            buffers.emplace_back(size, alignment);
    for (const auto & buffer : buffers)
        EXPECT_EQ(reinterpret_cast<uintptr_t>(buffer.data()) % CH_CACHE_LINE_SIZE, 0);
}
