#include <gtest/gtest.h>

#include <Common/AlignedBuffer.h>
#include <Common/CacheLine.h>

#include <array>
#include <cstdint>
#include <vector>

#if defined(OS_LINUX)
#    include <malloc.h>
#endif

using namespace DB;

TEST(AlignedBuffer, OccupiesWholeCacheLines)
{
    constexpr std::array<size_t, 6> sizes{1, 8, 16, 24, 64, 65};
    constexpr std::array<size_t, 3> alignments{1, 8, 16};
    std::vector<AlignedBuffer> buffers;
    std::vector<size_t> requested_sizes;
    buffers.reserve(sizes.size() * alignments.size());
    requested_sizes.reserve(sizes.size() * alignments.size());
    for (size_t size : sizes)
    {
        for (size_t alignment : alignments)
        {
            buffers.emplace_back(size, alignment);
            requested_sizes.push_back(size);
        }
    }
    for (size_t i = 0; i < buffers.size(); ++i)
    {
        EXPECT_EQ(reinterpret_cast<uintptr_t>(buffers[i].data()) % CH_CACHE_LINE_SIZE, 0);
#if defined(OS_LINUX)
        /// Sanitizer allocators report the requested size here, so a size that is not rounded up to whole lines fails.
        const size_t whole_lines = (requested_sizes[i] + CH_CACHE_LINE_SIZE - 1) / CH_CACHE_LINE_SIZE * CH_CACHE_LINE_SIZE;
        EXPECT_GE(malloc_usable_size(buffers[i].data()), whole_lines);
#endif
    }
}
