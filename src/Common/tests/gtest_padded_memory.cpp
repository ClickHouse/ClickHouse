#include <base/types.h>
#include <base/memcmpSmall.h>
#include <base/sanitizer_defs.h>
#include <Common/memcpySmall.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <memory>

#if defined(OS_LINUX) || defined(OS_DARWIN)
#include <sys/mman.h>
#include <unistd.h>
#include <stdexcept>
#endif

namespace
{
constexpr size_t padding = 15;
constexpr std::array<size_t, 11> lengths{0, 1, 15, 16, 17, 31, 32, 33, 47, 48, 49};

void poisonPadding(UInt8 * data, size_t size)
{
#if defined(MEMORY_SANITIZER)
    __msan_poison(data + size, padding);
#else
    static_cast<void>(data);
    static_cast<void>(size);
#endif
}

#if defined(OS_LINUX) || defined(OS_DARWIN)
struct Unmap
{
    size_t size;
    void operator()(void * data) const
    {
        munmap(data, size);
    }
};

class GuardedPage
{
    static void * allocate(size_t size)
    {
        void * data = mmap(nullptr, size, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (data == MAP_FAILED)
            throw std::bad_alloc();
        return data;
    }

    const size_t page_size = static_cast<size_t>(getpagesize());
    std::unique_ptr<void, Unmap> mapping{allocate(page_size * 3), Unmap{page_size * 3}};

public:
    GuardedPage()
    {
        if (mprotect(begin(), page_size, PROT_READ | PROT_WRITE) != 0)
            throw std::runtime_error("Cannot make the test page writable");
    }

    UInt8 * begin() const
    {
        return static_cast<UInt8 *>(mapping.get()) + page_size;
    }
    UInt8 * end() const
    {
        return begin() + page_size;
    }
};
#endif
}

TEST(PaddedMemory, CopyExactPaddingAndIndependentAlignments)
{
    for (size_t size : lengths)
        for (size_t source_offset = 0; source_offset < 16; ++source_offset)
            for (size_t destination_offset = 0; destination_offset < 16; ++destination_offset)
            {
                SCOPED_TRACE(::testing::Message() << size << ',' << source_offset << ',' << destination_offset);
                auto source = std::make_unique<UInt8[]>(source_offset + size + padding);
                auto destination = std::make_unique<UInt8[]>(destination_offset + size + padding);
                std::fill_n(destination.get(), destination_offset + size + padding, UInt8{0xa5});
                auto * src = source.get() + source_offset;
                auto * dst = destination.get() + destination_offset;
                for (size_t i = 0; i < size + padding; ++i)
                    src[i] = static_cast<UInt8>((i * 17 + 0x81) & 0xff);
                poisonPadding(src, size);

                memcpySmallAllowReadWriteOverflow15(dst, src, size);

                for (size_t i = 0; i < size; ++i)
                    ASSERT_EQ(dst[i], src[i]);
                for (size_t i = 0; i < destination_offset; ++i)
                    ASSERT_EQ(destination[i], UInt8{0xa5});
            }
}

TEST(PaddedMemory, ZeroPaddedEqualityExactPaddingAndIndependentAlignments)
{
    for (size_t size : lengths)
        for (size_t tail : {size_t{0}, size_t{1}, size_t{17}})
            for (size_t a_offset = 0; a_offset < 16; ++a_offset)
                for (size_t b_offset = 0; b_offset < 16; ++b_offset)
                {
                    SCOPED_TRACE(::testing::Message() << size << ',' << tail << ',' << a_offset << ',' << b_offset);
                    const size_t a_size = size + tail;
                    auto a_storage = std::make_unique<UInt8[]>(a_offset + a_size + padding);
                    auto b_storage = std::make_unique<UInt8[]>(b_offset + size + padding);
                    auto * a = a_storage.get() + a_offset;
                    auto * b = b_storage.get() + b_offset;
                    std::fill_n(a, size, UInt8{0x81});
                    std::fill_n(b, size, UInt8{0x81});
                    std::fill_n(a + size, tail, UInt8{0});
                    std::fill_n(a + a_size, padding, UInt8{0x11});
                    std::fill_n(b + size, padding, UInt8{0xee});
                    poisonPadding(a, a_size);
                    poisonPadding(b, size);

                    ASSERT_TRUE(memequalSmallLikeZeroPaddedAllowOverflow15(a, a_size, b, size));
                    EXPECT_TRUE(memequalSmallLikeZeroPaddedAllowOverflow15(b, size, a, a_size));
                    EXPECT_TRUE(memequalSmallLikeZeroPaddedAllowOverflow15(a, a_size, a, size));
                    if (tail)
                    {
                        a[a_size - 1] = UInt8{0x80};
                        EXPECT_FALSE(memequalSmallLikeZeroPaddedAllowOverflow15(a, a_size, b, size));
                        EXPECT_FALSE(memequalSmallLikeZeroPaddedAllowOverflow15(b, size, a, a_size));
                        a[a_size - 1] = UInt8{0};
                    }
                    if (size)
                    {
                        b[size - 1] ^= UInt8{0x80};
                        EXPECT_FALSE(memequalSmallLikeZeroPaddedAllowOverflow15(a, a_size, b, size));
                        EXPECT_FALSE(memequalSmallLikeZeroPaddedAllowOverflow15(b, size, a, a_size));
                    }
                }
}

#if defined(OS_LINUX) || defined(OS_DARWIN)
TEST(PaddedMemory, GuardPagesAtBothEdges)
{
    GuardedPage source;
    GuardedPage destination;
    volatile size_t zero_length = 0;
    const size_t zero = zero_length;
    memcpySmallAllowReadWriteOverflow15(destination.end(), source.end(), zero);
    EXPECT_TRUE(memequalSmallLikeZeroPaddedAllowOverflow15(source.end(), zero, destination.end(), zero));

    for (size_t size : lengths)
        for (bool at_end : {false, true})
        {
            SCOPED_TRACE(::testing::Message() << size << ',' << at_end);
            auto * src = at_end ? source.end() - size - padding : source.begin();
            auto * dst = at_end ? destination.end() - size - padding : destination.begin();
            std::fill_n(src, size + padding, UInt8{0x81});
            std::fill_n(dst, size + padding, UInt8{0xa5});
            poisonPadding(src, size);
            memcpySmallAllowReadWriteOverflow15(dst, src, size);
            for (size_t i = 0; i < size; ++i)
                ASSERT_EQ(dst[i], src[i]);

            std::fill_n(src + size, padding, UInt8{0x11});
            std::fill_n(dst + size, padding, UInt8{0xee});
            poisonPadding(src, size);
            poisonPadding(dst, size);
            EXPECT_TRUE(memequalSmallLikeZeroPaddedAllowOverflow15(src, size, dst, size));
        }
}
#endif
