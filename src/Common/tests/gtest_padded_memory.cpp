#include <base/types.h>
#include <base/memcmpSmall.h>
#include <base/sanitizer_defs.h>

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

void poisonPadding(const UInt8 * data, size_t size)
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

#if defined(OS_LINUX) || defined(OS_DARWIN)
TEST(PaddedMemory, ZeroPaddedEqualityGuardPages)
{
    GuardedPage source;
    GuardedPage destination;
    volatile size_t zero_length = 0;
    const size_t zero = zero_length;
    EXPECT_TRUE(memequalSmallLikeZeroPaddedAllowOverflow15(source.end(), zero, destination.end(), zero));

    for (size_t size : lengths)
    {
        for (bool at_end : {false, true})
        {
            SCOPED_TRACE(::testing::Message() << size << ',' << at_end);
            auto * src = at_end ? source.end() - size - padding : source.begin();
            auto * dst = at_end ? destination.end() - size - padding : destination.begin();
            std::fill_n(src, size + padding, UInt8{0x81});
            std::fill_n(dst, size + padding, UInt8{0x81});

            std::fill_n(src + size, padding, UInt8{0x11});
            std::fill_n(dst + size, padding, UInt8{0xee});
            poisonPadding(src, size);
            poisonPadding(dst, size);
            EXPECT_TRUE(memequalSmallLikeZeroPaddedAllowOverflow15(src, size, dst, size));
            if (size)
            {
                dst[size - 1] ^= UInt8{0x80};
                EXPECT_FALSE(memequalSmallLikeZeroPaddedAllowOverflow15(src, size, dst, size));
            }
        }
    }
}
#endif
