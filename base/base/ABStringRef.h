#pragma once

#include <base/StringRef.h>

struct ABStringRef
{
    uint64_t low;
    uint64_t high;

    uint8_t getSmallSize() const { return high >> 59; }

    bool isMedium() const { return getSmallSize() == 0; }

    bool isLarge() const { return (high & 0x8000000000000000ULL) != 0; }

    bool isSmall() const { return !isMedium() && !isLarge(); }

    uint32_t getSmallHash() const { return static_cast<uint32_t>(low); }

    char * getSmallPtr() { return reinterpret_cast<char *>(&low) + 4; }

    const char * getSmallPtr() const { return reinterpret_cast<const char *>(&low) + 4; }

    uint32_t getMediumHash() const { return getSmallHash(); }

    uint32_t getMediumSize() const { return static_cast<uint32_t>(low >> 32); }

    const char * getMediumPtr() const { return reinterpret_cast<const char *>(high); }

    uint64_t getLargeSize() const { return low; }

    const char * getLargePtr() const { return reinterpret_cast<const char *>(high & 0x7FFFFFFFFFFFFFFFULL); }

    size_t heapSize() const
    {
        if (isMedium())
            return getMediumSize();
        else if (isLarge())
            return getLargeSize();
        else
            return 0;
    }

    explicit operator StringRef() const
    {
        if (isMedium())
            return {getMediumPtr(), getMediumSize()};
        else if (isLarge())
            return {getLargePtr(), getLargeSize()};
        else
            return {getSmallPtr(), getSmallSize()};
    }

    static ABStringRef build(const char * ptr, size_t len, size_t hash)
    {
        ABStringRef r;
        if (len == 0)
        {
            r.low = 0;
            r.high = 0;
        }
        else if (len < 12)
        {
            r.low = hash;
            r.high = len << 59;
            memcpy(r.getSmallPtr(), ptr, len);
        }
        else if (len <= std::numeric_limits<uint32_t>::max())
        {
            r.low = (len << 32) | static_cast<uint32_t>(hash);
            r.high = reinterpret_cast<uintptr_t>(ptr);
        }
        else
        {
            r.low = len;
            r.high = reinterpret_cast<uintptr_t>(ptr) | 0x8000000000000000ULL;
        }
        return r;
    }
};

inline ALWAYS_INLINE bool operator==(ABStringRef lhs, ABStringRef rhs)
{
    if (lhs.low != rhs.low)
        return false;

    if (lhs.high == rhs.high)
        return true;

    if (lhs.isMedium() && rhs.isMedium()) [[likely]]
    {
#if defined(__SSE2__) || (defined(__aarch64__) && defined(__ARM_NEON))
        return memequalWide(lhs.getMediumPtr(), rhs.getMediumPtr(), lhs.getMediumSize());
#else
        return 0 == memcmp(lhs.getMediumPtr(), rhs.getMediumPtr(), lhs.getMediumSize());
#endif
    }

    if (lhs.isLarge() && rhs.isLarge()) [[unlikely]]
    {
#if defined(__SSE2__) || (defined(__aarch64__) && defined(__ARM_NEON))
        return memequalWide(lhs.getLargePtr(), rhs.getLargePtr(), lhs.getLargeSize());
#else
        return 0 == memcmp(lhs.getLargePtr(), rhs.getLargePtr(), lhs.getLargeSize());
#endif
    }

    return false;
}

namespace ZeroTraits
{
inline bool check(const ABStringRef & x)
{
    return 0 == x.high;
}
inline void set(ABStringRef & x)
{
    x.high = 0;
}
}
