#pragma once

/** Compatibility shim for `StringRef`. Master replaced `StringRef` with `std::string_view`,
 *  but the `ABStringRef`/`ABHashMap` work in PR #81944 still uses `StringRef` directly
 *  because it relies on public field access (`.data`, `.size`) rather than method access.
 *  This header keeps `StringRef` available for that code while delegating SSE/CRC machinery
 *  to `base/StringViewHash.h` to avoid duplicate definitions.
 */

#include <cassert>
#include <cstring>
#include <ostream>
#include <string>
#include <string_view>
#include <vector>

#include <base/defines.h>
#include <base/types.h>
#include <base/StringViewHash.h>

struct StringRef
{
    const char * data = nullptr;
    size_t size = 0;

    template <typename CharT>
    requires (sizeof(CharT) == 1)
    StringRef(const CharT * data_, size_t size_) : data(reinterpret_cast<const char *>(data_)), size(size_)
    {
        chassert(size < 0x8000000000000000ULL);
    }

    constexpr StringRef(const char * data_, size_t size_) : data(data_), size(size_) {}

    StringRef(const std::string & s) : data(s.data()), size(s.size()) {} /// NOLINT
    constexpr StringRef(std::string_view s) : data(s.data()), size(s.size()) {} /// NOLINT
    constexpr StringRef(const char * data_) : StringRef(std::string_view{data_}) {} /// NOLINT
    constexpr StringRef() = default;

    bool empty() const { return size == 0; }

    std::string toString() const { return std::string(data, size); }
    explicit operator std::string() const { return toString(); }

    std::string_view toView() const { return std::string_view(data, size); }
    constexpr operator std::string_view() const { return std::string_view(data, size); } /// NOLINT
};

using StringRefs = std::vector<StringRef>;


inline bool operator==(StringRef lhs, StringRef rhs)
{
    if (lhs.size != rhs.size)
        return false;

    if (lhs.size == 0)
        return true;

#if defined(__SSE2__) || (defined(__aarch64__) && defined(__ARM_NEON))
    return memequalWide(lhs.data, rhs.data, lhs.size);
#else
    return 0 == memcmp(lhs.data, rhs.data, lhs.size);
#endif
}

inline bool operator!=(StringRef lhs, StringRef rhs)
{
    return !(lhs == rhs);
}

inline bool operator<(StringRef lhs, StringRef rhs)
{
    int cmp = memcmp(lhs.data, rhs.data, std::min(lhs.size, rhs.size));
    return cmp < 0 || (cmp == 0 && lhs.size < rhs.size);
}

inline bool operator>(StringRef lhs, StringRef rhs)
{
    int cmp = memcmp(lhs.data, rhs.data, std::min(lhs.size, rhs.size));
    return cmp > 0 || (cmp == 0 && lhs.size > rhs.size);
}


/// Delegate hashing to `StringViewHash` from `base/StringViewHash.h` via the
/// implicit `StringRef` -> `std::string_view` conversion above.
struct StringRefHash
{
    size_t operator()(StringRef x) const { return StringViewHash{}(static_cast<std::string_view>(x)); }
};

struct StringRefHash64
{
    size_t operator()(StringRef x) const { return StringViewHash64{}(static_cast<std::string_view>(x)); }
};


namespace std
{
    template <>
    struct hash<StringRef> : public StringRefHash {};
}


namespace ZeroTraits
{
    inline bool check(const StringRef & x) { return 0 == x.size; }
    inline void set(StringRef & x) { x.size = 0; }
}

namespace PackedZeroTraits
{
    template <typename Second, template <typename, typename> class PackedPairNoInit>
    inline bool check(const PackedPairNoInit<StringRef, Second> p)
    {
        return 0 == p.key.size;
    }

    template <typename Second, template <typename, typename> class PackedPairNoInit>
    inline void set(PackedPairNoInit<StringRef, Second> & p)
    {
        p.key.size = 0;
    }
}


inline std::ostream & operator<<(std::ostream & os, const StringRef & str)
{
    if (str.data)
        os.write(str.data, str.size);
    return os;
}
