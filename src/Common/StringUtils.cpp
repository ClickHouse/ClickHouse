#include <Common/StringUtils.h>

#include <Common/TargetSpecific.h>
#include <base/defines.h>

#include <cstring>

namespace
{
/// Below this size the head scan costs more than the cache line splits it avoids.
constexpr size_t ALIGN_THRESHOLD = 64 * 1024;

/// OR of all bytes in `[data, data + size)` with explicit vector accumulators. The plain byte loop is not an option
/// since clang 23: it promotes the accumulator to 32-bit lanes (llvm/llvm-project#222142), so x86-64-v4 gets four
/// `vpmovzxbd` plus `vpord` per 64 bytes instead of one `vporq`, and aarch64 a `tbl` shuffle per load. The compiler
/// splits the 64-byte vectors to the register width of the target: two `ymm` at x86-64-v3, four `q` registers on NEON.
using Bytes = UInt8 __attribute__((vector_size(64)));

ALWAYS_INLINE UInt8 orBytes(const UInt8 * data, size_t size)
{
    Bytes acc0{};
    Bytes acc1{};
    Bytes acc2{};
    Bytes acc3{};
    size_t i = 0;
    for (; i + 4 * sizeof(Bytes) <= size; i += 4 * sizeof(Bytes))
    {
        Bytes v0;
        Bytes v1;
        Bytes v2;
        Bytes v3;
        memcpy(&v0, data + i, sizeof(Bytes));
        memcpy(&v1, data + i + sizeof(Bytes), sizeof(Bytes));
        memcpy(&v2, data + i + 2 * sizeof(Bytes), sizeof(Bytes));
        memcpy(&v3, data + i + 3 * sizeof(Bytes), sizeof(Bytes));
        acc0 |= v0;
        acc1 |= v1;
        acc2 |= v2;
        acc3 |= v3;
    }
    acc0 |= acc1;
    acc2 |= acc3;
    acc0 |= acc2;
    for (; i + sizeof(Bytes) <= size; i += sizeof(Bytes))
    {
        Bytes v;
        memcpy(&v, data + i, sizeof(Bytes));
        acc0 |= v;
    }

    UInt64 words[sizeof(Bytes) / sizeof(UInt64)];
    memcpy(words, &acc0, sizeof(Bytes));
    UInt64 word = 0;
    for (UInt64 w : words)
        word |= w;
    word |= word >> 32;
    word |= word >> 16;
    word |= word >> 8;

    UInt8 mask = static_cast<UInt8>(word);
    for (; i < size; ++i)
        mask |= data[i];
    return mask;
}

MULTITARGET_FUNCTION_X86_V4(
    MULTITARGET_FUNCTION_HEADER(static bool NO_INLINE),
    isAllASCIIImpl,
    MULTITARGET_FUNCTION_BODY((const UInt8 * data, size_t size) /// NOLINT
    {
        if (size < ALIGN_THRESHOLD)
            return !(orBytes(data, size) & 0x80);

        /// One overlapping scan of the first 64 bytes, so that the bulk loop starts on a 64-byte
        /// boundary and no wide load splits a cache line. Misaligned 512-bit loads run at half rate.
        UInt8 mask = orBytes(data, 64);

        const size_t start = 64 - (reinterpret_cast<uintptr_t>(data) & 63);
        const UInt8 * aligned = static_cast<const UInt8 *>(__builtin_assume_aligned(data + start, 64));
        mask |= orBytes(aligned, size - start);

        return !(mask & 0x80);
    }))
}

namespace impl
{

bool startsWith(const std::string & s, const char * prefix, size_t prefix_size)
{
    return s.size() >= prefix_size && 0 == memcmp(s.data(), prefix, prefix_size);
}

bool endsWith(const std::string & s, const char * suffix, size_t suffix_size)
{
    return s.size() >= suffix_size && 0 == memcmp(s.data() + s.size() - suffix_size, suffix, suffix_size);
}

}

bool isAllASCII(const UInt8 * data, size_t size)
{
#if USE_MULTITARGET_CODE
    if (DB::isArchSupported(DB::TargetArch::x86_64_v4))
        return isAllASCIIImpl_x86_64_v4(data, size);
#endif

    return isAllASCIIImpl(data, size);
}

LikePatternFixedPrefix extractFixedPrefixFromLikePattern(std::string_view like_pattern, bool requires_perfect_prefix)
{
    String fixed_prefix;
    fixed_prefix.reserve(like_pattern.size());

    const char * pos = like_pattern.data();
    const char * end = pos + like_pattern.size();
    while (pos < end)
    {
        switch (*pos)
        {
            case '%':
            case '_':
            {
                bool is_perfect_prefix = std::all_of(pos, end, [](auto c) { return c == '%'; });
                if (requires_perfect_prefix && !is_perfect_prefix)
                    return {};
                return {.prefix = fixed_prefix, .is_perfect = is_perfect_prefix};
            }
            case '\\':
            {
                ++pos;
                /// A trailing escape is an invalid pattern the matcher rejects; never report it as exact,
                /// or a point range would prune the granule and skip that exception.
                if (pos == end)
                {
                    if (requires_perfect_prefix)
                        return {};
                    return {.prefix = fixed_prefix};
                }
                /// Only '\%', '\_' and '\\' drop the backslash, an unknown escape keeps it.
                if (*pos != '%' && *pos != '_' && *pos != '\\')
                    fixed_prefix += '\\';
                fixed_prefix += *pos;
                break;
            }
            default:
            {
                fixed_prefix += *pos;
            }
        }

        ++pos;
    }
    /// No wildcard was found, so the pattern is an exact match of `fixed_prefix`.
    return {.prefix = fixed_prefix, .is_exact = true};
}

/** For a given string, get a minimum string that is strictly greater than all strings with this prefix,
  *  or return an empty string if there are no such strings.
  */
String firstStringThatIsGreaterThanAllStringsWithPrefix(const String & prefix)
{
    /** Increment the last byte of the prefix by one. But if it is max (255), then remove it and increase the previous one.
      * Example (for convenience, suppose that the maximum value of byte is `z`)
      * abcx -> abcy
      * abcz -> abd
      * zzz -> empty string
      * z -> empty string
      */

    String res = prefix;

    while (!res.empty() && static_cast<UInt8>(res.back()) == std::numeric_limits<UInt8>::max())
        res.pop_back();

    if (res.empty())
        return res;

    res.back() = static_cast<char>(1 + static_cast<UInt8>(res.back()));
    return res;
}
