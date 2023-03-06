#include "CharacterFinder.h"
#include <nmmintrin.h>


namespace DB
{

template <bool positive>
constexpr uint16_t maybe_negate(uint16_t x)
{
    if constexpr (positive)
        return x;
    else
        return ~x;
}

template <bool positive>
inline const char * find_first_symbols_sse42(const char * const begin, const char * const end, const char * needle, size_t num_chars)
{
    const char * pos = begin;

#if defined(__SSE4_2__)
    constexpr int mode = _SIDD_UBYTE_OPS | _SIDD_CMP_EQUAL_ANY | _SIDD_LEAST_SIGNIFICANT;

    if (num_chars >= 16)
        throw std::runtime_error("Needle is too big");

    char c[16] = {'\0'};
    memcpy(c, needle, num_chars);

    const __m128i set = _mm_loadu_si128(reinterpret_cast<const __m128i *>(c));

    for (; pos + 15 < end; pos += 16)
    {
        __m128i bytes = _mm_loadu_si128(reinterpret_cast<const __m128i *>(pos));

        if constexpr (positive)
        {
            if (_mm_cmpestrc(set, num_chars, bytes, 16, mode))
                return pos + _mm_cmpestri(set, num_chars, bytes, 16, mode);
        }
        else
        {
            if (_mm_cmpestrc(set, num_chars, bytes, 16, mode | _SIDD_NEGATIVE_POLARITY))
                return pos + _mm_cmpestri(set, num_chars, bytes, 16, mode | _SIDD_NEGATIVE_POLARITY);
        }
    }
#endif

    for (; pos < end; ++pos)
        if (   (num_chars >= 1 && maybe_negate<positive>(*pos == c[0]))
            || (num_chars >= 2 && maybe_negate<positive>(*pos == c[1]))
            || (num_chars >= 3 && maybe_negate<positive>(*pos == c[2]))
            || (num_chars >= 4 && maybe_negate<positive>(*pos == c[3]))
            || (num_chars >= 5 && maybe_negate<positive>(*pos == c[4]))
            || (num_chars >= 6 && maybe_negate<positive>(*pos == c[5]))
            || (num_chars >= 7 && maybe_negate<positive>(*pos == c[6]))
            || (num_chars >= 8 && maybe_negate<positive>(*pos == c[7]))
            || (num_chars >= 9 && maybe_negate<positive>(*pos == c[8]))
            || (num_chars >= 10 && maybe_negate<positive>(*pos == c[9]))
            || (num_chars >= 11 && maybe_negate<positive>(*pos == c[10]))
            || (num_chars >= 12 && maybe_negate<positive>(*pos == c[11]))
            || (num_chars >= 13 && maybe_negate<positive>(*pos == c[12]))
            || (num_chars >= 14 && maybe_negate<positive>(*pos == c[13]))
            || (num_chars >= 15 && maybe_negate<positive>(*pos == c[14]))
            || (num_chars >= 16 && maybe_negate<positive>(*pos == c[15])))
            return pos;

    return nullptr;
}

template <bool positive = true>
auto find_first_symbols_sse42(std::string_view haystack, std::string_view needle)
{
    return find_first_symbols_sse42<positive>(haystack.begin(), haystack.end(), needle.begin(), needle.size());
}

std::optional<CharacterFinder::Position> CharacterFinder::find_first(std::string_view haystack, const std::vector<char> & needles) const
{
    if (const auto * ptr = find_first_symbols_sse42(haystack, {needles.begin(), needles.end()}))
    {
        return ptr - haystack.begin();
    }

    return std::nullopt;
}

std::optional<CharacterFinder::Position> CharacterFinder::find_first(std::string_view haystack, std::size_t offset, const std::vector<char> & needles) const
{
    if (auto position = find_first({haystack.begin() + offset, haystack.end()}, needles))
    {
        return offset + *position;
    }

    return std::nullopt;
}

std::optional<CharacterFinder::Position> CharacterFinder::find_first_not(std::string_view haystack, const std::vector<char> & needles) const
{
    if (const auto * ptr = find_first_symbols_sse42<false>(haystack, {needles.begin(), needles.end()}))
    {
        return ptr - haystack.begin();
    }

    return std::nullopt;
}

std::optional<CharacterFinder::Position> CharacterFinder::find_first_not(std::string_view haystack, std::size_t offset, const std::vector<char> & needles) const
{
    return find_first_not({haystack.begin() + offset, haystack.end()}, needles);
}

}
