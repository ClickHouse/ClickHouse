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
constexpr bool maybe_negate(bool x)
{
    return positive == x;
}

static bool is_in(char c, const char * needles, size_t num_chars)
{
    for (auto i = 0u; i < num_chars; i++)
    {
        if (c == needles[i])
        {
            return true;
        }
    }

    return false;
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
        if (maybe_negate<positive>(is_in(*pos, needle, num_chars)))
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
    if (auto position = find_first_not({haystack.begin() + offset, haystack.end()}, needles))
    {
        return offset + *position;
    }

    return std::nullopt;
}

}
