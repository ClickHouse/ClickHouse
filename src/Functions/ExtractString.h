#include <Common/UTF8Helpers.h>

#include <algorithm>
#include <climits>
#include <cstring>
#include <memory>
#include <utility>

#ifdef __SSE4_2__
#    include <nmmintrin.h>
#endif

namespace DB
{
// used by FunctionsStringSimilarity and FunctionsStringHash
// includes extracting ASCII ngram, UTF8 ngram, ASCII word and UTF8 word
template <size_t N, bool CaseInsensitive>
struct ExtractStringImpl
{
    static constexpr size_t default_padding = 16;

    static ALWAYS_INLINE size_t readASCIICodePoints(UInt8 * code_points, const char *& pos, const char * end)
    {
        /// Offset before which we copy some data.
        constexpr size_t padding_offset = default_padding - N + 1;
        /// We have an array like this for ASCII (N == 4, other cases are similar)
        /// |a0|a1|a2|a3|a4|a5|a6|a7|a8|a9|a10|a11|a12|a13|a14|a15|a16|a17|a18|
        /// And we copy                                ^^^^^^^^^^^^^^^ these bytes to the start
        /// Actually it is enough to copy 3 bytes, but memcpy for 4 bytes translates into 1 instruction
        memcpy(code_points, code_points + padding_offset, roundUpToPowerOfTwoOrZero(N - 1) * sizeof(UInt8));
        /// Now we have an array
        /// |a13|a14|a15|a16|a4|a5|a6|a7|a8|a9|a10|a11|a12|a13|a14|a15|a16|a17|a18|
        ///              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        /// Doing unaligned read of 16 bytes and copy them like above
        /// 16 is also chosen to do two `movups`.
        /// Such copying allow us to have 3 codepoints from the previous read to produce the 4-grams with them.
        memcpy(code_points + (N - 1), pos, default_padding * sizeof(UInt8));

        if constexpr (CaseInsensitive)
        {
            /// We really need template lambdas with C++20 to do it inline
            unrollLowering<N - 1>(code_points, std::make_index_sequence<padding_offset>());
        }
        pos += padding_offset;
        if (pos > end)
            return default_padding - (pos - end);
        return default_padding;
    }

    // used by FunctionsStringHash
    // it's not easy to add padding for ColumnString, so we need safety check each memcpy
    static ALWAYS_INLINE size_t readASCIICodePointsNoPadding(UInt8 * code_points, const char *& pos, const char * end)
    {
        constexpr size_t padding_offset = default_padding - N + 1;
        memcpy(code_points, code_points + padding_offset, roundUpToPowerOfTwoOrZero(N - 1) * sizeof(UInt8));

        // safety check
        size_t cpy_size = (pos + padding_offset > end) ? end - pos : padding_offset;

        memcpy(code_points + (N - 1), pos, cpy_size * sizeof(UInt8));

        if constexpr (CaseInsensitive)
        {
            unrollLowering<N - 1>(code_points, std::make_index_sequence<padding_offset>());
        }
        pos += padding_offset;
        if (pos > end)
            return default_padding - (pos - end);
        return default_padding;
    }

    // read a ASCII word from pos to word
    // if the word size exceeds max_word_size, only read max_word_size byte
    // in  FuntionsStringHash, the default value of max_word_size is 128
    static ALWAYS_INLINE inline size_t readOneASCIIWord(UInt8 * word, const char *& pos, const char * end, const size_t & max_word_size)
    {
        // jump seperators
        while (pos < end && !isAlphaNum(*pos))
            ++pos;

        // word start from here
        const char * word_start = pos;
        while (pos < end && isAlphaNum(*pos))
            ++pos;

        size_t word_size = (static_cast<size_t>(pos - word_start) <= max_word_size) ? pos - word_start : max_word_size;

        memcpy(word, word_start, word_size);
        if (CaseInsensitive)
        {
            std::transform(word, word + word_size, word, [](UInt8 c) { return std::tolower(c); });
        }
        return word_size;
    }

    static ALWAYS_INLINE inline size_t readUTF8CodePoints(UInt32 * code_points, const char *& pos, const char * end)
    {
        memcpy(code_points, code_points + default_padding - N + 1, roundUpToPowerOfTwoOrZero(N - 1) * sizeof(UInt32));

        size_t num = N - 1;
        while (num < default_padding && pos < end)
        {
            code_points[num++] = readOneUTF8Code(pos, end);
        }
        return num;
    }

    // read one UTF8 word from pos to word
    // also, we assume that one word size cann't exceed max_word_size with default value 128
    static ALWAYS_INLINE inline size_t readOneUTF8Word(UInt32 * word, const char *& pos, const char * end, const size_t & max_word_size)
    {
        // jump UTF8 seperator
        while (pos < end && isUTF8Sep(*pos))
            ++pos;
        // UTF8 word's character number
        size_t num = 0;
        while (pos < end && num < max_word_size && !isUTF8Sep(*pos))
        {
            word[num++] = readOneUTF8Code(pos, end);
        }
        return num;
    }

private:
    static ALWAYS_INLINE inline bool isAlphaNum(const UInt8 c)
    {
        return (c >= 48 && c <= 57) || (c >= 65 && c <= 90) || (c >= 97 && c <= 122);
    }

    template <size_t Offset, typename Container, size_t... I>
    static ALWAYS_INLINE inline void unrollLowering(Container & cont, const std::index_sequence<I...> &)
    {
        ((cont[Offset + I] = std::tolower(cont[Offset + I])), ...);
    }

    // we use ASCII non-alphanum character as UTF8 seperator
    static ALWAYS_INLINE inline bool isUTF8Sep(const UInt8 c) { return c < 128 && !isAlphaNum(c); }

    // read one UTF8 character and return it
    static ALWAYS_INLINE inline UInt32 readOneUTF8Code(const char *& pos, const char * end)
    {
        size_t length = UTF8::seqLength(*pos);

        if (pos + length > end)
            length = end - pos;
        UInt32 res;
        switch (length)
        {
            case 1:
                res = 0;
                memcpy(&res, pos, 1);
                break;
            case 2:
                res = 0;
                memcpy(&res, pos, 2);
                break;
            case 3:
                res = 0;
                memcpy(&res, pos, 3);
                break;
            default:
                memcpy(&res, pos, 4);
        }

        if constexpr (CaseInsensitive)
        {
            switch (length)
            {
                case 4:
                    res &= ~(1u << (5 + 3 * CHAR_BIT));
                    [[fallthrough]];
                case 3:
                    res &= ~(1u << (5 + 2 * CHAR_BIT));
                    [[fallthrough]];
                case 2:
                    res &= ~(1u);
                    res &= ~(1u << (5 + CHAR_BIT));
                    [[fallthrough]];
                default:
                    res &= ~(1u << 5);
            }
        }
        pos += length;
        return res;
    }
};
}
