#include <Common/PODArray.h>
#include <Common/StringUtils/StringUtils.h>
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

    // the length of code_points = default_padding + N -1
    // pos: the current beginning location that we want to copy data
    // end: the end loction of the string
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

    // read a ASCII word
    static ALWAYS_INLINE inline size_t readOneASCIIWord(PaddedPODArray<UInt8> & word_buf, const char *& pos, const char * end)
    {
        // jump seperators
        while (pos < end && !isAlphaNumericASCII(*pos))
            ++pos;

        // word start from here
        const char * word_start = pos;
        while (pos < end && isAlphaNumericASCII(*pos))
            ++pos;

        word_buf.assign(word_start, pos);
        if (CaseInsensitive)
        {
            std::transform(word_buf.begin(), word_buf.end(), word_buf.begin(), [](UInt8 c) { return std::tolower(c); });
        }
        return word_buf.size();
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
    static ALWAYS_INLINE inline size_t readOneUTF8Word(PaddedPODArray<UInt32> & word_buf, const char *& pos, const char * end)
    {
        // jump UTF8 seperator
        while (pos < end && isUTF8Sep(*pos))
            ++pos;
        word_buf.clear();
        // UTF8 word's character number
        while (pos < end && !isUTF8Sep(*pos))
        {
            word_buf.push_back(readOneUTF8Code(pos, end));
        }
        return word_buf.size();
    }

private:
    template <size_t Offset, typename Container, size_t... I>
    static ALWAYS_INLINE inline void unrollLowering(Container & cont, const std::index_sequence<I...> &)
    {
        ((cont[Offset + I] = std::tolower(cont[Offset + I])), ...);
    }

    // we use ASCII non-alphanum character as UTF8 seperator
    static ALWAYS_INLINE inline bool isUTF8Sep(const UInt8 c) { return c < 128 && !isAlphaNumericASCII(c); }

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
