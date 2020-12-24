#pragma once
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

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

// used by FunctionsStringSimilarity and FunctionsStringHash
// includes extracting ASCII ngram, UTF8 ngram, ASCII word and UTF8 word
template <bool CaseInsensitive>
struct ExtractStringImpl
{
    /// Padding form ColumnsString. It is a number of bytes we can always read starting from pos if pos < end.
    static constexpr size_t default_padding = 16;

    const size_t shingle_size;
    const size_t tail_size;

    /// Functions are read `default_padding - (N - 1)` bytes into the buffer. Window of size N is used.
    /// Read copies `N - 1` last bytes from buffer into beginning, and then reads new bytes.
    const size_t buffer_size = default_padding + tail_size;

    explicit ExtractStringImpl(size_t shingle_size_)
            : shingle_size(shingle_size_)
            , tail_size(shingle_size > default_padding ? shingle_size : roundUpToPowerOfTwoOrZero(shingle_size - 1))
    {
        if (shingle_size == 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Shingle size can't be zero");
    }

    // read a ASCII word
    static ALWAYS_INLINE inline size_t readOneASCIIWord(PaddedPODArray<UInt8> & word_buf, const char *& pos, const char * end)
    {
        // jump separators
        while (pos < end && !isAlphaNumericASCII(*pos))
            ++pos;

        // word start from here
        const char * word_start = pos;
        while (pos < end && isAlphaNumericASCII(*pos))
            ++pos;

        word_buf.assign(word_start, pos);
        if (CaseInsensitive)
        {
            for (auto & symbol : word_buf)
                symbol = toLowerIfAlphaASCII(symbol);
        }
        return word_buf.size();
    }

    // read one UTF8 word from pos to word
    static ALWAYS_INLINE inline size_t readOneUTF8Word(PaddedPODArray<UInt32> & word_buf, const char *& pos, const char * end)
    {
        // jump UTF8 separator
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

    // we use ASCII non-alphanum character as UTF8 separator
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
