#pragma once

#include <Columns/ColumnString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/** Token search the string, means that needle must be surrounded by some separator chars, like whitespace or puctuation.
  */
template <typename TokenSearcher, bool negate_result = false>
struct HasTokenImpl
{
    using ResultType = UInt8;

    static constexpr bool use_default_implementation_for_constants = true;
    static constexpr bool supports_start_pos = false;

    static void vectorConstant(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        const std::string & pattern,
        const ColumnPtr & start_pos,
        PaddedPODArray<UInt8> & res)
    {
        if (start_pos != nullptr)
            throw Exception("Function 'hasToken' does not support start_pos argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (offsets.empty())
            return;

        const UInt8 * begin = data.data();
        const UInt8 * pos = begin;
        const UInt8 * end = pos + data.size();

        /// The current index in the array of strings.
        size_t i = 0;

        TokenSearcher searcher(pattern.data(), pattern.size(), end - pos);

        /// We will search for the next occurrence in all rows at once.
        while (pos < end && end != (pos = searcher.search(pos, end - pos)))
        {
            /// Let's determine which index it refers to.
            while (begin + offsets[i] <= pos)
            {
                res[i] = negate_result;
                ++i;
            }

            /// We check that the entry does not pass through the boundaries of strings.
            if (pos + pattern.size() < begin + offsets[i])
                res[i] = !negate_result;
            else
                res[i] = negate_result;

            pos = begin + offsets[i];
            ++i;
        }

        /// Tail, in which there can be no substring.
        if (i < res.size())
            memset(&res[i], negate_result, (res.size() - i) * sizeof(res[0]));
    }

    template <typename... Args>
    static void vectorVector(Args &&...)
    {
        throw Exception("Function 'hasToken' does not support non-constant needle argument", ErrorCodes::ILLEGAL_COLUMN);
    }

    /// Search different needles in single haystack.
    template <typename... Args>
    static void constantVector(Args &&...)
    {
        throw Exception("Function 'hasToken' does not support non-constant needle argument", ErrorCodes::ILLEGAL_COLUMN);
    }

    template <typename... Args>
    static void vectorFixedConstant(Args &&...)
    {
        throw Exception("Functions 'hasToken' don't support FixedString haystack argument", ErrorCodes::ILLEGAL_COLUMN);
    }
};

}
