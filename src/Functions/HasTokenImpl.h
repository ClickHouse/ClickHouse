#pragma once

#include <Columns/ColumnString.h>
#include <Core/ColumnNumbers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/** Token search the string, means that needle must be surrounded by some separator chars, like whitespace or puctuation.
  */
template <typename Name, typename TokenSearcher, bool negate>
struct HasTokenImpl
{
    using ResultType = UInt8;

    static constexpr bool use_default_implementation_for_constants = true;
    static constexpr bool supports_start_pos = false;
    static constexpr auto name = Name::name;

    static ColumnNumbers getArgumentsThatAreAlwaysConstant() { return {1, 2};}

    static void vectorConstant(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const std::string & pattern,
        const ColumnPtr & start_pos,
        PaddedPODArray<UInt8> & res)
    {
        if (start_pos != nullptr)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function '{}' does not support start_pos argument", name);

        if (haystack_offsets.empty())
            return;

        const UInt8 * const begin = haystack_data.data();
        const UInt8 * const end = haystack_data.data() + haystack_data.size();
        const UInt8 * pos = begin;

        /// The current index in the array of strings.
        size_t i = 0;

        TokenSearcher searcher(pattern.data(), pattern.size(), end - pos);

        /// We will search for the next occurrence in all rows at once.
        while (pos < end && end != (pos = searcher.search(pos, end - pos)))
        {
            /// Let's determine which index it refers to.
            while (begin + haystack_offsets[i] <= pos)
            {
                res[i] = negate;
                ++i;
            }

            /// We check that the entry does not pass through the boundaries of strings.
            if (pos + pattern.size() < begin + haystack_offsets[i])
                res[i] = !negate;
            else
                res[i] = negate;

            pos = begin + haystack_offsets[i];
            ++i;
        }

        /// Tail, in which there can be no substring.
        if (i < res.size())
            memset(&res[i], negate, (res.size() - i) * sizeof(res[0]));
    }

    template <typename... Args>
    static void vectorVector(Args &&...)
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Function '{}' doesn't support non-constant needle argument", name);
    }

    /// Search different needles in single haystack.
    template <typename... Args>
    static void constantVector(Args &&...)
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Function '{}' doesn't support non-constant needle argument", name);
    }

    template <typename... Args>
    static void vectorFixedConstant(Args &&...)
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Function '{}' doesn't support FixedString haystack argument", name);
    }

    template <typename... Args>
    static void vectorFixedVector(Args &&...)
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Function '{}' doesn't support FixedString haystack argument", name);
    }
};

}
