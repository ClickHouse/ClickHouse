#pragma once

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Common/Volnitsky.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>


/** Functions for retrieving "visit parameters".
 * Visit parameters in Metrica web analytics system are a special kind of JSONs.
 * These functions are applicable to almost any JSONs.
 * Implemented via templates from FunctionsStringSearch.h.
 *
 * Check if there is a parameter
 *         visitParamHas
 *
 * Retrieve the numeric value of the parameter
 *         visitParamExtractUInt
 *         visitParamExtractInt
 *         visitParamExtractFloat
 *         visitParamExtractBool
 *
 * Retrieve the string value of the parameter
 *         visitParamExtractString - unescape value
 *         visitParamExtractRaw
 */

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


template <typename NumericType>
struct ExtractNumericType
{
    using ResultType = NumericType;

    static ResultType extract(const UInt8 * begin, const UInt8 * end)
    {
        ReadBufferFromMemory in(begin, end - begin);

        /// Read numbers in double quotes
        if (!in.eof() && *in.position() == '"')
            ++in.position();

        ResultType x = 0;
        if (!in.eof())
        {
            if constexpr (std::is_floating_point_v<NumericType>)
                tryReadFloatText(x, in);
            else
                tryReadIntText(x, in);
        }
        return x;
    }
};


/** Searches for occurrences of a field in the visit parameter and calls ParamExtractor
 * for each occurrence of the field, passing it a pointer to the part of the string,
 * where the occurrence of the field value begins.
 * ParamExtractor must parse and return the value of the desired type.
 *
 * If a field was not found or an incorrect value is associated with the field,
 * then the default value used - 0.
 */
template <typename Name, typename ParamExtractor>
struct ExtractParamImpl
{
    using ResultType = typename ParamExtractor::ResultType;

    static constexpr bool use_default_implementation_for_constants = true;
    static constexpr bool supports_start_pos = false;
    static constexpr auto name = Name::name;

    static ColumnNumbers getArgumentsThatAreAlwaysConstant() { return {1, 2};}

    /// It is assumed that `res` is the correct size and initialized with zeros.
    static void vectorConstant(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        std::string needle,
        const ColumnPtr & start_pos,
        PaddedPODArray<ResultType> & res,
        [[maybe_unused]] ColumnUInt8 * res_null,
        size_t /*input_rows_count*/)
    {
        /// `res_null` serves as an output parameter for implementing an XYZOrNull variant.
        assert(!res_null);

        if (start_pos != nullptr)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function '{}' doesn't support start_pos argument", name);

        /// We are looking for a parameter simply as a substring of the form "name"
        needle = "\"" + needle + "\":";

        const UInt8 * const begin = haystack_data.data();
        const UInt8 * const end = haystack_data.data() + haystack_data.size();
        const UInt8 * pos = begin;

        /// The current index in the string array.
        size_t i = 0;

        Volnitsky searcher(needle.data(), needle.size(), end - pos);

        /// We will search for the next occurrence in all strings at once.
        while (pos < end && end != (pos = searcher.search(pos, end - pos)))
        {
            /// Let's determine which index it belongs to.
            while (begin + haystack_offsets[i] <= pos)
            {
                res[i] = 0;
                ++i;
            }

            /// We check that the entry does not pass through the boundaries of strings.
            if (pos + needle.size() < begin + haystack_offsets[i])
                res[i] = ParamExtractor::extract(pos + needle.size(), begin + haystack_offsets[i] - 1);  /// don't include terminating zero
            else
                res[i] = 0;

            pos = begin + haystack_offsets[i];
            ++i;
        }

        if (res.size() > i)
            memset(&res[i], 0, (res.size() - i) * sizeof(res[0]));
    }

    template <typename... Args> static void vectorVector(Args &&...)
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Function '{}' doesn't support non-constant needle argument", name);
    }

    template <typename... Args> static void constantVector(Args &&...)
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


/** For the case where the type of field to extract is a string.
 */
template <typename ParamExtractor>
struct ExtractParamToStringImpl
{
    static void vector(const ColumnString::Chars & haystack_data, const ColumnString::Offsets & haystack_offsets,
                       std::string needle,
                       ColumnString::Chars & res_data, ColumnString::Offsets & res_offsets,
                       size_t input_rows_count)
    {
        /// Constant 5 is taken from a function that performs a similar task FunctionsStringSearch.h::ExtractImpl
        res_data.reserve(haystack_data.size() / 5);
        res_offsets.resize(input_rows_count);

        /// We are looking for a parameter simply as a substring of the form "name"
        needle = "\"" + needle + "\":";

        const UInt8 * const begin = haystack_data.data();
        const UInt8 * const end = haystack_data.data() + haystack_data.size();
        const UInt8 * pos = begin;

        /// The current index in the string array.
        size_t i = 0;

        Volnitsky searcher(needle.data(), needle.size(), end - pos);

        /// We will search for the next occurrence in all strings at once.
        while (pos < end && end != (pos = searcher.search(pos, end - pos)))
        {
            /// Determine which index it belongs to.
            while (begin + haystack_offsets[i] <= pos)
            {
                res_data.push_back(0);
                res_offsets[i] = res_data.size();
                ++i;
            }

            /// We check that the entry does not pass through the boundaries of strings.
            if (pos + needle.size() < begin + haystack_offsets[i])
                ParamExtractor::extract(pos + needle.size(), begin + haystack_offsets[i], res_data);

            pos = begin + haystack_offsets[i];

            res_data.push_back(0);
            res_offsets[i] = res_data.size();
            ++i;
        }

        while (i < res_offsets.size())
        {
            res_data.push_back(0);
            res_offsets[i] = res_data.size();
            ++i;
        }
    }
};

}
