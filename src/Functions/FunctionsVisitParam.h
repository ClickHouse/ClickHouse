#pragma once

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Columns/ColumnString.h>
#include <Common/Volnitsky.h>
#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionHelpers.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>


/** Functions for retrieving "visit parameters".
 * Visit parameters in Yandex.Metrika are a special kind of JSONs.
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
template <typename ParamExtractor>
struct ExtractParamImpl
{
    using ResultType = typename ParamExtractor::ResultType;

    static constexpr bool use_default_implementation_for_constants = true;

    /// It is assumed that `res` is the correct size and initialized with zeros.
    static void vectorConstant(const ColumnString::Chars & data, const ColumnString::Offsets & offsets,
        std::string needle,
        PaddedPODArray<ResultType> & res)
    {
        /// We are looking for a parameter simply as a substring of the form "name"
        needle = "\"" + needle + "\":";

        const UInt8 * begin = data.data();
        const UInt8 * pos = begin;
        const UInt8 * end = pos + data.size();

        /// The current index in the string array.
        size_t i = 0;

        Volnitsky searcher(needle.data(), needle.size(), end - pos);

        /// We will search for the next occurrence in all strings at once.
        while (pos < end && end != (pos = searcher.search(pos, end - pos)))
        {
            /// Let's determine which index it belongs to.
            while (begin + offsets[i] <= pos)
            {
                res[i] = 0;
                ++i;
            }

            /// We check that the entry does not pass through the boundaries of strings.
            if (pos + needle.size() < begin + offsets[i])
                res[i] = ParamExtractor::extract(pos + needle.size(), begin + offsets[i] - 1);  /// don't include terminating zero
            else
                res[i] = 0;

            pos = begin + offsets[i];
            ++i;
        }

        if (res.size() > i)
            memset(&res[i], 0, (res.size() - i) * sizeof(res[0]));
    }

    template <typename... Args> static void vectorVector(Args &&...)
    {
        throw Exception("Functions 'visitParamHas' and 'visitParamExtract*' doesn't support non-constant needle argument", ErrorCodes::ILLEGAL_COLUMN);
    }

    template <typename... Args> static void constantVector(Args &&...)
    {
        throw Exception("Functions 'visitParamHas' and 'visitParamExtract*' doesn't support non-constant needle argument", ErrorCodes::ILLEGAL_COLUMN);
    }

    template <typename... Args>
    static void vectorFixedConstant(Args &&...)
    {
        throw Exception("Functions 'visitParamHas' don't support FixedString haystack argument", ErrorCodes::ILLEGAL_COLUMN);
    }
};


/** For the case where the type of field to extract is a string.
 */
template <typename ParamExtractor>
struct ExtractParamToStringImpl
{
    static void vector(const ColumnString::Chars & data, const ColumnString::Offsets & offsets,
                       std::string needle,
                       ColumnString::Chars & res_data, ColumnString::Offsets & res_offsets)
    {
        /// Constant 5 is taken from a function that performs a similar task FunctionsStringSearch.h::ExtractImpl
        res_data.reserve(data.size() / 5);
        res_offsets.resize(offsets.size());

        /// We are looking for a parameter simply as a substring of the form "name"
        needle = "\"" + needle + "\":";

        const UInt8 * begin = data.data();
        const UInt8 * pos = begin;
        const UInt8 * end = pos + data.size();

        /// The current index in the string array.
        size_t i = 0;

        Volnitsky searcher(needle.data(), needle.size(), end - pos);

        /// We will search for the next occurrence in all strings at once.
        while (pos < end && end != (pos = searcher.search(pos, end - pos)))
        {
            /// Determine which index it belongs to.
            while (begin + offsets[i] <= pos)
            {
                res_data.push_back(0);
                res_offsets[i] = res_data.size();
                ++i;
            }

            /// We check that the entry does not pass through the boundaries of strings.
            if (pos + needle.size() < begin + offsets[i])
                ParamExtractor::extract(pos + needle.size(), begin + offsets[i], res_data);

            pos = begin + offsets[i];

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
