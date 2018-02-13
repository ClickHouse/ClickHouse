#pragma once

#include <Poco/UTF8Encoding.h>
#include <Poco/Unicode.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Common/hex.h>
#include <Common/Volnitsky.h>
#include <Functions/IFunction.h>
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

struct HasParam
{
    using ResultType = UInt8;

    static UInt8 extract(const UInt8 *, const UInt8 *)
    {
        return true;
    }
};

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

struct ExtractBool
{
    using ResultType = UInt8;

    static UInt8 extract(const UInt8 * begin, const UInt8 * end)
    {
        return begin + 4 <= end && 0 == strncmp(reinterpret_cast<const char *>(begin), "true", 4);
    }
};


struct ExtractRaw
{
    static void extract(const UInt8 * pos, const UInt8 * end, ColumnString::Chars_t & res_data)
    {
        if (pos == end)
            return;

        UInt8 open_char = *pos;
        UInt8 close_char = 0;
        switch (open_char)
        {
            case '[':
                close_char = ']';
                break;
            case '{':
                close_char = '}';
                break;
            case '"':
                close_char = '"';
                break;
        }

        if (close_char != 0)
        {
            size_t balance = 1;
            char last_char = 0;

            res_data.push_back(*pos);

            ++pos;
            for (; pos != end && balance > 0; ++pos)
            {
                res_data.push_back(*pos);

                if (open_char == '"' && *pos == '"')
                {
                    if (last_char != '\\')
                        break;
                }
                else
                {
                    if (*pos == open_char)
                        ++balance;
                    if (*pos == close_char)
                        --balance;
                }

                if (last_char == '\\')
                    last_char = 0;
                else
                    last_char = *pos;
            }
        }
        else
        {
            for (; pos != end && *pos != ',' && *pos != '}'; ++pos)
                res_data.push_back(*pos);
        }
    }
};

struct ExtractString
{
    static void extract(const UInt8 * pos, const UInt8 * end, ColumnString::Chars_t & res_data)
    {
        size_t old_size = res_data.size();
        ReadBufferFromMemory in(pos, end - pos);
        if (!tryReadJSONStringInto(res_data, in))
            res_data.resize(old_size);
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

    /// It is assumed that `res` is the correct size and initialized with zeros.
    static void vector_constant(const ColumnString::Chars_t & data, const ColumnString::Offsets & offsets,
        std::string needle,
        PaddedPODArray<ResultType> & res)
    {
        /// We are looking for a parameter simply as a substring of the form "name"
        needle = "\"" + needle + "\":";

        const UInt8 * begin = &data[0];
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
                res[i] = ParamExtractor::extract(pos + needle.size(), begin + offsets[i]);
            else
                res[i] = 0;

            pos = begin + offsets[i];
            ++i;
        }

        memset(&res[i], 0, (res.size() - i) * sizeof(res[0]));
    }

    static void constant_constant(const std::string & data, std::string needle, ResultType & res)
    {
        needle = "\"" + needle + "\":";
        size_t pos = data.find(needle);
        if (pos == std::string::npos)
            res = 0;
        else
            res = ParamExtractor::extract(
                reinterpret_cast<const UInt8 *>(data.data() + pos + needle.size()),
                reinterpret_cast<const UInt8 *>(data.data() + data.size())
            );
    }

    template <typename... Args> static void vector_vector(Args &&...)
    {
        throw Exception("Functions 'visitParamHas' and 'visitParamExtract*' doesn't support non-constant needle argument", ErrorCodes::ILLEGAL_COLUMN);
    }

    template <typename... Args> static void constant_vector(Args &&...)
    {
        throw Exception("Functions 'visitParamHas' and 'visitParamExtract*' doesn't support non-constant needle argument", ErrorCodes::ILLEGAL_COLUMN);
    }
};


/** For the case where the type of field to extract is a string.
 */
template <typename ParamExtractor>
struct ExtractParamToStringImpl
{
    static void vector(const ColumnString::Chars_t & data, const ColumnString::Offsets & offsets,
                       std::string needle,
                       ColumnString::Chars_t & res_data, ColumnString::Offsets & res_offsets)
    {
        /// Constant 5 is taken from a function that performs a similar task FunctionsStringSearch.h::ExtractImpl
        res_data.reserve(data.size()  / 5);
        res_offsets.resize(offsets.size());

        /// We are looking for a parameter simply as a substring of the form "name"
        needle = "\"" + needle + "\":";

        const UInt8 * begin = &data[0];
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
