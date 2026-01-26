#pragma once

#include <Columns/ColumnString.h>
#include <Common/memcpySmall.h>


namespace DB
{

/** These helpers are used by URL processing functions. See implementation in separate .cpp files.
  * All functions do not strictly follow RFC, instead they are maximally simplified for performance reasons.
  *
  * Functions for extraction parts of URL.
  * If URL has nothing like, then empty string is returned.
  *
  *  domain
  *  domainWithoutWWW
  *  topLevelDomain
  *  protocol
  *  path
  *  queryString
  *  fragment
  *  queryStringAndFragment
  *  netloc
  *
  * Functions, removing parts from URL.
  * If URL has nothing like, then it is returned unchanged.
  *
  *  cutWWW
  *  cutFragment
  *  cutQueryString
  *  cutQueryStringAndFragment
  *
  * Extract value of parameter in query string or in fragment identifier. Return empty string, if URL has no such parameter.
  * If there are many parameters with same name - return value of first one. Value is not %-decoded.
  *
  *  extractURLParameter(URL, name)
  *
  * Extract all parameters from URL in form of array of strings name=value.
  *  extractURLParameters(URL)
  *
  * Extract names of all parameters from URL in form of array of strings.
  *  extractURLParameterNames(URL)
  *
  * Remove specified parameter from URL.
  *  cutURLParameter(URL, name)
  *
  * Get array of URL 'hierarchy' as in web-analytics tree-like reports. See the docs.
  *  URLHierarchy(URL)
  */

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

using Pos = const char *;


/** Select part of string using the Extractor.
  */
template <typename Extractor>
struct ExtractSubstringImpl
{
    static void vector(
        const ColumnString::Chars & data, const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data, ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        res_offsets.resize(input_rows_count);
        res_data.reserve(input_rows_count * Extractor::getReserveLengthForElement());

        size_t prev_offset = 0;
        size_t res_offset = 0;

        /// Matched part.
        Pos start;
        size_t length;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            Extractor::execute(reinterpret_cast<const char *>(&data[prev_offset]), offsets[i] - prev_offset - 1, start, length);

            res_data.resize(res_data.size() + length + 1);
            memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], start, length);
            res_offset += length + 1;
            res_data[res_offset - 1] = 0;

            res_offsets[i] = res_offset;
            prev_offset = offsets[i];
        }
    }

    static void constant(const std::string & data,
        std::string & res_data)
    {
        Pos start;
        size_t length;
        Extractor::execute(data.data(), data.size(), start, length);
        res_data.assign(start, length);
    }

    static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &, size_t)
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Column of type FixedString is not supported by this function");
    }
};


/** Delete part of string using the Extractor.
  */
template <typename Extractor>
struct CutSubstringImpl
{
    static void vector(
        const ColumnString::Chars & data, const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data, ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        res_data.reserve(data.size());
        res_offsets.resize(input_rows_count);

        size_t prev_offset = 0;
        size_t res_offset = 0;

        /// Matched part.
        Pos start;
        size_t length;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const char * current = reinterpret_cast<const char *>(&data[prev_offset]);
            Extractor::execute(current, offsets[i] - prev_offset - 1, start, length);
            size_t start_index = start - reinterpret_cast<const char *>(data.data());

            res_data.resize(res_data.size() + offsets[i] - prev_offset - length);
            memcpySmallAllowReadWriteOverflow15(
                &res_data[res_offset], current, start - current);
            memcpySmallAllowReadWriteOverflow15(
                &res_data[res_offset + start - current], start + length, offsets[i] - start_index - length);
            res_offset += offsets[i] - prev_offset - length;

            res_offsets[i] = res_offset;
            prev_offset = offsets[i];
        }
    }

    static void constant(const std::string & data,
        std::string & res_data)
    {
        Pos start;
        size_t length;
        Extractor::execute(data.data(), data.size(), start, length);
        res_data.reserve(data.size() - length);
        res_data.append(data.data(), start);
        res_data.append(start + length, data.data() + data.size());
    }

    static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &, size_t)
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Column of type FixedString is not supported by this function");
    }
};

}
