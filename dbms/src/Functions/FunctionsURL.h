#pragma once

#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/typeid_cast.h>
#include <common/find_symbols.h>
#include <common/StringRef.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/FunctionsStringArray.h>
#include <cstring>


namespace DB
{

/** URL processing functions.
  * All functions are not strictly follow RFC, instead they are maximally simplified for performance reasons.
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
  *
  * Functions, removing parts from URL.
  * If URL has nothing like, then it is retured unchanged.
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
  * Get array of URL 'hierarchy' as in Yandex.Metrica tree-like reports. See docs.
  *  URLHierarchy(URL)
  */

using Pos = const char *;


/// Extracts scheme from given url.
inline StringRef getURLScheme(const char * data, size_t size)
{
    // scheme = ALPHA *( ALPHA / DIGIT / "+" / "-" / "." )
    const char * pos = data;
    const char * end = data + size;

    if (isAlphaASCII(*pos))
    {
        for (++pos; pos < end; ++pos)
        {
            if (!(isAlphaNumericASCII(*pos) || *pos == '+' || *pos == '-' || *pos == '.'))
            {
                break;
            }
        }

        return StringRef(data, pos - data);
    }

    return {};
}


/// Extracts host from given url.
inline StringRef getURLHost(const char * data, size_t size)
{
    Pos pos = data;
    Pos end = data + size;

    if (end == (pos = find_first_symbols<'/'>(pos, end)))
        return {};

    if (pos != data)
    {
        StringRef scheme = getURLScheme(data, size);
        Pos scheme_end = data + scheme.size;

        // Colon must follows after scheme.
        if (pos - scheme_end != 1 || *scheme_end != ':')
            return {};
    }

    if (end - pos < 2 || *(pos) != '/' || *(pos + 1) != '/')
        return {};
    pos += 2;

    const char * start_of_host = pos;
    for (; pos < end; ++pos)
    {
        if (*pos == '@')
            start_of_host = pos + 1;
        else if (*pos == ':' || *pos == '/' || *pos == '?' || *pos == '#')
            break;
    }

    return (pos == start_of_host) ? StringRef{} : StringRef(start_of_host, pos - start_of_host);
}


struct ExtractProtocol
{
    static size_t getReserveLengthForElement();

    static void execute(Pos data, size_t size, Pos & res_data, size_t & res_size);
};

template <bool without_www>
struct ExtractDomain
{
    static size_t getReserveLengthForElement() { return 15; }

    static void execute(Pos data, size_t size, Pos & res_data, size_t & res_size)
    {
        StringRef host = getURLHost(data, size);

        if (host.size == 0)
        {
            res_data = data;
            res_size = 0;
        }
        else
        {
            if (without_www && host.size > 4 && !strncmp(host.data, "www.", 4))
                host = { host.data + 4, host.size - 4 };

            res_data = host.data;
            res_size = host.size;
        }
    }
};

struct ExtractFirstSignificantSubdomain
{
    static size_t getReserveLengthForElement() { return 10; }

    static void execute(const Pos data, const size_t size, Pos & res_data, size_t & res_size, Pos * out_domain_end = nullptr)
    {
        res_data = data;
        res_size = 0;

        Pos tmp;
        size_t domain_length;
        ExtractDomain<true>::execute(data, size, tmp, domain_length);

        if (domain_length == 0)
            return;

        if (out_domain_end)
            *out_domain_end = tmp + domain_length;

        /// cut useless dot
        if (tmp[domain_length - 1] == '.')
            --domain_length;

        res_data = tmp;
        res_size = domain_length;

        auto begin = tmp;
        auto end = begin + domain_length;
        const char * last_3_periods[3]{};

        auto pos = find_first_symbols<'.'>(begin, end);
        while (pos < end)
        {
            last_3_periods[2] = last_3_periods[1];
            last_3_periods[1] = last_3_periods[0];
            last_3_periods[0] = pos;
            pos = find_first_symbols<'.'>(pos + 1, end);
        }

        if (!last_3_periods[0])
            return;

        if (!last_3_periods[1])
        {
            res_size = last_3_periods[0] - begin;
            return;
        }

        if (!last_3_periods[2])
            last_3_periods[2] = begin - 1;

        size_t size_of_second_subdomain_plus_period = last_3_periods[0] - last_3_periods[1];
        if (size_of_second_subdomain_plus_period == 4 || size_of_second_subdomain_plus_period == 3)
        {
            /// We will key by four bytes that are either ".xyz" or ".xy.".
            UInt32 key = unalignedLoad<UInt32>(last_3_periods[1]);

            /// NOTE: assuming little endian.
            /// NOTE: does the compiler generate SIMD code?
            /// NOTE: for larger amount of cases we can use a perfect hash table (see 'gperf' as an example).
            if (   key == '.' + 'c' * 0x100U + 'o' * 0x10000U + 'm' * 0x1000000U
                || key == '.' + 'n' * 0x100U + 'e' * 0x10000U + 't' * 0x1000000U
                || key == '.' + 'o' * 0x100U + 'r' * 0x10000U + 'g' * 0x1000000U
                || key == '.' + 'b' * 0x100U + 'i' * 0x10000U + 'z' * 0x1000000U
                || key == '.' + 'g' * 0x100U + 'o' * 0x10000U + 'v' * 0x1000000U
                || key == '.' + 'm' * 0x100U + 'i' * 0x10000U + 'l' * 0x1000000U
                || key == '.' + 'e' * 0x100U + 'd' * 0x10000U + 'u' * 0x1000000U
                || key == '.' + 'c' * 0x100U + 'o' * 0x10000U + '.' * 0x1000000U)
            {
                res_data += last_3_periods[2] + 1 - begin;
                res_size = last_3_periods[1] - last_3_periods[2] - 1;
                return;
            }
        }

        res_data += last_3_periods[1] + 1 - begin;
        res_size = last_3_periods[0] - last_3_periods[1] - 1;
    }
};

struct CutToFirstSignificantSubdomain
{
    static size_t getReserveLengthForElement() { return 15; }

    static void execute(const Pos data, const size_t size, Pos & res_data, size_t & res_size)
    {
        res_data = data;
        res_size = 0;

        Pos tmp_data;
        size_t tmp_length;
        Pos domain_end;
        ExtractFirstSignificantSubdomain::execute(data, size, tmp_data, tmp_length, &domain_end);

        if (tmp_length == 0)
            return;

        res_data = tmp_data;
        res_size = domain_end - tmp_data;
    }
};

struct ExtractTopLevelDomain
{
    static size_t getReserveLengthForElement() { return 5; }

    static void execute(Pos data, size_t size, Pos & res_data, size_t & res_size)
    {
        StringRef host = getURLHost(data, size);

        res_data = data;
        res_size = 0;

        if (host.size != 0)
        {
            if (host.data[host.size - 1] == '.')
                host.size -= 1;

            auto host_end = host.data + host.size;

            Pos last_dot = find_last_symbols_or_null<'.'>(host.data, host_end);
            if (!last_dot)
                return;

            /// For IPv4 addresses select nothing.
            if (last_dot[1] <= '9')
                return;

            res_data = last_dot + 1;
            res_size = host_end - res_data;
        }
    }
};

struct ExtractPath
{
    static size_t getReserveLengthForElement() { return 25; }

    static void execute(Pos data, size_t size, Pos & res_data, size_t & res_size)
    {
        res_data = data;
        res_size = 0;

        Pos pos = data;
        Pos end = pos + size;

        if (end != (pos = find_first_symbols<'/'>(pos, end)) && pos[1] == '/' && end != (pos = find_first_symbols<'/'>(pos + 2, end)))
        {
            Pos query_string_or_fragment = find_first_symbols<'?', '#'>(pos, end);

            res_data = pos;
            res_size = query_string_or_fragment - res_data;
        }
    }
};

struct ExtractPathFull
{
    static size_t getReserveLengthForElement() { return 30; }

    static void execute(const Pos data, const size_t size, Pos & res_data, size_t & res_size)
    {
        res_data = data;
        res_size = 0;

        Pos pos = data;
        Pos end = pos + size;

        if (end != (pos = find_first_symbols<'/'>(pos, end)) && pos[1] == '/' && end != (pos = find_first_symbols<'/'>(pos + 2, end)))
        {
            res_data = pos;
            res_size = end - res_data;
        }
    }
};

template <bool without_leading_char>
struct ExtractQueryString
{
    static size_t getReserveLengthForElement() { return 10; }

    static void execute(Pos data, size_t size, Pos & res_data, size_t & res_size)
    {
        res_data = data;
        res_size = 0;

        Pos pos = data;
        Pos end = pos + size;

        if (end != (pos = find_first_symbols<'?'>(pos, end)))
        {
            Pos fragment = find_first_symbols<'#'>(pos, end);

            res_data = pos + (without_leading_char ? 1 : 0);
            res_size = fragment - res_data;
        }
    }
};

template <bool without_leading_char>
struct ExtractFragment
{
    static size_t getReserveLengthForElement() { return 10; }

    static void execute(Pos data, size_t size, Pos & res_data, size_t & res_size)
    {
        res_data = data;
        res_size = 0;

        Pos pos = data;
        Pos end = pos + size;

        if (end != (pos = find_first_symbols<'#'>(pos, end)))
        {
            res_data = pos + (without_leading_char ? 1 : 0);
            res_size = end - res_data;
        }
    }
};

template <bool without_leading_char>
struct ExtractQueryStringAndFragment
{
    static size_t getReserveLengthForElement() { return 20; }

    static void execute(Pos data, size_t size, Pos & res_data, size_t & res_size)
    {
        res_data = data;
        res_size = 0;

        Pos pos = data;
        Pos end = pos + size;

        if (end != (pos = find_first_symbols<'?'>(pos, end)))
        {
            res_data = pos + (without_leading_char ? 1 : 0);
            res_size = end - res_data;
        }
        else if (end != (pos = find_first_symbols<'#'>(pos, end)))
        {
            res_data = pos;
            res_size = end - res_data;
        }
    }
};

/// With dot at the end.
struct ExtractWWW
{
    static void execute(Pos data, size_t size, Pos & res_data, size_t & res_size)
    {
        res_data = data;
        res_size = 0;

        Pos pos = data;
        Pos end = pos + size;

        if (end != (pos = find_first_symbols<'/'>(pos, end)))
        {
            if (pos != data)
            {
                Pos tmp;
                size_t protocol_length;
                ExtractProtocol::execute(data, size, tmp, protocol_length);

                if (pos != data + protocol_length + 1)
                    return;
            }

            if (end - pos < 2 || *(pos) != '/' || *(pos + 1) != '/')
                return;

            const char *start_of_host = (pos += 2);
            for (; pos < end; ++pos)
            {
                if (*pos == '@')
                    start_of_host = pos + 1;
                else if (*pos == ':' || *pos == '/' || *pos == '?' || *pos == '#')
                    break;
            }

            if (start_of_host + 4 < end && !strncmp(start_of_host, "www.", 4))
            {
                res_data = start_of_host;
                res_size = 4;
            }
        }
    }
};


struct ExtractURLParameterImpl
{
    static void vector(const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        std::string pattern,
        ColumnString::Chars & res_data, ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(data.size() / 5);
        res_offsets.resize(offsets.size());

        pattern += '=';
        const char * param_str = pattern.c_str();
        size_t param_len = pattern.size();

        ColumnString::Offset prev_offset = 0;
        ColumnString::Offset res_offset = 0;

        for (size_t i = 0; i < offsets.size(); ++i)
        {
            ColumnString::Offset cur_offset = offsets[i];

            const char * str = reinterpret_cast<const char *>(&data[prev_offset]);
            const char * end = reinterpret_cast<const char *>(&data[cur_offset]);

            /// Find query string or fragment identifier.
            /// Note that we support parameters in fragment identifier in the same way as in query string.

            const char * const query_string_begin = find_first_symbols<'?', '#'>(str, end);

            /// Will point to the beginning of "name=value" pair. Then it will be reassigned to the beginning of "value".
            const char * param_begin = nullptr;

            if (query_string_begin + 1 < end)
            {
                param_begin = query_string_begin + 1;

                while (true)
                {
                    param_begin = static_cast<const char *>(memmem(param_begin, end - param_begin, param_str, param_len));

                    if (!param_begin)
                        break;

                    if (param_begin[-1] != '?' && param_begin[-1] != '#' && param_begin[-1] != '&')
                    {
                        /// Parameter name is different but has the same suffix.
                        param_begin += param_len;
                        continue;
                    }
                    else
                    {
                        param_begin += param_len;
                        break;
                    }
                }
            }

            if (param_begin)
            {
                const char * param_end = find_first_symbols<'&', '#'>(param_begin, end);
                if (param_end == end)
                    param_end = param_begin + strlen(param_begin);

                size_t param_size = param_end - param_begin;

                res_data.resize(res_offset + param_size + 1);
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], param_begin, param_size);
                res_offset += param_size;
            }
            else
            {
                /// No parameter found, put empty string in result.
                res_data.resize(res_offset + 1);
            }

            res_data[res_offset] = 0;
            ++res_offset;
            res_offsets[i] = res_offset;

            prev_offset = cur_offset;
        }
    }
};


struct CutURLParameterImpl
{
    static void vector(const ColumnString::Chars & data,
                        const ColumnString::Offsets & offsets,
                        std::string pattern,
                        ColumnString::Chars & res_data, ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(data.size());
        res_offsets.resize(offsets.size());

        pattern += '=';
        const char * param_str = pattern.c_str();
        size_t param_len = pattern.size();

        size_t prev_offset = 0;
        size_t res_offset = 0;

        for (size_t i = 0; i < offsets.size(); ++i)
        {
            size_t cur_offset = offsets[i];

            const char * url_begin = reinterpret_cast<const char *>(&data[prev_offset]);
            const char * url_end = reinterpret_cast<const char *>(&data[cur_offset]) - 1;
            const char * begin_pos = url_begin;
            const char * end_pos = begin_pos;

            do
            {
                const char * query_string_begin = find_first_symbols<'?', '#'>(url_begin, url_end);
                if (query_string_begin + 1 >= url_end)
                    break;

                const char * pos = static_cast<const char *>(memmem(query_string_begin + 1, url_end - query_string_begin - 1, param_str, param_len));
                if (pos == nullptr)
                    break;

                if (pos[-1] != '?' && pos[-1] != '#' && pos[-1] != '&')
                {
                    pos = nullptr;
                    break;
                }

                begin_pos = pos;
                end_pos = begin_pos + param_len;

                /// Skip the value.
                while (*end_pos && *end_pos != '&' && *end_pos != '#')
                    ++end_pos;

                /// Capture '&' before or after the parameter.
                if (*end_pos == '&')
                    ++end_pos;
                else if (begin_pos[-1] == '&')
                    --begin_pos;
            } while (false);

            size_t cut_length = (url_end - url_begin) - (end_pos - begin_pos);
            res_data.resize(res_offset + cut_length + 1);
            memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], url_begin, begin_pos - url_begin);
            memcpySmallAllowReadWriteOverflow15(&res_data[res_offset] + (begin_pos - url_begin), end_pos, url_end - end_pos);
            res_offset += cut_length + 1;
            res_data[res_offset - 1] = 0;
            res_offsets[i] = res_offset;

            prev_offset = cur_offset;
        }
    }
};


class ExtractURLParametersImpl
{
private:
    Pos pos;
    Pos end;
    bool first;

public:
    static constexpr auto name = "extractURLParameters";
    static String getName() { return name; }

    static size_t getNumberOfArguments() { return 1; }

    static void checkArguments(const DataTypes & arguments)
    {
        if (!isString(arguments[0]))
            throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName() + ". Must be String.",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    void init(Block & /*block*/, const ColumnNumbers & /*arguments*/) {}

    /// Returns the position of the argument that is the column of rows
    size_t getStringsArgumentPosition()
    {
        return 0;
    }

    /// Called for each next string.
    void set(Pos pos_, Pos end_)
    {
        pos = pos_;
        end = end_;
        first = true;
    }

    /// Get the next token, if any, or return false.
    bool get(Pos & token_begin, Pos & token_end)
    {
        if (pos == nullptr)
            return false;

        if (first)
        {
            first = false;
            pos = find_first_symbols<'?', '#'>(pos, end);
            if (pos + 1 >= end)
                return false;
            ++pos;
        }

        while (true)
        {
            token_begin = pos;
            pos = find_first_symbols<'=', '&', '#', '?'>(pos, end);
            if (pos == end)
                return false;

            if (*pos == '?')
            {
                ++pos;
                continue;
            }

            break;
        }

        if (*pos == '&' || *pos == '#')
        {
            token_end = pos++;
        }
        else
        {
            ++pos;
            pos = find_first_symbols<'&', '#'>(pos, end);
            if (pos == end)
                token_end = end;
            else
                token_end = pos++;
        }

        return true;
    }
};

class ExtractURLParameterNamesImpl
{
private:
    Pos pos;
    Pos end;
    bool first;

public:
    static constexpr auto name = "extractURLParameterNames";
    static String getName() { return name; }

    static size_t getNumberOfArguments() { return 1; }

    static void checkArguments(const DataTypes & arguments)
    {
        if (!isString(arguments[0]))
            throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName() + ". Must be String.",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    /// Returns the position of the argument that is the column of rows
    size_t getStringsArgumentPosition()
    {
        return 0;
    }

    void init(Block & /*block*/, const ColumnNumbers & /*arguments*/) {}

    /// Called for each next string.
    void set(Pos pos_, Pos end_)
    {
        pos = pos_;
        end = end_;
        first = true;
    }

    /// Get the next token, if any, or return false.
    bool get(Pos & token_begin, Pos & token_end)
    {
        if (pos == nullptr)
            return false;

        if (first)
        {
            first = false;
            pos = find_first_symbols<'?', '#'>(pos, end);
        }
        else
            pos = find_first_symbols<'&', '#'>(pos, end);

        if (pos + 1 >= end)
            return false;
        ++pos;

        while (true)
        {
            token_begin = pos;

            pos = find_first_symbols<'=', '&', '#', '?'>(pos, end);
            if (pos == end)
                return false;
            else
                token_end = pos;

            if (*pos == '?')
            {
                ++pos;
                continue;
            }

            break;
        }

        return true;
    }
};

class URLHierarchyImpl
{
private:
    Pos begin;
    Pos pos;
    Pos end;

public:
    static constexpr auto name = "URLHierarchy";
    static String getName() { return name; }

    static size_t getNumberOfArguments() { return 1; }

    static void checkArguments(const DataTypes & arguments)
    {
        if (!isString(arguments[0]))
            throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName() + ". Must be String.",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    void init(Block & /*block*/, const ColumnNumbers & /*arguments*/) {}

    /// Returns the position of the argument that is the column of rows
    size_t getStringsArgumentPosition()
    {
        return 0;
    }

    /// Called for each next string.
    void set(Pos pos_, Pos end_)
    {
        begin = pos = pos_;
        end = end_;
    }

    /// Get the next token, if any, or return false.
    bool get(Pos & token_begin, Pos & token_end)
    {
        /// Code from URLParser.
        if (pos == end)
            return false;

        if (pos == begin)
        {
            /// Let's parse everything that goes before the path

            /// Assume that the protocol has already been changed to lowercase.
            while (pos < end && ((*pos > 'a' && *pos < 'z') || (*pos > '0' && *pos < '9')))
                ++pos;

            /** We will calculate the hierarchy only for URLs in which there is a protocol, and after it there are two slashes.
             * (http, file - fit, mailto, magnet - do not fit), and after two slashes still at least something is there
             * For the rest, simply return the full URL as the only element of the hierarchy.
             */
            if (pos == begin || pos == end || !(*pos++ == ':' && pos < end && *pos++ == '/' && pos < end && *pos++ == '/' && pos < end))
            {
                pos = end;
                token_begin = begin;
                token_end = end;
                return true;
            }

            /// The domain for simplicity is everything that after the protocol and two slashes, until the next slash or `?` or `#`
            while (pos < end && !(*pos == '/' || *pos == '?' || *pos == '#'))
                ++pos;

            if (pos != end)
                ++pos;

            token_begin = begin;
            token_end = pos;

            return true;
        }

        /// We go to the next `/` or `?` or `#`, skipping all those at the beginning.
        while (pos < end && (*pos == '/' || *pos == '?' || *pos == '#'))
            ++pos;
        if (pos == end)
            return false;
        while (pos < end && !(*pos == '/' || *pos == '?' || *pos == '#'))
            ++pos;

        if (pos != end)
            ++pos;

        token_begin = begin;
        token_end = pos;

        return true;
    }
};


class URLPathHierarchyImpl
{
private:
    Pos begin;
    Pos pos;
    Pos end;
    Pos start;

public:
    static constexpr auto name = "URLPathHierarchy";
    static String getName() { return name; }

    static size_t getNumberOfArguments() { return 1; }

    static void checkArguments(const DataTypes & arguments)
    {
        if (!isString(arguments[0]))
            throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName() + ". Must be String.",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    void init(Block & /*block*/, const ColumnNumbers & /*arguments*/) {}

    /// Returns the position of the argument that is the column of rows
    size_t getStringsArgumentPosition()
    {
        return 0;
    }

    /// Called for each next string.
    void set(Pos pos_, Pos end_)
    {
        begin = pos = pos_;
        start = begin;
        end = end_;
    }

    /// Get the next token, if any, or return false.
    bool get(Pos & token_begin, Pos & token_end)
    {
        /// Code from URLParser.

        if (pos == end)
            return false;

        if (pos == begin)
        {
            /// Let's parse everything that goes before the path

            /// Assume that the protocol has already been changed to lowercase.
            while (pos < end && ((*pos > 'a' && *pos < 'z') || (*pos > '0' && *pos < '9')))
                ++pos;

            /** We will calculate the hierarchy only for URLs in which there is a protocol, and after it there are two slashes.
             * (http, file - fit, mailto, magnet - do not fit), and after two slashes still at least something is there.
             * For the rest, just return an empty array.
             */
            if (pos == begin || pos == end || !(*pos++ == ':' && pos < end && *pos++ == '/' && pos < end && *pos++ == '/' && pos < end))
            {
                pos = end;
                return false;
            }

            /// The domain for simplicity is everything that after the protocol and the two slashes, until the next slash or `?` or `#`
            while (pos < end && !(*pos == '/' || *pos == '?' || *pos == '#'))
                ++pos;

            start = pos;

            if (pos != end)
                ++pos;
        }

        /// We go to the next `/` or `?` or `#`, skipping all those at the beginning.
        while (pos < end && (*pos == '/' || *pos == '?' || *pos == '#'))
            ++pos;
        if (pos == end)
            return false;
        while (pos < end && !(*pos == '/' || *pos == '?' || *pos == '#'))
            ++pos;

        if (pos != end)
            ++pos;

        token_begin = start;
        token_end = pos;

        return true;
    }
};


/** Select part of string using the Extractor.
  */
template <typename Extractor>
struct ExtractSubstringImpl
{
    static void vector(const ColumnString::Chars & data, const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data, ColumnString::Offsets & res_offsets)
    {
        size_t size = offsets.size();
        res_offsets.resize(size);
        res_data.reserve(size * Extractor::getReserveLengthForElement());

        size_t prev_offset = 0;
        size_t res_offset = 0;

        /// Matched part.
        Pos start;
        size_t length;

        for (size_t i = 0; i < size; ++i)
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

    static void vector_fixed(const ColumnString::Chars &, size_t, ColumnString::Chars &)
    {
        throw Exception("Column of type FixedString is not supported by URL functions", ErrorCodes::ILLEGAL_COLUMN);
    }
};


/** Delete part of string using the Extractor.
  */
template <typename Extractor>
struct CutSubstringImpl
{
    static void vector(const ColumnString::Chars & data, const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data, ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(data.size());
        size_t size = offsets.size();
        res_offsets.resize(size);

        size_t prev_offset = 0;
        size_t res_offset = 0;

        /// Matched part.
        Pos start;
        size_t length;

        for (size_t i = 0; i < size; ++i)
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

    static void vector_fixed(const ColumnString::Chars &, size_t, ColumnString::Chars &)
    {
        throw Exception("Column of type FixedString is not supported by URL functions", ErrorCodes::ILLEGAL_COLUMN);
    }
};


/// Percent decode of url data.
struct DecodeURLComponentImpl
{
    static void vector(const ColumnString::Chars & data, const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data, ColumnString::Offsets & res_offsets);

    static void constant(const std::string & data,
        std::string & res_data);

    static void vector_fixed(const ColumnString::Chars & data, size_t n,
        ColumnString::Chars & res_data);
};

}
