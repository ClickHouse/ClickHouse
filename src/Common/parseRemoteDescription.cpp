#include "parseRemoteDescription.h"
#include <Common/Exception.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <base/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/// The Cartesian product of two sets of rows, the result is written in place of the first argument
static void append(std::vector<String> & to, const std::vector<String> & what, size_t max_addresses)
{
    if (what.empty())
        return;

    if (to.empty())
    {
        to = what;
        return;
    }

    if (what.size() * to.size() > max_addresses)
        throw Exception("Table function 'remote': first argument generates too many result addresses",
                        ErrorCodes::BAD_ARGUMENTS);
    std::vector<String> res;
    for (const auto & elem_to : to)
        for (const auto & elem_what : what)
            res.push_back(elem_to + elem_what);

    to.swap(res);
}


/// Parse number from substring
static bool parseNumber(const String & description, size_t l, size_t r, size_t & res)
{
    res = 0;
    for (size_t pos = l; pos < r; ++pos)
    {
        if (!isNumericASCII(description[pos]))
            return false;
        res = res * 10 + description[pos] - '0';
        if (res > 1e15)
            return false;
    }
    return true;
}


/* Parse a string that generates shards and replicas. Separator - one of two characters | or ,
 *  depending on whether shards or replicas are generated.
 * For example:
 * host1,host2,...      - generates set of shards from host1, host2, ...
 * host1|host2|...      - generates set of replicas from host1, host2, ...
 * abc{8..10}def        - generates set of shards abc8def, abc9def, abc10def.
 * abc{08..10}def       - generates set of shards abc08def, abc09def, abc10def.
 * abc{x,yy,z}def       - generates set of shards abcxdef, abcyydef, abczdef.
 * abc{x|yy|z} def      - generates set of replicas abcxdef, abcyydef, abczdef.
 * abc{1..9}de{f,g,h}   - is a direct product, 27 shards.
 * abc{1..9}de{0|1}     - is a direct product, 9 shards, in each 2 replicas.
 */
std::vector<String> parseRemoteDescription(const String & description, size_t l, size_t r, char separator, size_t max_addresses)
{
    std::vector<String> res;
    std::vector<String> cur;

    /// An empty substring means a set of an empty string
    if (l >= r)
    {
        res.push_back("");
        return res;
    }

    for (size_t i = l; i < r; ++i)
    {
        /// Either the numeric interval (8..10) or equivalent expression in brackets
        if (description[i] == '{')
        {
            int cnt = 1;
            int last_dot = -1; /// The rightmost pair of points, remember the index of the right of the two
            size_t m;
            std::vector<String> buffer;
            bool have_splitter = false;

            /// Look for the corresponding closing bracket
            for (m = i + 1; m < r; ++m)
            {
                if (description[m] == '{') ++cnt;
                if (description[m] == '}') --cnt;
                if (description[m] == '.' && description[m-1] == '.') last_dot = m;
                if (description[m] == separator) have_splitter = true;
                if (cnt == 0) break;
            }
            if (cnt != 0)
                throw Exception("Table function 'remote': incorrect brace sequence in first argument",
                                ErrorCodes::BAD_ARGUMENTS);
            /// The presence of a dot - numeric interval
            if (last_dot != -1)
            {
                size_t left, right;
                if (description[last_dot - 1] != '.')
                    throw Exception("Table function 'remote': incorrect argument in braces (only one dot): " + description.substr(i, m - i + 1),
                                    ErrorCodes::BAD_ARGUMENTS);
                if (!parseNumber(description, i + 1, last_dot - 1, left))
                    throw Exception("Table function 'remote': incorrect argument in braces (Incorrect left number): "
                                    + description.substr(i, m - i + 1),
                                    ErrorCodes::BAD_ARGUMENTS);
                if (!parseNumber(description, last_dot + 1, m, right))
                    throw Exception("Table function 'remote': incorrect argument in braces (Incorrect right number): "
                                    + description.substr(i, m - i + 1),
                                    ErrorCodes::BAD_ARGUMENTS);
                if (left > right)
                    throw Exception("Table function 'remote': incorrect argument in braces (left number is greater then right): "
                                    + description.substr(i, m - i + 1),
                                    ErrorCodes::BAD_ARGUMENTS);
                if (right - left + 1 >  max_addresses)
                    throw Exception("Table function 'remote': first argument generates too many result addresses",
                        ErrorCodes::BAD_ARGUMENTS);
                bool add_leading_zeroes = false;
                size_t len = last_dot - 1 - (i + 1);
                /// If the left and right borders have equal numbers, then you must add leading zeros.
                /// TODO The code is somewhat awful.
                if (last_dot - 1 - (i + 1) == m - (last_dot + 1))
                    add_leading_zeroes = true;
                for (size_t id = left; id <= right; ++id)
                {
                    String id_str = toString<UInt64>(id);
                    if (add_leading_zeroes)
                    {
                        while (id_str.size() < len)
                            id_str = "0" + id_str;
                    }
                    buffer.push_back(id_str);
                }
            }
            else if (have_splitter) /// If there is a current delimiter inside, then generate a set of resulting rows
                buffer = parseRemoteDescription(description, i + 1, m, separator, max_addresses);
            else                     /// Otherwise just copy, spawn will occur when you call with the correct delimiter
                buffer.push_back(description.substr(i, m - i + 1));
            /// Add all possible received extensions to the current set of lines
            append(cur, buffer, max_addresses);
            i = m;
        }
        else if (description[i] == separator)
        {
            /// If the delimiter, then add found rows
            res.insert(res.end(), cur.begin(), cur.end());
            cur.clear();
        }
        else
        {
            /// Otherwise, simply append the character to current lines
            std::vector<String> buffer;
            buffer.push_back(description.substr(i, 1));
            append(cur, buffer, max_addresses);
        }
    }

    res.insert(res.end(), cur.begin(), cur.end());
    if (res.size() > max_addresses)
        throw Exception("Table function 'remote': first argument generates too many result addresses",
            ErrorCodes::BAD_ARGUMENTS);

    return res;
}


std::vector<std::pair<String, uint16_t>> parseRemoteDescriptionForExternalDatabase(const String & description, size_t max_addresses, UInt16 default_port)
{
    auto addresses = parseRemoteDescription(description, 0, description.size(), '|', max_addresses);
    std::vector<std::pair<String, uint16_t>> result;

    for (const auto & address : addresses)
    {
        size_t colon = address.find(':');
        if (colon == String::npos)
        {
            LOG_WARNING(&Poco::Logger::get("ParseRemoteDescription"), "Port is not found for host: {}. Using default port {}", address, default_port);
            result.emplace_back(std::make_pair(address, default_port));
        }
        else
        {
            result.emplace_back(std::make_pair(address.substr(0, colon), DB::parseFromString<UInt16>(address.substr(colon + 1))));
        }
    }

    return result;
}

}
