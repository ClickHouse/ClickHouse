#include <map>
#include <cstring>
#include <algorithm>
#include <Poco/String.h>
#include <base/find_symbols.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include "validateODBCConnectionString.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ODBC_CONNECTION_STRING;
}


std::string validateODBCConnectionString(const std::string & connection_string)
{
    /// Connection string is a list of name, value pairs.
    /// name and value are separated by '='.
    /// names are case insensitive.
    /// name=value pairs are separated by ';'.
    /// ASCII whitespace characters are skipped before and after delimiters.
    /// value may be optionally enclosed by {}
    /// in enclosed value, } is escaped as }}.
    ///
    /// Example: PWD={a}}b} means that password is a}b
    ///
    /// https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqldriverconnect-function?view=sql-server-2017#comments

    /// unixODBC has fixed size buffers on stack and has buffer overflow bugs.
    /// We will limit string sizes to small values.

    static constexpr size_t MAX_ELEMENT_SIZE = 100;
    static constexpr size_t MAX_CONNECTION_STRING_SIZE = 1000;

    if (connection_string.empty())
        throw Exception(ErrorCodes::BAD_ODBC_CONNECTION_STRING, "ODBC connection string cannot be empty");

    if (connection_string.size() >= MAX_CONNECTION_STRING_SIZE)
        throw Exception(ErrorCodes::BAD_ODBC_CONNECTION_STRING, "ODBC connection string is too long");

    const char * pos = connection_string.data();
    const char * end = pos + connection_string.size();

    auto skip_whitespaces = [&]
    {
        while (pos < end && isWhitespaceASCII(*pos))
        {
            if (*pos != ' ')
                throw Exception(ErrorCodes::BAD_ODBC_CONNECTION_STRING, "ODBC connection string parameter contains unusual whitespace character");
            ++pos;
        }
    };

    auto read_name = [&]
    {
        const char * begin = pos;

        if (pos < end && isValidIdentifierBegin(*pos))
            ++pos;
        else
            throw Exception(ErrorCodes::BAD_ODBC_CONNECTION_STRING,
                            "ODBC connection string parameter name doesn't begin with valid identifier character");

        /// Additionally allow dash and dot symbols in names.
        /// Strictly speaking, the name with that characters should be escaped.
        /// But some ODBC drivers (e.g.) Postgres don't like escaping.

        while (pos < end && (isWordCharASCII(*pos) || *pos == '-' || *pos == '.'))
            ++pos;

        return std::string(begin, pos);
    };

    auto read_plain_value = [&]
    {
        const char * begin = pos;

        while (pos < end && *pos != ';' && !isWhitespaceASCII(*pos))
        {
            signed char c = *pos;
            if (c < 32 || strchr("[]{}(),;?*=!@'\"", c) != nullptr)
                throw Exception(ErrorCodes::BAD_ODBC_CONNECTION_STRING,
                                "ODBC connection string parameter value is unescaped and contains illegal character");
            ++pos;
        }

        return std::string(begin, pos);
    };

    auto read_escaped_value = [&]
    {
        std::string res;

        if (pos < end && *pos == '{')
            ++pos;
        else
            throw Exception(ErrorCodes::BAD_ODBC_CONNECTION_STRING, "ODBC connection string parameter value doesn't begin with opening curly brace");

        while (pos < end)
        {
            if (*pos == '}')
            {
                ++pos;
                if (pos >= end || *pos != '}')
                    return res;
            }

            if (*pos == 0)
                throw Exception(ErrorCodes::BAD_ODBC_CONNECTION_STRING, "ODBC connection string parameter value contains ASCII NUL character");

            res += *pos;
            ++pos;
        }

        throw Exception(ErrorCodes::BAD_ODBC_CONNECTION_STRING, "ODBC connection string parameter is escaped but there is no closing curly brace");
    };

    auto read_value = [&]
    {
        if (pos >= end)
            return std::string{};

        if (*pos == '{')
            return read_escaped_value();

        return read_plain_value();
    };

    std::map<std::string, std::string> parameters;

    while (pos < end)
    {
        skip_whitespaces();
        std::string name = read_name();
        skip_whitespaces();

        Poco::toUpperInPlace(name);
        if (name == "FILEDSN" || name == "SAVEFILE" || name == "DRIVER")
            throw Exception(ErrorCodes::BAD_ODBC_CONNECTION_STRING, "ODBC connection string has forbidden parameter");

        if (pos >= end)
            throw Exception(ErrorCodes::BAD_ODBC_CONNECTION_STRING, "ODBC connection string parameter doesn't have value");

        if (*pos == '=')
            ++pos;
        else
            throw Exception(ErrorCodes::BAD_ODBC_CONNECTION_STRING, "ODBC connection string parameter doesn't have value");

        skip_whitespaces();
        std::string value = read_value();
        skip_whitespaces();

        if (name.size() > MAX_ELEMENT_SIZE || value.size() > MAX_ELEMENT_SIZE)
            throw Exception(ErrorCodes::BAD_ODBC_CONNECTION_STRING, "ODBC connection string has too long keyword or value");

        if (!parameters.emplace(name, value).second)
            throw Exception(ErrorCodes::BAD_ODBC_CONNECTION_STRING, "Duplicate parameter found in ODBC connection string");

        if (pos >= end)
            break;

        if (*pos == ';')
            ++pos;
        else
            throw Exception(ErrorCodes::BAD_ODBC_CONNECTION_STRING, "Unexpected character found after parameter value in ODBC connection string");
    }

    /// Reconstruct the connection string.

    auto it = parameters.find("DSN");

    if (parameters.end() == it)
        throw Exception(ErrorCodes::BAD_ODBC_CONNECTION_STRING, "DSN parameter is mandatory for ODBC connection string");

    std::string dsn = it->second;

    if (dsn.empty())
        throw Exception(ErrorCodes::BAD_ODBC_CONNECTION_STRING, "DSN parameter cannot be empty in ODBC connection string");

    parameters.erase(it);

    std::string reconstructed_connection_string;

    auto write_plain_value = [&](const std::string & value)
    {
        reconstructed_connection_string += value;
    };

    auto write_escaped_value = [&](const std::string & value)
    {
        reconstructed_connection_string += '{';

        const char * value_pos = value.data();
        const char * value_end = value_pos + value.size();
        while (true)
        {
            const char * next_pos = find_first_symbols<'}'>(value_pos, value_end);

            if (next_pos == value_end)
            {
                reconstructed_connection_string.append(value_pos, next_pos - value_pos);
                break;
            }

            reconstructed_connection_string.append(value_pos, next_pos - value_pos);
            reconstructed_connection_string.append("}}");
            value_pos = next_pos + 1;
        }

        reconstructed_connection_string += '}';
    };

    auto write_value = [&](const std::string & value)
    {
        /// Additionally allow dash and dot symbols - for hostnames.
        /// Strictly speaking, hostname with that characters should be escaped.
        /// But some ODBC drivers (e.g.) Postgres don't like escaping.

        if (std::all_of(value.begin(), value.end(), [](char c) { return isWordCharASCII(c) || c == '.' || c == '-'; }))
            write_plain_value(value);
        else
            write_escaped_value(value);
    };

    auto write_element = [&](const std::string & name, const std::string & value)
    {
        reconstructed_connection_string.append(name);
        reconstructed_connection_string += '=';
        write_value(value);
        reconstructed_connection_string += ';';
    };

    /// Place DSN first because that's more safe.
    write_element("DSN", dsn);
    for (const auto & elem : parameters)
        write_element(elem.first, elem.second);

    if (reconstructed_connection_string.size() >= MAX_CONNECTION_STRING_SIZE)
        throw Exception(ErrorCodes::BAD_ODBC_CONNECTION_STRING, "ODBC connection string is too long");

    return reconstructed_connection_string;
}

}
