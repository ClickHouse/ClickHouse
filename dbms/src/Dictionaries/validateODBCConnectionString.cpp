#include <map>
#include <cstring>
#include <algorithm>
#include <Poco/String.h>
#include <common/find_first_symbols.h>
#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <Dictionaries/validateODBCConnectionString.h>


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
    /// name=value pairs are sepated by ';'.
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
        throw Exception("ODBC connection string cannot be empty", ErrorCodes::BAD_ODBC_CONNECTION_STRING);

    if (connection_string.size() >= MAX_CONNECTION_STRING_SIZE)
        throw Exception("ODBC connection string is too long", ErrorCodes::BAD_ODBC_CONNECTION_STRING);

    const char * pos = connection_string.data();
    const char * end = pos + connection_string.size();

    auto skip_whitespaces = [&]
    {
        while (pos < end && isWhitespaceASCII(*pos))
        {
            if (*pos != ' ')
                throw Exception("ODBC connection string parameter contains unusual whitespace character", ErrorCodes::BAD_ODBC_CONNECTION_STRING);
            ++pos;
        }
    };

    auto read_name = [&]
    {
        const char * begin = pos;

        if (pos < end && isValidIdentifierBegin(*pos))
            ++pos;
        else
            throw Exception("ODBC connection string parameter name doesn't begin with valid identifier character", ErrorCodes::BAD_ODBC_CONNECTION_STRING);

        while (pos < end && isWordCharASCII(*pos))
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
                throw Exception("ODBC connection string parameter value is unescaped and contains illegal character", ErrorCodes::BAD_ODBC_CONNECTION_STRING);
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
            throw Exception("ODBC connection string parameter value doesn't begin with opening curly brace", ErrorCodes::BAD_ODBC_CONNECTION_STRING);

        while (pos < end)
        {
            if (*pos == '}')
            {
                ++pos;
                if (pos >= end || *pos != '}')
                    return res;
            }

            if (*pos == 0)
                throw Exception("ODBC connection string parameter value contains ASCII NUL character", ErrorCodes::BAD_ODBC_CONNECTION_STRING);

            res += *pos;
            ++pos;
        }

        throw Exception("ODBC connection string parameter is escaped but there is no closing curly brace", ErrorCodes::BAD_ODBC_CONNECTION_STRING);
    };

    auto read_value = [&]
    {
        if (pos >= end)
            return std::string{};

        if (*pos == '{')
            return read_escaped_value();
        else
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
            throw Exception("ODBC connection string has forbidden parameter", ErrorCodes::BAD_ODBC_CONNECTION_STRING);

        if (pos >= end)
            throw Exception("ODBC connection string parameter doesn't have value", ErrorCodes::BAD_ODBC_CONNECTION_STRING);

        if (*pos == '=')
            ++pos;
        else
            throw Exception("ODBC connection string parameter doesn't have value", ErrorCodes::BAD_ODBC_CONNECTION_STRING);

        skip_whitespaces();
        std::string value = read_value();
        skip_whitespaces();

        if (name.size() > MAX_ELEMENT_SIZE || value.size() > MAX_ELEMENT_SIZE)
            throw Exception("ODBC connection string has too long keyword or value", ErrorCodes::BAD_ODBC_CONNECTION_STRING);

        if (!parameters.emplace(name, value).second)
            throw Exception("Duplicate parameter found in ODBC connection string", ErrorCodes::BAD_ODBC_CONNECTION_STRING);

        if (pos >= end)
            break;

        if (*pos == ';')
            ++pos;
        else
            throw Exception("Unexpected character found after parameter value in ODBC connection string", ErrorCodes::BAD_ODBC_CONNECTION_STRING);
    }

    /// Reconstruct the connection string.

    auto it = parameters.find("DSN");

    if (parameters.end() == it)
        throw Exception("DSN parameter is mandatory for ODBC connection string", ErrorCodes::BAD_ODBC_CONNECTION_STRING);

    std::string dsn = it->second;

    if (dsn.empty())
        throw Exception("DSN parameter cannot be empty in ODBC connection string", ErrorCodes::BAD_ODBC_CONNECTION_STRING);

    parameters.erase(it);

    std::string reconstructed_connection_string;

    auto write_plain_value = [&](const std::string & value)
    {
        reconstructed_connection_string += value;
    };

    auto write_escaped_value = [&](const std::string & value)
    {
        reconstructed_connection_string += '{';

        const char * pos = value.data();
        const char * end = pos + value.size();
        while (true)
        {
            const char * next_pos = find_first_symbols<'}'>(pos, end);

            if (next_pos == end)
            {
                reconstructed_connection_string.append(pos, next_pos - pos);
                break;
            }
            else
            {
                reconstructed_connection_string.append(pos, next_pos - pos);
                reconstructed_connection_string.append("}}");
                pos = next_pos + 1;
            }
        }

        reconstructed_connection_string += '}';
    };

    auto write_value = [&](const std::string & value)
    {
        if (std::all_of(value.begin(), value.end(), isWordCharASCII))
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
        throw Exception("ODBC connection string is too long", ErrorCodes::BAD_ODBC_CONNECTION_STRING);

    return reconstructed_connection_string;
}

}
