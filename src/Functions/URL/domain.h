#pragma once

#include "protocol.h"
#include <base/find_symbols.h>
#include <cstring>
#include <Common/StringUtils/StringUtils.h>

namespace DB
{

inline std::string_view checkAndReturnHost(const Pos & pos, const Pos & dot_pos, const Pos & start_of_host)
{
    if (!dot_pos || start_of_host >= pos || pos - dot_pos == 1)
        return std::string_view{};

    auto after_dot = *(dot_pos + 1);
    if (after_dot == ':' || after_dot == '/' || after_dot == '?' || after_dot == '#')
        return std::string_view{};

    return std::string_view(start_of_host, pos - start_of_host);
}

/// Extracts host from given url (RPC).
///
/// @return empty string view if the host is not valid (i.e. it does not have dot, or there no symbol after dot).
inline std::string_view getURLHostRFC(const char * data, size_t size)
{
    Pos pos = data;
    Pos end = data + size;

    if (*pos == '/' && *(pos + 1) == '/')
    {
        pos += 2;
    }
    else
    {
        Pos scheme_end = data + std::min(size, 16UL);
        for (++pos; pos < scheme_end; ++pos)
        {
            if (!isAlphaNumericASCII(*pos))
            {
                switch (*pos)
                {
                case '.':
                case '-':
                case '+':
                case '[':
                    break;
                case ' ': /// restricted symbols
                case '\t':
                case '<':
                case '>':
                case '%':
                case '{':
                case '}':
                case '|':
                case '\\':
                case '^':
                case '~':
                case ']':
                case ';':
                case '=':
                case '&':
                    return std::string_view{};
                default:
                    goto exloop;
                }
            }
        }
exloop: if ((scheme_end - pos) > 2 && *pos == ':' && *(pos + 1) == '/' && *(pos + 2) == '/')
            pos += 3;
        else
            pos = data;
    }

    bool has_open_bracket = false;
    bool has_end_bracket = false;
    if (*pos == '[') /// IPv6 [2001:db8::1]:80
    {
        has_open_bracket = true;
        ++pos;
    }
    Pos dot_pos = nullptr;
    Pos colon_pos = nullptr;
    bool has_sub_delims = false;
    bool has_at_symbol = false;
    bool has_terminator_after_colon = false;
    const auto * start_of_host = pos;
    for (; pos < end; ++pos)
    {
        switch (*pos)
        {
        case '.':
            if (has_open_bracket)
                return std::string_view{};
            if (has_at_symbol || colon_pos == nullptr)
                dot_pos = pos;
            break;
        case ':':
            if (has_open_bracket)
                continue;
            if (has_at_symbol || colon_pos) goto done;
            colon_pos = pos;
            break;
        case '/': /// end symbols
        case '?':
        case '#':
            goto done;
        case '@': /// myemail@gmail.com
            if (has_terminator_after_colon) return std::string_view{};
            if (has_at_symbol) goto done;
            has_sub_delims = false;
            has_at_symbol = true;
            start_of_host = pos + 1;
            break;
        case ';':
        case '=':
        case '&':
        case '~':
        case '%':
            /// Symbols above are sub-delims in RFC3986 and should be
            /// allowed for userinfo (named identification here).
            ///
            /// NOTE: that those symbols is allowed for reg-name (host)
            /// too, but right now host parsing looks more like in
            /// RFC1034 (in other words domains that are allowed to be
            /// registered).
            has_sub_delims = true;
            continue;
        case ']':
            if (has_open_bracket)
            {
                has_end_bracket = true;
                goto done;
            }
            [[fallthrough]];
        case ' ': /// restricted symbols in whole URL
        case '\t':
        case '<':
        case '>':
        case '{':
        case '}':
        case '|':
        case '\\':
        case '^':
        case '[':
            if (colon_pos == nullptr)
                return std::string_view{};
            else
                has_terminator_after_colon = true;
        }
    }

done:
    if (has_sub_delims)
        return std::string_view{};
    if (!has_at_symbol)
    {
        if (has_open_bracket && has_end_bracket)
            return std::string_view(start_of_host, pos - start_of_host);
        pos = colon_pos ? colon_pos : pos;
    }
    return checkAndReturnHost(pos, dot_pos, start_of_host);
}

/// Extracts host from given url.
///
/// @return empty string view if the host is not valid (i.e. it does not have dot, or there no symbol after dot).
inline std::string_view getURLHost(const char * data, size_t size)
{
    Pos pos = data;
    Pos end = data + size;

    if (*pos == '/' && *(pos + 1) == '/')
    {
        pos += 2;
    }
    else
    {
        Pos scheme_end = data + std::min(size, 16UL);
        for (++pos; pos < scheme_end; ++pos)
        {
            if (!isAlphaNumericASCII(*pos))
            {
                switch (*pos)
                {
                case '.':
                case '-':
                case '+':
                    break;
                case ' ': /// restricted symbols
                case '\t':
                case '<':
                case '>':
                case '%':
                case '{':
                case '}':
                case '|':
                case '\\':
                case '^':
                case '~':
                case '[':
                case ']':
                case ';':
                case '=':
                case '&':
                    return std::string_view{};
                default:
                    goto exloop;
                }
            }
        }
exloop: if ((scheme_end - pos) > 2 && *pos == ':' && *(pos + 1) == '/' && *(pos + 2) == '/')
            pos += 3;
        else
            pos = data;
    }

    Pos dot_pos = nullptr;
    const auto * start_of_host = pos;
    for (; pos < end; ++pos)
    {
        switch (*pos)
        {
        case '.':
            dot_pos = pos;
            break;
        case ':': /// end symbols
        case '/':
        case '?':
        case '#':
            return checkAndReturnHost(pos, dot_pos, start_of_host);
        case '@': /// myemail@gmail.com
            start_of_host = pos + 1;
            break;
        case ' ': /// restricted symbols in whole URL
        case '\t':
        case '<':
        case '>':
        case '%':
        case '{':
        case '}':
        case '|':
        case '\\':
        case '^':
        case '~':
        case '[':
        case ']':
        case ';':
        case '=':
        case '&':
            return std::string_view{};
        }
    }

    return checkAndReturnHost(pos, dot_pos, start_of_host);
}

template <bool without_www, bool conform_rfc>
struct ExtractDomain
{
    static size_t getReserveLengthForElement() { return 15; }

    static void execute(Pos data, size_t size, Pos & res_data, size_t & res_size)
    {
        std::string_view host;
        if constexpr (conform_rfc)
          host = getURLHostRFC(data, size);
        else
          host = getURLHost(data, size);

        if (host.empty())
        {
            res_data = data;
            res_size = 0;
        }
        else
        {
            if (without_www && host.size() > 4 && !strncmp(host.data(), "www.", 4))
                host = { host.data() + 4, host.size() - 4 };

            res_data = host.data();
            res_size = host.size();
        }
    }
};

}
