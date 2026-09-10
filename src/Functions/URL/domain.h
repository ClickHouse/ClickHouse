#pragma once

#include <Common/StringUtils.h>
#include <Common/formatIPv6.h>
#include <Functions/URL/protocol.h>
#include <base/find_symbols.h>

#include <cstring>

namespace DB
{

/// RFC 3986, 3.2.2: IPvFuture = "v" 1*HEXDIG "." 1*( unreserved / sub-delims / ":" )
/// A placeholder for IP address formats beyond IPv6, e.g. "[v1.a]".
inline bool isIPvFuture(const char * begin, const char * end)
{
    if (begin >= end || (*begin != 'v' && *begin != 'V'))
        return false;

    const char * pos = begin + 1;
    const char * version_start = pos;
    while (pos < end && isHexDigit(*pos))
        ++pos;
    if (pos == version_start)
        return false;

    if (pos >= end || *pos != '.')
        return false;
    ++pos;

    const char * address_start = pos;
    for (; pos < end; ++pos)
    {
        char c = *pos;
        bool is_unreserved = isAlphaNumericASCII(c) || c == '-' || c == '.' || c == '_' || c == '~';
        bool is_sub_delim = c == '!' || c == '$' || c == '&' || c == '\'' || c == '(' || c == ')'
            || c == '*' || c == '+' || c == ',' || c == ';' || c == '=';
        if (!is_unreserved && !is_sub_delim && c != ':')
            return false;
    }
    return pos > address_start;
}

inline std::string_view checkAndReturnHost(const Pos & pos, const Pos & dot_pos, const Pos & start_of_host)
{
    if (!dot_pos || start_of_host >= pos || pos - dot_pos == 1 || *start_of_host == '.')
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
    if (size < 2)
        return std::string_view{};

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
    Pos bracket_close_pos = nullptr;
    Pos dot_pos = nullptr;
    Pos colon_pos = nullptr;
    bool has_sub_delims = false;
    bool has_at_symbol = false;
    bool has_terminator_after_colon = false;
    const auto * start_of_host = pos;
    for (; pos < end; ++pos)
    {
        switch (*pos) /// NOLINT(bugprone-switch-missing-default-case)
        {
        case '.':
            if (has_open_bracket)
                continue; /// part of a mixed IPv6/IPv4 tail, e.g. "::ffff:192.0.2.128"; parseIPv6Whole validates it below
            /// Once colon_pos is set (whether or not '@' has been seen), a dot afterward is past
            /// the host, in what would be the port - it must not count as a dot within the host,
            /// e.g. the '.' in "user@foo:80.bar" must not make checkAndReturnHost think "foo" has
            /// a valid dot of its own.
            if (colon_pos == nullptr)
                dot_pos = pos;
            break;
        case ':':
            if (has_open_bracket)
                continue;
            /// Whether or not '@' has been seen yet, this ':' might still turn out to be
            /// followed by a later '@' (e.g. "user@host:80@evil.com"), which would mean it was
            /// never the port separator at all - keep only the first one as a fallback and keep
            /// scanning, instead of stopping the scan here.
            if (colon_pos == nullptr)
                colon_pos = pos;
            break;
        case '/': /// end symbols
        case '?':
        case '#':
            goto done;
        case '@': /// myemail@gmail.com
            /// Inside an IP-literal there is no userinfo: `@` is not allowed there at all.
            if (has_open_bracket) return std::string_view{};
            /// A bracket that already closed before this '@' would have to become part of
            /// userinfo now that '@' turned up - but raw '[' / ']' can never legally appear
            /// there, so an authority like "[::1]:80@evil.com" is invalid, not "host evil.com".
            if (has_end_bracket) return std::string_view{};
            if (has_terminator_after_colon) return std::string_view{};
            if (has_at_symbol) return std::string_view{};
            has_sub_delims = false;
            has_at_symbol = true;
            start_of_host = pos + 1;
            colon_pos = nullptr;
            dot_pos = nullptr;
            has_terminator_after_colon = false;

            /// An IP-literal host may follow the userinfo: `http://user:password@[2001:db8::1]:8080/`
            /// (RFC 3986, 3.2). Enter the same mode as for an authority that starts with the bracket,
            /// so that the colons inside it are not read as the port separator.
            if (pos + 1 < end && *(pos + 1) == '[')
            {
                has_open_bracket = true;
                start_of_host = pos + 2;
                ++pos;
            }
            break;
        case ';':
        case '=':
        case '&':
        case '~':
        case '%':
            if (has_open_bracket)
                continue; /// sub-delims are also valid in IPvFuture's address part; isIPvFuture validates it below
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
                /// Nothing may follow the closing bracket except a delimiter or end of input.
                Pos after_bracket = pos + 1;
                if (after_bracket < end && *after_bracket != ':' && *after_bracket != '/'
                    && *after_bracket != '?' && *after_bracket != '#')
                    return std::string_view{};
                has_end_bracket = true;
                has_open_bracket = false; /// the literal is closed; ':' after it is an ordinary port separator
                bracket_close_pos = pos;
                break; /// keep scanning: a later '@' (e.g. "user@[::1]:80@evil.com") must still be caught
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
    /// A complete IP-literal is the host as it stands, whether or not a userinfo preceded it: it has
    /// no dot to look for, and the colon that follows it belongs to the port. The scan kept going
    /// past the closing bracket (rather than stopping there) so a later '@' would still be caught,
    /// e.g. "user@[::1]:80@evil.com" - so the host itself ends at the bracket, not at `pos`.
    if (has_end_bracket)
    {
        /// RFC 3986, 3.2.2: IP-literal = "[" ( IPv6address / IPvFuture ) "]"
        unsigned char ipv6_bytes[IPV6_BINARY_LENGTH];
        bool is_valid_ip_literal = parseIPv6Whole(start_of_host, bracket_close_pos, ipv6_bytes)
            || isIPvFuture(start_of_host, bracket_close_pos);
        if (!is_valid_ip_literal)
            return std::string_view{};
        return std::string_view(start_of_host, bracket_close_pos - start_of_host);
    }
    pos = colon_pos ? colon_pos : pos;
    return checkAndReturnHost(pos, dot_pos, start_of_host);
}

/// Extracts host from given url.
///
/// @return empty string view if the host is not valid (i.e. it does not have dot, or there no symbol after dot).
inline std::string_view getURLHost(const char * data, size_t size)
{
    Pos pos = data;
    Pos end = data + size;

    if (size >= 2 && *pos == '/' && *(pos + 1) == '/')
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
        switch (*pos) /// NOLINT(bugprone-switch-missing-default-case)
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
            if (without_www && host.size() > 4 && !strncmp(host.data(), "www.", 4)) /// NOLINT(bugprone-suspicious-stringview-data-usage)
                host = { host.data() + 4, host.size() - 4 };

            res_data = host.data();
            res_size = host.size();
        }
    }
};

}
