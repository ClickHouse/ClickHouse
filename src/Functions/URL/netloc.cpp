#include <Common/StringUtils.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/StringHelpers.h>

#include <algorithm>


namespace DB
{

/// NOTE: Implementation is not RFC3986 compatible
struct ExtractNetloc
{
    /// We use the same as domain function
    static size_t getReserveLengthForElement() { return 15; }

    static std::string_view getNetworkLocation(const char * data, size_t size)
    {
        Pos pos = data;
        Pos end = data + size;

        /// Skip scheme.
        if (pos + 2 < end && pos[0] == '/' && pos[1] == '/')
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
                            return std::string_view();
                        default:
                            goto exloop;
                    }
                }
            }
            exloop:
            if (pos + 2 < scheme_end && pos[0] == ':' && pos[1] == '/' && pos[2] == '/')
                pos += 3;
            else
                pos = data;
        }

        /// Now pos points to the first byte after scheme (if there is).

        bool has_identification = false;
        Pos hostname_end = end;
        Pos question_mark_pos = end;
        Pos slash_pos = end;
        Pos start_of_host = pos;
        for (; pos < end; ++pos)
        {
            switch (*pos) // NOLINT(bugprone-switch-missing-default-case)
            {
                case '/':
                    if (has_identification)
                        return std::string_view(start_of_host, pos - start_of_host);
                    else
                        slash_pos = pos;
                    break;
                case '?':
                    if (has_identification)
                        return std::string_view(start_of_host, pos - start_of_host);
                    else
                        question_mark_pos = pos;
                    break;
                case '#':
                    return std::string_view(start_of_host, pos - start_of_host);
                case '@': /// foo:bar@example.ru
                    has_identification = true;
                    hostname_end = end;
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
                    if (!has_identification)
                    {
                        hostname_end = pos;
                        break;
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
                case ']':
                    return pos > start_of_host
                        ? std::string_view(start_of_host, std::min({pos, question_mark_pos, slash_pos}) - start_of_host)
                        : std::string_view();
            }
        }

        if (has_identification)
            return std::string_view(start_of_host, pos - start_of_host);
        return std::string_view(start_of_host, std::min({pos, question_mark_pos, slash_pos, hostname_end}) - start_of_host);
    }

    static void execute(Pos data, size_t size, Pos & res_data, size_t & res_size)
    {
        std::string_view host = getNetworkLocation(data, size);

        res_data = host.data();
        res_size = host.size();
    }
};


struct NameNetloc { static constexpr auto name = "netloc"; };
using FunctionNetloc = FunctionStringToString<ExtractSubstringImpl<ExtractNetloc>, NameNetloc>;

REGISTER_FUNCTION(Netloc)
{
    factory.registerFunction<FunctionNetloc>();
}

}
