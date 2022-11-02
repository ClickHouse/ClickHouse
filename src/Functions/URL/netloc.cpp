#include <Common/StringUtils/StringUtils.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/URL/FunctionsURL.h>


namespace DB
{

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
        Pos question_mark_pos = end;
        Pos slash_pos = end;
        Pos start_of_host = pos;
        for (; pos < end; ++pos)
        {
            switch (*pos)
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
                    return pos > start_of_host
                        ? std::string_view(start_of_host, std::min(std::min(pos - 1, question_mark_pos), slash_pos) - start_of_host)
                        : std::string_view();
            }
        }

        if (has_identification)
            return std::string_view(start_of_host, pos - start_of_host);
        else
            return std::string_view(start_of_host, std::min(std::min(pos, question_mark_pos), slash_pos) - start_of_host);
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

