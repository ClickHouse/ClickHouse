#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/URL/protocol.h>
#include <base/find_symbols.h>


namespace DB
{

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

struct NameCutWWW { static constexpr auto name = "cutWWW"; };
using FunctionCutWWW = FunctionStringToString<CutSubstringImpl<ExtractWWW>, NameCutWWW>;

REGISTER_FUNCTION(CutWWW)
{
    factory.registerFunction<FunctionCutWWW>();
}

}
