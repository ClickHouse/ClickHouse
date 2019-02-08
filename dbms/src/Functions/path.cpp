#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/FunctionsURL.h>
#include <common/find_symbols.h>

namespace DB
{

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

struct NamePath { static constexpr auto name = "path"; };
using FunctionPath = FunctionStringToString<ExtractSubstringImpl<ExtractPath>, NamePath>;

void registerFunctionPath(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPath>();
}

}
