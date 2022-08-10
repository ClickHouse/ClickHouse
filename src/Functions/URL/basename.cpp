#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <base/find_symbols.h>
#include "FunctionsURL.h"

namespace DB
{

/** Extract substring after the last slash or backslash.
  * If there are no slashes, return the string unchanged.
  * It is used to extract filename from path.
  */
struct ExtractBasename
{
    static size_t getReserveLengthForElement() { return 16; } /// Just a guess.

    static void execute(Pos data, size_t size, Pos & res_data, size_t & res_size)
    {
        res_data = data;
        res_size = size;

        Pos pos = data;
        Pos end = pos + size;

        if ((pos = find_last_symbols_or_null<'/', '\\'>(pos, end)))
        {
            ++pos;
            res_data = pos;
            res_size = end - pos;
        }
    }
};

struct NameBasename { static constexpr auto name = "basename"; };
using FunctionBasename = FunctionStringToString<ExtractSubstringImpl<ExtractBasename>, NameBasename>;

REGISTER_FUNCTION(Basename)
{
    factory.registerFunction<FunctionBasename>();
}

}
