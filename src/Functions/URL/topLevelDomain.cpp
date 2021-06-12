#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include "domain.h"

namespace DB
{

struct ExtractTopLevelDomain
{
    static size_t getReserveLengthForElement() { return 5; }

    static void execute(Pos data, size_t size, Pos & res_data, size_t & res_size)
    {
        StringRef host = getURLHost(data, size);

        res_data = data;
        res_size = 0;

        if (host.size != 0)
        {
            if (host.data[host.size - 1] == '.')
                host.size -= 1;

            const auto * host_end = host.data + host.size;

            Pos last_dot = find_last_symbols_or_null<'.'>(host.data, host_end);
            if (!last_dot)
                return;

            /// For IPv4 addresses select nothing.
            if (last_dot[1] <= '9')
                return;

            res_data = last_dot + 1;
            res_size = host_end - res_data;
        }
    }
};

struct NameTopLevelDomain { static constexpr auto name = "topLevelDomain"; };
using FunctionTopLevelDomain = FunctionStringToString<ExtractSubstringImpl<ExtractTopLevelDomain>, NameTopLevelDomain>;

void registerFunctionTopLevelDomain(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTopLevelDomain>();
}

}
