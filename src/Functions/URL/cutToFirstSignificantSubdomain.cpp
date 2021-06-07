#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include "ExtractFirstSignificantSubdomain.h"


namespace DB
{

template <bool without_www>
struct CutToFirstSignificantSubdomain
{
    static size_t getReserveLengthForElement() { return 15; }

    static void execute(const Pos data, const size_t size, Pos & res_data, size_t & res_size)
    {
        res_data = data;
        res_size = 0;

        Pos tmp_data;
        size_t tmp_length;
        Pos domain_end;
        ExtractFirstSignificantSubdomain<without_www>::execute(data, size, tmp_data, tmp_length, &domain_end);

        if (tmp_length == 0)
            return;

        res_data = tmp_data;
        res_size = domain_end - tmp_data;
    }
};

struct NameCutToFirstSignificantSubdomain { static constexpr auto name = "cutToFirstSignificantSubdomain"; };
using FunctionCutToFirstSignificantSubdomain = FunctionStringToString<ExtractSubstringImpl<CutToFirstSignificantSubdomain<true>>, NameCutToFirstSignificantSubdomain>;

struct NameCutToFirstSignificantSubdomainWithWWW { static constexpr auto name = "cutToFirstSignificantSubdomainWithWWW"; };
using FunctionCutToFirstSignificantSubdomainWithWWW = FunctionStringToString<ExtractSubstringImpl<CutToFirstSignificantSubdomain<false>>, NameCutToFirstSignificantSubdomainWithWWW>;

void registerFunctionCutToFirstSignificantSubdomain(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCutToFirstSignificantSubdomain>();
    factory.registerFunction<FunctionCutToFirstSignificantSubdomainWithWWW>();
}

}
