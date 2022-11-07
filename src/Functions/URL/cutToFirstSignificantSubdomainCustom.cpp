#include <Functions/FunctionFactory.h>
#include "ExtractFirstSignificantSubdomain.h"
#include "FirstSignificantSubdomainCustomImpl.h"

namespace DB
{

template <bool without_www>
struct CutToFirstSignificantSubdomainCustom
{
    static size_t getReserveLengthForElement() { return 15; }

    static void execute(FirstSignificantSubdomainCustomLookup & tld_lookup, const Pos data, const size_t size, Pos & res_data, size_t & res_size)
    {
        res_data = data;
        res_size = 0;

        Pos tmp_data;
        size_t tmp_length;
        Pos domain_end;
        ExtractFirstSignificantSubdomain<without_www>::executeCustom(tld_lookup, data, size, tmp_data, tmp_length, &domain_end);

        if (tmp_length == 0)
            return;

        res_data = tmp_data;
        res_size = domain_end - tmp_data;
    }
};

struct NameCutToFirstSignificantSubdomainCustom { static constexpr auto name = "cutToFirstSignificantSubdomainCustom"; };
using FunctionCutToFirstSignificantSubdomainCustom = FunctionCutToFirstSignificantSubdomainCustomImpl<CutToFirstSignificantSubdomainCustom<true>, NameCutToFirstSignificantSubdomainCustom>;

struct NameCutToFirstSignificantSubdomainCustomWithWWW { static constexpr auto name = "cutToFirstSignificantSubdomainCustomWithWWW"; };
using FunctionCutToFirstSignificantSubdomainCustomWithWWW = FunctionCutToFirstSignificantSubdomainCustomImpl<CutToFirstSignificantSubdomainCustom<false>, NameCutToFirstSignificantSubdomainCustomWithWWW>;

REGISTER_FUNCTION(CutToFirstSignificantSubdomainCustom)
{
    factory.registerFunction<FunctionCutToFirstSignificantSubdomainCustom>();
    factory.registerFunction<FunctionCutToFirstSignificantSubdomainCustomWithWWW>();
}

}
