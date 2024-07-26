#include <Functions/FunctionFactory.h>
#include <Functions/URL/ExtractFirstSignificantSubdomain.h>
#include <Functions/URL/FirstSignificantSubdomainCustomImpl.h>

namespace DB
{

template <bool without_www, bool conform_rfc>
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
        ExtractFirstSignificantSubdomain<without_www, conform_rfc>::executeCustom(tld_lookup, data, size, tmp_data, tmp_length, &domain_end);

        if (tmp_length == 0)
            return;

        res_data = tmp_data;
        res_size = domain_end - tmp_data;
    }
};

struct NameCutToFirstSignificantSubdomainCustom { static constexpr auto name = "cutToFirstSignificantSubdomainCustom"; };
using FunctionCutToFirstSignificantSubdomainCustom = FunctionCutToFirstSignificantSubdomainCustomImpl<CutToFirstSignificantSubdomainCustom<true, false>, NameCutToFirstSignificantSubdomainCustom>;

struct NameCutToFirstSignificantSubdomainCustomWithWWW { static constexpr auto name = "cutToFirstSignificantSubdomainCustomWithWWW"; };
using FunctionCutToFirstSignificantSubdomainCustomWithWWW = FunctionCutToFirstSignificantSubdomainCustomImpl<CutToFirstSignificantSubdomainCustom<false, false>, NameCutToFirstSignificantSubdomainCustomWithWWW>;

struct NameCutToFirstSignificantSubdomainCustomRFC { static constexpr auto name = "cutToFirstSignificantSubdomainCustomRFC"; };
using FunctionCutToFirstSignificantSubdomainCustomRFC = FunctionCutToFirstSignificantSubdomainCustomImpl<CutToFirstSignificantSubdomainCustom<true, true>, NameCutToFirstSignificantSubdomainCustomRFC>;

struct NameCutToFirstSignificantSubdomainCustomWithWWWRFC { static constexpr auto name = "cutToFirstSignificantSubdomainCustomWithWWWRFC"; };
using FunctionCutToFirstSignificantSubdomainCustomWithWWWRFC = FunctionCutToFirstSignificantSubdomainCustomImpl<CutToFirstSignificantSubdomainCustom<false, true>, NameCutToFirstSignificantSubdomainCustomWithWWWRFC>;

REGISTER_FUNCTION(CutToFirstSignificantSubdomainCustom)
{
    factory.registerFunction<FunctionCutToFirstSignificantSubdomainCustom>(
        FunctionDocumentation{
        .description=R"(
Returns the part of the domain that includes top-level subdomains up to the first significant subdomain. Accepts custom TLD list name.

Can be useful if you need fresh TLD list or you have custom.
        )",
        .examples{
            {"cutToFirstSignificantSubdomainCustom", "SELECT cutToFirstSignificantSubdomainCustom('bar.foo.there-is-no-such-domain', 'public_suffix_list');", ""},
        },
        .categories{"URL"}
        });
    factory.registerFunction<FunctionCutToFirstSignificantSubdomainCustomWithWWW>(
        FunctionDocumentation{
        .description=R"(
Returns the part of the domain that includes top-level subdomains up to the first significant subdomain without stripping `www`.
Accepts custom TLD list name from config.

Can be useful if you need fresh TLD list or you have custom.
        )",
        .examples{{"cutToFirstSignificantSubdomainCustomWithWWW", "SELECT cutToFirstSignificantSubdomainCustomWithWWW('www.foo', 'public_suffix_list')", ""}},
        .categories{"URL"}
        });
    factory.registerFunction<FunctionCutToFirstSignificantSubdomainCustomRFC>(
        FunctionDocumentation{
        .description=R"(Similar to `cutToFirstSignificantSubdomainCustom` but follows stricter rules according to RFC 3986.)",
        .examples{},
        .categories{"URL"}
        });
    factory.registerFunction<FunctionCutToFirstSignificantSubdomainCustomWithWWWRFC>(
        FunctionDocumentation{
        .description=R"(Similar to `cutToFirstSignificantSubdomainCustomWithWWW` but follows stricter rules according to RFC 3986.)",
        .examples{},
        .categories{"URL"}
        });
}

}
