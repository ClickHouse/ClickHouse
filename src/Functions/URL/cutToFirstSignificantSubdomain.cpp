#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/URL/ExtractFirstSignificantSubdomain.h>


namespace DB
{

template <bool without_www, bool conform_rfc>
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
        ExtractFirstSignificantSubdomain<without_www, conform_rfc>::execute(data, size, tmp_data, tmp_length, &domain_end);

        if (tmp_length == 0)
            return;

        res_data = tmp_data;
        res_size = domain_end - tmp_data;
    }
};

struct NameCutToFirstSignificantSubdomain { static constexpr auto name = "cutToFirstSignificantSubdomain"; };
using FunctionCutToFirstSignificantSubdomain = FunctionStringToString<ExtractSubstringImpl<CutToFirstSignificantSubdomain<true, false>>, NameCutToFirstSignificantSubdomain>;

struct NameCutToFirstSignificantSubdomainWithWWW { static constexpr auto name = "cutToFirstSignificantSubdomainWithWWW"; };
using FunctionCutToFirstSignificantSubdomainWithWWW = FunctionStringToString<ExtractSubstringImpl<CutToFirstSignificantSubdomain<false, false>>, NameCutToFirstSignificantSubdomainWithWWW>;

struct NameCutToFirstSignificantSubdomainRFC { static constexpr auto name = "cutToFirstSignificantSubdomainRFC"; };
using FunctionCutToFirstSignificantSubdomainRFC = FunctionStringToString<ExtractSubstringImpl<CutToFirstSignificantSubdomain<true, true>>, NameCutToFirstSignificantSubdomainRFC>;

struct NameCutToFirstSignificantSubdomainWithWWWRFC { static constexpr auto name = "cutToFirstSignificantSubdomainWithWWWRFC"; };
using FunctionCutToFirstSignificantSubdomainWithWWWRFC = FunctionStringToString<ExtractSubstringImpl<CutToFirstSignificantSubdomain<false, true>>, NameCutToFirstSignificantSubdomainWithWWWRFC>;

REGISTER_FUNCTION(CutToFirstSignificantSubdomain)
{
    factory.registerFunction<FunctionCutToFirstSignificantSubdomain>(
        FunctionDocumentation{
        .description=R"(Returns the part of the domain that includes top-level subdomains up to the "first significant subdomain" (see documentation of the `firstSignificantSubdomain`).)",
        .examples{
            {"cutToFirstSignificantSubdomain1", "SELECT cutToFirstSignificantSubdomain('https://news.clickhouse.com.tr/')", ""},
            {"cutToFirstSignificantSubdomain2", "SELECT cutToFirstSignificantSubdomain('www.tr')", ""},
            {"cutToFirstSignificantSubdomain3", "SELECT cutToFirstSignificantSubdomain('tr')", ""},
        },
        .categories{"URL"}
        });
    factory.registerFunction<FunctionCutToFirstSignificantSubdomainWithWWW>(
        FunctionDocumentation{
            .description=R"(Returns the part of the domain that includes top-level subdomains up to the "first significant subdomain", without stripping "www".)",
            .examples{},
            .categories{"URL"}
        });
    factory.registerFunction<FunctionCutToFirstSignificantSubdomainRFC>(
        FunctionDocumentation{
            .description=R"(Similar to `cutToFirstSignificantSubdomain` but follows stricter rules to be compatible with RFC 3986 and less performant.)",
            .examples{},
            .categories{"URL"}
        });
    factory.registerFunction<FunctionCutToFirstSignificantSubdomainWithWWWRFC>(
        FunctionDocumentation{
            .description=R"(Similar to `cutToFirstSignificantSubdomainWithWWW` but follows stricter rules to be compatible with RFC 3986 and less performant.)",
            .examples{},
            .categories{"URL"}
        });
}

}
