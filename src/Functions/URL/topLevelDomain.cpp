#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/URL/domain.h>

namespace DB
{

template<bool conform_rfc>
struct ExtractTopLevelDomain
{
    static size_t getReserveLengthForElement() { return 5; }

    static void execute(Pos data, size_t size, Pos & res_data, size_t & res_size)
    {
        std::string_view host;
        if constexpr (conform_rfc)
            host = getURLHostRFC(data, size);
        else
            host = getURLHost(data, size);

        res_data = data;
        res_size = 0;

        if (!host.empty())
        {
            if (host[host.size() - 1] == '.')
                host.remove_suffix(1);

            const auto * host_end = host.data() + host.size();

            Pos last_dot = find_last_symbols_or_null<'.'>(host.data(), host_end);
            if (!last_dot)
                return;

            /// For IPv4 addresses select nothing.
            ///
            /// NOTE: it is safe to access last_dot[1]
            /// since getURLHost() will not return a host if there is symbol after dot.
            if (isNumericASCII(last_dot[1]))
                return;

            res_data = last_dot + 1;
            res_size = host_end - res_data;
        }
    }
};

struct NameTopLevelDomain { static constexpr auto name = "topLevelDomain"; };
using FunctionTopLevelDomain = FunctionStringToString<ExtractSubstringImpl<ExtractTopLevelDomain<false>>, NameTopLevelDomain>;

struct NameTopLevelDomainRFC { static constexpr auto name = "topLevelDomainRFC"; };
using FunctionTopLevelDomainRFC = FunctionStringToString<ExtractSubstringImpl<ExtractTopLevelDomain<true>>, NameTopLevelDomainRFC>;

REGISTER_FUNCTION(TopLevelDomain)
{
    factory.registerFunction<FunctionTopLevelDomain>(FunctionDocumentation
    {
        .description=R"(
Extracts the the top-level domain from a URL.

Returns an empty string if the argument cannot be parsed as a URL or does not contain a top-level domain.
        )",
        .examples{{"topLevelDomain", "SELECT topLevelDomain('svn+ssh://www.some.svn-hosting.com:80/repo/trunk')", ""}},
        .categories{"URL"}
    });

    factory.registerFunction<FunctionTopLevelDomainRFC>(FunctionDocumentation
    {
        .description=R"(Similar to topLevelDomain, but conforms to RFC 3986.)",
        .examples{},
        .categories{"URL"}
    });
}

}
