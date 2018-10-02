#include <Common/hex.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsURL.h>
#include <Functions/FunctionsStringSearch.h>
#include <common/find_first_symbols.h>

namespace DB
{

/// We assume that size of the dst buf isn't less than src_size.
static size_t decodeURL(const char * src, size_t src_size, char * dst)
{
    const char * src_prev_pos = src;
    const char * src_curr_pos = src;
    const char * src_end = src + src_size;
    char * dst_pos = dst;

    while (true)
    {
        src_curr_pos = find_first_symbols<'%'>(src_curr_pos, src_end);

        if (src_curr_pos == src_end)
        {
            break;
        }
        else if (src_end - src_curr_pos < 3)
        {
            src_curr_pos = src_end;
            break;
        }
        else
        {
            unsigned char high = unhex(src_curr_pos[1]);
            unsigned char low = unhex(src_curr_pos[2]);

            if (high != 0xFF && low != 0xFF)
            {
                unsigned char octet = (high << 4) + low;

                size_t bytes_to_copy = src_curr_pos - src_prev_pos;
                memcpySmallAllowReadWriteOverflow15(dst_pos, src_prev_pos, bytes_to_copy);
                dst_pos += bytes_to_copy;

                *dst_pos = octet;
                ++dst_pos;

                src_prev_pos = src_curr_pos + 3;
            }

            src_curr_pos += 3;
        }
    }

    if (src_prev_pos < src_curr_pos)
    {
        size_t bytes_to_copy = src_curr_pos - src_prev_pos;
        memcpySmallAllowReadWriteOverflow15(dst_pos, src_prev_pos, bytes_to_copy);
        dst_pos += bytes_to_copy;
    }

    return dst_pos - dst;
}


size_t ExtractProtocol::getReserveLengthForElement()
{
    return strlen("https") + 1;
}


void ExtractProtocol::execute(Pos data, size_t size, Pos & res_data, size_t & res_size)
{
    res_data = data;
    res_size = 0;

    StringRef scheme = getURLScheme(data, size);
    Pos pos = data + scheme.size;

    if (scheme.size == 0 || (data + size) - pos < 4)
        return;

    if (pos[0] == ':')
        res_size = pos - data;
}


void DecodeURLComponentImpl::vector(const ColumnString::Chars_t & data, const ColumnString::Offsets & offsets,
    ColumnString::Chars_t & res_data, ColumnString::Offsets & res_offsets)
{
    res_data.resize(data.size());
    size_t size = offsets.size();
    res_offsets.resize(size);

    size_t prev_offset = 0;
    size_t res_offset = 0;

    for (size_t i = 0; i < size; ++i)
    {
        const char * src_data = reinterpret_cast<const char *>(&data[prev_offset]);
        size_t src_size = offsets[i] - prev_offset;
        size_t dst_size = decodeURL(src_data, src_size, reinterpret_cast<char *>(res_data.data() + res_offset));

        res_offset += dst_size;
        res_offsets[i] = res_offset;
        prev_offset = offsets[i];
    }

    res_data.resize(res_offset);
}


void DecodeURLComponentImpl::vector_fixed(const ColumnString::Chars_t &, size_t, ColumnString::Chars_t &)
{
    throw Exception("Column of type FixedString is not supported by URL functions", ErrorCodes::ILLEGAL_COLUMN);
}

struct NameProtocol                       { static constexpr auto name = "protocol"; };
struct NameDomain                         { static constexpr auto name = "domain"; };
struct NameDomainWithoutWWW               { static constexpr auto name = "domainWithoutWWW"; };
struct NameFirstSignificantSubdomain      { static constexpr auto name = "firstSignificantSubdomain"; };
struct NameTopLevelDomain                 { static constexpr auto name = "topLevelDomain"; };
struct NamePath                           { static constexpr auto name = "path"; };
struct NamePathFull                       { static constexpr auto name = "pathFull"; };
struct NameQueryString                    { static constexpr auto name = "queryString"; };
struct NameFragment                       { static constexpr auto name = "fragment"; };
struct NameQueryStringAndFragment         { static constexpr auto name = "queryStringAndFragment"; };
struct NameDecodeURLComponent             { static constexpr auto name = "decodeURLComponent"; };

struct NameCutToFirstSignificantSubdomain { static constexpr auto name = "cutToFirstSignificantSubdomain"; };

struct NameCutWWW                         { static constexpr auto name = "cutWWW"; };
struct NameCutQueryString                 { static constexpr auto name = "cutQueryString"; };
struct NameCutFragment                    { static constexpr auto name = "cutFragment"; };
struct NameCutQueryStringAndFragment      { static constexpr auto name = "cutQueryStringAndFragment"; };

struct NameExtractURLParameter            { static constexpr auto name = "extractURLParameter"; };
struct NameCutURLParameter                { static constexpr auto name = "cutURLParameter"; };

using FunctionProtocol = FunctionStringToString<ExtractSubstringImpl<ExtractProtocol>, NameProtocol>;
using FunctionDomain = FunctionStringToString<ExtractSubstringImpl<ExtractDomain<false>>, NameDomain>;
using FunctionDomainWithoutWWW = FunctionStringToString<ExtractSubstringImpl<ExtractDomain<true>>, NameDomainWithoutWWW>;
using FunctionFirstSignificantSubdomain = FunctionStringToString<ExtractSubstringImpl<ExtractFirstSignificantSubdomain>, NameFirstSignificantSubdomain>;
using FunctionTopLevelDomain = FunctionStringToString<ExtractSubstringImpl<ExtractTopLevelDomain>, NameTopLevelDomain>;
using FunctionPath = FunctionStringToString<ExtractSubstringImpl<ExtractPath>, NamePath>;
using FunctionPathFull = FunctionStringToString<ExtractSubstringImpl<ExtractPathFull>, NamePathFull>;
using FunctionQueryString = FunctionStringToString<ExtractSubstringImpl<ExtractQueryString<true>>, NameQueryString>;
using FunctionFragment = FunctionStringToString<ExtractSubstringImpl<ExtractFragment<true>>, NameFragment>;
using FunctionQueryStringAndFragment = FunctionStringToString<ExtractSubstringImpl<ExtractQueryStringAndFragment<true>>, NameQueryStringAndFragment>;
using FunctionDecodeURLComponent = FunctionStringToString<DecodeURLComponentImpl, NameDecodeURLComponent>;

using FunctionCutToFirstSignificantSubdomain = FunctionStringToString<ExtractSubstringImpl<CutToFirstSignificantSubdomain>, NameCutToFirstSignificantSubdomain>;

using FunctionCutWWW = FunctionStringToString<CutSubstringImpl<ExtractWWW>, NameCutWWW>;
using FunctionCutQueryString = FunctionStringToString<CutSubstringImpl<ExtractQueryString<false>>, NameCutQueryString>;
using FunctionCutFragment = FunctionStringToString<CutSubstringImpl<ExtractFragment<false>>, NameCutFragment>;
using FunctionCutQueryStringAndFragment = FunctionStringToString<CutSubstringImpl<ExtractQueryStringAndFragment<false>>, NameCutQueryStringAndFragment>;

using FunctionExtractURLParameter = FunctionsStringSearchToString<ExtractURLParameterImpl, NameExtractURLParameter>;
using FunctionCutURLParameter = FunctionsStringSearchToString<CutURLParameterImpl, NameCutURLParameter>;
using FunctionExtractURLParameters = FunctionTokens<ExtractURLParametersImpl>;
using FunctionURLHierarchy = FunctionTokens<URLHierarchyImpl>;
using FunctionURLPathHierarchy = FunctionTokens<URLPathHierarchyImpl>;
using FunctionExtractURLParameterNames = FunctionTokens<ExtractURLParameterNamesImpl>;


void registerFunctionsURL(FunctionFactory & factory)
{
    factory.registerFunction<FunctionProtocol>();
    factory.registerFunction<FunctionDomain>();
    factory.registerFunction<FunctionDomainWithoutWWW>();
    factory.registerFunction<FunctionFirstSignificantSubdomain>();
    factory.registerFunction<FunctionTopLevelDomain>();
    factory.registerFunction<FunctionPath>();
    factory.registerFunction<FunctionPathFull>();
    factory.registerFunction<FunctionQueryString>();
    factory.registerFunction<FunctionFragment>();
    factory.registerFunction<FunctionQueryStringAndFragment>();
    factory.registerFunction<FunctionExtractURLParameter>();
    factory.registerFunction<FunctionExtractURLParameters>();
    factory.registerFunction<FunctionExtractURLParameterNames>();
    factory.registerFunction<FunctionURLHierarchy>();
    factory.registerFunction<FunctionURLPathHierarchy>();
    factory.registerFunction<FunctionCutToFirstSignificantSubdomain>();
    factory.registerFunction<FunctionCutWWW>();
    factory.registerFunction<FunctionCutQueryString>();
    factory.registerFunction<FunctionCutFragment>();
    factory.registerFunction<FunctionCutQueryStringAndFragment>();
    factory.registerFunction<FunctionCutURLParameter>();
    factory.registerFunction<FunctionDecodeURLComponent>();
}

}
