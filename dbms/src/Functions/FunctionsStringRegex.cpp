#include "FunctionsStringRegex.h"
#include "FunctionsStringSearch.h"
#include "FunctionsMultiStringSearch.h"
#include "FunctionsStringSearchToString.h"
#include <Columns/ColumnFixedString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/Regexps.h>
#include <IO/WriteHelpers.h>
#include <re2/re2.h>
#include <re2/stringpiece.h>
#include <Poco/UTF8String.h>
#include <Common/Volnitsky.h>
#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "config_functions.h"
#if USE_HYPERSCAN
#    if __has_include(<hs/hs.h>)
#        include <hs/hs.h>
#    else
#        include <hs.h>
#    endif
#endif

#include <Common/config.h>
#if USE_RE2_ST
#    include <re2_st/re2.h>
#else
#    define re2_st re2
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
    extern const int TOO_MANY_BYTES;
    extern const int NOT_IMPLEMENTED;
    extern const int HYPERSCAN_CANNOT_SCAN_TEXT;
}


struct ExtractImpl
{
    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        const std::string & pattern,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(data.size() / 5);
        res_offsets.resize(offsets.size());

        const auto & regexp = Regexps::get<false, false>(pattern);

        unsigned capture = regexp->getNumberOfSubpatterns() > 0 ? 1 : 0;
        OptimizedRegularExpression::MatchVec matches;
        matches.reserve(capture + 1);
        size_t prev_offset = 0;
        size_t res_offset = 0;

        for (size_t i = 0; i < offsets.size(); ++i)
        {
            size_t cur_offset = offsets[i];

            unsigned count
                = regexp->match(reinterpret_cast<const char *>(&data[prev_offset]), cur_offset - prev_offset - 1, matches, capture + 1);
            if (count > capture && matches[capture].offset != std::string::npos)
            {
                const auto & match = matches[capture];
                res_data.resize(res_offset + match.length + 1);
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], &data[prev_offset + match.offset], match.length);
                res_offset += match.length;
            }
            else
            {
                res_data.resize(res_offset + 1);
            }

            res_data[res_offset] = 0;
            ++res_offset;
            res_offsets[i] = res_offset;

            prev_offset = cur_offset;
        }
    }
};


struct NameMatch
{
    static constexpr auto name = "match";
};
struct NameLike
{
    static constexpr auto name = "like";
};
struct NameNotLike
{
    static constexpr auto name = "notLike";
};
struct NameMultiMatchAny
{
    static constexpr auto name = "multiMatchAny";
};
struct NameMultiMatchAnyIndex
{
    static constexpr auto name = "multiMatchAnyIndex";
};
struct NameMultiMatchAllIndices
{
    static constexpr auto name = "multiMatchAllIndices";
};
struct NameMultiFuzzyMatchAny
{
    static constexpr auto name = "multiFuzzyMatchAny";
};
struct NameMultiFuzzyMatchAnyIndex
{
    static constexpr auto name = "multiFuzzyMatchAnyIndex";
};
struct NameMultiFuzzyMatchAllIndices
{
    static constexpr auto name = "multiFuzzyMatchAllIndices";
};
struct NameExtract
{
    static constexpr auto name = "extract";
};
struct NameReplaceOne
{
    static constexpr auto name = "replaceOne";
};
struct NameReplaceAll
{
    static constexpr auto name = "replaceAll";
};
struct NameReplaceRegexpOne
{
    static constexpr auto name = "replaceRegexpOne";
};
struct NameReplaceRegexpAll
{
    static constexpr auto name = "replaceRegexpAll";
};


using FunctionMatch = FunctionsStringSearch<MatchImpl<false>, NameMatch>;

using FunctionMultiMatchAny = FunctionsMultiStringSearch<
    MultiMatchAnyImpl<UInt8, true, false, false>,
    NameMultiMatchAny,
    std::numeric_limits<UInt32>::max()>;

using FunctionMultiMatchAnyIndex = FunctionsMultiStringSearch<
    MultiMatchAnyImpl<UInt64, false, true, false>,
    NameMultiMatchAnyIndex,
    std::numeric_limits<UInt32>::max()>;

using FunctionMultiMatchAllIndices = FunctionsMultiStringSearch<
    MultiMatchAllIndicesImpl<UInt64, false>,
    NameMultiMatchAllIndices,
    std::numeric_limits<UInt32>::max()>;

using FunctionMultiFuzzyMatchAny = FunctionsMultiStringFuzzySearch<
    MultiMatchAnyImpl<UInt8, true, false, true>,
    NameMultiFuzzyMatchAny,
    std::numeric_limits<UInt32>::max()>;

using FunctionMultiFuzzyMatchAnyIndex = FunctionsMultiStringFuzzySearch<
    MultiMatchAnyImpl<UInt64, false, true, true>,
    NameMultiFuzzyMatchAnyIndex,
    std::numeric_limits<UInt32>::max()>;

using FunctionMultiFuzzyMatchAllIndices = FunctionsMultiStringFuzzySearch<
    MultiMatchAllIndicesImpl<UInt64, true>,
    NameMultiFuzzyMatchAllIndices,
    std::numeric_limits<UInt32>::max()>;

using FunctionLike = FunctionsStringSearch<MatchImpl<true>, NameLike>;
using FunctionNotLike = FunctionsStringSearch<MatchImpl<true, true>, NameNotLike>;
using FunctionExtract = FunctionsStringSearchToString<ExtractImpl, NameExtract>;
using FunctionReplaceOne = FunctionStringReplace<ReplaceStringImpl<true>, NameReplaceOne>;
using FunctionReplaceAll = FunctionStringReplace<ReplaceStringImpl<false>, NameReplaceAll>;
using FunctionReplaceRegexpOne = FunctionStringReplace<ReplaceRegexpImpl<true>, NameReplaceRegexpOne>;
using FunctionReplaceRegexpAll = FunctionStringReplace<ReplaceRegexpImpl<false>, NameReplaceRegexpAll>;

void registerFunctionsStringRegex(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMatch>();
    factory.registerFunction<FunctionLike>();
    factory.registerFunction<FunctionNotLike>();
    factory.registerFunction<FunctionExtract>();

    factory.registerFunction<FunctionReplaceOne>();
    factory.registerFunction<FunctionReplaceAll>();
    factory.registerFunction<FunctionReplaceRegexpOne>();
    factory.registerFunction<FunctionReplaceRegexpAll>();

    factory.registerFunction<FunctionMultiMatchAny>();
    factory.registerFunction<FunctionMultiMatchAnyIndex>();
    factory.registerFunction<FunctionMultiMatchAllIndices>();
    factory.registerFunction<FunctionMultiFuzzyMatchAny>();
    factory.registerFunction<FunctionMultiFuzzyMatchAnyIndex>();
    factory.registerFunction<FunctionMultiFuzzyMatchAllIndices>();
    factory.registerAlias("replace", NameReplaceAll::name, FunctionFactory::CaseInsensitive);
}
}
