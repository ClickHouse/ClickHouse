#include <Parsers/Kusto/KQLFunctions.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>

#include <Common/FieldVisitorConvertToNumber.h>

#include <fmt/format.h>

#include <functional>
#include <map>


namespace DB
{

namespace
{

using Builder = std::function<ASTPtr(const ASTs &)>;

constexpr size_t VARIADIC = std::numeric_limits<size_t>::max();

struct Entry
{
    size_t min_arguments;
    size_t max_arguments;
    Builder build;
};

ASTPtr lit(Field value)
{
    return make_intrusive<ASTLiteral>(std::move(value));
}

ASTPtr litI(Int64 value)
{
    return lit(Field(value));
}

ASTPtr litS(const String & value)
{
    return lit(Field(value));
}

/// KQL treats null as the empty string when concatenating or converting.
ASTPtr asString(const ASTPtr & argument)
{
    return makeASTFunction("ifNull", makeASTFunction("toString", argument), litS(""));
}

/// The common case: the same call with a different name.
Entry rename(std::string_view clickhouse_name, size_t min_arguments, size_t max_arguments)
{
    return Entry{
        min_arguments,
        max_arguments,
        [name = String(clickhouse_name)](const ASTs & arguments)
        {
            auto function = makeASTFunction(name);
            function->arguments->children = arguments;
            return ASTPtr(function);
        }};
}

/// Datetimes are `DateTime64(7, 'UTC')` throughout: 7 digits is the 100 ns tick KQL uses.
ASTPtr toDateTime(const ASTPtr & argument)
{
    return makeASTFunction("parseDateTime64BestEffortOrNull", asString(argument), lit(static_cast<UInt64>(7)), litS("UTC"));
}

/// Builds `match(haystack, <anchored, quoted needle>)`.
///
/// The needle is quoted at *runtime* with `regexpQuoteMeta`, so a needle containing regex
/// metacharacters matches literally. This is the operator family where the old
/// implementation spliced the needle into a LIKE pattern, which is why `contains '50%'`
/// used to match '50x'.
///
/// A KQL term is a maximal run of ASCII alphanumerics, so the boundaries are spelled out
/// rather than using `\b` (which counts '_' as a word character and Kusto does not).
ASTPtr termMatch(const ASTPtr & haystack, const ASTPtr & needle, bool case_sensitive, bool anchor_left, bool anchor_right)
{
    ASTs parts;
    parts.push_back(litS(case_sensitive ? "" : "(?i)"));
    if (anchor_left)
        parts.push_back(litS("(^|[^0-9A-Za-z])"));
    parts.push_back(makeASTFunction("regexpQuoteMeta", asString(needle)));
    if (anchor_right)
        parts.push_back(litS("($|[^0-9A-Za-z])"));

    auto pattern = makeASTFunction("concat");
    pattern->arguments->children = parts;

    return makeASTFunction("match", asString(haystack), pattern);
}

/// Case-insensitive comparison in Kusto is ordinal, so lower-casing both sides matches it
/// closely enough for the substring operators.
ASTPtr foldCase(const ASTPtr & argument, bool case_sensitive)
{
    return case_sensitive ? asString(argument) : ASTPtr(makeASTFunction("lowerUTF8", asString(argument)));
}

const std::map<String, Entry> & scalarFunctions()
{
    static const std::map<String, Entry> functions = []
    {
        std::map<String, Entry> result;

        /// ---- Conditional ------------------------------------------------------------
        result.emplace("iif", rename("if", 3, 3));
        result.emplace("iff", rename("if", 3, 3));
        result.emplace("coalesce", rename("coalesce", 1, VARIADIC));
        result.emplace("isnull", rename("isNull", 1, 1));
        result.emplace("isnotnull", rename("isNotNull", 1, 1));
        result.emplace("notnull", rename("isNotNull", 1, 1));
        result.emplace(
            "case",
            Entry{
                3,
                VARIADIC,
                [](const ASTs & arguments) -> ASTPtr
                {
                    /// `case(p1, v1, ..., pn, vn, else)` - always an odd number of arguments.
                    if (arguments.size() % 2 == 0)
                        return nullptr;
                    auto function = makeASTFunction("multiIf");
                    function->arguments->children = arguments;
                    return function;
                }});
        result.emplace(
            "isempty",
            Entry{1, 1, [](const ASTs & a) -> ASTPtr { return makeASTFunction("equals", asString(a[0]), litS("")); }});
        result.emplace(
            "isnotempty",
            Entry{1, 1, [](const ASTs & a) -> ASTPtr { return makeASTFunction("notEquals", asString(a[0]), litS("")); }});
        result.emplace(
            "notempty",
            Entry{1, 1, [](const ASTs & a) -> ASTPtr { return makeASTFunction("notEquals", asString(a[0]), litS("")); }});

        /// ---- Strings ----------------------------------------------------------------
        result.emplace("strlen", rename("lengthUTF8", 1, 1));
        result.emplace("string_size", rename("lengthUTF8", 1, 1));
        result.emplace("toupper", rename("upperUTF8", 1, 1));
        result.emplace("tolower", rename("lowerUTF8", 1, 1));
        result.emplace("reverse", rename("reverseUTF8", 1, 1));
        result.emplace("trim_start", rename("trimLeft", 1, 1));
        result.emplace("trim_end", rename("trimRight", 1, 1));
        result.emplace("url_encode_component", rename("encodeURLComponent", 1, 1));
        result.emplace("url_decode", rename("decodeURLComponent", 1, 1));
        result.emplace("base64_encode_tostring", rename("base64Encode", 1, 1));
        result.emplace("base64_decode_tostring", rename("base64Decode", 1, 1));
        result.emplace("hash_md5", rename("MD5", 1, 1));
        result.emplace("hash_sha256", rename("SHA256", 1, 1));

        result.emplace(
            "strcat",
            Entry{
                1,
                VARIADIC,
                [](const ASTs & arguments) -> ASTPtr
                {
                    /// Kusto renders each argument and treats null as empty.
                    auto function = makeASTFunction("concat");
                    for (const auto & argument : arguments)
                        function->arguments->children.push_back(asString(argument));
                    return function;
                }});
        result.emplace(
            "strcat_delim",
            Entry{
                3,
                VARIADIC,
                [](const ASTs & arguments) -> ASTPtr
                {
                    auto function = makeASTFunction("concatWithSeparator");
                    function->arguments->children.push_back(asString(arguments[0]));
                    for (size_t i = 1; i < arguments.size(); ++i)
                        function->arguments->children.push_back(asString(arguments[i]));
                    return function;
                }});
        result.emplace(
            "substring",
            Entry{
                2,
                3,
                [](const ASTs & arguments) -> ASTPtr
                {
                    /// KQL indexes from 0; a negative start counts back from the end, which
                    /// ClickHouse's `substringUTF8` already does. So only the non-negative
                    /// case needs shifting.
                    ASTPtr start = makeASTFunction(
                        "if",
                        makeASTFunction("less", arguments[1], litI(0)),
                        arguments[1]->clone(),
                        makeASTFunction("plus", arguments[1]->clone(), litI(1)));
                    if (arguments.size() == 2)
                        return makeASTFunction("substringUTF8", asString(arguments[0]), start);
                    /// A negative length means "empty" in Kusto, not "count from the end".
                    ASTPtr length = makeASTFunction("greatest", arguments[2], litI(0));
                    return makeASTFunction("substringUTF8", asString(arguments[0]), start, length);
                }});
        result.emplace(
            "indexof",
            Entry{
                2,
                2,
                [](const ASTs & a) -> ASTPtr
                {
                    /// Kusto reports a 0-based offset, or -1 when absent; `position` is
                    /// 1-based and reports 0.
                    return makeASTFunction("minus", makeASTFunction("positionUTF8", asString(a[0]), asString(a[1])), litI(1));
                }});
        result.emplace(
            "split",
            Entry{
                2,
                3,
                [](const ASTs & arguments) -> ASTPtr
                {
                    /// Note the operand order: KQL is `split(source, delimiter)` and
                    /// ClickHouse is `splitByString(delimiter, source)`.
                    ASTPtr parts = makeASTFunction("splitByString", asString(arguments[1]), asString(arguments[0]));
                    if (arguments.size() == 2)
                        return parts;
                    /// The optional third argument selects one part, 0-based.
                    return makeASTFunction("arrayElement", parts, makeASTFunction("plus", arguments[2], litI(1)));
                }});
        result.emplace(
            "replace_string",
            Entry{
                3,
                3,
                [](const ASTs & a) -> ASTPtr
                { return makeASTFunction("replaceAll", asString(a[0]), asString(a[1]), asString(a[2])); }});
        result.emplace(
            "replace_regex",
            Entry{
                3,
                3,
                [](const ASTs & a) -> ASTPtr
                { return makeASTFunction("replaceRegexpAll", asString(a[0]), asString(a[1]), asString(a[2])); }});
        result.emplace(
            "strrep",
            Entry{
                2,
                3,
                [](const ASTs & arguments) -> ASTPtr
                {
                    if (arguments.size() == 2)
                        return makeASTFunction("repeat", asString(arguments[0]), arguments[1]);
                    /// With a delimiter the result is a join, not a plain repeat.
                    return makeASTFunction(
                        "arrayStringConcat",
                        makeASTFunction("arrayMap", makeASTFunction("lambda", makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x")), asString(arguments[0])),
                                        makeASTFunction("range", arguments[1])),
                        asString(arguments[2]));
                }});
        result.emplace(
            "countof",
            Entry{
                2,
                2,
                [](const ASTs & a) -> ASTPtr { return makeASTFunction("countSubstrings", asString(a[0]), asString(a[1])); }});
        result.emplace(
            "extract",
            Entry{
                3,
                3,
                [](const ASTs & arguments) -> ASTPtr
                {
                    /// `extract(regex, captureGroup, text)`. Group 0 is the whole match, so
                    /// `extractGroups` (which starts at group 1) needs the index as written.
                    const auto * group = arguments[1]->as<ASTLiteral>();
                    if (!group || !isInt64OrUInt64FieldType(group->value.getType()))
                        return nullptr;
                    const Int64 index = applyVisitor(FieldVisitorConvertToNumber<Int64>(), group->value);
                    if (index < 0)
                        return nullptr;
                    if (index == 0)
                        return makeASTFunction(
                            "arrayElement",
                            makeASTFunction(
                                "extractGroups",
                                asString(arguments[2]),
                                makeASTFunction("concat", litS("("), asString(arguments[0]), litS(")"))),
                            litI(1));
                    return makeASTFunction(
                        "arrayElement",
                        makeASTFunction("extractGroups", asString(arguments[2]), asString(arguments[0])),
                        litI(index));
                }});
        result.emplace(
            "extract_all",
            Entry{
                2,
                2,
                [](const ASTs & a) -> ASTPtr { return makeASTFunction("extractAll", asString(a[1]), asString(a[0])); }});
        result.emplace(
            "trim",
            Entry{
                2,
                2,
                [](const ASTs & arguments) -> ASTPtr
                {
                    /// `trim(regex, text)` strips a leading and a trailing match. The pattern
                    /// is anchored at runtime with `concat`, so a user-supplied regex stays
                    /// data rather than becoming part of a larger expression's syntax.
                    ASTPtr leading = makeASTFunction("concat", litS("^("), asString(arguments[0]), litS(")"));
                    ASTPtr trailing = makeASTFunction("concat", litS("("), asString(arguments[0]), litS(")$"));
                    return makeASTFunction(
                        "replaceRegexpOne",
                        makeASTFunction("replaceRegexpOne", asString(arguments[1]), leading, litS("")),
                        trailing,
                        litS(""));
                }});

        /// ---- Casts ------------------------------------------------------------------
        /// Kusto's `to*` functions yield null on failure rather than raising.
        result.emplace("toint", rename("toInt32OrNull", 1, 1));
        result.emplace("tolong", rename("toInt64OrNull", 1, 1));
        result.emplace("todouble", rename("toFloat64OrNull", 1, 1));
        result.emplace("toreal", rename("toFloat64OrNull", 1, 1));
        result.emplace(
            "todecimal",
            Entry{
                1,
                1,
                [](const ASTs & a) -> ASTPtr
                { return makeASTFunction("accurateCastOrNull", asString(a[0]), litS("Decimal128(20)")); }});
        result.emplace("toguid", rename("toUUIDOrNull", 1, 1));
        result.emplace("tostring", Entry{1, 1, [](const ASTs & a) -> ASTPtr { return asString(a[0]); }});
        result.emplace(
            "tobool",
            Entry{1, 1, [](const ASTs & a) -> ASTPtr { return makeASTFunction("accurateCastOrNull", a[0], litS("Bool")); }});
        result.emplace("todatetime", Entry{1, 1, [](const ASTs & a) -> ASTPtr { return toDateTime(a[0]); }});

        /// ---- Maths ------------------------------------------------------------------
        result.emplace("abs", rename("abs", 1, 1));
        result.emplace("ceiling", rename("ceil", 1, 1));
        result.emplace("floor", rename("floor", 1, 2));
        result.emplace("exp", rename("exp", 1, 1));
        result.emplace("exp2", rename("exp2", 1, 1));
        result.emplace("exp10", rename("exp10", 1, 1));
        result.emplace("log", rename("log", 1, 1));
        result.emplace("log2", rename("log2", 1, 1));
        result.emplace("log10", rename("log10", 1, 1));
        result.emplace("pow", rename("pow", 2, 2));
        result.emplace("sqrt", rename("sqrt", 1, 1));
        result.emplace("sign", rename("sign", 1, 1));
        result.emplace("round", rename("round", 1, 2));
        result.emplace("isnan", rename("isNaN", 1, 1));
        result.emplace("isinf", rename("isInfinite", 1, 1));
        result.emplace("isfinite", rename("isFinite", 1, 1));
        result.emplace("rand", rename("rand", 0, 1));
        result.emplace("max_of", rename("greatest", 2, VARIADIC));
        result.emplace("min_of", rename("least", 2, VARIADIC));
        result.emplace("bin", rename("kqlBin", 2, 2));
        result.emplace("floor_of", rename("kqlBin", 2, 2));
        result.emplace("bin_at", rename("kqlBinAt", 3, 3));

        /// ---- Dates and times --------------------------------------------------------
        result.emplace("now", Entry{0, 0, [](const ASTs &) -> ASTPtr { return makeASTFunction("now64", litI(7), litS("UTC")); }});
        result.emplace(
            "ago",
            Entry{1, 1, [](const ASTs & a) -> ASTPtr { return makeASTFunction("minus", makeASTFunction("now64", litI(7), litS("UTC")), a[0]); }});
        result.emplace("startofday", Entry{1, 1, [](const ASTs & a) -> ASTPtr { return makeASTFunction("toStartOfDay", a[0]); }});
        result.emplace("startofmonth", Entry{1, 1, [](const ASTs & a) -> ASTPtr { return makeASTFunction("toStartOfMonth", a[0]); }});
        result.emplace("startofyear", Entry{1, 1, [](const ASTs & a) -> ASTPtr { return makeASTFunction("toStartOfYear", a[0]); }});
        result.emplace("startofweek", Entry{1, 1, [](const ASTs & a) -> ASTPtr { return makeASTFunction("toStartOfWeek", a[0], litI(0)); }});
        result.emplace("getyear", Entry{1, 1, [](const ASTs & a) -> ASTPtr { return makeASTFunction("toYear", a[0]); }});
        result.emplace("getmonth", Entry{1, 1, [](const ASTs & a) -> ASTPtr { return makeASTFunction("toMonth", a[0]); }});
        result.emplace("monthofyear", Entry{1, 1, [](const ASTs & a) -> ASTPtr { return makeASTFunction("toMonth", a[0]); }});
        result.emplace("dayofmonth", Entry{1, 1, [](const ASTs & a) -> ASTPtr { return makeASTFunction("toDayOfMonth", a[0]); }});
        result.emplace("dayofyear", Entry{1, 1, [](const ASTs & a) -> ASTPtr { return makeASTFunction("toDayOfYear", a[0]); }});
        result.emplace("hourofday", Entry{1, 1, [](const ASTs & a) -> ASTPtr { return makeASTFunction("toHour", a[0]); }});
        result.emplace("weekofyear", Entry{1, 1, [](const ASTs & a) -> ASTPtr { return makeASTFunction("toISOWeek", a[0]); }});
        result.emplace(
            "dayofweek",
            Entry{
                1,
                1,
                [](const ASTs & a) -> ASTPtr
                {
                    /// Kusto counts from Sunday = 0; ClickHouse's `toDayOfWeek` counts from
                    /// Monday = 1.
                    return makeASTFunction("modulo", makeASTFunction("toDayOfWeek", a[0]), litI(7));
                }});
        result.emplace(
            "unixtime_seconds_todatetime",
            Entry{1, 1, [](const ASTs & a) -> ASTPtr { return makeASTFunction("toDateTime64", a[0], litI(7), litS("UTC")); }});
        result.emplace(
            "unixtime_milliseconds_todatetime",
            Entry{
                1,
                1,
                [](const ASTs & a) -> ASTPtr
                { return makeASTFunction("toDateTime64", makeASTFunction("divide", a[0], litI(1000)), litI(7), litS("UTC")); }});
        result.emplace(
            "datetime_diff",
            Entry{
                3,
                3,
                [](const ASTs & arguments) -> ASTPtr
                {
                    /// `datetime_diff(unit, later, earlier)`; ClickHouse's `dateDiff` takes
                    /// the same unit names but the earlier date first.
                    const auto * unit = arguments[0]->as<ASTLiteral>();
                    if (!unit || unit->value.getType() != Field::Types::String)
                        return nullptr;
                    return makeASTFunction("dateDiff", arguments[0], arguments[2], arguments[1]);
                }});
        result.emplace(
            "format_datetime",
            Entry{2, 2, [](const ASTs & a) -> ASTPtr { return makeASTFunction("formatDateTime", a[0], asString(a[1])); }});
        result.emplace(
            "make_datetime",
            Entry{
                3,
                6,
                [](const ASTs & arguments) -> ASTPtr
                {
                    ASTs parts(arguments.begin(), arguments.end());
                    while (parts.size() < 6)
                        parts.push_back(litI(0));
                    auto function = makeASTFunction("makeDateTime64");
                    function->arguments->children = {parts[0], parts[1], parts[2], parts[3], parts[4], parts[5], litI(0), litI(7), litS("UTC")};
                    return ASTPtr(function);
                }});

        /// ---- Dynamic (arrays) -------------------------------------------------------
        result.emplace("array_length", rename("length", 1, 1));
        result.emplace("array_sum", rename("arraySum", 1, 1));
        result.emplace("array_concat", rename("arrayConcat", 2, VARIADIC));
        result.emplace("array_reverse", rename("reverse", 1, 1));
        result.emplace(
            "set_union",
            Entry{
                1,
                VARIADIC,
                [](const ASTs & arguments) -> ASTPtr
                {
                    auto concatenated = makeASTFunction("arrayConcat");
                    concatenated->arguments->children = arguments;
                    return makeASTFunction("arrayDistinct", concatenated);
                }});
        result.emplace(
            "array_index_of",
            Entry{
                2,
                2,
                [](const ASTs & a) -> ASTPtr
                { return makeASTFunction("minus", makeASTFunction("indexOf", a[0], a[1]), litI(1)); }});
        result.emplace(
            "array_slice",
            Entry{
                3,
                3,
                [](const ASTs & a) -> ASTPtr
                {
                    /// KQL takes 0-based inclusive bounds; `arraySlice` takes a 1-based
                    /// offset and a length.
                    ASTPtr length = makeASTFunction("greatest", makeASTFunction("minus", makeASTFunction("plus", a[2], litI(1)), a[1]), litI(0));
                    return makeASTFunction("arraySlice", a[0], makeASTFunction("plus", a[1], litI(1)), length);
                }});
        result.emplace(
            "set_has_element",
            Entry{2, 2, [](const ASTs & a) -> ASTPtr { return makeASTFunction("has", a[0], a[1]); }});
        result.emplace(
            "array_iff",
            Entry{
                3,
                3,
                [](const ASTs & a) -> ASTPtr
                {
                    return makeASTFunction(
                        "arrayMap",
                        makeASTFunction(
                            "lambda",
                            makeASTFunction("tuple", make_intrusive<ASTIdentifier>("c"), make_intrusive<ASTIdentifier>("t"), make_intrusive<ASTIdentifier>("f")),
                            makeASTFunction("if", make_intrusive<ASTIdentifier>("c"), make_intrusive<ASTIdentifier>("t"), make_intrusive<ASTIdentifier>("f"))),
                        a[0],
                        a[1],
                        a[2]);
                }});

        return result;
    }();
    return functions;
}

const std::map<String, Entry> & aggregateFunctions()
{
    static const std::map<String, Entry> functions = []
    {
        std::map<String, Entry> result;

        result.emplace("count", rename("count", 0, 1));
        result.emplace("sum", rename("sum", 1, 1));
        result.emplace("avg", rename("avg", 1, 1));
        result.emplace("min", rename("min", 1, 1));
        result.emplace("max", rename("max", 1, 1));
        result.emplace("stdev", rename("stddevSamp", 1, 1));
        result.emplace("variance", rename("varSamp", 1, 1));
        result.emplace("make_list", rename("groupArray", 1, 1));
        result.emplace("make_set", rename("groupUniqArray", 1, 1));
        result.emplace("take_any", rename("any", 1, 1));
        result.emplace("dcount", rename("uniq", 1, 1));
        result.emplace("arg_max", rename("argMax", 2, 2));
        result.emplace("arg_min", rename("argMin", 2, 2));

        /// The `*if` family: `sumif(value, predicate)` maps onto ClickHouse's `-If` combinator.
        const auto conditional = [](std::string_view base)
        {
            return Entry{
                2,
                2,
                [name = String(base) + "If"](const ASTs & arguments) -> ASTPtr
                { return makeASTFunction(name, arguments[0], arguments[1]); }};
        };
        result.emplace("sumif", conditional("sum"));
        result.emplace("avgif", conditional("avg"));
        result.emplace("minif", conditional("min"));
        result.emplace("maxif", conditional("max"));
        result.emplace("stdevif", conditional("stddevSamp"));
        result.emplace("varianceif", conditional("varSamp"));
        result.emplace("dcountif", conditional("uniq"));
        result.emplace("take_anyif", conditional("any"));
        result.emplace(
            "countif", Entry{1, 1, [](const ASTs & a) -> ASTPtr { return makeASTFunction("countIf", a[0]); }});

        result.emplace(
            "percentile",
            Entry{
                2,
                2,
                [](const ASTs & arguments) -> ASTPtr
                {
                    /// Kusto's percentile is on a 0..100 scale; `quantile` is on 0..1.
                    const auto * level = arguments[1]->as<ASTLiteral>();
                    if (!level)
                        return nullptr;
                    const Float64 value = applyVisitor(FieldVisitorConvertToNumber<Float64>(), level->value);
                    if (!(value >= 0 && value <= 100))
                        return nullptr;
                    auto quantile = makeASTFunction("quantile", arguments[0]);
                    quantile->parameters = make_intrusive<ASTExpressionList>();
                    quantile->parameters->children.push_back(lit(value / 100.0));
                    quantile->children.push_back(quantile->parameters);
                    return ASTPtr(quantile);
                }});

        return result;
    }();
    return functions;
}

}

bool isKQLAggregateFunction(const String & name)
{
    return aggregateFunctions().contains(name);
}

ASTPtr buildKQLStringOperator(const String & op, const ASTPtr & haystack, const ASTPtr & needle, String & error)
{
    /// Substring tests use `position`, not a LIKE pattern, so `%` and `_` in the needle are
    /// ordinary characters.
    if (op == "contains" || op == "contains_cs")
    {
        const bool cs = op == "contains_cs";
        return makeASTFunction(
            "greater", makeASTFunction("positionUTF8", foldCase(haystack, cs), foldCase(needle, cs)), litI(0));
    }

    if (op == "startswith" || op == "startswith_cs")
    {
        const bool cs = op == "startswith_cs";
        return makeASTFunction("startsWith", foldCase(haystack, cs), foldCase(needle, cs));
    }

    if (op == "endswith" || op == "endswith_cs")
    {
        const bool cs = op == "endswith_cs";
        return makeASTFunction("endsWith", foldCase(haystack, cs), foldCase(needle, cs));
    }

    /// The term family. A KQL term is a maximal run of ASCII alphanumerics.
    if (op == "has" || op == "has_cs")
        return termMatch(haystack, needle, op == "has_cs", true, true);
    if (op == "hasprefix" || op == "hasprefix_cs")
        return termMatch(haystack, needle, op == "hasprefix_cs", true, false);
    if (op == "hassuffix" || op == "hassuffix_cs")
        return termMatch(haystack, needle, op == "hassuffix_cs", false, true);

    error = fmt::format("'{}' is not a supported KQL operator", op);
    return nullptr;
}

ASTPtr translateKQLFunction(const String & name, const ASTs & arguments, String & error)
{
    const auto * entry = [&]() -> const Entry *
    {
        if (auto it = scalarFunctions().find(name); it != scalarFunctions().end())
            return &it->second;
        if (auto it = aggregateFunctions().find(name); it != aggregateFunctions().end())
            return &it->second;
        return nullptr;
    }();

    if (!entry)
    {
        error = fmt::format("'{}' is not a supported KQL function", name);
        return nullptr;
    }

    if (arguments.size() < entry->min_arguments || arguments.size() > entry->max_arguments)
    {
        if (entry->min_arguments == entry->max_arguments)
            error = fmt::format("'{}' takes {} argument(s), got {}", name, entry->min_arguments, arguments.size());
        else if (entry->max_arguments == VARIADIC)
            error = fmt::format("'{}' takes at least {} argument(s), got {}", name, entry->min_arguments, arguments.size());
        else
            error = fmt::format(
                "'{}' takes between {} and {} arguments, got {}", name, entry->min_arguments, entry->max_arguments, arguments.size());
        return nullptr;
    }

    ASTPtr result = entry->build(arguments);
    if (!result)
    {
        error = fmt::format("'{}' was called with arguments it does not support", name);
        return nullptr;
    }
    return result;
}

}
