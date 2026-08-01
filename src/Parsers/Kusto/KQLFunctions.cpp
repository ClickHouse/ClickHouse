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
                3,
                [](const ASTs & arguments) -> ASTPtr
                {
                    /// Kusto reports a 0-based offset, or -1 when absent; `position` is
                    /// 1-based and reports 0. The optional third argument is where to start,
                    /// also 0-based.
                    auto position = makeASTFunction("positionUTF8", asString(arguments[0]), asString(arguments[1]));
                    if (arguments.size() == 3)
                        /// `positionUTF8` wants an unsigned start offset, and a literal
                        /// small enough to infer as Int16 would not do.
                        position = makeASTFunction(
                            "positionUTF8",
                            asString(arguments[0]),
                            asString(arguments[1]),
                            makeASTFunction(
                                "toUInt64", makeASTFunction("plus", makeASTFunction("greatest", arguments[2], litI(0)), litI(1))));
                    return makeASTFunction("minus", position, litI(1));
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
                    ASTPtr parts = makeASTFunction(
                        "if",
                        makeASTFunction("equals", asString(arguments[1]), litS("")),
                        makeASTFunction("array", asString(arguments[0])),
                        makeASTFunction("splitByString", asString(arguments[1]), asString(arguments[0])));
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
                3,
                [](const ASTs & arguments) -> ASTPtr
                {
                    /// The optional `kind` is 'normal' (a plain substring) or 'regex'.
                    bool regex = false;
                    if (arguments.size() == 3)
                    {
                        const auto * kind = arguments[2]->as<ASTLiteral>();
                        if (!kind || kind->value.getType() != Field::Types::String)
                            return nullptr;
                        const String text = kind->value.safeGet<String>();
                        if (text == "regex")
                            regex = true;
                        else if (text != "normal")
                            return nullptr;
                    }
                    return makeASTFunction(
                        regex ? "countMatches" : "countSubstrings", asString(arguments[0]), asString(arguments[1]));
                }});
        result.emplace(
            "extract",
            Entry{
                3,
                4,
                [](const ASTs & arguments) -> ASTPtr
                {
                    /// `extract(regex, captureGroup, text [, typeof(T)])`. Group 0 is the whole match, so
                    /// `extractGroups` (which starts at group 1) needs the index as written.
                    const auto * group = arguments[1]->as<ASTLiteral>();
                    if (!group || !isInt64OrUInt64FieldType(group->value.getType()))
                        return nullptr;
                    const Int64 index = applyVisitor(FieldVisitorConvertToNumber<Int64>(), group->value);
                    if (index < 0)
                        return nullptr;
                    ASTPtr captured;
                    if (index == 0)
                        captured = makeASTFunction(
                            "arrayElement",
                            makeASTFunction(
                                "extractGroups",
                                asString(arguments[2]),
                                makeASTFunction("concat", litS("("), asString(arguments[0]), litS(")"))),
                            litI(1));
                    else
                        captured = makeASTFunction(
                            "arrayElement",
                            makeASTFunction("extractGroups", asString(arguments[2]), asString(arguments[0])),
                            litI(index));

                    if (arguments.size() == 3)
                        return captured;

                    /// The optional last argument is `typeof(T)`, which the parser has already
                    /// turned into the ClickHouse type name.
                    const auto * type = arguments[3]->as<ASTLiteral>();
                    if (!type || type->value.getType() != Field::Types::String)
                        return nullptr;
                    return makeASTFunction("accurateCastOrNull", captured, arguments[3]);
                }});
        /// `trim(regex, text)` strips a leading and a trailing match; `trim_start` and
        /// `trim_end` strip only one end. The pattern is anchored at runtime with `concat`,
        /// so a user-supplied regex stays data rather than becoming part of the syntax.
        const auto trimmer = [](bool leading, bool trailing)
        {
            return Entry{
                2,
                2,
                [leading, trailing](const ASTs & arguments) -> ASTPtr
                {
                    ASTPtr text = asString(arguments[1]);
                    if (leading)
                        text = makeASTFunction(
                            "replaceRegexpOne",
                            text,
                            makeASTFunction("concat", litS("^("), asString(arguments[0]), litS(")")),
                            litS(""));
                    if (trailing)
                        text = makeASTFunction(
                            "replaceRegexpOne",
                            text,
                            makeASTFunction("concat", litS("("), asString(arguments[0]), litS(")$")),
                            litS(""));
                    return text;
                }};
        };
        result.emplace("trim_start", trimmer(true, false));
        result.emplace("trim_end", trimmer(false, true));
        result.emplace("trim", trimmer(true, true));

        /// The string operators also written as calls: `endswith(a, b)` beside `a endswith b`.
        /// Kusto documents only the operator form, but it costs nothing to accept both, and
        /// the previous implementation accepted the call form.
        for (const auto * op : {"contains", "contains_cs", "startswith", "startswith_cs", "endswith",
                                "endswith_cs", "has", "has_cs", "hasprefix", "hasprefix_cs",
                                "hassuffix", "hassuffix_cs"})
        {
            result.emplace(
                op,
                Entry{
                    2,
                    2,
                    [name = String(op)](const ASTs & arguments) -> ASTPtr
                    {
                        String ignored;
                        return buildKQLStringOperator(name, arguments[0], arguments[1], ignored);
                    }});
        }

        /// ---- Casts ------------------------------------------------------------------
        /// Kusto's `to*` functions yield null on failure rather than raising.
        /// Kusto reads a `0x` prefix as hexadecimal, which the plain converters do not.
        const auto to_integer = [](std::string_view target)
        {
            return Entry{
                1,
                1,
                [name = String(target)](const ASTs & arguments) -> ASTPtr
                {
                    ASTPtr text = asString(arguments[0]);
                    ASTPtr hex_digits = makeASTFunction("substring", text, litI(3));
                    ASTPtr from_hex = makeASTFunction(
                        "reinterpretAsUInt64",
                        makeASTFunction("reverse", makeASTFunction("unhex", makeASTFunction("lpad", hex_digits, litI(16), litS("0")))));
                    return makeASTFunction(
                        "if",
                        makeASTFunction("startsWith", makeASTFunction("lowerUTF8", text), litS("0x")),
                        makeASTFunction("accurateCastOrNull", from_hex, litS(name)),
                        makeASTFunction("accurateCastOrNull", text, litS(name)));
                }};
        };
        result.emplace("toint", to_integer("Nullable(Int32)"));
        result.emplace("tolong", to_integer("Nullable(Int64)"));
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
            "toboolean",
            Entry{1, 1, [](const ASTs & a) -> ASTPtr { return makeASTFunction("accurateCastOrNull", a[0], litS("Bool")); }});
        result.emplace(
            "tobool",
            Entry{1, 1, [](const ASTs & a) -> ASTPtr { return makeASTFunction("accurateCastOrNull", a[0], litS("Bool")); }});
        result.emplace("todatetime", Entry{1, 1, [](const ASTs & a) -> ASTPtr { return toDateTime(a[0]); }});

        /// ---- Maths ------------------------------------------------------------------
        result.emplace("abs", rename("abs", 1, 1));
        result.emplace("ceiling", rename("ceil", 1, 1));
        result.emplace(
            "floor",
            Entry{
                1,
                2,
                [](const ASTs & arguments) -> ASTPtr
                {
                    /// With a second argument Kusto's `floor` is an alias for `bin`, which
                    /// rounds down to a multiple - not ClickHouse's `floor(x, precision)`,
                    /// which rounds to a number of decimal places.
                    if (arguments.size() == 1)
                        return makeASTFunction("floor", arguments[0]);
                    return makeASTFunction("kqlBin", arguments[0], arguments[1]);
                }});
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
        result.emplace("bin_at", rename("kqlBinAt", 3, 3));

        /// ---- Dates and times --------------------------------------------------------
        /// `now([offset])` and the `startof*`/`endof*` family all take an optional offset.
        result.emplace(
            "now",
            Entry{
                0,
                1,
                [](const ASTs & arguments) -> ASTPtr
                {
                    ASTPtr now = makeASTFunction("now64", litI(7), litS("UTC"));
                    return arguments.empty() ? now : ASTPtr(makeASTFunction("plus", now, arguments[0]));
                }});
        result.emplace(
            "ago",
            Entry{1, 1, [](const ASTs & a) -> ASTPtr { return makeASTFunction("minus", makeASTFunction("now64", litI(7), litS("UTC")), a[0]); }});
        /// `startofday(t, -1)` means the start of the day *before* `t`; the offset counts
        /// whole periods, so it is applied before truncating.
        const auto start_of = [](std::string_view truncate, std::string_view unit)
        {
            return Entry{
                1,
                2,
                [name = String(truncate), interval = String(unit)](const ASTs & arguments) -> ASTPtr
                {
                    ASTPtr moment = arguments[0];
                    if (arguments.size() == 2)
                        moment = makeASTFunction("plus", moment, makeASTFunction(interval, arguments[1]));
                    if (name == "toStartOfWeek")
                        return makeASTFunction(name, moment, litI(0));
                    return makeASTFunction(name, moment);
                }};
        };
        result.emplace("startofday", start_of("toStartOfDay", "toIntervalDay"));
        result.emplace("startofweek", start_of("toStartOfWeek", "toIntervalWeek"));
        result.emplace("startofmonth", start_of("toStartOfMonth", "toIntervalMonth"));
        result.emplace("startofyear", start_of("toStartOfYear", "toIntervalYear"));

        /// `endofday(t)` is the last tick of the period, which Kusto renders as
        /// `...T23:59:59.9999999`.
        const auto end_of = [](std::string_view truncate, std::string_view unit)
        {
            return Entry{
                1,
                2,
                [name = String(truncate), interval = String(unit)](const ASTs & arguments) -> ASTPtr
                {
                    ASTPtr moment = arguments[0];
                    if (arguments.size() == 2)
                        moment = makeASTFunction("plus", moment, makeASTFunction(interval, arguments[1]));
                    ASTPtr start = name == "toStartOfWeek" ? ASTPtr(makeASTFunction(name, moment, litI(0)))
                                                           : ASTPtr(makeASTFunction(name, moment));
                    ASTPtr next = makeASTFunction("plus", start, makeASTFunction(interval, litI(1)));
                    return makeASTFunction("minus", next, makeASTFunction("toIntervalNanosecond", litI(100)));
                }};
        };
        result.emplace("endofday", end_of("toStartOfDay", "toIntervalDay"));
        result.emplace("endofweek", end_of("toStartOfWeek", "toIntervalWeek"));
        result.emplace("endofmonth", end_of("toStartOfMonth", "toIntervalMonth"));
        result.emplace("endofyear", end_of("toStartOfYear", "toIntervalYear"));
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
                    /// KQL takes 0-based inclusive bounds, either of which may count back
                    /// from the end. `arraySlice` takes a 1-based offset and a length, so
                    /// both bounds are normalised first.
                    const auto normalise = [&](const ASTPtr & bound)
                    {
                        return makeASTFunction(
                            "if",
                            makeASTFunction("less", bound, litI(0)),
                            makeASTFunction("plus", makeASTFunction("length", a[0]), bound->clone()),
                            bound->clone());
                    };
                    ASTPtr first = normalise(a[1]);
                    ASTPtr last = normalise(a[2]);
                    ASTPtr length = makeASTFunction(
                        "greatest", makeASTFunction("plus", makeASTFunction("minus", last, first->clone()), litI(1)), litI(0));
                    return makeASTFunction("arraySlice", a[0], makeASTFunction("plus", first->clone(), litI(1)), length);
                }});
        result.emplace("pack_array", rename("array", 1, VARIADIC));
        result.emplace("set_intersect", rename("arrayIntersect", 2, VARIADIC));
        result.emplace(
            "set_difference",
            Entry{
                2,
                VARIADIC,
                [](const ASTs & arguments) -> ASTPtr
                {
                    /// Everything in the first array that is in none of the others.
                    ASTPtr excluded = arguments[1];
                    for (size_t i = 2; i < arguments.size(); ++i)
                        excluded = makeASTFunction("arrayConcat", excluded, arguments[i]);

                    auto lambda = makeASTFunction(
                        "lambda",
                        makeASTFunction("tuple", make_intrusive<ASTIdentifier>("kql_element")),
                        makeASTFunction("not", makeASTFunction("has", excluded, make_intrusive<ASTIdentifier>("kql_element"))));
                    return makeASTFunction("arrayDistinct", makeASTFunction("arrayFilter", lambda, arguments[0]));
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

static const std::set<String> & unsupportedKQLFunctions()
{
    static const std::set<String> names{
        "array_rotate_left", "array_rotate_right", "array_shift_left", "array_shift_right",
        "array_sort_asc",    "array_sort_desc",    "bag_has_key",      "bag_keys",
        "bag_merge",         "bag_pack",           "bag_pack_columns", "bag_remove_keys",
        "bag_set_key",       "bag_unpack",         "base64_decode_toarray",
        "binary_all_and",    "binary_all_or",      "binary_all_xor",   "buildschema",
        "column_ifexists",   "current_cluster_endpoint", "current_database",
        "current_principal", "current_principal_details", "current_principal_is_member_of",
        "cursor_after",      "cursor_before_or_at", "cursor_current", "datatable",
        "dcount_hll",        "dynamic_to_json",    "estimate_data_size", "extent_id",
        "extent_tags",       "externaldata",       "extract_all",      "format_bytes",     "format_datetime",     "format_ipv4",
        "format_ipv4_mask",  "format_timespan",    "geo_distance_2points",
        "geo_geohash_to_central_point", "geo_point_in_circle", "geo_point_in_polygon",
        "geo_point_to_geohash", "geo_point_to_s2cell", "has_any_index", "hll_merge",
        "ingestion_time",    "ipv4_compare",       "ipv4_is_in_range", "ipv4_is_in_any_range",
        "ipv4_is_match",     "ipv4_is_private",    "ipv4_netmask_suffix", "ipv6_compare",
        "ipv6_is_match",     "make_bag",           "make_bag_if",      "materialize",
        "pack",              "pack_all",                 "pack_dictionary",
        "parse_command_line", "parse_csv",         "parse_ipv4",       "parse_ipv4_mask",
        "parse_ipv6",        "parse_ipv6_mask",    "parse_json",       "parse_path",
        "parse_url",         "parse_urlquery",     "parse_user_agent", "parse_version",
        "parse_xml",         "percentile_array",   "percentiles",      "percentiles_array",
        "percentilesw",      "percentilesw_array", "percentilew",      "punycode_from_string",
        "punycode_to_string", "range",             "row_cumsum",       "row_number",
        "row_rank_dense",    "row_rank_min",       "row_window_session", "series_abs",
        "series_acos",       "series_add",         "series_decompose", "series_decompose_anomalies",
        "series_decompose_forecast", "series_divide", "series_equals", "series_fft",
        "series_fill_backward", "series_fill_const", "series_fill_forward", "series_fill_linear",
        "series_fir",        "series_fit_2lines",  "series_fit_line",  "series_greater",
        "series_iir",        "series_less",        "series_multiply",  "series_not_equals",
        "series_outliers",   "series_pearson_correlation", "series_periods_detect",
        "series_periods_validate", "series_seasonal", "series_stats", "series_stats_dynamic",
        "series_subtract",   "series_sum",           "todynamic",         "toscalar",           "treepath",
        "unixtime_microseconds_todatetime", "unixtime_nanoseconds_todatetime",
        "zip",
    };
    return names;
}

bool isUnsupportedKQLFunction(const String & name)
{
    return unsupportedKQLFunctions().contains(name);
}

ASTPtr translateKQLFunction(const String & name, const String & original_name, const ASTs & arguments, String & error)
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
        if (isUnsupportedKQLFunction(name))
        {
            error = fmt::format("'{}' is not supported by the KQL dialect", name);
            return nullptr;
        }

        /// Not a Kusto name at all, so treat it as a ClickHouse function. This is the escape
        /// hatch that lets a KQL query reach the rest of ClickHouse; the name keeps the
        /// user's spelling, because ClickHouse function names are case-sensitive. An
        /// unknown name is reported by the analyzer, which also suggests near misses.
        auto function = makeASTFunction(original_name);
        function->arguments->children = arguments;
        return function;
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
