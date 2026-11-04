#include <Parsers/Kusto/KQLFunctions.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Common/FieldVisitorConvertToNumber.h>

#include <Poco/String.h>

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

/// Distance in metres between two longitude/latitude pairs, on a sphere or on the WGS-84
/// ellipsoid. `greatCircleDistance` is a fast approximation and disagrees with Kusto in the
/// fourth significant figure -- Kusto uses an exact haversine on a sphere of radius
/// 6371010 m -- which is the right trade for a function that usually filters rather than
/// reports. Both ClickHouse functions want floats, so integer coordinates are widened.
ASTPtr distance(const ASTs & arguments, bool spheroid)
{
    return makeASTFunction(
        spheroid ? "geoDistance" : "greatCircleDistance",
        makeASTFunction("toFloat64", arguments[0]),
        makeASTFunction("toFloat64", arguments[1]),
        makeASTFunction("toFloat64", arguments[2]),
        makeASTFunction("toFloat64", arguments[3]));
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

/// Case-insensitive comparison in Kusto is ordinal. The `*CaseInsensitiveUTF8` search functions
/// implement exactly that and, unlike `lowerUTF8`, do not need ICU - so the string operators keep
/// working in a build without it.
ASTPtr caseInsensitivePosition(const ASTPtr & haystack, const ASTPtr & needle)
{
    return makeASTFunction("positionCaseInsensitiveUTF8", asString(haystack), asString(needle));
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
        /// Case mapping in Kusto is Unicode, so these are the UTF-8 functions rather than the
        /// ASCII ones - which means they are only available in a build with ICU.
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
                            "arrayElementOrNull",
                            makeASTFunction(
                                "extractGroups",
                                asString(arguments[2]),
                                makeASTFunction("concat", litS("("), asString(arguments[0]), litS(")"))),
                            litI(1));
                    else
                        captured = makeASTFunction(
                            "arrayElementOrNull",
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
                        /// `lower` rather than `lowerUTF8`: the prefix tested for is ASCII, and
                        /// `lowerUTF8` needs ICU.
                        makeASTFunction("startsWith", makeASTFunction("lower", text), litS("0x")),
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

        /// ---- Binary ------------------------------------------------------------------
        result.emplace("binary_and", rename("bitAnd", 2, 2));
        result.emplace("binary_or", rename("bitOr", 2, 2));
        result.emplace("binary_xor", rename("bitXor", 2, 2));
        result.emplace("binary_not", rename("bitNot", 1, 1));
        result.emplace("bitset_count_ones", rename("bitCount", 1, 1));
        /// Kusto reduces the shift amount modulo 64; ClickHouse does not.
        const auto shift = [](std::string_view target)
        {
            return Entry{
                2,
                2,
                [name = String(target)](const ASTs & a) -> ASTPtr
                {
                    /// Kusto shifts by `n % 64` and answers null for a negative n, where
                    /// ClickHouse would raise. The shift amount uses `positiveModulo`: the
                    /// negative branch answers NULL anyway, but constant folding evaluates
                    /// both branches, and `bitShiftLeft` raises on a negative amount.
                    return makeASTFunction(
                        "if",
                        makeASTFunction("less", a[1], litI(0)),
                        makeASTFunction("CAST", lit(Field()), litS("Nullable(Int64)")),
                        makeASTFunction(name, a[0], makeASTFunction("positiveModulo", a[1], litI(64))));
                }};
        };
        result.emplace("binary_shift_left", shift("bitShiftLeft"));
        result.emplace("binary_shift_right", shift("bitShiftRight"));

        /// ---- More mathematics ----------------------------------------------------------
        result.emplace("acos", rename("acos", 1, 1));
        result.emplace("asin", rename("asin", 1, 1));
        result.emplace("atan", rename("atan", 1, 1));
        result.emplace("atan2", rename("atan2", 2, 2));
        result.emplace("cos", rename("cos", 1, 1));
        result.emplace("sin", rename("sin", 1, 1));
        result.emplace("tan", rename("tan", 1, 1));
        result.emplace("degrees", rename("degrees", 1, 1));
        result.emplace("radians", rename("radians", 1, 1));
        result.emplace("pi", rename("pi", 0, 0));
        result.emplace("gamma", rename("tgamma", 1, 1));
        result.emplace("loggamma", rename("lgamma", 1, 1));
        result.emplace("erf", rename("erf", 1, 1));
        result.emplace("erfc", rename("erfc", 1, 1));
        result.emplace("cot", Entry{1, 1, [](const ASTs & a) -> ASTPtr { return makeASTFunction("divide", litI(1), makeASTFunction("tan", a[0])); }});

        /// ---- More strings --------------------------------------------------------------
        result.emplace(
            "tohex",
            Entry{
                1,
                2,
                [](const ASTs & arguments) -> ASTPtr
                {
                    /// `hex()` pads to whole bytes and upper-cases; Kusto does neither, and a
                    /// negative value keeps its two's complement at the width of its type.
                    ASTPtr digits = makeASTFunction(
                        "lower", makeASTFunction("hex", makeASTFunction("toInt64", arguments[0])));
                    ASTPtr trimmed = makeASTFunction(
                        "if",
                        makeASTFunction("equals", arguments[0], litI(0)),
                        litS("0"),
                        makeASTFunction("replaceRegexpOne", digits, litS("^0+"), litS("")));
                    if (arguments.size() == 1)
                        return trimmed;
                    /// `minLength` left-pads, and is ignored when the value is already longer.
                    ASTPtr width = makeASTFunction("least", arguments[1], litI(16));
                    return makeASTFunction(
                        "if",
                        makeASTFunction("greaterOrEquals", makeASTFunction("length", trimmed->clone()), width->clone()),
                        trimmed,
                        makeASTFunction("leftPad", trimmed->clone(), width, litS("0")));
                }});
        result.emplace("hash_sha1", rename("SHA1", 1, 1));
        result.emplace("new_guid", rename("generateUUIDv4", 0, 0));
        result.emplace("url_encode", rename("encodeURLComponent", 1, 1));
        result.emplace("isutf8", Entry{1, 1, [](const ASTs & a) -> ASTPtr { return makeASTFunction("isValidUTF8", asString(a[0])); }});
        result.emplace(
            "isascii",
            Entry{
                1,
                1,
                [](const ASTs & a) -> ASTPtr
                {
                    /// True when every byte is below 0x80, which for a UTF-8 string means the
                    /// byte length and the character length agree.
                    return makeASTFunction(
                        "equals", makeASTFunction("length", asString(a[0])), makeASTFunction("lengthUTF8", asString(a[0])));
                }});
        result.emplace(
            "strcmp",
            Entry{
                2,
                2,
                [](const ASTs & a) -> ASTPtr
                {
                    /// -1, 0 or 1 by ordinal comparison.
                    return makeASTFunction(
                        "multiIf",
                        makeASTFunction("less", asString(a[0]), asString(a[1])),
                        litI(-1),
                        makeASTFunction("greater", asString(a[0]), asString(a[1])),
                        litI(1),
                        litI(0));
                }});

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
                    /// Kusto returns the *timespan* since the preceding Sunday, so a Monday
                    /// is `1.00:00:00` and not `1`. ClickHouse's `toDayOfWeek` counts from
                    /// Monday = 1, hence the modulo.
                    return makeASTFunction(
                        "toIntervalDay", makeASTFunction("modulo", makeASTFunction("toDayOfWeek", a[0]), litI(7)));
                }});
        /// `datetime_add(period, amount, datetime)` and `datetime_part(part, datetime)` name
        /// the unit with a string literal, so the right ClickHouse function is chosen here
        /// rather than dispatched on at runtime.
        result.emplace(
            "datetime_add",
            Entry{
                3,
                3,
                [](const ASTs & arguments) -> ASTPtr
                {
                    static const std::map<String, String> periods{
                        {"year", "addYears"},         {"quarter", "addQuarters"},
                        {"month", "addMonths"},       {"week", "addWeeks"},
                        {"day", "addDays"},           {"hour", "addHours"},
                        {"minute", "addMinutes"},     {"second", "addSeconds"},
                        {"millisecond", "addMilliseconds"}, {"microsecond", "addMicroseconds"},
                        {"nanosecond", "addNanoseconds"},
                    };
                    const auto * period = arguments[0]->as<ASTLiteral>();
                    if (!period || period->value.getType() != Field::Types::String)
                        return nullptr;
                    auto it = periods.find(Poco::toLower(period->value.safeGet<String>()));
                    if (it == periods.end())
                        return nullptr;
                    return makeASTFunction(it->second, arguments[2], arguments[1]);
                }});
        result.emplace(
            "datetime_part",
            Entry{
                2,
                2,
                [](const ASTs & arguments) -> ASTPtr
                {
                    static const std::map<String, String> parts{
                        {"year", "toYear"},           {"quarter", "toQuarter"},
                        {"month", "toMonth"},         {"week_of_year", "toISOWeek"},
                        {"weekofyear", "toISOWeek"},  {"day", "toDayOfMonth"},
                        {"dayofyear", "toDayOfYear"}, {"hour", "toHour"},
                        {"minute", "toMinute"},       {"second", "toSecond"},
                    };
                    /// The sub-second parts are cumulative rather than disjoint: for
                    /// `.7654321` Kusto answers 765, 765432 and 765432100.
                    static const std::map<String, Int64> fractions{
                        {"millisecond", 1000}, {"microsecond", 1000000}, {"nanosecond", 1000000000},
                    };

                    const auto * part = arguments[0]->as<ASTLiteral>();
                    if (!part || part->value.getType() != Field::Types::String)
                        return nullptr;
                    const String name = Poco::toLower(part->value.safeGet<String>());

                    if (auto it = parts.find(name); it != parts.end())
                        return makeASTFunction(it->second, arguments[1]);

                    if (auto it = fractions.find(name); it != fractions.end())
                        return makeASTFunction(
                            "modulo",
                            makeASTFunction(
                                "intDiv",
                                makeASTFunction(
                                    "toUnixTimestamp64Nano",
                                    makeASTFunction("toDateTime64", arguments[1], makeASTFunction("toUInt8", litI(9)))),
                                litI(1000000000 / it->second)),
                            litI(it->second));

                    return nullptr;
                }});
        result.emplace(
            "datetime_utc_to_local",
            Entry{2, 2, [](const ASTs & a) -> ASTPtr { return makeASTFunction("toTimezone", a[0], asString(a[1])); }});
        result.emplace(
            "datetime_local_to_utc",
            Entry{
                2,
                2,
                [](const ASTs & a) -> ASTPtr
                {
                    /// The input is a `DateTime64(7, 'UTC')` whose wall clock names a local
                    /// time in `timezone`. Re-reading that wall clock in `timezone` finds the
                    /// instant it stands for; the result then renders in UTC.
                    return makeASTFunction(
                        "toTimezone",
                        makeASTFunction(
                            "toDateTime64", makeASTFunction("toString", a[0]), lit(static_cast<UInt64>(7)), asString(a[1])),
                        litS("UTC"));
                }});
        result.emplace(
            "make_timespan",
            Entry{
                2,
                4,
                [](const ASTs & arguments) -> ASTPtr
                {
                    /// `make_timespan(h, m)`, `(h, m, s)` or `(d, h, m, s)`. The components are
                    /// clock fields, and Kusto validates them as such: an hour of 25 or a minute
                    /// of 61 makes the whole result null instead of rolling into the next field.
                    ASTs parts(arguments.begin(), arguments.end());
                    ASTPtr days = parts.size() == 4 ? parts[0] : litI(0);
                    const size_t base = parts.size() == 4 ? 1 : 0;
                    ASTPtr hours = parts[base];
                    ASTPtr minutes = parts[base + 1];
                    ASTPtr seconds = parts.size() >= base + 3 ? parts[base + 2] : litI(0);

                    const auto in_range = [](const ASTPtr & part, Int64 limit)
                    {
                        return makeASTFunction(
                            "and",
                            makeASTFunction("greaterOrEquals", part->clone(), litI(0)),
                            makeASTFunction("less", part->clone(), litI(limit)));
                    };
                    ASTPtr valid = makeASTFunction(
                        "and",
                        makeASTFunction("greaterOrEquals", days->clone(), litI(0)),
                        in_range(hours, 24),
                        in_range(minutes, 60),
                        in_range(seconds, 60));

                    ASTPtr total = makeASTFunction(
                        "plus",
                        makeASTFunction(
                            "plus",
                            makeASTFunction("multiply", days, litI(86400)),
                            makeASTFunction("multiply", hours, litI(3600))),
                        makeASTFunction("plus", makeASTFunction("multiply", minutes, litI(60)), seconds));
                    return makeASTFunction(
                        "if",
                        valid,
                        makeASTFunction("toIntervalNanosecond", makeASTFunction("multiply", total, litI(1000000000))),
                        lit(Field()));
                }});
        result.emplace(
            "totimespan", Entry{1, 1, [](const ASTs & a) -> ASTPtr { return makeASTFunction("kqlToTimespan", a[0]); }});
        result.emplace(
            "unixtime_microseconds_todatetime",
            Entry{
                1,
                1,
                [](const ASTs & a) -> ASTPtr
                { return makeASTFunction("toDateTime64", makeASTFunction("divide", a[0], litI(1000000)), litI(7), litS("UTC")); }});
        result.emplace(
            "unixtime_nanoseconds_todatetime",
            Entry{
                1,
                1,
                [](const ASTs & a) -> ASTPtr
                { return makeASTFunction("toDateTime64", makeASTFunction("divide", a[0], litI(1000000000)), litI(7), litS("UTC")); }});
        result.emplace("week_of_year", Entry{1, 1, [](const ASTs & a) -> ASTPtr { return makeASTFunction("toISOWeek", a[0]); }});
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

        /// ---- IPv4 / IPv6 -----------------------------------------------------------
        ///
        /// A Kusto IPv4 string may carry its own prefix (`192.168.1.1/24`), and where one
        /// does the low bits are *masked away* rather than ignored. Comparisons combine the
        /// prefixes of both operands with any explicit one and use the narrowest.

        /// The address part, without any `/suffix`.
        const auto ip_address = [](const ASTPtr & text)
        { return makeASTFunction("arrayElement", makeASTFunction("splitByChar", litS("/"), asString(text)), litI(1)); };

        /// The prefix a string carries, or 32 when it carries none.
        const auto ip_prefix = [](const ASTPtr & text)
        {
            ASTPtr parts = makeASTFunction("splitByChar", litS("/"), asString(text));
            return makeASTFunction(
                "if",
                makeASTFunction("greater", makeASTFunction("length", parts->clone()), litI(1)),
                makeASTFunction("toInt64OrNull", makeASTFunction("arrayElement", parts, litI(2))),
                litI(32));
        };

        /// `value` with everything below the top `prefix` bits cleared.
        const auto ip_mask = [](const ASTPtr & value, const ASTPtr & prefix)
        {
            ASTPtr all_ones = litI(0xFFFFFFFF);
            ASTPtr mask = makeASTFunction(
                "bitAnd",
                makeASTFunction("bitShiftLeft", all_ones->clone(), makeASTFunction("minus", litI(32), prefix)),
                all_ones);
            return makeASTFunction("bitAnd", value, mask);
        };

        result.emplace(
            "ipv4_netmask_suffix", Entry{1, 1, [ip_prefix](const ASTs & a) -> ASTPtr { return ip_prefix(a[0]); }});

        result.emplace(
            "parse_ipv4",
            Entry{
                1,
                1,
                [ip_address, ip_prefix, ip_mask](const ASTs & a) -> ASTPtr
                {
                    ASTPtr value = makeASTFunction("toInt64OrNull", makeASTFunction("toString", makeASTFunction("IPv4StringToNumOrNull", ip_address(a[0]))));
                    return ip_mask(value, ip_prefix(a[0]));
                }});
        result.emplace(
            "parse_ipv4_mask",
            Entry{
                2,
                2,
                [ip_address, ip_mask](const ASTs & a) -> ASTPtr
                {
                    ASTPtr value = makeASTFunction("toInt64OrNull", makeASTFunction("toString", makeASTFunction("IPv4StringToNumOrNull", ip_address(a[0]))));
                    return ip_mask(value, a[1]);
                }});

        /// `format_ipv4` renders the dotted quad; `format_ipv4_mask` appends `/prefix`.
        /// Both answer the empty string rather than null when the input does not parse.
        const auto format_ipv4 = [ip_address, ip_prefix, ip_mask](bool with_suffix)
        {
            return Entry{
                1,
                2,
                [ip_address, ip_prefix, ip_mask, with_suffix](const ASTs & a) -> ASTPtr
                {
                    ASTPtr embedded = ip_prefix(a[0]);
                    ASTPtr requested = a.size() == 2 ? a[1] : litI(32);
                    ASTPtr prefix = makeASTFunction("least", embedded, requested);
                    ASTPtr value = makeASTFunction(
                        "toInt64OrNull", makeASTFunction("toString", makeASTFunction("IPv4StringToNumOrNull", ip_address(a[0]))));
                    ASTPtr text = makeASTFunction(
                        "IPv4NumToString", makeASTFunction("toUInt32", ip_mask(value, prefix->clone())));
                    if (with_suffix)
                        text = makeASTFunction("concat", text, litS("/"), makeASTFunction("toString", prefix->clone()));
                    /// A negative or out-of-range prefix is a failure, and so is a bad address.
                    return makeASTFunction(
                        "if",
                        makeASTFunction(
                            "or",
                            makeASTFunction("isNull", makeASTFunction("IPv4StringToNumOrNull", ip_address(a[0]))),
                            makeASTFunction(
                                "or",
                                makeASTFunction("less", prefix->clone(), litI(0)),
                                makeASTFunction("greater", prefix, litI(32)))),
                        litS(""),
                        text);
                }};
        };
        result.emplace("format_ipv4", format_ipv4(false));
        result.emplace("format_ipv4_mask", format_ipv4(true));

        /// The narrowest of both operands' prefixes and any explicit one.
        const auto ipv4_common = [ip_address, ip_prefix, ip_mask](const ASTs & a, bool as_sign)
        {
            ASTPtr prefix = makeASTFunction("least", ip_prefix(a[0]), ip_prefix(a[1]));
            if (a.size() == 3)
                prefix = makeASTFunction("least", prefix, a[2]);

            const auto value = [&](const ASTPtr & text)
            {
                return makeASTFunction(
                    "toInt64OrNull", makeASTFunction("toString", makeASTFunction("IPv4StringToNumOrNull", ip_address(text))));
            };
            ASTPtr left = ip_mask(value(a[0]), prefix->clone());
            ASTPtr right = ip_mask(value(a[1]), prefix);
            if (!as_sign)
                return ASTPtr(makeASTFunction("equals", left, right));
            return ASTPtr(makeASTFunction(
                "multiIf",
                makeASTFunction("less", left->clone(), right->clone()),
                litI(-1),
                makeASTFunction("greater", left, right),
                litI(1),
                litI(0)));
        };
        result.emplace("ipv4_is_match", Entry{2, 3, [ipv4_common](const ASTs & a) { return ipv4_common(a, false); }});
        result.emplace("ipv4_compare", Entry{2, 3, [ipv4_common](const ASTs & a) { return ipv4_common(a, true); }});

        /// `isIPAddressInRange` insists on a `/prefix`, while Kusto accepts a bare address
        /// meaning `/32`.
        const auto in_range = [](const ASTPtr & address, const ASTPtr & range)
        {
            ASTPtr cidr = makeASTFunction(
                "if",
                makeASTFunction("greater", makeASTFunction("positionUTF8", asString(range), litS("/")), litI(0)),
                asString(range),
                makeASTFunction("concat", asString(range), litS("/32")));
            return makeASTFunction("isIPAddressInRange", asString(address), cidr);
        };
        const auto in_range_v6 = [](const ASTPtr & address, const ASTPtr & range)
        {
            ASTPtr cidr = makeASTFunction(
                "if",
                makeASTFunction("greater", makeASTFunction("positionUTF8", asString(range), litS("/")), litI(0)),
                asString(range),
                makeASTFunction("concat", asString(range), litS("/128")));
            return makeASTFunction("isIPAddressInRange", asString(address), cidr);
        };

        result.emplace(
            "ipv4_is_in_range",
            Entry{2, 2, [ip_address, in_range](const ASTs & a) -> ASTPtr { return in_range(ip_address(a[0]), a[1]); }});
        result.emplace(
            "ipv6_is_in_range",
            Entry{2, 2, [in_range_v6](const ASTs & a) -> ASTPtr { return in_range_v6(a[0], a[1]); }});

        /// `..._is_in_any_range` takes either several range arguments or one array of them.
        /// Only the variadic string form is accepted here; the `dynamic` form needs the
        /// object mapping this dialect does not have.
        const auto in_any_range = [](auto range_test)
        {
            return [range_test](const ASTs & arguments) -> ASTPtr
            {
                ASTPtr combined;
                for (size_t i = 1; i < arguments.size(); ++i)
                {
                    ASTPtr one = range_test(arguments[0]->clone(), arguments[i]);
                    combined = combined ? ASTPtr(makeASTFunction("or", combined, one)) : one;
                }
                return combined;
            };
        };
        result.emplace(
            "ipv4_is_in_any_range",
            Entry{2, VARIADIC, in_any_range([ip_address, in_range](const ASTPtr & ip, const ASTPtr & r) { return in_range(ip_address(ip), r); })});
        result.emplace(
            "ipv6_is_in_any_range",
            Entry{2, VARIADIC, in_any_range([in_range_v6](const ASTPtr & ip, const ASTPtr & r) { return in_range_v6(ip, r); })});

        result.emplace(
            "ipv4_is_private",
            Entry{
                1,
                1,
                [ip_address, in_range](const ASTs & a) -> ASTPtr
                {
                    /// RFC 1918 only -- Kusto does not count loopback as private.
                    ASTPtr address = ip_address(a[0]);
                    ASTPtr result_expr;
                    for (const auto * range : {"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"})
                    {
                        ASTPtr one = in_range(address->clone(), litS(range));
                        result_expr = result_expr ? ASTPtr(makeASTFunction("or", result_expr, one)) : one;
                    }
                    return result_expr;
                }});

        /// IPv6 comparison works on the 16-byte form, which also lets an IPv4 string be
        /// compared against an IPv6 one, as Kusto allows.
        const auto ipv6_common = [](const ASTs & a, bool as_sign) -> ASTPtr
        {
            const auto value = [](const ASTPtr & text)
            {
                ASTPtr address
                    = makeASTFunction("arrayElement", makeASTFunction("splitByChar", litS("/"), asString(text)), litI(1));
                return makeASTFunction("toIPv6OrNull", address);
            };
            ASTPtr left = value(a[0]);
            ASTPtr right = value(a[1]);
            if (!as_sign)
                return makeASTFunction("equals", left, right);
            return makeASTFunction(
                "multiIf",
                makeASTFunction("less", left->clone(), right->clone()),
                litI(-1),
                makeASTFunction("greater", left, right),
                litI(1),
                litI(0));
        };
        result.emplace("ipv6_is_match", Entry{2, 2, [ipv6_common](const ASTs & a) { return ipv6_common(a, false); }});
        result.emplace("ipv6_compare", Entry{2, 2, [ipv6_common](const ASTs & a) { return ipv6_common(a, true); }});

        /// ---- Geospatial (the point-based subset) --------------------------------------
        ///
        /// Every `geo_*` function takes longitude before latitude. ClickHouse is not
        /// consistent about that -- `geohashEncode` is lon-first but `geoToH3` is lat-first --
        /// so the order is spelled out at each call rather than assumed.
        ///
        /// Kusto answers null for a coordinate outside its valid range. Neither ClickHouse
        /// function checks, so an out-of-range coordinate gives a meaningless number instead.
        /// Validating would cost eight comparisons on every row, which is the wrong trade for
        /// the fast path.
        result.emplace(
            "geo_distance_2points",
            Entry{
                4,
                5,
                [](const ASTs & a) -> ASTPtr
                {
                    /// The optional last argument asks for the ellipsoid formula instead.
                    bool spheroid = false;
                    if (a.size() == 5)
                    {
                        const auto * flag = a[4]->as<ASTLiteral>();
                        if (!flag || flag->value.getType() != Field::Types::Bool)
                            return nullptr;
                        spheroid = flag->value.safeGet<bool>();
                    }
                    return distance(a, spheroid);
                }});
        result.emplace(
            "geo_point_in_circle",
            Entry{5, 5, [](const ASTs & a) -> ASTPtr { return makeASTFunction("lessOrEquals", distance(a, false), a[4]); }});
        result.emplace(
            "geo_point_to_geohash",
            Entry{
                2,
                3,
                [](const ASTs & a) -> ASTPtr
                {
                    /// `geohashEncode` is longitude-first, like Kusto. Default accuracy is 5.
                    return makeASTFunction(
                        "geohashEncode",
                        makeASTFunction("toFloat64", a[0]),
                        makeASTFunction("toFloat64", a[1]),
                        makeASTFunction("toUInt8", a.size() == 3 ? a[2] : litI(5)));
                }});
        result.emplace(
            "geo_point_to_h3cell",
            Entry{
                2,
                3,
                [](const ASTs & a) -> ASTPtr
                {
                    /// `geoToH3` takes LATITUDE first, the opposite of Kusto. Kusto returns
                    /// the cell as a hexadecimal token string, not a number. Default is 6.
                    ASTPtr resolution = makeASTFunction("toUInt8", a.size() == 3 ? a[2] : litI(6));
                    return makeASTFunction(
                        "h3ToString",
                        makeASTFunction(
                            "geoToH3",
                            makeASTFunction("toFloat64", a[1]),
                            makeASTFunction("toFloat64", a[0]),
                            resolution));
                }});
        result.emplace(
            "geo_h3cell_level",
            Entry{
                1,
                1,
                [](const ASTs & a) -> ASTPtr
                { return makeASTFunction("h3GetResolution", makeASTFunction("stringToH3", asString(a[0]))); }});
        result.emplace(
            "geo_h3cell_parent",
            Entry{
                1,
                2,
                [](const ASTs & a) -> ASTPtr
                {
                    /// Without a resolution the immediate parent is meant.
                    ASTPtr cell = makeASTFunction("stringToH3", asString(a[0]));
                    ASTPtr level = makeASTFunction(
                        "toUInt8",
                        a.size() == 2
                            ? a[1]
                            : ASTPtr(makeASTFunction("minus", makeASTFunction("h3GetResolution", cell->clone()), litI(1))));
                    return makeASTFunction("h3ToString", makeASTFunction("h3ToParent", cell, level));
                }});
        result.emplace(
            "geo_h3cell_children",
            Entry{
                1,
                2,
                [](const ASTs & a) -> ASTPtr
                {
                    ASTPtr cell = makeASTFunction("stringToH3", asString(a[0]));
                    ASTPtr level = makeASTFunction(
                        "toUInt8",
                        a.size() == 2
                            ? a[1]
                            : ASTPtr(makeASTFunction("plus", makeASTFunction("h3GetResolution", cell->clone()), litI(1))));
                    return makeASTFunction(
                        "arrayMap",
                        makeASTFunction(
                            "lambda",
                            makeASTFunction("tuple", make_intrusive<ASTIdentifier>("kql_cell")),
                            makeASTFunction("h3ToString", make_intrusive<ASTIdentifier>("kql_cell"))),
                        makeASTFunction("h3ToChildren", cell, level));
                }});
        result.emplace(
            "geo_h3cell_neighbors",
            Entry{
                1,
                1,
                [](const ASTs & a) -> ASTPtr
                {
                    /// Kusto returns the immediate neighbours and excludes the cell itself,
                    /// which `h3kRing` includes.
                    ASTPtr cell = makeASTFunction("stringToH3", asString(a[0]));
                    ASTPtr ring = makeASTFunction("h3kRing", cell->clone(), makeASTFunction("toUInt16", litI(1)));
                    ASTPtr without_self = makeASTFunction(
                        "arrayFilter",
                        makeASTFunction(
                            "lambda",
                            makeASTFunction("tuple", make_intrusive<ASTIdentifier>("kql_cell")),
                            makeASTFunction("notEquals", make_intrusive<ASTIdentifier>("kql_cell"), cell)),
                        ring);
                    return makeASTFunction(
                        "arrayMap",
                        makeASTFunction(
                            "lambda",
                            makeASTFunction("tuple", make_intrusive<ASTIdentifier>("kql_cell")),
                            makeASTFunction("h3ToString", make_intrusive<ASTIdentifier>("kql_cell"))),
                        without_self);
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

        result.emplace("count", rename("count", 0, 0));
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
        ASTPtr position = op == "contains_cs" ? makeASTFunction("positionUTF8", asString(haystack), asString(needle))
                                              : caseInsensitivePosition(haystack, needle);
        return makeASTFunction("greater", position, litI(0));
    }

    if (op == "startswith" || op == "startswith_cs")
    {
        if (op == "startswith_cs")
            return makeASTFunction("startsWith", asString(haystack), asString(needle));
        /// The needle occurs, and it occurs at the very beginning.
        return makeASTFunction("equals", caseInsensitivePosition(haystack, needle), litI(1));
    }

    if (op == "endswith" || op == "endswith_cs")
    {
        if (op == "endswith_cs")
            return makeASTFunction("endsWith", asString(haystack), asString(needle));
        /// Compare the tail of the haystack, because `position` finds the *first* occurrence:
        /// 'abab' does end with 'AB' even though the first match is at 1, not at 3.
        ASTPtr tail = makeASTFunction("rightUTF8", asString(haystack), makeASTFunction("lengthUTF8", asString(needle)));
        return makeASTFunction("equals", caseInsensitivePosition(tail, needle), litI(1));
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

ASTPtr kqlCaseInsensitiveEquals(const ASTPtr & left, const ASTPtr & right)
{
    /// Equal ignoring case means: the second string occurs at the start of the first, and there
    /// is nothing after it.
    return makeASTFunction(
        "and",
        makeASTFunction("equals", makeASTFunction("lengthUTF8", asString(left)), makeASTFunction("lengthUTF8", asString(right))),
        makeASTFunction("equals", caseInsensitivePosition(left, right), litI(1)));
}

static const std::set<String> & unsupportedKQLFunctions()
{
    static const std::set<String> names{
        "array_rotate_left", "array_rotate_right", "array_shift_left", "array_shift_right",
        "array_sort_asc", "array_sort_desc", "bag_has_key", "bag_keys", "bag_merge", "bag_pack",
        "bag_pack_columns", "bag_remove_keys", "bag_set_key", "bag_unpack",
        "base64_decode_toarray", "binary_all_and", "binary_all_or", "binary_all_xor",
        "buildschema", "column_ifexists", "current_cluster_endpoint", "current_database",
        "current_principal", "current_principal_details", "current_principal_is_member_of",
        "cursor_after", "cursor_before_or_at", "cursor_current", "datatable", "dcount_hll",
        "dynamic_to_json", "estimate_data_size", "extent_id", "extent_tags", "externaldata",
        "extract_all", "format_bytes", "format_datetime", "format_timespan",
        "geo_geohash_to_central_point", "geo_h3cell_to_central_point", "geo_point_in_polygon",
        "geo_point_to_s2cell", "geo_s2cell_to_central_point", "has_any_index", "hll_merge",
        "ingestion_time", "make_bag", "make_bag_if", "materialize", "pack", "pack_all",
        "pack_dictionary", "parse_command_line", "parse_csv", "parse_ipv6", "parse_ipv6_mask",
        "parse_json", "parse_path", "parse_url", "parse_urlquery", "parse_user_agent",
        "parse_version", "parse_xml", "percentile_array", "percentiles", "percentiles_array",
        "percentilesw", "percentilesw_array", "percentilew", "punycode_from_string",
        "punycode_to_string", "range", "repeat", "replace", "row_cumsum", "row_number",
        "row_rank_dense", "row_rank_min", "row_window_session", "series_abs", "series_acos",
        "series_add", "series_decompose", "series_decompose_anomalies",
        "series_decompose_forecast", "series_divide", "series_equals", "series_fft",
        "series_fill_backward", "series_fill_const", "series_fill_forward", "series_fill_linear",
        "series_fir", "series_fit_2lines", "series_fit_line", "series_greater", "series_iir",
        "series_less", "series_multiply", "series_not_equals", "series_outliers",
        "series_pearson_correlation", "series_periods_detect", "series_periods_validate",
        "series_seasonal", "series_stats", "series_stats_dynamic", "series_subtract", "series_sum",
        "todynamic", "toscalar", "translate", "treepath", "unixtime_microseconds_todatetime",
        "unixtime_nanoseconds_todatetime", "zip",
    };
    return names;
}

bool isUnsupportedKQLFunction(const String & name)
{
    return unsupportedKQLFunctions().contains(name);
}

ASTPtr translateKQLFunction(const String & name, const String & original_name, const ASTs & arguments, bool allow_aggregates, String & error)
{
    const auto * entry = [&]() -> const Entry *
    {
        if (auto it = scalarFunctions().find(name); it != scalarFunctions().end())
            return &it->second;
        if (auto it = aggregateFunctions().find(name); it != aggregateFunctions().end())
            return &it->second;
        return nullptr;
    }();

    if (entry && !allow_aggregates && !scalarFunctions().contains(name) && aggregateFunctions().contains(name))
    {
        error = fmt::format("'{}' is an aggregate function, and may only be used in the aggregation of a 'summarize'", name);
        return nullptr;
    }

    if (!entry)
    {
        if (!allow_aggregates && AggregateFunctionFactory::instance().isAggregateFunctionName(original_name))
        {
            error = fmt::format("'{}' is an aggregate function, and may only be used in the aggregation of a 'summarize'", name);
            return nullptr;
        }

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
