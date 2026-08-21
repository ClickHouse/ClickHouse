#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
#include <Parsers/Kusto/KustoFunctions/KQLAggregationFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLBinaryFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLCastingFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLDateTimeFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLDynamicFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLGeneralFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLIPFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLStringFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLTimeSeriesFunctions.h>
#include <Parsers/Kusto/ParserKQLDateTypeTimespan.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLStatement.h>
#include <Parsers/Kusto/Utilities.h>
#include <Parsers/ParserSetQuery.h>
#include <Common/Exception.h>
#include <boost/lexical_cast.hpp>

#include <algorithm>
#include <cctype>
#include <fmt/format.h>
#include <stdexcept>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

bool Bin::convertImpl(String & out, IParser::Pos & pos)
{
    double bin_size;
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;


    ++pos;

    String original_expr(pos->begin, pos->end);

    String value = getConvertedArgument(fn_name, pos);
    if (value.empty())
        return false;

    ++pos;
    String round_to = getConvertedArgument(fn_name, pos);
    round_to.erase(std::remove_if(round_to.begin(), round_to.end(), [](unsigned char c) { return std::isspace(c); }), round_to.end());
    if (round_to.empty())
        return false;

    String value_no_spaces = value;
    value_no_spaces.erase(std::remove_if(value_no_spaces.begin(), value_no_spaces.end(), [](unsigned char c) { return std::isspace(c); }), value_no_spaces.end());
    if (value_no_spaces.empty())
        return false;
    try
    {
        size_t pos_end = 0;
        (void)std::stod(value_no_spaces, &pos_end);
        if (pos_end != value_no_spaces.size())
            throw std::invalid_argument("not a number");
    }
    catch (const std::exception &)
    {
        ParserKQLDateTypeTimespan value_tsp;
        if (value_tsp.parseConstKQLTimespan(value_no_spaces))
            value = std::to_string(value_tsp.toSeconds());
    }

    /// If the value is a datetime or timespan, ensure it's numeric for the bin formula
    String t;
    if (original_expr == "datetime" || original_expr == "date")
        t = fmt::format("toFloat64(parseDateTime64BestEffortOrNull(replaceOne(toString({}), 'T', ' '), 9, 'UTC'))", value);
    else
        t = fmt::format("toFloat64({})", value);

    bool is_const_bin_size = false;
    try
    {
        bin_size = std::stod(round_to);
        is_const_bin_size = true;
    }
    catch (const std::exception &)
    {
        ParserKQLDateTypeTimespan time_span_parser;
        if (time_span_parser.parseConstKQLTimespan(round_to))
        {
            bin_size = time_span_parser.toSeconds();
            is_const_bin_size = true;
        }
    }

    if (is_const_bin_size && bin_size <= 0)
    {
        out = "NULL";
        return true;
    }

    // Use datetime output whenever first argument is datetime/date (whether bin size is numeric or timespan)
    if (original_expr == "datetime" || original_expr == "date")
    {
        auto bin_sz = is_const_bin_size ? std::to_string(bin_size) : round_to;
        auto inner = fmt::format("toDateTime64(toInt64(floor({0}/{1})) * {1}, 9, 'UTC')", t, bin_sz);
        auto result = fmt::format("substring(replaceOne(toString({}), ' ', 'T'), 1, 27)", inner);
        /// Guard against runtime division by zero when bin_size is not a constant
        if (!is_const_bin_size)
            out = fmt::format("if(({}) <= 0, NULL, {})", bin_sz, result);
        else
            out = result;
    }
    else if (original_expr == "timespan" || original_expr == "time" || ParserKQLDateTypeTimespan().parseConstKQLTimespan(original_expr))
    {
        auto bin_sz = is_const_bin_size ? std::to_string(bin_size) : round_to;
        String bin_value = fmt::format("toInt64(floor({0}/{1})) * {1}", t, bin_sz);
        auto result = fmt::format(
            "concat("
            "if((({0}) as _bv) < 0, '-', ''), "
            "if(abs(toInt64(_bv)) >= 86400, concat(toString(intDiv(abs(toInt64(_bv)), 86400)), '.'), ''), "
            "leftPad(toString(intDiv(abs(toInt64(_bv)) % 86400, 3600)), 2, '0'), ':', "
            "leftPad(toString(intDiv(abs(toInt64(_bv)) % 3600, 60)), 2, '0'), ':', "
            "leftPad(toString(abs(toInt64(_bv)) % 60), 2, '0'))",
            bin_value);
        /// Guard against runtime division by zero when bin_size is not a constant
        if (!is_const_bin_size)
            out = fmt::format("if(({}) <= 0, NULL, {})", bin_sz, result);
        else
            out = result;
    }
    else
    {
        auto bin_sz = is_const_bin_size ? std::to_string(bin_size) : fmt::format("toFloat64({})", round_to);
        /// Use floor() for correct behavior with negative numbers
        auto result = fmt::format("toInt64(floor({0} / {1})) * {1}", t, bin_sz);
        /// Guard against runtime division by zero when bin_size is not a constant
        if (!is_const_bin_size)
            out = fmt::format("if(({}) <= 0, NULL, {})", bin_sz, result);
        else
            out = result;
    }

    return true;
}

bool BinAt::convertImpl(String & out, IParser::Pos & pos)
{
    double bin_size;
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    String original_expr(pos->begin, pos->end);

    String expression_str = getConvertedArgument(fn_name, pos);
    String expression_no_spaces = expression_str;
    expression_no_spaces.erase(std::remove_if(expression_no_spaces.begin(), expression_no_spaces.end(), [](unsigned char c) { return std::isspace(c); }), expression_no_spaces.end());
    if (expression_no_spaces.empty())
        return false;

    ++pos;
    String bin_size_str = getConvertedArgument(fn_name, pos);
    bin_size_str.erase(std::remove_if(bin_size_str.begin(), bin_size_str.end(), [](unsigned char c) { return std::isspace(c); }), bin_size_str.end());
    if (bin_size_str.empty())
        return false;

    ++pos;
    String fixed_point_str = getConvertedArgument(fn_name, pos);
    String fixed_point_no_spaces = fixed_point_str;
    fixed_point_no_spaces.erase(std::remove_if(fixed_point_no_spaces.begin(), fixed_point_no_spaces.end(), [](unsigned char c) { return std::isspace(c); }), fixed_point_no_spaces.end());
    if (fixed_point_no_spaces.empty())
        return false;

    auto t1 = fmt::format("toFloat64({})", fixed_point_str);
    auto t2 = fmt::format("toFloat64({})", expression_str);

    try
    {
        bin_size = std::stod(bin_size_str);
    }
    catch (const std::exception &)
    {
        ParserKQLDateTypeTimespan time_span_parser;
        if (!time_span_parser.parseConstKQLTimespan(bin_size_str))
            return false;
        bin_size = time_span_parser.toSeconds();
    }

    // validate if bin_size is a positive number
    if (bin_size <= 0)
        return false;

    if (original_expr == "datetime" || original_expr == "date")
    {
        auto inner = fmt::format("{0} + toInt64(floor(({1} - {0}) / {2})) * {2}", t1, t2, bin_size);
        out = fmt::format("substring(replaceOne(toString(toDateTime64({}, 9, 'UTC')), ' ', 'T'), 1, 27)", inner);
    }
    else if (original_expr == "timespan" || original_expr == "time" || ParserKQLDateTypeTimespan().parseConstKQLTimespan(original_expr))
    {
        String bin_value = fmt::format("{0} + toInt64(floor(({1} - {0}) / {2})) * {2}", t1, t2, bin_size);
        out = fmt::format(
            "concat("
            "if((({0}) as _bv) < 0, '-', ''), "
            "if(abs(toInt64(_bv)) >= 86400, concat(toString(intDiv(abs(toInt64(_bv)), 86400)), '.'), ''), "
            "leftPad(toString(intDiv(abs(toInt64(_bv)) % 86400, 3600)), 2, '0'), ':', "
            "leftPad(toString(intDiv(abs(toInt64(_bv)) % 3600, 60)), 2, '0'), ':', "
            "leftPad(toString(abs(toInt64(_bv)) % 60), 2, '0'))",
            bin_value);
    }
    else
    {
        out = fmt::format("{0} + toInt64(floor(({1} - {0}) / {2})) * {2}", t1, t2, bin_size);
    }
    return true;
}

bool Floor::convertImpl(String & out, IParser::Pos & pos)
{
    /// KQL `floor(value [, roundTo])`. When `roundTo` is omitted it defaults to 1
    /// (rounding the value down to the nearest integer). When provided, the
    /// behaviour is identical to `bin(value, roundTo)`.
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    String value = getConvertedArgument(fn_name, pos);
    value.erase(std::remove_if(value.begin(), value.end(), [](unsigned char c) { return std::isspace(c); }), value.end());
    if (value.empty())
        return false;

    /// Single-argument form: `floor(x)`. `getConvertedArgument` stops at the closing
    /// bracket without consuming it, so we leave `pos` there for the caller.
    if (pos->type == TokenType::ClosingRoundBracket)
    {
        out = fmt::format("floor(toFloat64({}))", value);
        return true;
    }

    /// Two-argument form: `floor(x, roundTo)`. Reuse `Bin` semantics.
    if (pos->type != TokenType::Comma)
        return false;

    ++pos;
    String round_to = getConvertedArgument(fn_name, pos);
    round_to.erase(std::remove_if(round_to.begin(), round_to.end(), [](unsigned char c) { return std::isspace(c); }), round_to.end());
    if (round_to.empty())
        return false;

    /// Try to interpret round_to as a constant for the zero-bin guard.
    bool is_const_bin_size = false;
    double bin_size = 0;
    try
    {
        bin_size = std::stod(round_to);
        is_const_bin_size = true;
    }
    catch (const std::exception &)
    {
        ParserKQLDateTypeTimespan time_span_parser;
        if (time_span_parser.parseConstKQLTimespan(round_to))
        {
            bin_size = time_span_parser.toSeconds();
            is_const_bin_size = true;
        }
    }

    if (is_const_bin_size && bin_size <= 0)
    {
        out = "NULL";
        return true;
    }

    auto bin_sz = is_const_bin_size ? std::to_string(bin_size) : fmt::format("toFloat64({})", round_to);
    auto result = fmt::format("toInt64(floor(toFloat64({0}) / {1})) * {1}", value, bin_sz);
    if (!is_const_bin_size)
        out = fmt::format("if(({}) <= 0, NULL, {})", bin_sz, result);
    else
        out = result;
    return true;
}

bool Iif::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    String predicate = getConvertedArgument(fn_name, pos);
    if (predicate.empty())
        return false;

    ++pos;
    /// Check if the if_true argument is a timespan literal
    String if_true_raw(pos->begin, pos->end);
    String if_true = getConvertedArgument(fn_name, pos);
    if (if_true.empty())
        return false;

    ++pos;
    String if_false = getConvertedArgument(fn_name, pos);
    if (if_false.empty())
        return false;

    /// Detect if arguments are timespans: raw token differs from converted,
    /// the raw token looks like a timespan literal (e.g., "1s", "10h", "2d"),
    /// and is NOT a function name like "datetime"
    ParserKQLDateTypeTimespan tsp_check;
    bool is_timespan_result = (if_true_raw != if_true)
        && tsp_check.parseConstKQLTimespan(if_true_raw);

    auto result = fmt::format("if({}, {}, {})", predicate, if_true, if_false);
    if (is_timespan_result)
        out = IParserKQLFunction::formatTimespanSQL(result);
    else
        out = result;
    return true;
}

bool Iff::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    String predicate = getConvertedArgument(fn_name, pos);
    if (predicate.empty())
        return false;

    ++pos;
    String if_true_raw(pos->begin, pos->end);
    String if_true = getConvertedArgument(fn_name, pos);
    if (if_true.empty())
        return false;

    ++pos;
    String if_false = getConvertedArgument(fn_name, pos);
    if (if_false.empty())
        return false;

    ParserKQLDateTypeTimespan tsp_check2;
    bool is_timespan_result = (if_true_raw != if_true)
        && tsp_check2.parseConstKQLTimespan(if_true_raw);

    auto result = fmt::format("if({}, {}, {})", predicate, if_true, if_false);
    if (is_timespan_result)
        out = IParserKQLFunction::formatTimespanSQL(result);
    else
        out = result;
    return true;
}

bool Not::convertImpl(String & out, IParser::Pos & pos)
{
    const auto fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;
    const auto arg = getArgument(fn_name, pos);
    out = fmt::format("toBool(not({}))", arg);
    return true;
}

bool MinOf::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "least");
}

bool MaxOf::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "greatest");
}

bool Coalesce::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "coalesce");
}

bool Case::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "multiIf");
}

bool GeoDistance2Points::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    String lon1 = getConvertedArgument(fn_name, pos);
    ++pos;
    String lat1 = getConvertedArgument(fn_name, pos);
    ++pos;
    String lon2 = getConvertedArgument(fn_name, pos);
    ++pos;
    String lat2 = getConvertedArgument(fn_name, pos);

    /// KQL returns NULL for invalid coordinates or NULL inputs
    out = fmt::format(
        "if(isNull({0}) or isNull({1}) or isNull({2}) or isNull({3}) or "
        "({0}) < -180 or ({0}) > 180 or ({1}) < -90 or ({1}) > 90 or "
        "({2}) < -180 or ({2}) > 180 or ({3}) < -90 or ({3}) > 90, "
        "NULL, geoDistance({0}, {1}, {2}, {3}))",
        lon1, lat1, lon2, lat2);
    return true;
}

bool GeoPointToGeohash::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    String lon = getConvertedArgument(fn_name, pos);
    ++pos;
    String lat = getConvertedArgument(fn_name, pos);

    auto accuracy = getOptionalArgument(fn_name, pos);
    /// KQL default precision is 5 characters
    if (accuracy)
        out = fmt::format("geohashEncode({}, {}, {})", lon, lat, *accuracy);
    else
        out = fmt::format("geohashEncode({}, {}, 5)", lon, lat);

    return true;
}

bool ReplaceString::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "replaceAll");
}

bool URLEncodeComponent::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "encodeURLComponent");
}

bool PadLeft::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    String source = getConvertedArgument(fn_name, pos);
    ++pos;
    String total_len = getConvertedArgument(fn_name, pos);

    auto pad_char = getOptionalArgument(fn_name, pos);
    if (pad_char && *pad_char != "''")
        out = fmt::format("leftPad({}, {}, {})", source, total_len, *pad_char);
    else
        out = fmt::format("leftPad({}, {})", source, total_len);

    return true;
}

bool PadRight::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    String source = getConvertedArgument(fn_name, pos);
    ++pos;
    String total_len = getConvertedArgument(fn_name, pos);

    auto pad_char = getOptionalArgument(fn_name, pos);
    if (pad_char && *pad_char != "''")
        out = fmt::format("rightPad({}, {}, {})", source, total_len, *pad_char);
    else
        out = fmt::format("rightPad({}, {})", source, total_len);

    return true;
}

bool TrimWs::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "trimBoth");
}

bool ParseHex::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    const auto argument = getArgument(fn_name, pos);
    out = fmt::format(
        "reinterpretAsInt64(reverse(unhex(right(leftPad(replaceOne(toString({}), '0x', ''), 16, '0'), 16))))",
        argument);
    return true;
}

bool ToHex::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    const auto argument = getArgument(fn_name, pos);
    /// Remove leading zeros from hex output, but preserve at least one digit for zero
    out = fmt::format(
        "lower(if(empty(trimLeft(hex(toInt64({0})), '0')), '0', trimLeft(hex(toInt64({0})), '0')))",
        argument);
    return true;
}

bool IsAscii::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    const auto argument = getArgument(fn_name, pos);
    out = fmt::format("toBool(not(match(toString({}), '[^\\x00-\\x7F]')))", argument);
    return true;
}

bool IsUtf8::convertImpl(String & out, IParser::Pos & pos)
{
    const auto fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;
    const auto arg = getArgument(fn_name, pos);
    out = fmt::format("toBool(isValidUTF8({}))", arg);
    return true;
}

bool StrcatArray::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "arrayStringConcat");
}

bool DatetimeUtcToLocal::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    String datetime_arg = getConvertedArgument(fn_name, pos);
    ++pos;
    String timezone = getConvertedArgument(fn_name, pos);

    out = fmt::format("toTimeZone(toDateTime64({}, 9, 'UTC'), {})", datetime_arg, timezone);
    return true;
}

bool ToDateTimeFmt::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    String datetime_str = getConvertedArgument(fn_name, pos);
    ++pos;
    [[maybe_unused]] String format_str = getConvertedArgument(fn_name, pos);

    /// The format_str argument is accepted for KQL compatibility but not wired into parsing.
    /// ClickHouse's parseDateTimeBestEffort handles standard datetime formats automatically,
    /// which covers the common cases. Custom/ambiguous format strings that depend on the
    /// explicit format for correct interpretation may parse differently from KQL semantics.
    auto inner = fmt::format("parseDateTime64BestEffortOrNull({}, 9, 'UTC')", datetime_str);
    out = fmt::format("substring(replaceOne(toString({}), ' ', 'T'), 1, 27)", inner);
    return true;
}

bool EndsWith::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "endsWith");
}

bool Any::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "any");
}

bool RowNumber::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    auto start_arg = getOptionalArgument(fn_name, pos);
    int64_t start = 1;
    if (start_arg)
        start = std::stoll(*start_arg);

    /// KQL `row_number(start, restart)` resets the counter when `restart` evaluates to true.
    /// ClickHouse window functions don't have a corresponding reset construct, so we reject
    /// this form rather than silently returning a value that ignores the predicate.
    if (auto reset_arg = getOptionalArgument(fn_name, pos))
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "KQL function `row_number` with the optional `restart` predicate is not supported");

    out = fmt::format("(row_number() OVER () + {} - 1)", start);
    return true;
}

bool Format::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    String value = getConvertedArgument(fn_name, pos);
    ++pos;
    String fmt_str = getConvertedArgument(fn_name, pos);

    if (fmt_str == "'x'")
        out = fmt::format("lower(if(empty(trimLeft(hex(toInt64({0})), '0')), '0', trimLeft(hex(toInt64({0})), '0')))", value);
    else if (fmt_str == "'X'")
        out = fmt::format("upper(if(empty(trimLeft(hex(toInt64({0})), '0')), '0', trimLeft(hex(toInt64({0})), '0')))", value);
    else
        out = fmt::format("toString({})", value);

    return true;
}

bool FormatInterp::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    String fmt_str = getConvertedArgument(fn_name, pos);

    std::vector<String> args;
    while (auto arg = getOptionalArgument(fn_name, pos))
        args.push_back(*arg);

    String result = fmt_str;
    for (size_t i = 0; i < args.size(); ++i)
    {
        String placeholder_plain = fmt::format("{{{}:x}}", i);
        String placeholder_simple = fmt::format("{{{}}}", i);
        result = fmt::format(
            "replaceAll({0}, '{1}', lower(if(empty(trimLeft(hex(toInt64({2})), '0')), '0', trimLeft(hex(toInt64({2})), '0'))))",
            result, placeholder_plain, args[i]);
        result = fmt::format("replaceAll({}, '{}', toString({}))", result, placeholder_simple, args[i]);
    }

    out = result;
    return true;
}

}
