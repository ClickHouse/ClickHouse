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
        std::stod(value_no_spaces, &pos_end);
        if (pos_end != value_no_spaces.size())
            throw std::invalid_argument("not a number");
    }
    catch (const std::exception &)
    {
        ParserKQLDateTypeTimespan value_tsp;
        if (value_tsp.parseConstKQLTimespan(value_no_spaces))
            value = std::to_string(value_tsp.toSeconds());
    }

    auto t = fmt::format("toFloat64({})", value);

    try
    {
        bin_size = std::stod(round_to);
    }
    catch (const std::exception &)
    {
        ParserKQLDateTypeTimespan time_span_parser;
        if (!time_span_parser.parseConstKQLTimespan(round_to))
            return false;
        bin_size = time_span_parser.toSeconds();
    }

    if (bin_size <= 0)
        return false;

    // Use datetime output whenever first argument is datetime/date (whether bin size is numeric or timespan)
    if (original_expr == "datetime" || original_expr == "date")
    {
        out = fmt::format("toDateTime64(toInt64({0}/{1}) * {1}, 9, 'UTC')", t, bin_size);
    }
    else if (original_expr == "timespan" || original_expr == "time" || ParserKQLDateTypeTimespan().parseConstKQLTimespan(original_expr))
    {
        String bin_value = fmt::format("toInt64({0}/{1}) * {1}", t, bin_size);
        out = fmt::format(
            "concat(toString(toInt32((({}) as x) / 3600)),':', toString(toInt32(x % 3600 / 60)),':',toString(toInt32(x % 3600 % 60)))",
            bin_value);
    }
    else
    {
        out = fmt::format("toInt64({0} / {1}) * {1}", t, bin_size);
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
    int dir = t2 >= t1 ? 0 : -1;

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
        out = fmt::format("toDateTime64({} + toInt64(({} - {}) / {} + {}) * {}, 9, 'UTC')", t1, t2, t1, bin_size, dir, bin_size);
    }
    else if (original_expr == "timespan" || original_expr == "time" || ParserKQLDateTypeTimespan().parseConstKQLTimespan(original_expr))
    {
        String bin_value = fmt::format("{} + toInt64(({} - {}) / {} + {}) * {}", t1, t2, t1, bin_size, dir, bin_size);
        out = fmt::format(
            "concat(toString(toInt32((({}) as x) / 3600)),':', toString(toInt32(x % 3600 / 60)), ':', toString(toInt32(x % 3600 % 60)))",
            bin_value);
    }
    else
    {
        out = fmt::format("{} + toInt64(({} - {}) / {} + {}) * {}", t1, t2, t1, bin_size, dir, bin_size);
    }
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
    String if_true = getConvertedArgument(fn_name, pos);
    if (if_true.empty())
        return false;

    ++pos;
    String if_false = getConvertedArgument(fn_name, pos);
    if (if_false.empty())
        return false;

    out = fmt::format("if({}, {}, {})", predicate, if_true, if_false);
    return true;
}

}
