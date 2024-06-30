#include <format>
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
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLStatement.h>
#include <Parsers/Kusto/Utilities.h>
#include <Parsers/ParserSetQuery.h>
#include "Poco/String.h"
namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int SYNTAX_ERROR;
}
namespace DB
{

bool TimeSpan::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool Ago::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    if (pos->type != TokenType::ClosingRoundBracket)
    {
        const auto offset = getConvertedArgument(fn_name, pos);
        out = std::format("now64(9,'UTC') - {}", offset);
    }
    else
        out = "now64(9,'UTC')";
    return true;
}

bool DatetimeAdd::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    String period = getConvertedArgument(fn_name, pos);
    //remove quotes from period.
    trim(period);
    if (period.front() == '\"' || period.front() == '\'')
    {
        //period.remove
        period.erase(0, 1); // erase the first quote
        period.erase(period.size() - 1); // erase the last quote
    }
    ++pos;
    const String offset = getConvertedArgument(fn_name, pos);
    ++pos;
    const String datetime = getConvertedArgument(fn_name, pos);

    out = std::format("date_add({}, {}, {})", period, offset, datetime);

    return true;
};

bool DatetimePart::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    String part = Poco::toUpper(getConvertedArgument(fn_name, pos));
    trim(part);
    if (part.front() == '\"' || part.front() == '\'')
    {
        //period.remove
        part.erase(0, 1); // erase the first quote
        part.erase(part.size() - 1); // erase the last quote
    }
    String date;
    if (pos->type == TokenType::Comma)
    {
        ++pos;
        date = getConvertedArgument(fn_name, pos);
    }
    String format;

    if (part == "YEAR")
        format = "%G";
    else if (part == "QUARTER")
        format = "%Q";
    else if (part == "MONTH")
        format = "%m";
    else if (part == "WEEK_OF_YEAR")
        format = "%V";
    else if (part == "DAY")
        format = "%e";
    else if (part == "DAYOFYEAR")
        format = "%j";
    else if (part == "HOUR")
        format = "%H";
    else if (part == "MINUTE")
        format = "%i";
    else if (part == "SECOND")
        format = "%S";
    else
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Unexpected argument {} for {}", part, fn_name);

    out = std::format("formatDateTime({}, '{}')", date, format);
    return true;
}

bool DatetimeDiff::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;
    ++pos;
    String arguments;

    arguments = arguments + getConvertedArgument(fn_name, pos) + ",";
    ++pos;
    arguments = arguments + getConvertedArgument(fn_name, pos) + ",";
    ++pos;
    arguments = arguments + getConvertedArgument(fn_name, pos);

    out = std::format("DateDiff({}) * -1", arguments);
    return true;
}

bool DayOfMonth::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "toDayOfMonth");
}

bool DayOfWeek::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;
    ++pos;
    const String datetime_str = getConvertedArgument(fn_name, pos);

    out = std::format("concat((toDayOfWeek({})%7)::String, '.00:00:00')", datetime_str);
    return true;
}

bool DayOfYear::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "toDayOfYear");
}

bool EndOfMonth::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    const String datetime_str = getConvertedArgument(fn_name, pos);
    String offset = "0";

    if (pos->type == TokenType::Comma)
    {
        ++pos;
        offset = getConvertedArgument(fn_name, pos);
        if (offset.empty())
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Number of arguments do not match in function: {}", fn_name);
    }
    out = std::format(
        "toDateTime(toLastDayOfMonth(toDateTime({}, 9, 'UTC') + toIntervalMonth({})), 9, 'UTC') + toIntervalHour(23) + "
        "toIntervalMinute(59) + toIntervalSecond(60) - toIntervalMicrosecond(1)",
        datetime_str,
        toString(offset));

    return true;
}

bool EndOfDay::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    const String datetime_str = getConvertedArgument(fn_name, pos);
    String offset = "0";

    if (pos->type == TokenType::Comma)
    {
        ++pos;
        offset = getConvertedArgument(fn_name, pos);
    }
    out = std::format(
        "toDateTime(toStartOfDay({}),9,'UTC') + (INTERVAL {} +1 DAY) - (INTERVAL 1 microsecond)", datetime_str, toString(offset));

    return true;
}

bool EndOfWeek::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    const String datetime_str = getConvertedArgument(fn_name, pos);
    String offset = "0";

    if (pos->type == TokenType::Comma)
    {
        ++pos;
        offset = getConvertedArgument(fn_name, pos);
    }
    out = std::format(
        "toDateTime(toStartOfDay({}),9,'UTC') + (INTERVAL {} +1 WEEK) - (INTERVAL 1 microsecond)", datetime_str, toString(offset));

    return true;
}

bool EndOfYear::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    const String datetime_str = getConvertedArgument(fn_name, pos);

    if (datetime_str.empty())
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Number of arguments do not match in function: {}", fn_name);

    String offset = "0";
    if (pos->type == TokenType::Comma)
    {
        ++pos;
        offset = getConvertedArgument(fn_name, pos);
        if (offset.empty())
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Number of arguments do not match in function: {}", fn_name);
        offset.erase(remove(offset.begin(), offset.end(), ' '), offset.end());
    }

    out = std::format(
        "(((((toDateTime(toString(toLastDayOfMonth(toDateTime({0}, 9, 'UTC') + toIntervalYear({1}) + toIntervalMonth(12 - "
        "toInt8(substring(toString(toDateTime({0}, 9, 'UTC')), 6, 2))))), 9, 'UTC') + toIntervalHour(23)) + toIntervalMinute(59)) + "
        "toIntervalSecond(60)) - toIntervalMicrosecond(1)))",
        datetime_str,
        toString(offset));

    return true;
}

bool FormatDateTime::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;
    String formatspecifier;
    ++pos;
    const auto datetime = getConvertedArgument(fn_name, pos);
    ++pos;
    auto format = getConvertedArgument(fn_name, pos);
    trim(format);

    //remove quotes and end space from format argument.
    if (format.front() == '\"' || format.front() == '\'')
    {
        format.erase(0, 1); // erase the first quote
        format.erase(format.size() - 1); // erase the last quote
    }

    std::vector<String> res;
    getTokens(format, res);
    std::string::size_type i = 0;
    size_t decimal = 0;
    while (i < format.size())
    {
        char c = format[i];
        if (!isalpha(c))
        {
            //delimiter
            if (c == ' ' || c == '-' || c == '_' || c == '[' || c == ']' || c == '/' || c == ',' || c == '.' || c == ':')
                formatspecifier = formatspecifier + c;
            else
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Invalid format delimiter in function: {}", fn_name);
            ++i;
        }
        else
        {
            //format specifier
            String arg = res.back();

            if (arg == "y" || arg == "yy")
                formatspecifier = formatspecifier + "%y";
            else if (arg == "yyyy")
                formatspecifier = formatspecifier + "%Y";
            else if (arg == "M" || arg == "MM")
                formatspecifier = formatspecifier + "%m";
            else if (arg == "s" || arg == "ss")
                formatspecifier = formatspecifier + "%S";
            else if (arg == "m" || arg == "mm")
                formatspecifier = formatspecifier + "%i";
            else if (arg == "h" || arg == "hh")
                formatspecifier = formatspecifier + "%h";
            else if (arg == "H" || arg == "HH")
                formatspecifier = formatspecifier + "%H";
            else if (arg == "d")
                formatspecifier = formatspecifier + "%e";
            else if (arg == "dd")
                formatspecifier = formatspecifier + "%d";
            else if (arg == "tt")
                formatspecifier = formatspecifier + "%p";
            else if (arg.starts_with('f') || arg.starts_with('F'))
                decimal = arg.size();
            else
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Format specifier {} in function: {} is not supported", arg, fn_name);
            res.pop_back();
            i = i + arg.size();
        }
    }
    if (decimal > 0 && formatspecifier.find('.') != String::npos)
    {
        out = std::format(
            "concat("
            "substring(toString(formatDateTime({0} , '{1}')),1, position(toString(formatDateTime({0},'{1}')),'.')) ,"
            "substring(substring(toString({0}), position(toString({0}),'.')+1),1,{2}),"
            "substring(toString(formatDateTime({0},'{1}')), position(toString(formatDateTime({0},'{1}')), '.')+1, length "
            "(toString(formatDateTime({0},'{1}'))))) ",
            datetime,
            formatspecifier,
            decimal);
    }
    else
        out = std::format("formatDateTime({0},'{1}')", datetime, formatspecifier);

    return true;
}
bool FormatTimeSpan::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;
    String formatspecifier;
    ++pos;
    const auto datetime = getConvertedArgument(fn_name, pos);
    ++pos;
    auto format = getConvertedArgument(fn_name, pos);
    size_t decimal = 0;
    trim(format);
    //remove quotes and end space from format argument.
    if (format.front() == '\"' || format.front() == '\'')
    {
        format.erase(0, 1); // erase the first quote
        format.erase(format.size() - 1); // erase the last quote
    }
    std::vector<String> res;
    getTokens(format, res);
    size_t pad = 0;
    std::string::size_type i = 0;


    bool is_day_in_format = false;
    String day_val = std::to_string(std::stoi(datetime) / 86400);
    bool is_hour_zero = std::stoi(datetime) % 86400 > 3600 ? false : true;

    while (i < format.size())
    {
        char c = format[i];
        if (!isalpha(c))
        {
            //delimiter
            if (c == ' ' || c == '-' || c == '_' || c == '[' || c == ']' || c == '/' || c == ',' || c == '.' || c == ':')
                formatspecifier = formatspecifier + c;
            else
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Invalid format delimiter in function: {}", fn_name);
            ++i;
        }
        else
        {
            //format specifier
            String arg = res.back();

            if (arg == "s" || arg == "ss")
                formatspecifier = formatspecifier + "%S";
            else if (arg == "m" || arg == "mm")
                formatspecifier = formatspecifier + "%i";
            else if (arg == "h" || arg == "hh")
            {
                if (is_hour_zero) //To handle the CH limit for 12hr format(01-12). If not handled, 1.00:00:00 returned as 1.12:00:00(in 12 hr format)
                    formatspecifier = formatspecifier + "%h";
                else
                    formatspecifier = formatspecifier + "%H";
            }
            else if (arg == "H" || arg == "HH")
                formatspecifier = formatspecifier + "%H";
            else if (arg.starts_with('d')) //&& arg.size() >2)
            {
                pad = std::max(arg.size(), day_val.length());
                is_day_in_format = true;
            }
            else if (arg.starts_with('f') || arg.starts_with('F'))
                decimal = arg.size();
            else
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Format specifier {} in function:{} is not supported", arg, fn_name);
            res.pop_back();
            i = i + arg.size();
        }
    }
    auto last_delim = formatspecifier.substr(formatspecifier.length() - 1, formatspecifier.length());

    if (!is_day_in_format)
    {
        if (decimal > 0)
        {
            if (format.substr(format.length() - decimal - 1, 1) == last_delim)
                out = std::format(
                    "concat(substring(toString(formatDateTime(toDateTime64({0},9,'UTC') ,'{1}')),1, length(toString(formatDateTime("
                    "toDateTime64({0},9,'UTC'),'{1}'))) - position("
                    "reverse(toString(formatDateTime(toDateTime64({0},9,'UTC'),'{1}'))),'{3}')+1),substring(SUBSTRING(toString("
                    "toDateTime64({0},9,'UTC')),position(toString(toDateTime64({0},9,'UTC')),'.')+1),1,{2}))",
                    datetime,
                    formatspecifier,
                    decimal,
                    last_delim);
            else
                out = std::format(
                    "concat(substring(toString(formatDateTime(toDateTime64({0},9,'UTC') ,'{1}')),1, length(toString(formatDateTime("
                    "toDateTime64({0},9,'UTC'),'{1}'))) - position("
                    "reverse(toString(formatDateTime(toDateTime64({0},9,'UTC'),'{1}'))),'{3}')),substring(SUBSTRING(toString(toDateTime64({"
                    "0},9,'UTC')),position(toString(toDateTime64({0},9,'UTC')),'.')+1),1,{2}))",
                    datetime,
                    formatspecifier,
                    decimal,
                    last_delim);
        }
        else
            out = std::format("formatDateTime(toDateTime64({0},9,'UTC'),'{1}')", datetime, formatspecifier);
    }
    else
    {
        if (decimal > 0)
        {
            if (format.substr(format.length() - decimal - 1, 1) == last_delim)
                out = std::format(
                    "concat(leftPad('{5}', {3},'0'),substring(toString(formatDateTime(toDateTime64({0},9,'UTC'),'{1}')),1,"
                    "length(toString(formatDateTime(toDateTime64({0},9,'UTC'),'{1}'))) - position("
                    "reverse(toString(formatDateTime(toDateTime64({0},9,'UTC'),'{1}'))),'{4}') "
                    "+1),substring(SUBSTRING(toString(toDateTime64({0},9,'UTC')),position(toString(toDateTime64({0},9,'UTC')),'.')+1),1,{2}"
                    "))",
                    datetime,
                    formatspecifier,
                    decimal,
                    pad,
                    last_delim,
                    day_val);
            else
                out = std::format(
                    "concat(leftPad('{5}', {3}, '0'),substring(toString(formatDateTime(toDateTime64({0},9,'UTC'),'{1}')),1,"
                    "length(toString(formatDateTime(toDateTime64({0},9,'UTC'),'{1}'))) - position("
                    "reverse(toString(formatDateTime(toDateTime64({0},9,'UTC'),'{1}'))),'{4}')),substring(SUBSTRING(toString(toDateTime64({"
                    "0},9,'UTC')),position(toString(toDateTime64({0},9,'UTC')),'.')+1),1,{2}),substring(toString(formatDateTime("
                    "toDateTime64({0},9,'UTC'),'{1}')),position(toString(formatDateTime("
                    "toDateTime64({0},9,'UTC'),'{1}')),'{4}'),length(toString(formatDateTime(toDateTime64({0},9,'UTC'),'{1}')))))",
                    datetime,
                    formatspecifier,
                    decimal,
                    pad,
                    last_delim,
                    day_val);
        }
        else if (decimal == 0)
            out = std::format(
                "concat(leftPad('{3}',{2},'0'),toString(formatDateTime(toDateTime64({0},9,'UTC'),'{1}')))",
                datetime,
                formatspecifier,
                pad,
                day_val);
    }
    return true;
}

bool GetMonth::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "toMonth");
}

bool GetYear::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "toYear");
}

bool HoursOfDay::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "toHour");
}

bool MakeTimeSpan::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;

    ++pos;
    String datetime_str;
    String hour;
    String day;
    String minute;
    String second;
    int arg_count = 0;
    std::vector<String> args;
    while (isValidKQLPos(pos) && pos->type != TokenType::ClosingRoundBracket)
    {
        String arg = getConvertedArgument(fn_name, pos);
        args.insert(args.begin(), arg);
        if (pos->type == TokenType::Comma)
            ++pos;
        ++arg_count;
    }

    if (arg_count < 2 || arg_count > 4)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "argument count out of bound in function: {}", fn_name);

    if (arg_count == 2)
    {
        hour = args.back();
        args.pop_back();
        minute = args.back();
        args.pop_back();
        datetime_str = hour + ":" + minute;
    }
    else if (arg_count == 3)
    {
        hour = args.back();
        args.pop_back();
        minute = args.back();
        args.pop_back();
        second = args.back();
        args.pop_back();

        datetime_str = hour + ":" + minute + ":" + second;
    }
    else if (arg_count == 4)
    {
        day = args.back();
        args.pop_back();
        hour = args.back();
        args.pop_back();
        minute = args.back();
        args.pop_back();
        second = args.back();
        args.pop_back();

        datetime_str = hour + ":" + minute + ":" + second;
        day = day + ".";
    }
    else
        throw Exception(ErrorCodes::SYNTAX_ERROR, "argument count out of bound in function: {}", fn_name);

    //Add dummy yyyy-mm-dd to parse datetime in CH
    datetime_str = "0000-00-00 " + datetime_str;

    out = std::format(
        "CONCAT('{}',toString(SUBSTRING(toString(toTime(parseDateTime64BestEffortOrNull('{}', 9,'UTC'))),12)))", day, datetime_str);
    return true;
}

bool MakeDateTime::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;

    ++pos;
    String arguments;
    int arg_count = 0;

    while (isValidKQLPos(pos) && pos->type != TokenType::ClosingRoundBracket)
    {
        String arg = getConvertedArgument(fn_name, pos);
        if (pos->type == TokenType::Comma)
            ++pos;
        arguments = arguments + arg + ",";
        ++arg_count;
    }

    if (arg_count < 1 || arg_count > 7)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "argument count out of bound in function: {}", fn_name);

    if (arg_count < 7)
    {
        for (int i = arg_count; i < 7; ++i)
            arguments = arguments + "0,";
    }

    arguments = arguments + "7,'UTC'";
    out = std::format("makeDateTime64({})", arguments);

    return true;
}

bool Now::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    if (pos->type != TokenType::ClosingRoundBracket)
    {
        const auto offset = getConvertedArgument(fn_name, pos);
        out = std::format("now64(9,'UTC') + {}", offset);
    }
    else
        out = "now64(9,'UTC')";
    return true;
}

bool StartOfDay::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;

    ++pos;
    const String datetime_str = getConvertedArgument(fn_name, pos);
    String offset = "0";

    if (pos->type == TokenType::Comma)
    {
        ++pos;
        offset = getConvertedArgument(fn_name, pos);
    }
    out = std::format("date_add(DAY,{}, parseDateTime64BestEffortOrNull(toString((toStartOfDay({}))), 9, 'UTC')) ", offset, datetime_str);
    return true;
}

bool StartOfMonth::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    const String datetime_str = getConvertedArgument(fn_name, pos);
    String offset = "0";

    if (pos->type == TokenType::Comma)
    {
        ++pos;
        offset = getConvertedArgument(fn_name, pos);
    }
    out = std::format(
        "date_add(MONTH,{}, parseDateTime64BestEffortOrNull(toString((toStartOfMonth({}))), 9, 'UTC')) ", offset, datetime_str);
    return true;
}

bool StartOfWeek::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    const String datetime_str = getConvertedArgument(fn_name, pos);
    String offset = "0";

    if (pos->type == TokenType::Comma)
    {
        ++pos;
        offset = getConvertedArgument(fn_name, pos);
    }
    out = std::format(
        "date_add(Week,{}, parseDateTime64BestEffortOrNull(toString((toStartOfWeek({}))), 9, 'UTC')) ", offset, datetime_str);
    return true;
}

bool StartOfYear::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    const String datetime_str = getConvertedArgument(fn_name, pos);
    String offset = "0";

    if (pos->type == TokenType::Comma)
    {
        ++pos;
        offset = getConvertedArgument(fn_name, pos);
    }
    out = std::format(
        "date_add(YEAR,{}, parseDateTime64BestEffortOrNull(toString((toStartOfYear({}, 'UTC'))), 9, 'UTC'))", offset, datetime_str);
    return true;
}

bool UnixTimeMicrosecondsToDateTime::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    const String value = getConvertedArgument(fn_name, pos);

    out = std::format("fromUnixTimestamp64Micro({},'UTC')", value);
    return true;
}

bool UnixTimeMillisecondsToDateTime::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    const String value = getConvertedArgument(fn_name, pos);

    out = std::format("fromUnixTimestamp64Milli({},'UTC')", value);
    return true;
}

bool UnixTimeNanosecondsToDateTime::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    const String value = getConvertedArgument(fn_name, pos);

    out = std::format("fromUnixTimestamp64Nano({},'UTC')", value);
    return true;
}

bool UnixTimeSecondsToDateTime::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    if (pos->type == TokenType::QuotedIdentifier || pos->type == TokenType::StringLiteral)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} accepts only long, int and double type of arguments", fn_name);

    String expression = getConvertedArgument(fn_name, pos);
    out = std::format(
        " if(toTypeName({0}) = 'Int64' OR toTypeName({0}) = 'Int32'OR toTypeName({0}) = 'Float64' OR  toTypeName({0}) = 'UInt32' OR  "
        "toTypeName({0}) = 'UInt64', toDateTime64({0}, 9, 'UTC'), toDateTime64(throwIf(true, '{1} only accepts Int, Long and double type "
        "of arguments'), 9, 'UTC'))",
        expression,
        fn_name);

    return true;
}

bool WeekOfYear::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;
    ++pos;
    const String time_str = getConvertedArgument(fn_name, pos);
    out = std::format("toWeek({},3,'UTC')", time_str);
    return true;
}

bool MonthOfYear::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "toMonth");
}

}
