#include <Parsers/IParserBase.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLStatement.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
#include <Parsers/Kusto/KustoFunctions/KQLDateTimeFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLStringFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLDynamicFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLCastingFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLAggregationFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLTimeSeriesFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLIPFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLBinaryFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLGeneralFunctions.h>
#include <format>

namespace DB
{

bool TimeSpan::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}
/*
bool DateTime::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}*/

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
    return directMapping(out, pos, "date_add");
};

bool DatetimePart::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
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

    out = std::format("ABS(DateDiff({}))",arguments);
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
    
    out = std::format("toDayOfWeek({})%7",datetime_str);
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
    }
    
    out = std::format("toDateTime(toStartOfDay({}),9,'UTC') + (INTERVAL {} +1 MONTH) - (INTERVAL 1 microsecond)", datetime_str, toString(offset));

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
    out = std::format("toDateTime(toStartOfDay({}),9,'UTC') + (INTERVAL {} +1 DAY) - (INTERVAL 1 microsecond)", datetime_str, toString(offset));

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
        out = std::format("toDateTime(toStartOfDay({}),9,'UTC') + (INTERVAL {} +1 WEEK) - (INTERVAL 1 microsecond)", datetime_str, toString(offset));

    return true;
   
}

bool EndOfYear::convertImpl(String & out, IParser::Pos & pos)
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
        out = std::format("toDateTime(toStartOfDay({}),9,'UTC') + (INTERVAL {} +1 YEAR) - (INTERVAL 1 microsecond)", datetime_str, toString(offset));

    return true;
   
}

bool FormatDateTime::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool FormatTimeSpan::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
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
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool MakeDateTime::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;

    ++pos;
    String arguments;
    int number_of_arguments=1;
    String argument;
    while (pos->type != TokenType::ClosingRoundBracket)
    {
        argument = String(pos->begin,pos->end);
        auto dot_pos = argument.find('.');

        if (dot_pos == String::npos)
            arguments = arguments +  String(pos->begin,pos->end);
        else
        {
            arguments = arguments + argument.substr(dot_pos-1, dot_pos) + "," + argument.substr(dot_pos+1,argument.length());
            number_of_arguments++;
        }
            
        ++pos;
        if(pos->type == TokenType::Comma)
            number_of_arguments++;
    }
    
    while(number_of_arguments < 7)
    {
        arguments = arguments+ ",";
        arguments = arguments+ "0";
        number_of_arguments++;
    }
    arguments = arguments + ",7,'UTC'";

    out = std::format("makeDateTime64({})",arguments);
    
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
    out = std::format("date_add(DAY,{}, toDateTime64((toStartOfDay({})) , 9 , 'UTC')) ", offset, datetime_str);
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
    out = std::format("date_add(MONTH,{}, toDateTime64((toStartOfMonth({})) , 9 , 'UTC')) ", offset, datetime_str);
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
    out = std::format("date_add(Week,{}, toDateTime64((toStartOfWeek({})) , 9 , 'UTC')) ", offset, datetime_str);
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
    out = std::format("date_add(YEAR,{}, toDateTime64((toStartOfYear({}, 'UTC')) , 9 , 'UTC'))", offset, datetime_str);
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
    const String value = getConvertedArgument(fn_name, pos);
    out = std::format("toDateTime64({},9,'UTC')", value);
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
