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
#include <Parsers/Kusto/ParserKQLDateTypeTimespan.h>
#include <format>

namespace DB
{

bool Bin::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool BinAt::convertImpl(String & out,IParser::Pos & pos)
{
    ParserKQLDateTypeTimespan time_span;
    double bin_size;

    const String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;
    ++pos;
    String expression_str = getConvertedArgument(fn_name, pos);
    ++pos;
    String bin_size_str = getConvertedArgument(fn_name, pos);
    ++pos;
    String fixed_point_str = getConvertedArgument(fn_name, pos);

    bin_size_str = bin_size_str.substr(0, bin_size_str.size()-1);

    auto t1 = std::format("toFloat64({})", fixed_point_str);
    auto t2 = std::format("toFloat64({})", expression_str);
    int dir = t2 >= t1 ? 0 : -1;

    if (time_span.parseConstKQLTimespan(bin_size_str))
    {
        bin_size = time_span.toSeconds();

        out = std::format("toDateTime64({} + toInt64(({} -{}) / {} + {}) * {}, 9, 'UTC')", t1, t2, t1, bin_size, dir, bin_size);
    }
    else
    {
        bin_size =  std::stod(bin_size_str);
        out = std::format("{} + toInt64(({} -{}) / {} + {}) * {}", t1, t2, t1, bin_size, dir, bin_size);
    }
    return true;
}

}
