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
#include <Parsers/ParserSetQuery.h>

namespace DB
{

bool SeriesFir::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool SeriesIir::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool SeriesFitLine::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool SeriesFitLineDynamic::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool SeriesFit2lines::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool SeriesFit2linesDynamic::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool SeriesOutliers::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool SeriesPeriodsDetect::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool SeriesPeriodsValidate::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool SeriesStatsDynamic::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool SeriesStats::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool SeriesFillBackward::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool SeriesFillConst::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool SeriesFillForward::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool SeriesFillLinear::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

}
