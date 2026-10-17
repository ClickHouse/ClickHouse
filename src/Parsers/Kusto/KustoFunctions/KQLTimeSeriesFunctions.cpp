#include <Parsers/Kusto/KustoFunctions/KQLTimeSeriesFunctions.h>

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
