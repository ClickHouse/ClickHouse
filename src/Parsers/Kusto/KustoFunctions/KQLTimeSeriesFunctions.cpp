#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
#include <Parsers/Kusto/KustoFunctions/KQLTimeSeriesFunctions.h>
#include <Parsers/Kusto/ParserKQLQuery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

bool SeriesFir::convertImpl([[maybe_unused]] String & out, [[maybe_unused]] IKQLParser::KQLPos & pos)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not yet implemented", getName());
}

bool SeriesIir::convertImpl([[maybe_unused]] String & out, [[maybe_unused]] IKQLParser::KQLPos & pos)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not yet implemented", getName());}

bool SeriesFitLine::convertImpl([[maybe_unused]] String & out, [[maybe_unused]] IKQLParser::KQLPos & pos)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not yet implemented", getName());
}

bool SeriesFitLineDynamic::convertImpl([[maybe_unused]] String & out, [[maybe_unused]] IKQLParser::KQLPos & pos)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not yet implemented", getName());
}

bool SeriesFit2lines::convertImpl([[maybe_unused]] String & out, [[maybe_unused]] IKQLParser::KQLPos & pos)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not yet implemented", getName());
}

bool SeriesFit2linesDynamic::convertImpl([[maybe_unused]] String & out, [[maybe_unused]] IKQLParser::KQLPos & pos)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not yet implemented", getName());
}

bool SeriesOutliers::convertImpl([[maybe_unused]] String & out, [[maybe_unused]] IKQLParser::KQLPos & pos)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not yet implemented", getName());
}

bool SeriesPeriodsDetect::convertImpl([[maybe_unused]] String & out, [[maybe_unused]] IKQLParser::KQLPos & pos)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not yet implemented", getName());
}

bool SeriesPeriodsValidate::convertImpl([[maybe_unused]] String & out, [[maybe_unused]] IKQLParser::KQLPos & pos)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not yet implemented", getName());
}

bool SeriesStatsDynamic::convertImpl([[maybe_unused]] String & out, [[maybe_unused]] IKQLParser::KQLPos & pos)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not yet implemented", getName());
}

bool SeriesStats::convertImpl([[maybe_unused]] String & out, [[maybe_unused]] IKQLParser::KQLPos & pos)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not yet implemented", getName());
}

bool SeriesFillBackward::convertImpl([[maybe_unused]] String & out, [[maybe_unused]] IKQLParser::KQLPos & pos)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not yet implemented", getName());
}

bool SeriesFillConst::convertImpl([[maybe_unused]] String & out, [[maybe_unused]] IKQLParser::KQLPos & pos)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not yet implemented", getName());
}

bool SeriesFillForward::convertImpl([[maybe_unused]] String & out, [[maybe_unused]] IKQLParser::KQLPos & pos)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not yet implemented", getName());
}

bool SeriesFillLinear::convertImpl([[maybe_unused]] String & out, [[maybe_unused]] IKQLParser::KQLPos & pos)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not yet implemented", getName());
}

}
