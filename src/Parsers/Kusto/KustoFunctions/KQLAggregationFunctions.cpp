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

namespace DB
{

bool ArgMax::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out,pos,"argMax");
}

bool ArgMin::convertImpl(String &out,IParser::Pos &pos)
{

    return directMapping(out,pos,"argMin");
}

bool Avg::convertImpl(String &out,IParser::Pos &pos)
{

    return directMapping(out,pos,"avg");
}

bool AvgIf::convertImpl(String &out,IParser::Pos &pos)
{

    return directMapping(out,pos,"avgIf");

}

bool BinaryAllAnd::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool BinaryAllOr::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool BinaryAllXor::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool BuildSchema::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool Count::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    return directMapping(out,pos,"count");

}

bool CountIf::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    return directMapping(out,pos,"countIf");
}

bool DCount::convertImpl(String &out,IParser::Pos &pos)
{

    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;
    ++pos;
    String value = getConvertedArgument(fn_name,pos);
    
    out = "count ( DISTINCT " + value + " ) ";
    return true;

}

bool DCountIf::convertImpl(String &out,IParser::Pos &pos)
{

    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;
    ++pos;
    String value = getConvertedArgument(fn_name,pos);
    ++pos;
    String condition = getConvertedArgument(fn_name,pos);
    out = "countIf ( DISTINCT " + value + " , " + condition + " ) ";
    return true;

}

bool MakeBag::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool MakeBagIf::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool MakeList::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool MakeListIf::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool MakeListWithNulls::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool MakeSet::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool MakeSetIf::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool Max::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    return directMapping(out,pos,"max");

}

bool MaxIf::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);

    return directMapping(out,pos,"maxIf");
}

bool Min::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);

    return directMapping(out,pos,"min");
}

bool MinIf::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    return directMapping(out,pos,"minIf");

}

bool Percentiles::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool PercentilesArray::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool Percentilesw::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool PercentileswArray::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool Stdev::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool StdevIf::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool Sum::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);

    return directMapping(out,pos,"sum");

}

bool SumIf::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    return directMapping(out,pos,"sumIf");
}

bool TakeAny::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool TakeAnyIf::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool Variance::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool VarianceIf::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

}
