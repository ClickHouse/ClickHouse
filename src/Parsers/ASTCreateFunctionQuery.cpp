#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>


namespace DB
{

ASTPtr ASTCreateFunctionQuery::clone() const
{
    auto res = std::make_shared<ASTCreateFunctionQuery>(*this);
    res->children.clear();

    res->function_name = function_name->clone();
    res->children.push_back(res->function_name);

    res->function_core = function_core->clone();
    res->children.push_back(res->function_core);
    return res;
}

void ASTCreateFunctionQuery::formatImpl(WriteBuffer & ostr, const IAST::FormatSettings & settings, IAST::FormatState & state, IAST::FormatStateStacked frame) const
{
    ostr << "CREATE ";

    if (or_replace)
        ostr << "OR REPLACE ";

    ostr << "FUNCTION ";

    if (if_not_exists)
        ostr << "IF NOT EXISTS ";

    ostr << backQuoteIfNeed(getFunctionName());

    formatOnCluster(ostr, settings);

    ostr << " AS ";
    function_core->format(ostr, settings, state, frame);
}

String ASTCreateFunctionQuery::getFunctionName() const
{
    String name;
    tryGetIdentifierNameInto(function_name, name);
    return name;
}

}
