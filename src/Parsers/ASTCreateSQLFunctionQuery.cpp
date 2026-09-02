#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/ASTCreateSQLFunctionQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

ASTPtr ASTCreateSQLFunctionQuery::clone() const
{
    auto res = make_intrusive<ASTCreateSQLFunctionQuery>(*this);
    res->children.clear();

    res->function_name = function_name->clone();
    res->children.push_back(res->function_name);

    res->function_core = function_core->clone();
    res->children.push_back(res->function_core);
    return res;
}

void ASTCreateSQLFunctionQuery::formatImpl(WriteBuffer & ostr, const IAST::FormatSettings & settings, IAST::FormatState & state, IAST::FormatStateStacked frame) const
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

String ASTCreateSQLFunctionQuery::getFunctionName() const
{
    String name;
    bool is_ok = tryGetIdentifierNameInto(function_name, name);
    if (!is_ok)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected function name, got '{}'", function_name->formatForErrorMessage());
    return name;
}


}
