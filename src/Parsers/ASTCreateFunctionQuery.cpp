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

void ASTCreateFunctionQuery::formatImpl(const IAST::FormatSettings & settings, IAST::FormatState & state, IAST::FormatStateStacked frame) const
{
    settings.writeKeyword("CREATE ");

    if (or_replace)
        settings.writeKeyword("OR REPLACE ");

    settings.writeKeyword("FUNCTION ");

    if (if_not_exists)
        settings.writeKeyword("IF NOT EXISTS ");

    settings.writeProbablyBackQuotedIdentifier(getFunctionName());

    formatOnCluster(settings);

    settings.writeKeyword(" AS ");
    function_core->formatImpl(settings, state, frame);
}

String ASTCreateFunctionQuery::getFunctionName() const
{
    String name;
    tryGetIdentifierNameInto(function_name, name);
    return name;
}

}
