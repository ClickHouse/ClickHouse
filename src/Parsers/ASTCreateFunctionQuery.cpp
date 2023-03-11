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

void ASTCreateFunctionQuery::formatImpl(const FormattingBuffer & out) const
{
    out.writeKeyword("CREATE ");

    if (or_replace)
        out.writeKeyword("OR REPLACE ");

    out.writeKeyword("FUNCTION ");

    if (if_not_exists)
        out.writeKeyword("IF NOT EXISTS ");

    out.writeProbablyBackQuotedIdentifier(getFunctionName());

    formatOnCluster(out);

    out.writeKeyword(" AS ");
    function_core->formatImpl(out);
}

String ASTCreateFunctionQuery::getFunctionName() const
{
    String name;
    tryGetIdentifierNameInto(function_name, name);
    return name;
}

}
