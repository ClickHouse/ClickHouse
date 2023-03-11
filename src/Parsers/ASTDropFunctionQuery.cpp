#include <Parsers/ASTDropFunctionQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTDropFunctionQuery::clone() const
{
    return std::make_shared<ASTDropFunctionQuery>(*this);
}

void ASTDropFunctionQuery::formatImpl(const FormattingBuffer & out) const
{
    out.writeKeyword("DROP FUNCTION ");
    if (if_exists)
        out.writeKeyword("IF EXISTS ");
    out.writeProbablyBackQuotedIdentifier(function_name);
    formatOnCluster(out);
}

}
