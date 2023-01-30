#include <Parsers/ASTDropFunctionQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTDropFunctionQuery::clone() const
{
    return std::make_shared<ASTDropFunctionQuery>(*this);
}

void ASTDropFunctionQuery::formatImpl(const IAST::FormatSettings & settings, IAST::FormatState &, IAST::FormatStateStacked) const
{
    settings.writeKeyword("DROP FUNCTION ");
    if (if_exists)
        settings.writeKeyword("IF EXISTS ");
    settings.writeProbablyBackQuotedIdentifier(function_name);
    formatOnCluster(settings);
}

}
