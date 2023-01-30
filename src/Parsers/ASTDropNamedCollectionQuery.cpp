#include <Parsers/ASTDropNamedCollectionQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTDropNamedCollectionQuery::clone() const
{
    return std::make_shared<ASTDropNamedCollectionQuery>(*this);
}

void ASTDropNamedCollectionQuery::formatImpl(const IAST::FormatSettings & settings, IAST::FormatState &, IAST::FormatStateStacked) const
{
    settings.writeKeyword("DROP NAMED COLLECTION ");
    settings.writeProbablyBackQuotedIdentifier(collection_name);
    formatOnCluster(settings);
}

}
