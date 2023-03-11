#include <Parsers/ASTDropNamedCollectionQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTDropNamedCollectionQuery::clone() const
{
    return std::make_shared<ASTDropNamedCollectionQuery>(*this);
}

void ASTDropNamedCollectionQuery::formatImpl(const FormattingBuffer & out) const
{
    out.writeKeyword("DROP NAMED COLLECTION ");
    out.writeProbablyBackQuotedIdentifier(collection_name);
    formatOnCluster(out);
}

}
