#include <Parsers/ASTDropNamedCollectionQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTDropNamedCollectionQuery::clone() const
{
    return make_intrusive<ASTDropNamedCollectionQuery>(*this);
}

void ASTDropNamedCollectionQuery::formatImpl(WriteBuffer & ostr, const IAST::FormatSettings & settings, IAST::FormatState &, IAST::FormatStateStacked) const
{
    ostr << "DROP NAMED COLLECTION ";
    if (if_exists)
        ostr << "IF EXISTS ";
    ostr << backQuoteIfNeed(collection_name);
    formatOnCluster(ostr, settings);
}

}
