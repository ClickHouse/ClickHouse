#include <Parsers/ASTDropNamedCollectionQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTDropNamedCollectionQuery::clone() const
{
    return std::make_shared<ASTDropNamedCollectionQuery>(*this);
}

void ASTDropNamedCollectionQuery::formatImpl(WriteBuffer & ostr, const IAST::FormatSettings & settings, IAST::FormatState &, IAST::FormatStateStacked) const
{
    ostr << (settings.hilite ? hilite_keyword : "") << "DROP NAMED COLLECTION ";
    if (if_exists)
        ostr << "IF EXISTS ";
    ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(collection_name) << (settings.hilite ? hilite_none : "");
    formatOnCluster(ostr, settings);
}

}
