#include <Parsers/ASTDropHandlerQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

ASTPtr ASTDropHandlerQuery::clone() const
{
    return make_intrusive<ASTDropHandlerQuery>(*this);
}

void ASTDropHandlerQuery::formatImpl(WriteBuffer & ostr, const IAST::FormatSettings & settings, IAST::FormatState &, IAST::FormatStateStacked) const
{
    ostr << "DROP HANDLER ";
    if (if_exists)
        ostr << "IF EXISTS ";
    ostr << backQuoteIfNeed(handler_name);
    formatOnCluster(ostr, settings);
}

}
