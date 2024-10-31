#include <Parsers/ASTDropResourceQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTDropResourceQuery::clone() const
{
    return std::make_shared<ASTDropResourceQuery>(*this);
}

void ASTDropResourceQuery::formatImpl(const IAST::FormatSettings & settings, IAST::FormatState &, IAST::FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "DROP RESOURCE ";

    if (if_exists)
        settings.ostr << "IF EXISTS ";

    settings.ostr << (settings.hilite ? hilite_none : "");
    settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(resource_name) << (settings.hilite ? hilite_none : "");
    formatOnCluster(settings);
}

}
