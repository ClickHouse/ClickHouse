#include <Parsers/ASTDropDataTypeQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTDropDataTypeQuery::clone() const
{
    return std::make_shared<ASTDropDataTypeQuery>(*this);
}

void ASTDropDataTypeQuery::formatImpl(const IAST::FormatSettings & settings, IAST::FormatState &, IAST::FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "DROP TYPE " << (settings.hilite ? hilite_none : "");
    settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(type_name) << (settings.hilite ? hilite_none : "");
}

}
