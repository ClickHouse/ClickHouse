#include <Parsers/ASTCreateDataTypeQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTCreateDataTypeQuery::clone() const
{
    return std::make_shared<ASTCreateDataTypeQuery>(*this);
}

void ASTCreateDataTypeQuery::formatImpl(const IAST::FormatSettings & settings, IAST::FormatState & state, IAST::FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "CREATE TYPE " << (settings.hilite ? hilite_none : "");
    settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(type_name) << (settings.hilite ? hilite_none : "");
    settings.ostr << (settings.hilite ? hilite_keyword : "") << " AS " << (settings.hilite ? hilite_none : "");
    nested->formatImpl(settings, state, frame);
    settings.ostr << "(" << (settings.hilite ? hilite_keyword : "") << " INPUT  " << (settings.hilite ? hilite_none : "") << " = ";
    settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(input_function) << (settings.hilite ? hilite_none : "");
    settings.ostr << "," << (settings.hilite ? hilite_keyword : "") << " OUTPUT  " << (settings.hilite ? hilite_none : "") << " = ";
    settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(output_function) << (settings.hilite ? hilite_none : "") << ")";

}

}
