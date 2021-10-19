#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/ASTCreateFunctionQuery.h>

namespace DB
{

ASTPtr ASTCreateFunctionQuery::clone() const
{
    return std::make_shared<ASTCreateFunctionQuery>(*this);
}

void ASTCreateFunctionQuery::formatImpl(const IAST::FormatSettings & settings, IAST::FormatState & state, IAST::FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "CREATE FUNCTION " << (settings.hilite ? hilite_none : "");
    settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(function_name) << (settings.hilite ? hilite_none : "");
    settings.ostr << (settings.hilite ? hilite_keyword : "") << " AS " << (settings.hilite ? hilite_none : "");
    function_core->formatImpl(settings, state, frame);
}

}
