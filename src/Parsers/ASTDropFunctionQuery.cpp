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
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "DROP FUNCTION ";

    if (if_exists)
        settings.ostr << "IF EXISTS ";

    settings.ostr << (settings.hilite ? hilite_none : "");
    settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(function_name) << (settings.hilite ? hilite_none : "");
    formatOnCluster(settings);
}

}
