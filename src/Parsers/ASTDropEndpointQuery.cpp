#include <Parsers/ASTDropEndpointQuery.h>

#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

ASTPtr ASTDropEndpointQuery::clone() const
{
    return make_intrusive<ASTDropEndpointQuery>(*this);
}

void ASTDropEndpointQuery::formatImpl(WriteBuffer & ostr, const FormatSettings &, FormatState &, FormatStateStacked) const
{
    ostr << "DROP ENDPOINT ";
    if (if_exists)
        ostr << "IF EXISTS ";
    ostr << backQuoteIfNeed(endpoint_name);
}

}
