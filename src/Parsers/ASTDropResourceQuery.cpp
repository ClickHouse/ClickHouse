#include <Parsers/ASTDropResourceQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTDropResourceQuery::clone() const
{
    return std::make_shared<ASTDropResourceQuery>(*this);
}

void ASTDropResourceQuery::formatImpl(WriteBuffer & ostr, const IAST::FormatSettings & settings, IAST::FormatState &, IAST::FormatStateStacked) const
{
    ostr << "DROP RESOURCE ";

    if (if_exists)
        ostr << "IF EXISTS ";

    ostr << backQuoteIfNeed(resource_name);
    formatOnCluster(ostr, settings);
}

}
