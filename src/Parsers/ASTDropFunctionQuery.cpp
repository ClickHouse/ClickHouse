#include <Parsers/ASTDropFunctionQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTDropFunctionQuery::clone() const
{
    return std::make_shared<ASTDropFunctionQuery>(*this);
}

void ASTDropFunctionQuery::formatImpl(WriteBuffer & ostr, const IAST::FormatSettings & settings, IAST::FormatState &, IAST::FormatStateStacked) const
{
    ostr << "DROP FUNCTION ";

    if (if_exists)
        ostr << "IF EXISTS ";

    ostr << backQuoteIfNeed(function_name);
    formatOnCluster(ostr, settings);
}

}
