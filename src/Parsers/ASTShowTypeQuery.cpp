#include <Parsers/ASTShowTypeQuery.h>
#include <IO/Operators.h>
#include <Common/quoteString.h>

namespace DB
{

ASTPtr ASTShowTypeQuery::clone() const 
{
    auto res = std::make_shared<ASTShowTypeQuery>(*this);
    res->children.clear();
    cloneOutputOptions(*res);
    return res;
}

void ASTShowTypeQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings &, FormatState &, FormatStateStacked) const
{
    ostr << "SHOW TYPE " << backQuoteIfNeed(type_name);
}

}
