#include <Parsers/ASTShowTypesQuery.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTShowTypesQuery::clone() const
{
    auto res = make_intrusive<ASTShowTypesQuery>(*this);
    res->children.clear();
    cloneOutputOptions(*res);
    return res;
}

void ASTShowTypesQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings &, FormatState &, FormatStateStacked) const
{
    ostr << "SHOW TYPES";
}

}
