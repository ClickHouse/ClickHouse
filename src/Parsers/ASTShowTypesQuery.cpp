#include <Parsers/ASTShowTypesQuery.h>
#include <IO/Operators.h> // Для оператора << у WriteBuffer

namespace DB
{

ASTPtr ASTShowTypesQuery::clone() const 
{
    auto res = std::make_shared<ASTShowTypesQuery>(*this);
    res->children.clear();
    cloneOutputOptions(*res);
    return res;
}

void ASTShowTypesQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    ostr << (settings.hilite ? hilite_keyword : "") << "SHOW TYPES" << (settings.hilite ? hilite_none : "");
}

}
