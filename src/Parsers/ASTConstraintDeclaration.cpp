#include <Parsers/ASTConstraintDeclaration.h>
#include <Common/quoteString.h>


namespace DB
{

ASTPtr ASTConstraintDeclaration::clone() const
{
    auto res = std::make_shared<ASTConstraintDeclaration>();

    res->name = name;

    if (expr)
        res->set(res->expr, expr->clone());

    return res;
}

void ASTConstraintDeclaration::formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    s.ostr << backQuoteIfNeed(name);
    s.ostr << (s.hilite ? hilite_keyword : "") << " CHECK " << (s.hilite ? hilite_none : "");
    expr->formatImpl(s, state, frame);
}

}
