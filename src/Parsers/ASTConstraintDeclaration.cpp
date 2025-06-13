#include <Parsers/ASTConstraintDeclaration.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

ASTPtr ASTConstraintDeclaration::clone() const
{
    auto res = std::make_shared<ASTConstraintDeclaration>();

    res->name = name;
    res->type = type;

    if (expr)
        res->set(res->expr, expr->clone());

    return res;
}

void ASTConstraintDeclaration::formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    s.ostr << backQuoteIfNeed(name);
    s.ostr << (s.hilite ? hilite_keyword : "") << (type == Type::CHECK ? " CHECK " : " ASSUME ") << (s.hilite ? hilite_none : "");
    expr->formatImpl(s, state, frame);
}

}
