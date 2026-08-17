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

void ASTConstraintDeclaration::formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    ostr << backQuoteIfNeed(name);
    ostr << (type == Type::CHECK ? " CHECK " : " ASSUME ");
    expr->format(ostr, s, state, frame);
}

}
