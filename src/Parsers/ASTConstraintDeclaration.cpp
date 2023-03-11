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

void ASTConstraintDeclaration::formatImpl(const FormattingBuffer & out) const
{
    out.ostr << backQuoteIfNeed(name);
    out.writeKeyword(type == Type::CHECK ? " CHECK " : " ASSUME ");
    expr->formatImpl(out);
}

}
