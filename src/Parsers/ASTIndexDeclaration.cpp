#include <Parsers/ASTIndexDeclaration.h>

#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/ASTFunction.h>


namespace DB
{

ASTPtr ASTIndexDeclaration::clone() const
{
    auto res = std::make_shared<ASTIndexDeclaration>();

    res->name = name;
    res->granularity = granularity;

    if (expr)
        res->set(res->expr, expr->clone());
    if (type)
        res->set(res->type, type->clone());
    return res;
}


void ASTIndexDeclaration::formatImpl(const FormattingBuffer & out) const
{
    if (part_of_create_index_query)
    {
        out.ostr << "(";
        expr->formatImpl(out);
        out.ostr << ")";
    }
    else
    {
        out.ostr << backQuoteIfNeed(name);
        out.ostr << " ";
        expr->formatImpl(out);
    }

    out.writeKeyword(" TYPE ");
    type->formatImpl(out);
    out.writeKeyword(" GRANULARITY ");
    out.ostr << granularity;
}

}

