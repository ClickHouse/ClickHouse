#include <Parsers/ASTIndexDeclaration.h>
#include <Common/quoteString.h>


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


void ASTIndexDeclaration::formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    s.ostr << backQuoteIfNeed(name);
    s.ostr << " ";
    expr->formatImpl(s, state, frame);
    s.ostr << (s.hilite ? hilite_keyword : "") << " TYPE " << (s.hilite ? hilite_none : "");
    type->formatImpl(s, state, frame);
    s.ostr << (s.hilite ? hilite_keyword : "") << " GRANULARITY " << (s.hilite ? hilite_none : "");
    s.ostr << granularity;
}

}

