#include <Parsers/ASTStatisticDeclaration.h>

#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/ASTFunction.h>


namespace DB
{

ASTPtr ASTStatisticDeclaration::clone() const
{
    auto res = std::make_shared<ASTStatisticDeclaration>();

    res->name = name;

    if (columns)
        res->set(res->columns, columns->clone());
    if (type)
        res->set(res->type, type->clone());
    return res;
}


void ASTStatisticDeclaration::formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    s.ostr << backQuoteIfNeed(name);
    s.ostr << " ";
    columns->formatImpl(s, state, frame);
    s.ostr << (s.hilite ? hilite_keyword : "") << " TYPE " << (s.hilite ? hilite_none : "");
    type->formatImpl(s, state, frame);
}

}

