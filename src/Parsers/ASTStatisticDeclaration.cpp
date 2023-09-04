#include <Parsers/ASTStatisticDeclaration.h>

#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/ASTFunction.h>


namespace DB
{

ASTPtr ASTStatisticDeclaration::clone() const
{
    auto res = std::make_shared<ASTStatisticDeclaration>();

    res->column_name = column_name;
    res->type = type;

    return res;
}


void ASTStatisticDeclaration::formatImpl(const FormatSettings & s, FormatState &, FormatStateStacked) const
{
    s.ostr << backQuoteIfNeed(column_name);
    s.ostr << " ";
    s.ostr << (s.hilite ? hilite_keyword : "") << " TYPE " << (s.hilite ? hilite_none : "");
    s.ostr << backQuoteIfNeed(type);
}

}

