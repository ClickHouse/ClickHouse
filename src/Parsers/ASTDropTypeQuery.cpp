#include <Parsers/ASTDropTypeQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

String ASTDropTypeQuery::getID(char delim) const
{
    return "DropTypeQuery" + (delim + type_name);
}

ASTPtr ASTDropTypeQuery::clone() const
{
    auto res = std::make_shared<ASTDropTypeQuery>(*this);
    // res->type_name и res->if_exists уже скопированы конструктором по умолчанию
    return res;
}

void ASTDropTypeQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    ostr << (settings.hilite ? hilite_keyword : "") << "DROP TYPE " << (settings.hilite ? hilite_none : "");

    if (if_exists)
        ostr << (settings.hilite ? hilite_keyword : "") << "IF EXISTS " << (settings.hilite ? hilite_none : "");

    ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(type_name) << (settings.hilite ? hilite_none : "");
}

}
