#include <Parsers/ASTDropAccessQuery.h>


namespace DB
{
ASTPtr ASTDropAccessQuery::clone() const
{
    return std::make_shared<ASTDropAccessQuery>(*this);
}


void ASTDropAccessQuery::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "")
                  << "DROP " << getSQLTypeName()
                  << (if_exists ? " IF EXISTS" : "")
                  << (settings.hilite ? hilite_none : "");

    for (size_t i = 0; i != names.size(); ++i)
        settings.ostr << (i ? ", " : " ") << backQuoteIfNeed(names[i]);
}


const char * ASTDropAccessQuery::getSQLTypeName() const
{
    return kind == Kind::ROLE ? "ROLE" : "USER";
}
}
