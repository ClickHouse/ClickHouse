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
    switch (kind)
    {
        case Kind::ROLE: return "ROLE";
        case Kind::USER: return "USER";
        case Kind::SETTINGS_PROFILE: return "SETTINGS PROFILE";
        case Kind::QUOTA: return "QUOTA";
        case Kind::ROW_POLICY: return "ROW POLICY";
        default:
            throw Exception("Unknown kind of the drop query: " + std::to_string(static_cast<int>(kind)), ErrorCodes::LOGICAL_ERROR);
    }
}
}
