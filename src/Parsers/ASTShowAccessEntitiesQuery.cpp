#include <Parsers/ASTShowAccessEntitiesQuery.h>
#include <Common/quoteString.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


const char * ASTShowAccessEntitiesQuery::getKeyword() const
{
    switch (type)
    {
        case EntityType::ROW_POLICY:
            return "SHOW ROW POLICIES";
        case EntityType::QUOTA:
            return current_quota ? "SHOW CURRENT QUOTA" : "SHOW QUOTAS";
        case EntityType::SETTINGS_PROFILE:
            return "SHOW SETTINGS PROFILES";
        default:
            throw Exception(toString(type) + ": type is not supported by SHOW query", ErrorCodes::NOT_IMPLEMENTED);
    }
}


String ASTShowAccessEntitiesQuery::getID(char) const
{
    return String(getKeyword()) + " query";
}

void ASTShowAccessEntitiesQuery::formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    const char * keyword = getKeyword();
    settings.ostr << (settings.hilite ? hilite_keyword : "") << keyword << (settings.hilite ? hilite_none : "");

    if ((type == EntityType::ROW_POLICY) && !table_name.empty())
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " ON " << (settings.hilite ? hilite_none : "");
        if (!database.empty())
            settings.ostr << backQuoteIfNeed(database) << ".";
        settings.ostr << backQuoteIfNeed(table_name);
    }
}

}
