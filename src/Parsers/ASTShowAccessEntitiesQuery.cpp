#include <Parsers/ASTShowAccessEntitiesQuery.h>
#include <Common/quoteString.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


String ASTShowAccessEntitiesQuery::getID(char) const
{
    if (type == EntityType::ROW_POLICY)
        return "SHOW ROW POLICIES query";
    else if (type == EntityType::QUOTA)
        return current_quota ? "SHOW CURRENT QUOTA query" : "SHOW QUOTAS query";
    else
        throw Exception(toString(type) + ": type is not supported by SHOW query", ErrorCodes::NOT_IMPLEMENTED);
}

void ASTShowAccessEntitiesQuery::formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    if (type == EntityType::ROW_POLICY)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "SHOW ROW POLICIES" << (settings.hilite ? hilite_none : "");
    else if (type == EntityType::QUOTA)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << (current_quota ? "SHOW CURRENT QUOTA" : "SHOW QUOTAS") << (settings.hilite ? hilite_none : "");
    else
        throw Exception(toString(type) + ": type is not supported by SHOW query", ErrorCodes::NOT_IMPLEMENTED);

    if ((type == EntityType::ROW_POLICY) && !table_name.empty())
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " ON " << (settings.hilite ? hilite_none : "");
        if (!database.empty())
            settings.ostr << backQuoteIfNeed(database) << ".";
        settings.ostr << backQuoteIfNeed(table_name);
    }
}

}
