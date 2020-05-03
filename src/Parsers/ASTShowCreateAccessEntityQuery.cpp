#include <Parsers/ASTShowCreateAccessEntityQuery.h>
#include <Common/quoteString.h>


namespace DB
{
using EntityTypeInfo = IAccessEntity::TypeInfo;


String ASTShowCreateAccessEntityQuery::getID(char) const
{
    return String("SHOW CREATE ") + toString(type) + " query";
}


ASTPtr ASTShowCreateAccessEntityQuery::clone() const
{
    return std::make_shared<ASTShowCreateAccessEntityQuery>(*this);
}


void ASTShowCreateAccessEntityQuery::formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "")
                  << "SHOW CREATE " << EntityTypeInfo::get(type).name
                  << (settings.hilite ? hilite_none : "");

    if (current_user)
    {
    }
    else if (current_quota)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " CURRENT" << (settings.hilite ? hilite_none : "");
    else if (type == EntityType::ROW_POLICY)
    {
        const String & database = row_policy_name_parts.database;
        const String & table_name = row_policy_name_parts.table_name;
        const String & short_name = row_policy_name_parts.short_name;
        settings.ostr << ' ' << backQuoteIfNeed(short_name) << (settings.hilite ? hilite_keyword : "") << " ON "
                      << (settings.hilite ? hilite_none : "") << (database.empty() ? String{} : backQuoteIfNeed(database) + ".")
                      << backQuoteIfNeed(table_name);
    }
    else
        settings.ostr << " " << backQuoteIfNeed(name);
}
}
