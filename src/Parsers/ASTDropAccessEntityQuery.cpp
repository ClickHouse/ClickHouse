#include <Parsers/ASTDropAccessEntityQuery.h>
#include <Common/quoteString.h>


namespace DB
{
using EntityTypeInfo = IAccessEntity::TypeInfo;


String ASTDropAccessEntityQuery::getID(char) const
{
    return String("DROP ") + toString(type) + " query";
}


ASTPtr ASTDropAccessEntityQuery::clone() const
{
    return std::make_shared<ASTDropAccessEntityQuery>(*this);
}


void ASTDropAccessEntityQuery::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "")
                  << "DROP " << EntityTypeInfo::get(type).name
                  << (if_exists ? " IF EXISTS" : "")
                  << (settings.hilite ? hilite_none : "");

    if (type == EntityType::ROW_POLICY)
    {
        bool need_comma = false;
        for (const auto & name_parts : row_policies_name_parts)
        {
            if (need_comma)
                settings.ostr << ',';
            need_comma = true;
            const String & database = name_parts.database;
            const String & table_name = name_parts.table_name;
            const String & short_name = name_parts.short_name;
            settings.ostr << ' ' << backQuoteIfNeed(short_name) << (settings.hilite ? hilite_keyword : "") << " ON "
                          << (settings.hilite ? hilite_none : "") << (database.empty() ? String{} : backQuoteIfNeed(database) + ".")
                          << backQuoteIfNeed(table_name);
        }
    }
    else
    {
        bool need_comma = false;
        for (const auto & name : names)
        {
            if (need_comma)
                settings.ostr << ',';
            need_comma = true;
            settings.ostr << ' ' << backQuoteIfNeed(name);
        }
    }

    formatOnCluster(settings);
}
}
