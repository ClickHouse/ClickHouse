#include <Parsers/ASTShowCreateAccessEntityQuery.h>
#include <Parsers/ASTRowPolicyName.h>
#include <Common/quoteString.h>


namespace DB
{
namespace
{
    using EntityTypeInfo = IAccessEntity::TypeInfo;

    void formatNames(const Strings & names, const IAST::FormatSettings & settings)
    {
        bool need_comma = false;
        for (const auto & name : names)
        {
            if (std::exchange(need_comma, true))
                settings.ostr << ',';
            settings.ostr << ' ' << backQuoteIfNeed(name);
        }
    }
}


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

    if (current_user || current_quota)
    {
    }
    else if (type == EntityType::ROW_POLICY)
    {
        settings.ostr << " ";
        row_policy_names->format(settings);
    }
    else
        formatNames(names, settings);
}


void ASTShowCreateAccessEntityQuery::replaceEmptyDatabaseWithCurrent(const String & current_database)
{
    if (row_policy_names)
        row_policy_names->replaceEmptyDatabaseWithCurrent(current_database);
}

}
