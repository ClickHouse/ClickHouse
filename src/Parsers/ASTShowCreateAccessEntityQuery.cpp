#include <Parsers/ASTShowCreateAccessEntityQuery.h>
#include <Common/quoteString.h>


namespace DB
{
namespace
{
    using Kind = ASTShowCreateAccessEntityQuery::Kind;

    const char * getKeyword(Kind kind)
    {
        switch (kind)
        {
            case Kind::USER: return "USER";
            case Kind::ROLE: return "ROLE";
            case Kind::QUOTA: return "QUOTA";
            case Kind::ROW_POLICY: return "ROW POLICY";
            case Kind::SETTINGS_PROFILE: return "SETTINGS PROFILE";
        }
        __builtin_unreachable();
    }
}


ASTShowCreateAccessEntityQuery::ASTShowCreateAccessEntityQuery(Kind kind_)
    : kind(kind_)
{
}


String ASTShowCreateAccessEntityQuery::getID(char) const
{
    return String("SHOW CREATE ") + getKeyword(kind) + " query";
}


ASTPtr ASTShowCreateAccessEntityQuery::clone() const
{
    return std::make_shared<ASTShowCreateAccessEntityQuery>(*this);
}


void ASTShowCreateAccessEntityQuery::formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "")
                  << "SHOW CREATE " << getKeyword(kind)
                  << (settings.hilite ? hilite_none : "");

    if (current_user)
    {
    }
    else if (current_quota)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " CURRENT" << (settings.hilite ? hilite_none : "");
    else if (kind == Kind::ROW_POLICY)
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
