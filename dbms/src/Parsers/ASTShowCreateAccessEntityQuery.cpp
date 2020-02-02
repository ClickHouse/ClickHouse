#include <Parsers/ASTShowCreateAccessEntityQuery.h>
#include <Common/quoteString.h>


namespace DB
{
namespace
{
    using Kind = ASTShowCreateAccessEntityQuery::Kind;

    const char * kindToKeyword(Kind kind)
    {
        switch (kind)
        {
            case Kind::QUOTA: return "QUOTA";
            case Kind::ROW_POLICY: return "POLICY";
        }
        __builtin_unreachable();
    }
}


ASTShowCreateAccessEntityQuery::ASTShowCreateAccessEntityQuery(Kind kind_)
    : kind(kind_), keyword(kindToKeyword(kind_))
{
}


String ASTShowCreateAccessEntityQuery::getID(char) const
{
    return String("SHOW CREATE ") + keyword + " query";
}


ASTPtr ASTShowCreateAccessEntityQuery::clone() const
{
    return std::make_shared<ASTShowCreateAccessEntityQuery>(*this);
}


void ASTShowCreateAccessEntityQuery::formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "")
                  << "SHOW CREATE " << keyword
                  << (settings.hilite ? hilite_none : "");

    if (kind == Kind::ROW_POLICY)
    {
        const String & database = row_policy_name.database;
        const String & table_name = row_policy_name.table_name;
        const String & policy_name = row_policy_name.policy_name;
        settings.ostr << ' ' << backQuoteIfNeed(policy_name) << (settings.hilite ? hilite_keyword : "") << " ON "
                      << (settings.hilite ? hilite_none : "") << (database.empty() ? String{} : backQuoteIfNeed(database) + ".")
                      << backQuoteIfNeed(table_name);
    }
    else if ((kind == Kind::QUOTA) && current_quota)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " CURRENT" << (settings.hilite ? hilite_none : "");
    else
        settings.ostr << " " << backQuoteIfNeed(name);
}
}
