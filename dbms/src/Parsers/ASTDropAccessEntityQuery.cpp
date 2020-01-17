#include <Parsers/ASTDropAccessEntityQuery.h>
#include <Common/quoteString.h>


namespace DB
{
namespace
{
    using Kind = ASTDropAccessEntityQuery::Kind;

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


ASTDropAccessEntityQuery::ASTDropAccessEntityQuery(Kind kind_)
    : kind(kind_), keyword(kindToKeyword(kind_))
{
}


String ASTDropAccessEntityQuery::getID(char) const
{
    return String("DROP ") + keyword + " query";
}


ASTPtr ASTDropAccessEntityQuery::clone() const
{
    return std::make_shared<ASTDropAccessEntityQuery>(*this);
}


void ASTDropAccessEntityQuery::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "")
                  << "DROP " << keyword
                  << (if_exists ? " IF EXISTS" : "")
                  << (settings.hilite ? hilite_none : "");

    if (kind == Kind::ROW_POLICY)
    {
        bool need_comma = false;
        for (const auto & row_policy_name : row_policies_names)
        {
            if (need_comma)
                settings.ostr << ',';
            need_comma = true;
            const String & database = row_policy_name.database;
            const String & table_name = row_policy_name.table_name;
            const String & policy_name = row_policy_name.policy_name;
            settings.ostr << ' ' << backQuoteIfNeed(policy_name) << (settings.hilite ? hilite_keyword : "") << " ON "
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
}
}
