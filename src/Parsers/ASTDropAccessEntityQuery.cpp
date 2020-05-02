#include <Parsers/ASTDropAccessEntityQuery.h>
#include <Common/quoteString.h>


namespace DB
{
namespace
{
    using Kind = ASTDropAccessEntityQuery::Kind;

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


ASTDropAccessEntityQuery::ASTDropAccessEntityQuery(Kind kind_)
    : kind(kind_)
{
}


String ASTDropAccessEntityQuery::getID(char) const
{
    return String("DROP ") + getKeyword(kind) + " query";
}


ASTPtr ASTDropAccessEntityQuery::clone() const
{
    return std::make_shared<ASTDropAccessEntityQuery>(*this);
}


void ASTDropAccessEntityQuery::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "")
                  << "DROP " << getKeyword(kind)
                  << (if_exists ? " IF EXISTS" : "")
                  << (settings.hilite ? hilite_none : "");

    if (kind == Kind::ROW_POLICY)
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
