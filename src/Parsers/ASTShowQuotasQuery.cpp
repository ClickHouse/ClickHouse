#include <Parsers/ASTShowQuotasQuery.h>
#include <Common/quoteString.h>


namespace DB
{
String ASTShowQuotasQuery::getID(char) const
{
    if (usage)
        return "SHOW QUOTA USAGE query";
    else
        return "SHOW QUOTAS query";
}


ASTPtr ASTShowQuotasQuery::clone() const
{
    return std::make_shared<ASTShowQuotasQuery>(*this);
}


void ASTShowQuotasQuery::formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "");

    if (usage && current)
        settings.ostr << "SHOW QUOTA USAGE";
    else if (usage)
        settings.ostr << "SHOW QUOTA USAGE ALL";
    else
        settings.ostr << "SHOW QUOTAS";

    settings.ostr << (settings.hilite ? hilite_none : "");
}
}
