#include <iomanip>
#include <Parsers/ASTShowTablesQuery.h>


namespace DB
{

ASTPtr ASTShowTablesQuery::clone() const
{
    auto res = std::make_shared<ASTShowTablesQuery>(*this);
    res->children.clear();
    cloneOutputOptions(*res);
    return res;
}

void ASTShowTablesQuery::formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    if (databases)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "SHOW DATABASES" << (settings.hilite ? hilite_none : "");
    }
    else
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "SHOW " << (temporary ? "TEMPORARY " : "") << "TABLES" << (settings.hilite ? hilite_none : "");

        if (!from.empty())
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " FROM " << (settings.hilite ? hilite_none : "")
                << backQuoteIfNeed(from);

        if (!like.empty())
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " LIKE " << (settings.hilite ? hilite_none : "")
                << std::quoted(like, '\'');
    }
}

}

