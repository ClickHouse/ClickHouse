#include <iomanip>
#include <Parsers/ASTShowTablesQuery.h>
#include <Common/quoteString.h>


namespace DB
{

ASTPtr ASTShowTablesQuery::clone() const
{
    auto res = std::make_shared<ASTShowTablesQuery>(*this);
    res->children.clear();
    res->named.clone(named);
    cloneOutputOptions(*res);
    return res;
}

void ASTShowTablesQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (databases)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "SHOW DATABASES" << (settings.hilite ? hilite_none : "");
    }
    else
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "SHOW " << (temporary ? "TEMPORARY " : "") <<
             (dictionaries ? "DICTIONARIES" : "TABLES") << (settings.hilite ? hilite_none : "");

        if (const auto & from = getChild(ASTShowTablesQueryChildren::FROM))
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " FROM " << (settings.hilite ? hilite_none : "");
            from->formatImpl(settings, state, frame);
        }

        if (const auto & like = getChild(ASTShowTablesQueryChildren::LIKE))
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " LIKE " << (settings.hilite ? hilite_none : "");
            like->formatImpl(settings, state, frame);
        }

        if (const auto & limit_length = getChild(ASTShowTablesQueryChildren::LIMIT_LENGTH))
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " LIMIT " << (settings.hilite ? hilite_none : "");
            limit_length->formatImpl(settings, state, frame);
        }
    }
}

}
