#include <iomanip>
#include <Parsers/ASTShowTablesQuery.h>
#include <Common/quoteString.h>

namespace DB
{

ASTPtr ASTShowTablesQuery::clone() const
{
    auto res = std::make_shared<ASTShowTablesQuery>(*this);
    res->children.clear();
    cloneOutputOptions(*res);
    return res;
}

void ASTShowTablesQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (databases)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "SHOW DATABASES" << (settings.hilite ? hilite_none : "");
    }
    else if (clusters)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "SHOW CLUSTERS" << (settings.hilite ? hilite_none : "");

        if (!like.empty())
            settings.ostr
                << (settings.hilite ? hilite_keyword : "")
                << (not_like ? " NOT" : "")
                << (case_insensitive_like ? " ILIKE " : " LIKE ")
                << (settings.hilite ? hilite_none : "")
                << std::quoted(like, '\'');

        if (limit_length)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " LIMIT " << (settings.hilite ? hilite_none : "");
            limit_length->formatImpl(settings, state, frame);
        }
    }
    else if (cluster)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "SHOW CLUSTER" << (settings.hilite ? hilite_none : "");
        settings.ostr << " " << backQuoteIfNeed(cluster_str);
    }
    else
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "SHOW " << (temporary ? "TEMPORARY " : "") <<
             (dictionaries ? "DICTIONARIES" : "TABLES") << (settings.hilite ? hilite_none : "");

        if (!from.empty())
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " FROM " << (settings.hilite ? hilite_none : "")
                << backQuoteIfNeed(from);

        if (!like.empty())
            settings.ostr
                << (settings.hilite ? hilite_keyword : "")
                << (not_like ? " NOT" : "")
                << (case_insensitive_like ? " ILIKE " : " LIKE ")
                << (settings.hilite ? hilite_none : "")
                << std::quoted(like, '\'');

        else if (where_expression)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " WHERE " << (settings.hilite ? hilite_none : "");
            where_expression->formatImpl(settings, state, frame);
        }

        if (limit_length)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " LIMIT " << (settings.hilite ? hilite_none : "");
            limit_length->formatImpl(settings, state, frame);
        }
    }
}

}
