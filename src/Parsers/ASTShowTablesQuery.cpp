#include <iomanip>
#include <Parsers/ASTShowTablesQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTShowTablesQuery::clone() const
{
    auto res = std::make_shared<ASTShowTablesQuery>(*this);
    res->children.clear();
    cloneOutputOptions(*res);
    return res;
}

void ASTShowTablesQuery::formatLike(const FormatSettings & settings) const
{
    if (!like.empty())
    {
        settings.writeKeyword(not_like ? " NOT" : "");
        settings.writeKeyword(case_insensitive_like ? " ILIKE " : " LIKE ");
        settings.ostr << DB::quote << like;
    }
}

void ASTShowTablesQuery::formatLimit(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (limit_length)
    {
        settings.writeKeyword(" LIMIT ");
        limit_length->formatImpl(settings, state, frame);
    }
}

void ASTShowTablesQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (databases)
    {
        settings.writeKeyword("SHOW DATABASES");
        formatLike(settings);
        formatLimit(settings, state, frame);

    }
    else if (clusters)
    {
        settings.writeKeyword("SHOW CLUSTERS");
        formatLike(settings);
        formatLimit(settings, state, frame);

    }
    else if (cluster)
    {
        settings.writeKeyword("SHOW CLUSTER");
        settings.ostr << " " << backQuoteIfNeed(cluster_str);
    }
    else if (caches)
    {
        settings.writeKeyword("SHOW FILESYSTEM CACHES");
        formatLike(settings);
        formatLimit(settings, state, frame);
    }
    else if (m_settings)
    {
        settings.writeKeyword("SHOW ");
        settings.writeKeyword(changed ? "CHANGED " : "");
        settings.writeKeyword("SETTINGS");
        formatLike(settings);
    }
    else
    {
        settings.writeKeyword("SHOW ");
        settings.writeKeyword(temporary ? "TEMPORARY " : "");
        settings.writeKeyword(dictionaries ? "DICTIONARIES" : "TABLES");

        if (!from.empty())
        {
            settings.writeKeyword(" FROM ");
            settings.ostr << backQuoteIfNeed(from);
        }

        formatLike(settings);

        if (where_expression)
        {
            settings.writeKeyword(" WHERE ");
            where_expression->formatImpl(settings, state, frame);
        }

        formatLimit(settings, state, frame);
    }
}

}
