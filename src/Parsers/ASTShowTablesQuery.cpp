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

void ASTShowTablesQuery::formatLike(const FormattingBuffer & out) const
{
    if (!like.empty())
    {
        out.writeKeyword(not_like ? " NOT" : "");
        out.writeKeyword(case_insensitive_like ? " ILIKE " : " LIKE ");
        out.ostr << DB::quote << like;
    }
}

void ASTShowTablesQuery::formatLimit(const FormattingBuffer & out) const
{
    if (limit_length)
    {
        out.writeKeyword(" LIMIT ");
        limit_length->formatImpl(out);
    }
}

void ASTShowTablesQuery::formatQueryImpl(const FormattingBuffer & out) const
{
    if (databases)
    {
        out.writeKeyword("SHOW DATABASES");
        formatLike(out.copy());
        formatLimit(out);

    }
    else if (clusters)
    {
        out.writeKeyword("SHOW CLUSTERS");
        formatLike(out.copy());
        formatLimit(out);

    }
    else if (cluster)
    {
        out.writeKeyword("SHOW CLUSTER");
        out.ostr << " " << backQuoteIfNeed(cluster_str);
    }
    else if (caches)
    {
        out.writeKeyword("SHOW FILESYSTEM CACHES");
        formatLike(out.copy());
        formatLimit(out);
    }
    else if (m_settings)
    {
        out.writeKeyword("SHOW ");
        out.writeKeyword(changed ? "CHANGED " : "");
        out.writeKeyword("SETTINGS");
        formatLike(out.copy());
    }
    else
    {
        out.writeKeyword("SHOW ");
        out.writeKeyword(temporary ? "TEMPORARY " : "");
        out.writeKeyword(dictionaries ? "DICTIONARIES" : "TABLES");

        if (!from.empty())
        {
            out.writeKeyword(" FROM ");
            out.ostr << backQuoteIfNeed(from);
        }

        formatLike(out.copy());

        if (where_expression)
        {
            out.writeKeyword(" WHERE ");
            where_expression->formatImpl(out);
        }

        formatLimit(out);
    }
}

}
