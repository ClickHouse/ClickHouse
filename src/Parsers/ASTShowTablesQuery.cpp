#include <iomanip>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTShowTablesQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTShowTablesQuery::clone() const
{
    auto res = std::make_shared<ASTShowTablesQuery>(*this);
    res->children.clear();
    if (from)
        res->set(res->from, from->clone());

    cloneOutputOptions(*res);
    return res;
}

String ASTShowTablesQuery::getFrom() const
{
    String name;
    tryGetIdentifierNameInto(from, name);
    return name;
}

void ASTShowTablesQuery::formatLike(WriteBuffer & ostr, const FormatSettings &) const
{
    if (!like.empty())
    {
        ostr << (not_like ? " NOT" : "")
            << (case_insensitive_like ? " ILIKE " : " LIKE ")
            << quoteString(like);
    }
}

void ASTShowTablesQuery::formatLimit(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (limit_length)
    {
        ostr << " LIMIT ";
        limit_length->format(ostr, settings, state, frame);
    }
}

void ASTShowTablesQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (databases)
    {
        ostr << "SHOW DATABASES";
        formatLike(ostr, settings);
        formatLimit(ostr, settings, state, frame);

    }
    else if (clusters)
    {
        ostr << "SHOW CLUSTERS";
        formatLike(ostr, settings);
        formatLimit(ostr, settings, state, frame);

    }
    else if (cluster)
    {
        ostr << "SHOW CLUSTER";
        ostr << " " << backQuoteIfNeed(cluster_str);
    }
    else if (caches)
    {
        ostr << "SHOW FILESYSTEM CACHES";
        formatLike(ostr, settings);
        formatLimit(ostr, settings, state, frame);
    }
    else if (m_settings)
    {
        ostr << "SHOW " << (changed ? "CHANGED " : "") << "SETTINGS";
        formatLike(ostr, settings);
    }
    else if (merges)
    {
        ostr << "SHOW MERGES";
        formatLike(ostr, settings);
        formatLimit(ostr, settings, state, frame);
    }
    else
    {
        ostr << "SHOW " << (temporary ? "TEMPORARY " : "") <<
             (dictionaries ? "DICTIONARIES" : "TABLES");

        if (from)
        {
            ostr << " FROM ";
            from->format(ostr, settings, state, frame);
        }

        formatLike(ostr, settings);

        if (where_expression)
        {
            ostr << " WHERE ";
            where_expression->format(ostr, settings, state, frame);
        }

        formatLimit(ostr, settings, state, frame);
    }
}

}
