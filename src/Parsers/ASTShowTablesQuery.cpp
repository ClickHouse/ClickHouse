#include <iomanip>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTShowTablesQuery.h>
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

void ASTShowTablesQuery::formatLike(WriteBuffer & ostr, const FormatSettings & settings) const
{
    if (!like.empty())
        ostr
            << (settings.hilite ? hilite_keyword : "")
            << (not_like ? " NOT" : "")
            << (case_insensitive_like ? " ILIKE " : " LIKE ")
            << (settings.hilite ? hilite_none : "")
            << DB::quote << like;
}

void ASTShowTablesQuery::formatLimit(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (limit_length)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << " LIMIT " << (settings.hilite ? hilite_none : "");
        limit_length->formatImpl(ostr, settings, state, frame);
    }
}

void ASTShowTablesQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (databases)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "SHOW DATABASES" << (settings.hilite ? hilite_none : "");
        formatLike(ostr, settings);
        formatLimit(ostr, settings, state, frame);

    }
    else if (clusters)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "SHOW CLUSTERS" << (settings.hilite ? hilite_none : "");
        formatLike(ostr, settings);
        formatLimit(ostr, settings, state, frame);

    }
    else if (cluster)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "SHOW CLUSTER" << (settings.hilite ? hilite_none : "");
        ostr << " " << backQuoteIfNeed(cluster_str);
    }
    else if (caches)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "SHOW FILESYSTEM CACHES" << (settings.hilite ? hilite_none : "");
        formatLike(ostr, settings);
        formatLimit(ostr, settings, state, frame);
    }
    else if (m_settings)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "SHOW " << (changed ? "CHANGED " : "") << "SETTINGS" <<
            (settings.hilite ? hilite_none : "");
        formatLike(ostr, settings);
    }
    else if (merges)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "SHOW MERGES" << (settings.hilite ? hilite_none : "");
        formatLike(ostr, settings);
        formatLimit(ostr, settings, state, frame);
    }
    else
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "SHOW " << (temporary ? "TEMPORARY " : "") <<
             (dictionaries ? "DICTIONARIES" : "TABLES") << (settings.hilite ? hilite_none : "");

        if (from)
        {
            ostr << (settings.hilite ? hilite_keyword : "") << " FROM " << (settings.hilite ? hilite_none : "");
            from->formatImpl(ostr, settings, state, frame);
        }

        formatLike(ostr, settings);

        if (where_expression)
        {
            ostr << (settings.hilite ? hilite_keyword : "") << " WHERE " << (settings.hilite ? hilite_none : "");
            where_expression->formatImpl(ostr, settings, state, frame);
        }

        formatLimit(ostr, settings, state, frame);
    }
}

}
