#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTShowTablesQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserShowTablesQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>

#include <Common/typeid_cast.h>


namespace DB
{


bool ParserShowTablesQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_show(Keyword::SHOW);
    ParserKeyword s_full(Keyword::FULL);
    ParserKeyword s_temporary(Keyword::TEMPORARY);
    ParserKeyword s_tables(Keyword::TABLES);
    ParserKeyword s_databases(Keyword::DATABASES);
    ParserKeyword s_clusters(Keyword::CLUSTERS);
    ParserKeyword s_cluster(Keyword::CLUSTER);
    ParserKeyword s_dictionaries(Keyword::DICTIONARIES);
    ParserKeyword s_caches(Keyword::FILESYSTEM_CACHES);
    ParserKeyword s_settings(Keyword::SETTINGS);
    ParserKeyword s_merges(Keyword::MERGES);
    ParserKeyword s_changed(Keyword::CHANGED);
    ParserKeyword s_from(Keyword::FROM);
    ParserKeyword s_in(Keyword::IN);
    ParserKeyword s_not(Keyword::NOT);
    ParserKeyword s_like(Keyword::LIKE);
    ParserKeyword s_ilike(Keyword::ILIKE);
    ParserKeyword s_where(Keyword::WHERE);
    ParserKeyword s_limit(Keyword::LIMIT);
    ParserStringLiteral like_p(Highlight::string_like);
    ParserIdentifier name_p(true);
    ParserExpressionWithOptionalAlias exp_elem(false);

    ASTPtr like;
    ASTPtr database;

    auto query = make_intrusive<ASTShowTablesQuery>();

    if (!s_show.ignore(pos, expected))
        return false;

    if (s_full.ignore(pos, expected))
    {
        query->full = true;
    }

    if (s_databases.ignore(pos, expected))
    {
        query->databases = true;

        if (s_not.ignore(pos, expected))
            query->not_like = true;

        if (bool insensitive = s_ilike.ignore(pos, expected); insensitive || s_like.ignore(pos, expected))
        {
            if (insensitive)
                query->case_insensitive_like = true;

            if (!like_p.parse(pos, like, expected))
                return false;
        }
        else if (query->not_like)
            return false;
        if (s_limit.ignore(pos, expected))
        {
            if (!exp_elem.parse(pos, query->limit_length, expected))
                return false;
        }
    }
    else if (s_clusters.ignore(pos, expected))
    {
        query->clusters = true;

        if (s_not.ignore(pos, expected))
            query->not_like = true;

        if (bool insensitive = s_ilike.ignore(pos, expected); insensitive || s_like.ignore(pos, expected))
        {
            if (insensitive)
                query->case_insensitive_like = true;

            if (!like_p.parse(pos, like, expected))
                return false;
        }
        else if (query->not_like)
            return false;
        if (s_limit.ignore(pos, expected))
        {
            if (!exp_elem.parse(pos, query->limit_length, expected))
                return false;
        }
    }
    else if (s_merges.ignore(pos, expected))
    {
        query->merges = true;

        if (s_not.ignore(pos, expected))
            query->not_like = true;

        if (bool insensitive = s_ilike.ignore(pos, expected); insensitive || s_like.ignore(pos, expected))
        {
            if (insensitive)
                query->case_insensitive_like = true;

            if (!like_p.parse(pos, like, expected))
                return false;
        }
        else if (query->not_like)
            return false;
        if (s_limit.ignore(pos, expected))
        {
            if (!exp_elem.parse(pos, query->limit_length, expected))
                return false;
        }
    }
    else if (s_caches.ignore(pos, expected))
    {
        query->caches = true;
    }
    else if (s_cluster.ignore(pos, expected))
    {
        query->cluster = true;

        String cluster_str;
        if (!parseIdentifierOrStringLiteral(pos, expected, cluster_str))
            return false;

        query->cluster_str = std::move(cluster_str);
    }
    else if (bool changed = s_changed.ignore(pos, expected); changed || s_settings.ignore(pos, expected))
    {
        query->m_settings = true;

        if (changed)
        {
            query->changed = true;
            if (!s_settings.ignore(pos, expected))
                return false;
        }

        /// Not expected due to Keyword::SHOW SETTINGS PROFILES
        if (bool insensitive = s_ilike.ignore(pos, expected); insensitive || s_like.ignore(pos, expected))
        {
            if (insensitive)
                query->case_insensitive_like = true;

            if (!like_p.parse(pos, like, expected))
                return false;
        }
        else
            return false;
    }
    else
    {
        if (s_temporary.ignore(pos, expected))
            query->temporary = true;

        if (!s_tables.ignore(pos, expected))
        {
            if (s_dictionaries.ignore(pos, expected))
                query->dictionaries = true;
            else
                return false;
        }

        if (s_from.ignore(pos, expected) || s_in.ignore(pos, expected))
        {
            if (!name_p.parse(pos, database, expected))
                return false;

            ParserToken dot(TokenType::Dot);
            std::vector<String> parts;
            ASTs params;
            auto append_part = [&](const ASTPtr & part_node)
            {
                parts.push_back(getIdentifierName(part_node));
                if (parts.back().empty())
                    params.push_back(part_node->as<ASTIdentifier>()->getParam());
            };
            bool folded = false;
            while (dot.ignore(pos, expected))
            {
                if (!folded)
                {
                    /// a quoted component with a literal dot would alias another path - reject it
                    if (getIdentifierName(database).find('.') != String::npos)
                        return false;
                    append_part(database);
                }
                ASTPtr part;
                if (!name_p.parse(pos, part, expected))
                    return false;
                /// a quoted component with a literal dot would alias another path - reject it
                if (getIdentifierName(part).find('.') != String::npos)
                    return false;
                append_part(part);
                folded = true;
            }
            /// a compound identifier keeps parameter children; substitution fills them later.
            /// `special` marks it as a database path so substituted parts are validated
            if (folded)
                database = make_intrusive<ASTIdentifier>(std::move(parts), /*special*/ true, std::move(params));
        }

        if (s_not.ignore(pos, expected))
            query->not_like = true;

        if (bool insensitive = s_ilike.ignore(pos, expected); insensitive || s_like.ignore(pos, expected))
        {
            if (insensitive)
                query->case_insensitive_like = true;

            if (!like_p.parse(pos, like, expected))
                return false;
        }
        else if (query->not_like)
            return false;
        else if (s_where.ignore(pos, expected))
            if (!exp_elem.parse(pos, query->where_expression, expected))
                return false;

        if (s_limit.ignore(pos, expected))
            if (!exp_elem.parse(pos, query->limit_length, expected))
                return false;
    }

    query->set(query->from, database);

    if (like)
        query->like = like->as<ASTLiteral &>().value.safeGet<String>();

    node = query;

    return true;
}

}
