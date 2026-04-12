#pragma once

#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserSetQuery.h>
#include <Common/SettingsChanges.h>
#include <Core/Field.h>


namespace DB
{

/// Helpers for optional `PROPERTIES ...` in SQL **cluster catalog** DDL.
///
/// **Responsibility (this header only)**  
/// - Recognize the `PROPERTIES` keyword and parse a list of `name = value` pairs (optional surrounding `(...)`, commas between pairs).  
/// - That is **syntax only**: valid assignments as for `ParserSetQuery::parseNameValuePair`, no notion of “allowed keys” per replica/shard/cluster.
///
/// **Not done here** (see `Common/Clusters/SQLClusterCatalogPropertyValidation.h` + interpreters)  
/// - Which names are legal for **replica** vs **shard** vs **cluster** context.  
/// - Defaults, ranges, required fields (e.g. replica `host` / `port`), duplicates.
///
/// **Conceptual map** (aligned with `remote_servers` / `<replica>` / `<shard>` / cluster block ideas; **enforced only in validators + interpreters**):
///
/// *Replica* — endpoint / named-collection body (`CREATE REPLICA`, `ALTER SHARD ...` replica `PROPERTIES`):  
/// `host` (required for a usable endpoint), `port` (TCP, required), `user` (default `default`), `password` (default empty),  
/// `secure` (default false), `compression` (default true), `priority` (default 1), `bind_host` (optional), `default_database` (optional).
///
/// *Shard* — replica lists use `REPLICA id1, id2` / `REPLICA (id1, id2)` / `shard_name(id1, id2)` after `CREATE SHARD`; per-shard options in `PROPERTIES`:  
/// `weight` (default 1), `internal_replication` (default false).
///
/// *Cluster* — `CREATE CLUSTER ... PROPERTIES` (see `SQLClusterCatalogPropertyValidation.h`):  
/// `secret` (optional string), `allow_distributed_ddl_queries` (default true). Membership is the `(shard, ...)` list, not replica keys.

/// Parse comma-separated `name = value` after `PROPERTIES`, with optional wrapping `(...)`. Syntax only.
inline bool parseSQLClusterCatalogPropertiesAssignments(SettingsChanges & changes, IParser::Pos & pos, Expected & expected)
{
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);
    ParserToken s_comma(TokenType::Comma);

    if (s_lparen.ignore(pos, expected))
    {
        if (s_rparen.ignore(pos, expected))
            return true;

        while (true)
        {
            changes.push_back(SettingChange{});
            if (!ParserSetQuery::parseNameValuePair(changes.back(), pos, expected))
                return false;
            if (s_rparen.ignore(pos, expected))
                return true;
            if (!s_comma.ignore(pos, expected))
                return false;
        }
    }

    changes.push_back(SettingChange{});
    if (!ParserSetQuery::parseNameValuePair(changes.back(), pos, expected))
        return false;

    while (s_comma.ignore(pos, expected))
    {
        changes.push_back(SettingChange{});
        if (!ParserSetQuery::parseNameValuePair(changes.back(), pos, expected))
            return false;
    }

    return true;
}

/// If the next token is `PROPERTIES`, consume it and parse assignments into `changes`. Otherwise `parsed_options` is false and `changes` is left unchanged.
inline bool parseSQLClusterCatalogOptionalProperties(SettingsChanges & changes, bool & parsed_options, IParser::Pos & pos, Expected & expected)
{
    ParserKeyword s_properties(Keyword::PROPERTIES);

    if (!s_properties.ignore(pos, expected))
    {
        parsed_options = false;
        return true;
    }

    if (!parseSQLClusterCatalogPropertiesAssignments(changes, pos, expected))
        return false;

    parsed_options = true;
    return true;
}

}
