#include <Parsers/ParserNamedScalarDDLQuery.h>

#include <Common/Exception.h>
#include <limits>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTNamedScalarDDLQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

namespace
{
/// Parse the body of `REFRESH EVERY <N> <unit>` after the `REFRESH` keyword
/// has already been consumed. Output is the period normalised to seconds.
/// Units: SECOND, MINUTE, HOUR, DAY (with optional plural).
bool parseRefreshEvery(IParser::Pos & pos, Expected & expected, UInt64 & out_seconds)
{
    ParserKeyword s_every(Keyword::EVERY);
    if (!s_every.ignore(pos, expected))
        return false;

    ParserNumber number_p;
    ASTPtr number_ast;
    if (!number_p.parse(pos, number_ast, expected))
        return false;
    const auto & number_field = number_ast->as<ASTLiteral &>().value;
    UInt64 amount = 0;
    if (number_field.getType() == Field::Types::UInt64)
        amount = number_field.safeGet<UInt64>();
    else if (number_field.getType() == Field::Types::Int64)
    {
        Int64 v = number_field.safeGet<Int64>();
        if (v <= 0)
            return false;
        amount = static_cast<UInt64>(v);
    }
    else
        return false;
    if (amount == 0)
        return false;

    UInt64 unit_seconds = 0;
    if (ParserKeyword(Keyword::SECOND).ignore(pos, expected) || ParserKeyword(Keyword::SECONDS).ignore(pos, expected))
        unit_seconds = 1;
    else if (ParserKeyword(Keyword::MINUTE).ignore(pos, expected) || ParserKeyword(Keyword::MINUTES).ignore(pos, expected))
        unit_seconds = 60;
    else if (ParserKeyword(Keyword::HOUR).ignore(pos, expected) || ParserKeyword(Keyword::HOURS).ignore(pos, expected))
        unit_seconds = 3600;
    else if (ParserKeyword(Keyword::DAY).ignore(pos, expected) || ParserKeyword(Keyword::DAYS).ignore(pos, expected))
        unit_seconds = 86400;
    else
        return false;

    /// Reject overflow + cap at UInt32::max seconds (~136 years). Without
    /// this, `amount * unit_seconds` wraps to 0 and the refresh task
    /// fires every tick, hammering source/Keeper.
    if (amount > std::numeric_limits<UInt64>::max() / unit_seconds)
        throw Exception(ErrorCodes::SYNTAX_ERROR,
            "REFRESH EVERY {} {}: refresh interval overflows after unit conversion",
            amount, unit_seconds == 1 ? "SECOND"
                : unit_seconds == 60 ? "MINUTE"
                : unit_seconds == 3600 ? "HOUR" : "DAY");
    out_seconds = amount * unit_seconds;
    if (out_seconds == 0)
        return false;
    constexpr UInt64 max_period_seconds = std::numeric_limits<UInt32>::max();  // ~136 years
    if (out_seconds > max_period_seconds)
        throw Exception(ErrorCodes::SYNTAX_ERROR,
            "REFRESH EVERY interval exceeds the supported maximum of {} seconds (~136 years)",
            max_period_seconds);
    return true;
}
}

bool ParserNamedScalarDDLQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_drop(Keyword::DROP);
    ParserKeyword s_named_scalar(Keyword::NAMED_SCALAR);
    ParserKeyword s_or_replace(Keyword::OR_REPLACE);
    ParserKeyword s_if_not_exists(Keyword::IF_NOT_EXISTS);
    ParserKeyword s_if_exists(Keyword::IF_EXISTS);
    ParserKeyword s_on(Keyword::ON);
    ParserKeyword s_refresh(Keyword::REFRESH);
    ParserKeyword s_as(Keyword::AS);
    ParserKeyword s_local(Keyword::LOCAL);
    ParserKeyword s_shared(Keyword::SHARED);
    ParserKeyword s_uuid(Keyword::UUID);
    ParserIdentifier name_p;
    ParserSelectWithUnionQuery select_p;

    using Action = ASTNamedScalarDDLQuery::Action;
    Action action;
    if (s_create.ignore(pos, expected))
        action = Action::Create;
    else if (s_drop.ignore(pos, expected))
        action = Action::Drop;
    else
        return false;

    bool if_not_exists = false;
    bool or_replace = false;
    auto cache_kind = ASTNamedScalarDDLQuery::CacheKind::Default;
    bool if_exists = false;
    String cluster_str;

    ASTPtr named_scalar_name;
    ASTPtr expression;
    ASTPtr sql_security;
    std::optional<UInt64> refresh_period_seconds;
    UUID uuid = UUIDHelpers::Nil;

    if (action == Action::Create && s_or_replace.ignore(pos, expected))
        or_replace = true;

    if (action == Action::Create)
    {
        if (s_local.ignore(pos, expected))
            cache_kind = ASTNamedScalarDDLQuery::CacheKind::Local;
        else if (s_shared.ignore(pos, expected))
            cache_kind = ASTNamedScalarDDLQuery::CacheKind::Shared;
    }

    if (!s_named_scalar.ignore(pos, expected))
        return false;

    if (action == Action::Create)
    {
        if (s_if_not_exists.ignore(pos, expected))
        {
            /// OR REPLACE and IF NOT EXISTS are mutually exclusive.
            if (or_replace)
                return false;
            if_not_exists = true;
        }
    }
    else
    {
        if (s_if_exists.ignore(pos, expected))
            if_exists = true;
    }

    if (!name_p.parse(pos, named_scalar_name, expected))
        return false;

    if (action == Action::Create && s_uuid.ignore(pos, expected))
    {
        ParserStringLiteral uuid_p;
        ASTPtr ast_uuid;
        if (!uuid_p.parse(pos, ast_uuid, expected))
            return false;
        uuid = parseFromString<UUID>(ast_uuid->as<ASTLiteral>()->value.safeGet<String>());
    }

    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    if (action == Action::Create)
    {
        ParserSQLSecurity sql_security_p;
        sql_security_p.parse(pos, sql_security, expected);

        if (s_refresh.ignore(pos, expected))
        {
            UInt64 seconds = 0;
            if (!parseRefreshEvery(pos, expected, seconds))
                return false;
            refresh_period_seconds = seconds;
        }

        if (!s_as.ignore(pos, expected))
            return false;

        /// AS SELECT only.
        if (!select_p.parse(pos, expression, expected))
            return false;
    }

    auto query = make_intrusive<ASTNamedScalarDDLQuery>();
    query->action = action;
    query->cache_kind = cache_kind;
    query->named_scalar_name = named_scalar_name;
    query->children.push_back(named_scalar_name);

    if (action == Action::Create)
    {
        query->sql_security = sql_security;
        if (query->sql_security)
            query->children.push_back(query->sql_security);
        query->expression = expression;
        query->children.push_back(expression);
        query->refresh_period_seconds = refresh_period_seconds;
        query->if_not_exists = if_not_exists;
        query->or_replace = or_replace;
        query->uuid = uuid;
    }
    else
    {
        query->if_exists = if_exists;
    }

    query->cluster = std::move(cluster_str);
    node = query;
    return true;
}

}
