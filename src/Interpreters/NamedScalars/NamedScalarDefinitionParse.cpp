#include <Interpreters/NamedScalars/NamedScalarDefinitionParse.h>

#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <Core/Settings.h>
#include <Core/UUID.h>

#include <IO/WriteHelpers.h>

#include <Interpreters/Context.h>
#include <Interpreters/NamedScalars/INamedScalarValueBackend.h>

#include <Parsers/ASTNamedScalarDDLQuery.h>
#include <Parsers/ASTSQLSecurity.h>
#include <Parsers/ParserNamedScalarDDLQuery.h>
#include <Parsers/parseQuery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace Setting
{
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_parser_backtracks;
}

namespace
{

ASTPtr parseDDLBlob(const String & blob, const ContextPtr & context)
{
    ParserNamedScalarDDLQuery parser;
    auto ast = parseQuery(
        parser,
        blob.data(),
        blob.data() + blob.size(),
        "",
        0,
        context->getSettingsRef()[Setting::max_parser_depth],
        context->getSettingsRef()[Setting::max_parser_backtracks]);
    if (ast && !ast->as<ASTNamedScalarDDLQuery>())
        return nullptr;
    return ast;
}

}

ParsedDefinitionBlob parseDefinitionBlob(const String & blob, const ContextPtr & context)
{
    ParsedDefinitionBlob result;

    result.ast = parseDDLBlob(blob, context);
    const auto * create_query = result.ast ? result.ast->as<ASTNamedScalarDDLQuery>() : nullptr;
    if (!create_query)
        return result;

    if (create_query->uuid == UUIDHelpers::Nil)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Named scalar definition SQL must contain an explicit UUID");
    result.uuid = toString(create_query->uuid);

    const auto * sql_security = create_query->sql_security ? create_query->sql_security->as<ASTSQLSecurity>() : nullptr;
    if (!sql_security || sql_security->type != SQLSecurityType::DEFINER || !sql_security->definer)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Named scalar definition SQL must contain an explicit SQL SECURITY DEFINER identity");
    result.definer = sql_security->definer->toString();

    return result;
}

std::optional<ParsedDefinition> parseAndValidateDefinition(
    const String & name,
    const String & definition_blob,
    std::chrono::system_clock::time_point load_time,
    const ContextPtr & context,
    LoggerPtr log)
{
    ParsedDefinitionBlob blob;
    try
    {
        blob = parseDefinitionBlob(definition_blob, context);
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("parsing named scalar definition '{}'", name));
        return std::nullopt;
    }

    const auto * create_query = blob.ast ? blob.ast->as<ASTNamedScalarDDLQuery>() : nullptr;
    if (!create_query)
    {
        LOG_WARNING(log,
            "Persisted definition for named scalar '{}' is not a CREATE NAMED SCALAR query",
            name);
        return std::nullopt;
    }

    return ParsedDefinition{
        .name = name,
        .uuid = blob.uuid,
        .expression = create_query->expression,
        .definer = blob.definer,
        .refresh_period_seconds = create_query->refresh_period_seconds,
        .load_time = load_time,
    };
}

std::optional<String> getNamedScalarUUIDFromSerializedDefinition(
    const String & definition_blob,
    const ContextPtr & context,
    LoggerPtr log)
{
    try
    {
        return parseDefinitionBlob(definition_blob, context).uuid;
    }
    catch (...)
    {
        tryLogCurrentException(log, "parsing named scalar UUID");
        return std::nullopt;
    }
}

std::optional<NamedScalarCacheKind> getNamedScalarCacheKindFromSerializedDefinition(
    const String & definition_blob,
    const ContextPtr & context,
    LoggerPtr log)
{
    try
    {
        auto parsed = parseDefinitionBlob(definition_blob, context);
        const auto * create_query = parsed.ast ? parsed.ast->as<ASTNamedScalarDDLQuery>() : nullptr;
        if (!create_query)
            return std::nullopt;
        switch (create_query->cache_kind)
        {
            case ASTNamedScalarDDLQuery::CacheKind::Shared:
                return NamedScalarCacheKind::Shared;
            case ASTNamedScalarDDLQuery::CacheKind::Default:
            case ASTNamedScalarDDLQuery::CacheKind::Local:
                return NamedScalarCacheKind::Local;
        }
        UNREACHABLE();
    }
    catch (...)
    {
        tryLogCurrentException(log, "parsing named scalar cache kind");
        return std::nullopt;
    }
}

}
