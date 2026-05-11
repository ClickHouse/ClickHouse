#pragma once

#include <Common/Logger.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <base/types.h>

#include <chrono>
#include <optional>

namespace DB
{

enum class NamedScalarCacheKind : UInt8;

/// Fully resolved named-scalar definition. Manager builds one from a
/// definition blob (CREATE-NAMED-SCALAR text envelope) and hands it to
/// NamedScalar's constructor. Once built, the definition is immutable -
/// any subsequent CREATE OR REPLACE produces a brand-new ParsedDefinition
/// (and a brand-new NamedScalar) instead of mutating an existing one.
struct ParsedDefinition
{
    String name;

    /// UUID materialised at CREATE time. Used as the value-backend key,
    /// so OR REPLACE picks up a fresh value slot.
    String uuid;

    /// SELECT expression body. Parsed once; refresh task evaluates it
    /// under DEFINER context on every tick.
    ASTPtr expression;

    /// SQL SECURITY DEFINER user (mandatory).
    String definer;

    /// Optional REFRESH EVERY N period. Absent ⇒ scalar is one-shot.
    std::optional<UInt64> refresh_period_seconds;

    /// When the scalar was first parsed by this server. Stable across
    /// restarts only insofar as the CREATE query is unchanged.
    std::chrono::system_clock::time_point load_time;
};

/// Result of parsing a named-scalar definition blob (the textual envelope
/// stored in the definition store: full CREATE query + UUID + DEFINER).
struct ParsedDefinitionBlob
{
    /// Parsed CREATE query AST. Null if the blob did not parse cleanly
    /// (logged at the call site that uses the result).
    ASTPtr ast;

    /// SQL SECURITY DEFINER identity (mandatory).
    String definer;

    /// Materialized UUID from the CREATE query (mandatory). Used by value
    /// backends as the value-key, so OR REPLACE picks up a fresh slot.
    String uuid;
};

/// Parse a definition blob. Throws BAD_ARGUMENTS if the blob parses but
/// is missing the mandatory UUID or DEFINER. Returns a result with null
/// `ast` if the blob does not parse as a CREATE NAMED SCALAR query.
ParsedDefinitionBlob parseDefinitionBlob(const String & blob, const ContextPtr & context);

/// Build a fully-validated ParsedDefinition from a definition blob.
/// Returns nullopt and logs if the blob does not parse cleanly; throws
/// BAD_ARGUMENTS if the blob parses but is missing required fields.
std::optional<ParsedDefinition> parseAndValidateDefinition(
    const String & name,
    const String & definition_blob,
    std::chrono::system_clock::time_point load_time,
    const ContextPtr & context,
    LoggerPtr log);

/// Extract just the UUID. Returns nullopt and logs if parsing fails.
std::optional<String> getNamedScalarUUIDFromSerializedDefinition(
    const String & definition_blob,
    const ContextPtr & context,
    LoggerPtr log);

/// Extract just the cache kind. Returns nullopt and logs if parsing fails.
std::optional<NamedScalarCacheKind> getNamedScalarCacheKindFromSerializedDefinition(
    const String & definition_blob,
    const ContextPtr & context,
    LoggerPtr log);

}
