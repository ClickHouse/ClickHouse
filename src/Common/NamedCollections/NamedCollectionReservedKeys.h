#pragma once

#include <base/types.h>
#include <Common/SettingsChanges.h>

#include <string_view>
#include <vector>


namespace DB
{

/// Named collections are an untyped key/value store: anything can be put in, and nothing but a naming convention
/// tells two feature-specific collections apart (e.g. "is this a SQL replica endpoint?" vs "is this a Kafka
/// bootstrap config?"). To support feature-specific system views (`system.replicas_collection`, ...), we reserve
/// a small internal key namespace that only ClickHouse itself — not user DDL — may populate.
///
/// Invariants established here:
///
/// - `NAMED_COLLECTION_RESERVED_KEY_PREFIX` ("__") marks the entire reserved namespace. Any key whose name starts
///   with this prefix is off-limits for `CREATE/ALTER NAMED COLLECTION` coming from user SQL; they are rejected
///   at the interpreter layer via `assertNoReservedKeys`.
/// - `NAMED_COLLECTION_KIND_KEY` ("__type__") holds the feature tag for collections that are used as a backing
///   store by a higher-level DDL (currently `CREATE REPLICA`, which writes `NAMED_COLLECTION_KIND_REPLICA`).
///   Internal interpreters inject this key by constructing the NC AST directly — they don't go through user DDL
///   parsing/validation, so the assert does not fire for them.
/// - Reserved keys are filtered out of user-facing views (`system.named_collections.collection`, `create_query`,
///   `SHOW CREATE NAMED COLLECTION`) to preserve the illusion of a clean user schema, but are kept verbatim in
///   the persistence path so that a restart round-trips the feature tag.
///
/// Adding a second "kind" (say `__type__ = 'source'`) does not require a new reserved prefix — define another
/// well-known value for `NAMED_COLLECTION_KIND_KEY` and wire the corresponding interpreter to inject it.
constexpr std::string_view NAMED_COLLECTION_RESERVED_KEY_PREFIX = "__";
constexpr std::string_view NAMED_COLLECTION_KIND_KEY = "__type__";
constexpr std::string_view NAMED_COLLECTION_KIND_REPLICA = "replica";


inline bool isReservedNamedCollectionKey(std::string_view key)
{
    return key.starts_with(NAMED_COLLECTION_RESERVED_KEY_PREFIX);
}


/// Reject any user-supplied key that falls in the reserved namespace. Used by the two user-entry interpreters
/// (`CREATE NAMED COLLECTION`, `ALTER NAMED COLLECTION`). Throws `BAD_ARGUMENTS`; never silently ignores.
void assertNoReservedKeys(const SettingsChanges & changes);


/// Same contract for the `ALTER NAMED COLLECTION ... DELETE k1, k2` path — user shouldn't be able to
/// delete internal keys either, or they could forge a replica back into a plain NC.
void assertNoReservedKeys(const std::vector<String> & delete_keys);

}
