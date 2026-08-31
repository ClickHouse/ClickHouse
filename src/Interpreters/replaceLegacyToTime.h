#pragma once

#include <string_view>

#include <base/types.h>

#include <Parsers/IAST_fwd.h>

namespace DB
{

struct Settings;

/// The canonical spelling `toTime` must be replaced with in a definition that is about to be
/// persisted, so that later sessions, metadata reloads and replicas resolve it independently of
/// `use_legacy_to_time`: `toTimeWithFixedDate` under the legacy setting, `toTimeWithoutDate` when
/// the session set the new meaning explicitly. An empty result means the session did not express
/// an intention and the ambiguous spelling is kept as written.
std::string_view legacyToTimeReplacement(const Settings & settings);

/// Replaces the setting-dependent `toTime` spelling with `replacement` in a definition that is
/// about to be persisted.
/// A changed top-level SELECT expression without an alias keeps its automatic column name through
/// an alias: materialized views and views match columns by name against the stored column list,
/// and an outer query may reference the automatic name as an identifier.
/// Returns whether anything was changed.
bool replaceLegacyToTime(IAST & ast, std::string_view replacement);

}
