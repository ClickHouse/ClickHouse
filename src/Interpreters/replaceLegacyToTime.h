#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

/// Replaces the setting-dependent `toTime` spelling with the explicit legacy `toTimeWithFixedDate`
/// in a definition that is about to be persisted, so that later sessions, metadata reloads and
/// replicas resolve it independently of `use_legacy_to_time`.
/// A changed top-level SELECT expression without an alias keeps its automatic column name through
/// an alias: materialized views and views match columns by name against the stored column list,
/// and an outer query may reference the automatic name as an identifier.
/// The `FUNCTION` grant for the written `toTime` is checked in `context` before the name is replaced.
/// Returns whether anything was changed.
bool replaceLegacyToTime(IAST & ast, const ContextPtr & context);

}
