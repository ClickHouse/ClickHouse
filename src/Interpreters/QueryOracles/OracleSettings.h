#pragma once

#include <Core/Field.h>

#include <span>
#include <string_view>

namespace DB
{

/// Output-size guards for oracle sub-queries. Shared by `makeOracleContext` (which pins
/// `max_result_rows`/`max_result_bytes` to these) and by the result collectors, which skip
/// any query whose serialized output exceeds `MAX_ORACLE_OUTPUT_SIZE` rather than risk a
/// false pass on a silently truncated result.
constexpr size_t MAX_ORACLE_OUTPUT_SIZE = 10 * 1024 * 1024;
constexpr size_t MAX_ORACLE_RESULT_ROWS = 10'000'000;

/// One pinned setting neutralized on every oracle sub-query / fixture statement.
struct PinnedSetting
{
    std::string_view name;
    Field value;
    std::string_view why;
};

/// The single source of truth for the settings `makeOracleContext` neutralizes. It is the
/// only place these pins are defined: `makeOracleContext` applies the whole list, and the
/// setting-flip sweep consults `isPinnedByOracleContext` so it never flips a setting that is
/// pinned (which would be either a vacuous pass or an unsound re-enable).
///
/// Growing this list is expected maintenance: any future "a leaked `SET x` breaks oracle
/// sub-queries or fixture DDL" bug adds a row here. These are internal neutralizations, not
/// user-facing flags, and are exempt from the no-new-oracle-settings rule.
std::span<const PinnedSetting> oraclePinnedSettings();

/// True iff `name` is neutralized by `makeOracleContext`. The sweep (and reviews) consult this
/// to keep "the sweep flips a pinned setting" unrepresentable.
bool isPinnedByOracleContext(std::string_view name);

}
