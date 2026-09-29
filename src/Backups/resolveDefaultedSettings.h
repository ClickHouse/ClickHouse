#pragma once

#include <Common/SettingsChanges.h>

#include <span>
#include <string_view>
#include <vector>


namespace DB
{
class ASTBackupQuery;

/// A BACKUP/RESTORE `SETTINGS` clause with its `name = DEFAULT` items resolved.
struct SettingsWithDefaultsResolved
{
    /// `changes` with every entry naming a defaulted BACKUP/RESTORE-specific setting removed, so that
    /// setting keeps its default value.
    SettingsChanges changes;

    /// The defaulted core (or unknown) names, to be reset on the query context.
    std::vector<String> core_default_names;
};

/// The core (non-BACKUP/RESTORE-specific) part of a SETTINGS clause, as it applies to the query context.
struct CoreSettingsFromQuery
{
    /// The core overrides to apply.
    SettingsChanges changes;

    /// The core settings to reset. A name may also be in `changes`, and then the reset wins, as in
    /// `SET X = 1, X = DEFAULT`.
    std::vector<String> default_names;
};

/// Maps an alias to the canonical name of the setting it addresses; returns a canonical name unchanged.
using CanonicalSettingNameFn = std::string_view (*)(std::string_view);

/// `specific_names` are the canonical BACKUP/RESTORE-specific names; any other name, unknown ones included,
/// is a core setting.
///
/// The two carriers are separate vectors, so a reset matches a change in either textual order, and both
/// orders end at the default.
SettingsWithDefaultsResolved resolveDefaultedSettings(
    const ASTBackupQuery & query, std::span<const std::string_view> specific_names, CanonicalSettingNameFn canonical_name);

/// The core part of the clause: the core changes `resolveDefaultedSettings` left, and the defaulted core
/// names.
CoreSettingsFromQuery extractCoreSettings(
    const ASTBackupQuery & query, std::span<const std::string_view> specific_names, CanonicalSettingNameFn canonical_name);

/// Removes from `changes` every override of a setting in `default_names`, under any of its names, for a clause sent to other hosts.
///
/// The reset itself is not sent: it cleared the `changed` bit on the initiator, so the DDL settings packet already omits the setting.
/// A receiver applies the text over that packet, so `max_threads = 4, max_threads = DEFAULT` must not arrive as `max_threads = 4`.
void eraseOverridesOfResetSettings(SettingsChanges & changes, const std::vector<String> & default_names);

}
