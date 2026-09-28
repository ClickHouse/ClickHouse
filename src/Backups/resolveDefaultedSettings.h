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

    /// The defaulted names that are not BACKUP/RESTORE-specific. They are core (or unknown) settings, so
    /// resolving them means resetting them on the query context rather than dropping a `changes` entry.
    std::vector<String> core_default_names;
};

/// The core (non-BACKUP/RESTORE-specific) part of a SETTINGS clause, as it applies to the query context.
struct CoreSettingsFromQuery
{
    /// The core overrides to apply.
    SettingsChanges changes;

    /// The core settings to reset to their default value. A name may appear here and in `changes` at the
    /// same time, and then the reset wins, which is what a `SET X = 1, X = DEFAULT` does.
    std::vector<String> default_names;
};

/// Maps a setting name to the name of the field it addresses, so that an alias and its canonical
/// spelling resolve as one setting. Returns the name unchanged if it is already canonical.
using CanonicalSettingNameFn = std::string_view (*)(std::string_view);

/// `specific_names` lists the canonical names of the BACKUP/RESTORE-specific settings; everything else is
/// treated as a core setting, which is also how an unknown name is treated today.
///
/// A defaulted name is matched against `changes` irrespective of textual order, because the two carriers
/// are separate vectors: `ParserSetQuery` appends to `changes` and to `default_settings` independently, so
/// `X = 1, X = DEFAULT` and `X = DEFAULT, X = 1` are the same AST modulo vector order. Both must end at
/// the default, which is what `SET` does.
SettingsWithDefaultsResolved resolveDefaultedSettings(
    const ASTBackupQuery & query, std::span<const std::string_view> specific_names, CanonicalSettingNameFn canonical_name);

/// The core part of the clause: the changes `resolveDefaultedSettings` left that are not
/// BACKUP/RESTORE-specific, plus the defaulted names it classified as core.
CoreSettingsFromQuery extractCoreSettings(
    const ASTBackupQuery & query, std::span<const std::string_view> specific_names, CanonicalSettingNameFn canonical_name);

/// Remove from `changes` every override of a setting named in `default_names`, under any name of that
/// setting. For rebuilding a SETTINGS clause that is sent to other hosts: see
/// `BackupSettings::copySettingsToQuery`.
///
/// A reset is not forwarded in any form. The host that parsed the clause resets the setting on its query
/// context, which clears the setting's `changed` bit, so the DDL settings packet built from that context
/// (`DDLLogEntry::setSettingsIfRequired`) leaves the setting out, and each receiving host ends with the
/// setting unset too. What must not survive is an override of it in the rebuilt text, because a receiving
/// host applies that text on top of the packet: `SETTINGS max_threads = 4, max_threads = DEFAULT` must not
/// arrive as `max_threads = 4`.
void eraseOverridesOfResetSettings(SettingsChanges & changes, const std::vector<String> & default_names);

}
