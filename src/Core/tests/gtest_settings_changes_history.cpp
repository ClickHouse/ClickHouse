#include <gtest/gtest.h>

#include <Core/Settings.h>
#include <Core/SettingsChangesHistory.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Common/Exception.h>
#include <Common/FieldVisitorToString.h>

#include <map>

/// `SettingsChangesHistory.cpp` is the source of truth for the `compatibility` setting: for an old
/// version it walks the history and restores the `previous_value` of every change made after that
/// version. This only works while the history agrees with the code: the newest recorded `new_value`
/// of a setting must be exactly its current default. When a default is changed in `Settings.cpp`
/// (or `MergeTreeSettings.cpp`) without recording the change, or a wrong value is recorded,
/// `compatibility` silently restores a value that was never a default of any release.
///
/// These tests catch such drift at build time. See https://github.com/ClickHouse/ClickHouse/issues/111750

namespace
{

using namespace DB;

String toString(const Field & field)
{
    return applyVisitor(FieldVisitorToString(), field);
}

/// The value that `compatibility` treats as "current" for a setting: the `new_value` of the last
/// entry that would be applied - the highest version and, within one version, the last occurrence.
struct NewestChange
{
    String version;
    Field new_value;
};

template <typename SettingsT>
void checkHistoryAgreesWithDefaults(const VersionToSettingsChangesMap & history)
{
    const SettingsT defaults;

    /// The map is ordered by version, so a later iteration always overwrites an older record.
    std::map<String, NewestChange> newest_change;

    for (const auto & [version, changes] : history)
    {
        /// Entries are applied in the order they are written in a version block, so within one
        /// version the last entry for a setting is the one that wins.
        for (const auto & change : changes)
        {
            if (!SettingsT::hasBuiltin(change.name))
                continue; /// The setting was removed; nothing to compare against.

            /// A setting can be recorded under an alias, e.g. under the name it had before a rename.
            const String name{SettingsT::resolveName(change.name)};

            /// Obsolete settings do nothing, so it does not matter what `compatibility` sets them to.
            if (defaults.getTier(name) == SettingsTierType::OBSOLETE)
                continue;

            newest_change[name] = {version.toString(), change.new_value};
        }
    }

    for (const auto & [name, change] : newest_change)
    {
        Field recorded;
        try
        {
            recorded = SettingsT::castValueUtil(name, change.new_value);
        }
        catch (const Exception & e)
        {
            ADD_FAILURE() << "The value " << toString(change.new_value) << " recorded for the setting `" << name
                          << "` in version " << change.version << " of SettingsChangesHistory.cpp is not a valid value"
                          << " for this setting: " << e.message();
            continue;
        }

        EXPECT_EQ(toString(recorded), toString(defaults.get(name)))
            << "The newest value recorded for the setting `" << name << "` in SettingsChangesHistory.cpp (version "
            << change.version << ") is not the current default of this setting."
            << " Either the default was changed without recording it in SettingsChangesHistory.cpp,"
            << " or the recorded value is wrong. Because of this, the `compatibility` setting restores a wrong value.";
    }
}

}

GTEST_TEST(SettingsChangesHistory, SessionSettingsAgreeWithDefaults)
{
    checkHistoryAgreesWithDefaults<Settings>(getSettingsChangesHistory());
}

GTEST_TEST(SettingsChangesHistory, MergeTreeSettingsAgreeWithDefaults)
{
    checkHistoryAgreesWithDefaults<MergeTreeSettings>(getMergeTreeSettingsChangesHistory());
}
