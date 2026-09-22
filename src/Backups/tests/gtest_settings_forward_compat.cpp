#include <Backups/BackupSettings.h>
#include <Backups/RestoreSettings.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTSetQuery.h>

#include <gtest/gtest.h>

#include <algorithm>

/// A leg must ignore a setting a newer initiator forwards, or it fails the query with `UNKNOWN_SETTING`.
/// Only for `internal = 1`, so a user's typo is still reported.

using namespace DB;

namespace
{

boost::intrusive_ptr<ASTSetQuery> makeSettings(const SettingsChanges & changes)
{
    auto settings = make_intrusive<ASTSetQuery>();
    settings->is_standalone = false;
    settings->changes = changes;
    return settings;
}

bool mentions(const SettingsChanges & changes, const String & name)
{
    return std::ranges::any_of(changes, [&](const auto & change) { return change.name == name; });
}

}

TEST(SettingsForwardCompat, RestoreIgnoresUnknownSettingFromNewerInitiator)
{
    ASTBackupQuery query;
    query.settings = makeSettings({{"internal", Field{true}}, {"setting_from_a_newer_version", Field{true}}});

    const auto settings = RestoreSettings::fromRestoreQuery(query);

    EXPECT_TRUE(settings.internal);
    EXPECT_FALSE(mentions(settings.core_settings, "setting_from_a_newer_version"))
        << "an unknown setting forwarded by a newer initiator must not become a core setting";
}

TEST(SettingsForwardCompat, RestoreKeepsUnknownSettingFromAUser)
{
    ASTBackupQuery query;
    query.settings = makeSettings({{"setting_from_a_newer_version", Field{true}}});

    const auto settings = RestoreSettings::fromRestoreQuery(query);

    EXPECT_FALSE(settings.internal);
    EXPECT_TRUE(mentions(settings.core_settings, "setting_from_a_newer_version"))
        << "a user-issued query must still carry the name through, so that a typo is reported";
}

TEST(SettingsForwardCompat, RestoreStillForwardsRealCoreSettings)
{
    ASTBackupQuery query;
    query.settings = makeSettings({{"internal", Field{true}}, {"max_threads", Field{UInt64{4}}}});

    const auto settings = RestoreSettings::fromRestoreQuery(query);

    EXPECT_TRUE(mentions(settings.core_settings, "max_threads"))
        << "a real core setting the initiator passed through must still be applied on the leg";
}

TEST(SettingsForwardCompat, BackupIgnoresUnknownSettingFromNewerInitiator)
{
    ASTBackupQuery query;
    query.settings = makeSettings({{"internal", Field{true}}, {"setting_from_a_newer_version", Field{true}}});

    const auto settings = BackupSettings::fromBackupQuery(query);

    EXPECT_TRUE(settings.internal);
    EXPECT_FALSE(mentions(settings.core_settings, "setting_from_a_newer_version"));
}

TEST(SettingsForwardCompat, BackupStillForwardsRealCoreSettings)
{
    ASTBackupQuery query;
    query.settings = makeSettings({{"internal", Field{true}}, {"max_threads", Field{UInt64{4}}}});

    const auto settings = BackupSettings::fromBackupQuery(query);

    EXPECT_TRUE(mentions(settings.core_settings, "max_threads"));
}
