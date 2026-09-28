#include <Backups/BackupSettings.h>
#include <Backups/RestoreSettings.h>
#include <Backups/SettingsFieldOptionalUUID.h>
#include <Core/Settings.h>
#include <Core/SettingsFields.h>
#include <Core/UUID.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

ASTBackupQuery * parseBackupQuery(ASTPtr & holder, const String & query)
{
    ParserQuery parser(query.data() + query.size());
    holder = parseQuery(parser, query, "", 0, 0, 0);
    return holder ? holder->as<ASTBackupQuery>() : nullptr;
}

}

/// `copySettingsToQuery` runs only from `BackupsWorker` on the non-internal ON CLUSTER path, which
/// stateless tests cannot reach: their configs offer a single-host cluster only, and
/// `BACKUP/RESTORE ON CLUSTER` coverage lives in integration tests.
///
/// The rebuild emits the RESOLVED effective state, and a `name = DEFAULT` does not ride along in any form.
/// The clause reaches the other hosts as SQL text, which each of them re-parses, so a `= DEFAULT` in it
/// would break a cluster that is mid-rolling-upgrade. A core reset must not be replaced with its declared
/// default either: that would mark the setting as changed on every receiving host, while the reset leaves
/// it unset on the initiator, and the initiator's DDL settings packet already omits it. A backup-specific
/// reset must not ride along because `backup_uuid` is empty at parse time, generated later by
/// `BackupsWorker` and emitted as a change here, so a surviving `backup_uuid = DEFAULT` would reset it away
/// on every receiving host.
TEST(BackupSettingsDefault, BackupCopySettingsToQueryForwardsNoReset)
{
    const String query = "BACKUP TABLE t TO Disk('d', 'b') "
                         "SETTINGS max_execution_time = DEFAULT, backup_uuid = DEFAULT";
    ASTPtr holder;
    ASTBackupQuery * backup_query = parseBackupQuery(holder, query);
    ASSERT_NE(nullptr, backup_query) << "query: " << query;

    BackupSettings settings = BackupSettings::fromBackupQuery(*backup_query);
    const UUID assigned_uuid = UUIDHelpers::generateV4();
    settings.backup_uuid = assigned_uuid;

    settings.copySettingsToQuery(*backup_query);

    ASSERT_NE(nullptr, backup_query->settings);
    const auto & rebuilt = backup_query->settings->as<const ASTSetQuery &>();
    EXPECT_TRUE(rebuilt.default_settings.empty())
        << "the per-host text must contain no `= DEFAULT`, got: " << backup_query->formatWithSecretsOneLine();
    /// The exact text every receiving host parses, produced by the same call `executeDDLQueryOnCluster`
    /// makes. This is the cross-version property, not just the AST shape.
    EXPECT_EQ(String::npos, backup_query->formatWithSecretsOneLine().find("DEFAULT"))
        << "a parser without this fix rejects a `= DEFAULT` item that follows a comma";

    EXPECT_EQ(nullptr, rebuilt.changes.tryGet("max_execution_time"))
        << "the core reset arrives as an explicit value, which marks the setting as changed on the receiver: "
        << backup_query->formatWithSecretsOneLine();

    const auto * uuid_change = rebuilt.changes.tryGet("backup_uuid");
    ASSERT_NE(nullptr, uuid_change) << "the generated backup_uuid was not emitted";
    EXPECT_EQ(assigned_uuid, SettingFieldOptionalUUID{*uuid_change}.value)
        << "the generated backup_uuid was discarded";
}

/// A core name written in BOTH carriers. On the host that parsed the clause the reset wins, because it is
/// applied after every override, so the rebuilt clause must not carry the override: a receiving host
/// applies the text on top of the DDL settings packet, so a surviving `max_threads = 4` would silently
/// diverge the hosts. The same holds for an override written through an alias of the reset setting
/// (`insert_distributed_sync` is an alias of `distributed_foreground_insert`), while an unrelated override
/// stays.
TEST(BackupSettingsDefault, BackupCopySettingsToQueryDropsTheOverridesOfAReset)
{
    const String query = "BACKUP TABLE t TO Disk('d', 'b') "
                         "SETTINGS max_threads = 4, max_threads = DEFAULT, "
                         "insert_distributed_sync = 1, distributed_foreground_insert = DEFAULT, max_block_size = 1000";
    ASTPtr holder;
    ASTBackupQuery * backup_query = parseBackupQuery(holder, query);
    ASSERT_NE(nullptr, backup_query) << "query: " << query;

    BackupSettings settings = BackupSettings::fromBackupQuery(*backup_query);
    settings.copySettingsToQuery(*backup_query);

    ASSERT_NE(nullptr, backup_query->settings);
    const auto & rebuilt = backup_query->settings->as<const ASTSetQuery &>();

    EXPECT_EQ(nullptr, rebuilt.changes.tryGet("max_threads"))
        << "the override the reset cancels arrives on every other host: " << backup_query->formatWithSecretsOneLine();
    EXPECT_EQ(nullptr, rebuilt.changes.tryGet("insert_distributed_sync"))
        << "an override written through an alias of the reset setting survived: "
        << backup_query->formatWithSecretsOneLine();

    const auto * kept = rebuilt.changes.tryGet("max_block_size");
    ASSERT_NE(nullptr, kept) << "an unrelated override was dropped with it";
    EXPECT_EQ(Field(UInt64{1000}), *kept);
}

/// A custom setting the reset removed on the host that parsed the clause must not arrive set on any other
/// host. The unrelated override pins that only the reset name is dropped.
TEST(BackupSettingsDefault, BackupCopySettingsToQueryDropsAResetCustomSetting)
{
    const String query = "BACKUP TABLE t TO Disk('d', 'b') "
                         "SETTINGS SQL_x = 1, SQL_x = DEFAULT, max_threads = 4";
    ASTPtr holder;
    ASTBackupQuery * backup_query = parseBackupQuery(holder, query);
    ASSERT_NE(nullptr, backup_query) << "query: " << query;

    BackupSettings settings = BackupSettings::fromBackupQuery(*backup_query);
    settings.copySettingsToQuery(*backup_query);

    ASSERT_NE(nullptr, backup_query->settings);
    const auto & rebuilt = backup_query->settings->as<const ASTSetQuery &>();

    EXPECT_EQ(nullptr, rebuilt.changes.tryGet("SQL_x"))
        << "a reset custom setting arrives set on every other host: " << backup_query->formatWithSecretsOneLine();

    const auto * kept = rebuilt.changes.tryGet("max_threads");
    ASSERT_NE(nullptr, kept) << "an unrelated override was dropped with it";
    EXPECT_EQ(Field(UInt64{4}), *kept);
}

/// A `merge_tree_`-prefixed setting is stored under the exact name that wrote it, and a reset clears what
/// every one of its names wrote (`Context::resetSettingsToDefaultValue`), so the rebuild has to drop the
/// overrides of all of them: a surviving one arrives set on hosts that never saw the reset.
TEST(BackupSettingsDefault, BackupCopySettingsToQueryDropsEveryNameOfAResetMergeTreeSetting)
{
    /// `merge_tree_enable_block_number_column` and `merge_tree_allow_experimental_block_number_column` are
    /// the two names of one setting (`DECLARE_WITH_ALIAS`); `merge_tree_enable_block_offset_column` is a
    /// separate setting and the control.
    const String query = "BACKUP TABLE t TO Disk('d', 'b') SETTINGS "
                         "merge_tree_enable_block_number_column = 1, merge_tree_enable_block_offset_column = 1, "
                         "merge_tree_allow_experimental_block_number_column = DEFAULT";
    ASTPtr holder;
    ASTBackupQuery * backup_query = parseBackupQuery(holder, query);
    ASSERT_NE(nullptr, backup_query) << "query: " << query;

    BackupSettings settings = BackupSettings::fromBackupQuery(*backup_query);
    settings.copySettingsToQuery(*backup_query);

    ASSERT_NE(nullptr, backup_query->settings);
    const auto & rebuilt = backup_query->settings->as<const ASTSetQuery &>();

    EXPECT_EQ(nullptr, rebuilt.changes.tryGet("merge_tree_enable_block_number_column"))
        << "the reset setting's other name arrives set on every other host: "
        << backup_query->formatWithSecretsOneLine();
    EXPECT_EQ(nullptr, rebuilt.changes.tryGet("merge_tree_allow_experimental_block_number_column"))
        << "rebuilt: " << backup_query->formatWithSecretsOneLine();

    const auto * kept = rebuilt.changes.tryGet("merge_tree_enable_block_offset_column");
    ASSERT_NE(nullptr, kept) << "an unrelated MergeTree override was dropped with it";
    EXPECT_EQ(Field(UInt64{1}), *kept);
}

/// The RESTORE twin of `BackupCopySettingsToQueryForwardsNoReset`. `restore_uuid` is generated after
/// parsing exactly like `backup_uuid` and emitted by the `LIST_OF_RESTORE_SETTINGS` copy loop, so the same
/// defect is possible on this side and is pinned the same way.
TEST(BackupSettingsDefault, RestoreCopySettingsToQueryForwardsNoReset)
{
    const String query = "RESTORE TABLE t FROM Disk('d', 'b') "
                         "SETTINGS max_execution_time = 10, max_execution_time = DEFAULT, restore_uuid = DEFAULT";
    ASTPtr holder;
    ASTBackupQuery * restore_query = parseBackupQuery(holder, query);
    ASSERT_NE(nullptr, restore_query) << "query: " << query;

    RestoreSettings settings = RestoreSettings::fromRestoreQuery(*restore_query);
    const UUID assigned_uuid = UUIDHelpers::generateV4();
    settings.restore_uuid = assigned_uuid;

    settings.copySettingsToQuery(*restore_query);

    ASSERT_NE(nullptr, restore_query->settings);
    const auto & rebuilt = restore_query->settings->as<const ASTSetQuery &>();
    EXPECT_TRUE(rebuilt.default_settings.empty())
        << "the per-host text must contain no `= DEFAULT`, got: " << restore_query->formatWithSecretsOneLine();
    EXPECT_EQ(String::npos, restore_query->formatWithSecretsOneLine().find("DEFAULT"))
        << "a parser without this fix rejects a `= DEFAULT` item that follows a comma";

    EXPECT_EQ(nullptr, rebuilt.changes.tryGet("max_execution_time"))
        << "the core reset or the override it cancels arrives on the receiver: "
        << restore_query->formatWithSecretsOneLine();

    const auto * uuid_change = rebuilt.changes.tryGet("restore_uuid");
    ASSERT_NE(nullptr, uuid_change) << "the generated restore_uuid was not emitted";
    EXPECT_EQ(assigned_uuid, SettingFieldOptionalUUID{*uuid_change}.value)
        << "the generated restore_uuid was discarded";
}

/// `isAsync` decides whether the client waits in `InterpreterBackupQuery::execute` while
/// `fromBackupQuery` decides the operation's effective `async`. They read the same clause separately, so
/// they must agree on it, over duplicates and over value spellings alike.
TEST(BackupSettingsDefault, IsAsyncAgreesWithFromBackupQuery)
{
    struct Case
    {
        const char * settings;
        bool expected;
    };

    /// A repeated setting takes its last value, as `SET` does; a string value converts as the Bool field
    /// does. The `= DEFAULT` forms resolve to the field's default, which is false.
    const Case cases[] = {
        {"async = 0, async = 1", true},
        {"async = 1, async = 0", false},
        {"async = 1, async = 1", true},
        {"async = '1'", true},
        {"async = 'true'", true},
        {"async = '0'", false},
        {"async = 1", true},
        {"async = 0", false},
        {"async = 1, async = DEFAULT", false},
        {"async = DEFAULT, async = 1", false},
        {"max_execution_time = 1", false},
    };

    for (const auto & test_case : cases)
    {
        const String query = String("BACKUP TABLE t TO Disk('d', 'b') SETTINGS ") + test_case.settings;
        ASTPtr holder;
        ASTBackupQuery * backup_query = parseBackupQuery(holder, query);
        ASSERT_NE(nullptr, backup_query) << "query: " << query;

        EXPECT_EQ(test_case.expected, BackupSettings::isAsync(*backup_query)) << "query: " << query;
        EXPECT_EQ(BackupSettings::fromBackupQuery(*backup_query).async, BackupSettings::isAsync(*backup_query))
            << "the wait decision disagrees with the effective setting, query: " << query;
    }
}
