#include <Backups/BackupSettings.h>
#include <Backups/RestoreSettings.h>
#include <Backups/SettingsFieldOptionalUUID.h>
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
/// The rebuild emits the RESOLVED effective state, so only a CORE `name = DEFAULT` may ride along. A
/// backup-specific one must not: `backup_uuid` is empty at parse time, generated later by
/// `BackupsWorker` and emitted as a change here, so a surviving `backup_uuid = DEFAULT` would reset
/// it away on every receiving host.
TEST(BackupSettingsDefault, BackupCopySettingsToQueryCarriesOnlyCoreDefaults)
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
    EXPECT_EQ((std::vector<String>{"max_execution_time"}), rebuilt.default_settings)
        << "a backup-specific `name = DEFAULT` rode along, or the core one was dropped";
    const auto * uuid_change = rebuilt.changes.tryGet("backup_uuid");
    ASSERT_NE(nullptr, uuid_change) << "the generated backup_uuid was not emitted";
    EXPECT_EQ(assigned_uuid, SettingFieldOptionalUUID{*uuid_change}.value)
        << "the generated backup_uuid was discarded";
}

/// The RESTORE twin of the case above. `restore_uuid` is generated after parsing exactly like
/// `backup_uuid` and emitted by the `LIST_OF_RESTORE_SETTINGS` copy loop, so the same defect is
/// possible on this side and is pinned the same way.
TEST(BackupSettingsDefault, RestoreCopySettingsToQueryCarriesOnlyCoreDefaults)
{
    const String query = "RESTORE TABLE t FROM Disk('d', 'b') "
                         "SETTINGS max_execution_time = DEFAULT, restore_uuid = DEFAULT";
    ASTPtr holder;
    ASTBackupQuery * restore_query = parseBackupQuery(holder, query);
    ASSERT_NE(nullptr, restore_query) << "query: " << query;

    RestoreSettings settings = RestoreSettings::fromRestoreQuery(*restore_query);
    const UUID assigned_uuid = UUIDHelpers::generateV4();
    settings.restore_uuid = assigned_uuid;

    settings.copySettingsToQuery(*restore_query);

    ASSERT_NE(nullptr, restore_query->settings);
    const auto & rebuilt = restore_query->settings->as<const ASTSetQuery &>();
    EXPECT_EQ((std::vector<String>{"max_execution_time"}), rebuilt.default_settings)
        << "a restore-specific `name = DEFAULT` rode along, or the core one was dropped";
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
