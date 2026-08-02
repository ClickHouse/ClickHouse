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

/// `copySettingsToQuery` runs only from `BackupsWorker` on the non-internal ON CLUSTER path
/// (`BackupsWorker.cpp:1135` for RESTORE), which stateless tests cannot reach: their configs offer a
/// single-host cluster only, and `BACKUP/RESTORE ON CLUSTER` coverage lives in integration tests.
///
/// The rebuild emits the RESOLVED effective state, so only a CORE `name = DEFAULT` may ride along. A
/// backup-specific one must not: `backup_uuid` is empty at parse time, generated later by BackupsWorker
/// (`BackupsWorker.cpp:408-409`) and emitted as a change here, so a surviving `backup_uuid = DEFAULT`
/// would reset it away on every receiving host.
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
/// `backup_uuid` (`BackupsWorker.cpp:923-924`) and emitted by the `LIST_OF_RESTORE_SETTINGS` copy loop,
/// so the same defect is possible on this side and is pinned the same way.
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
