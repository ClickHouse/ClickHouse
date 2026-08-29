#include <Backups/BackupSettings.h>
#include <Backups/RestoreSettings.h>
#include <Backups/SettingsFieldOptionalUUID.h>
#include <Core/Settings.h>
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
/// The rebuild emits the RESOLVED effective state, so only a CORE `name = DEFAULT` may ride along, and
/// only as an ordinary change carrying the declared default: the clause reaches the other hosts as SQL
/// text, which each of them re-parses, so a `= DEFAULT` in it would break a cluster that is
/// mid-rolling-upgrade. A backup-specific one must not ride along in any form: `backup_uuid` is empty at
/// parse time, generated later by `BackupsWorker` and emitted as a change here, so a surviving
/// `backup_uuid = DEFAULT` would reset it away on every receiving host.
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
    EXPECT_TRUE(rebuilt.default_settings.empty())
        << "the per-host text must contain no `= DEFAULT`, got: " << backup_query->formatWithSecretsOneLine();
    /// The exact text every receiving host parses, produced by the same call `executeDDLQueryOnCluster`
    /// makes. This is the cross-version property, not just the AST shape.
    EXPECT_EQ(String::npos, backup_query->formatWithSecretsOneLine().find("DEFAULT"))
        << "a parser without this fix rejects a `= DEFAULT` item that follows a comma";

    const auto * reset_change = rebuilt.changes.tryGet("max_execution_time");
    ASSERT_NE(nullptr, reset_change) << "the core reset was dropped instead of being resolved";
    EXPECT_EQ(Settings{}.get("max_execution_time"), *reset_change)
        << "the forwarded value is not the declared default a reset would produce";

    const auto * uuid_change = rebuilt.changes.tryGet("backup_uuid");
    ASSERT_NE(nullptr, uuid_change) << "the generated backup_uuid was not emitted";
    EXPECT_EQ(assigned_uuid, SettingFieldOptionalUUID{*uuid_change}.value)
        << "the generated backup_uuid was discarded";
}

/// A core name written in BOTH carriers. On the host that parsed the clause the reset wins, because it is
/// applied after every override, so the forwarded clause must resolve the same way. The receiver applies
/// the changes in order, so what pins it is that the declared default is the LAST change for that name -
/// dropping the reset here would ship the pre-reset value and silently diverge the hosts.
TEST(BackupSettingsDefault, BackupCopySettingsToQueryResolvesANameInBothCarriers)
{
    const String query = "BACKUP TABLE t TO Disk('d', 'b') "
                         "SETTINGS max_threads = 4, max_threads = DEFAULT";
    ASTPtr holder;
    ASTBackupQuery * backup_query = parseBackupQuery(holder, query);
    ASSERT_NE(nullptr, backup_query) << "query: " << query;

    BackupSettings settings = BackupSettings::fromBackupQuery(*backup_query);
    settings.copySettingsToQuery(*backup_query);

    ASSERT_NE(nullptr, backup_query->settings);
    const auto & rebuilt = backup_query->settings->as<const ASTSetQuery &>();

    std::vector<Field> max_threads_values;
    for (const auto & change : rebuilt.changes)
        if (change.name == "max_threads")
            max_threads_values.push_back(change.value);

    ASSERT_FALSE(max_threads_values.empty()) << "the whole setting vanished from the rebuild";
    EXPECT_EQ(Settings{}.get("max_threads"), max_threads_values.back())
        << "the last value the receiver applies is not the declared default, so the reset lost";
}

/// A name with no declared default cannot be forwarded as a value, so the rebuild drops its overrides
/// instead: a setting the reset removed on the host that parsed the clause must not arrive set on any
/// other host. The unrelated override pins that only the reset name is dropped.
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
    EXPECT_TRUE(rebuilt.default_settings.empty())
        << "the per-host text must contain no `= DEFAULT`, got: " << restore_query->formatWithSecretsOneLine();
    EXPECT_EQ(String::npos, restore_query->formatWithSecretsOneLine().find("DEFAULT"))
        << "a parser without this fix rejects a `= DEFAULT` item that follows a comma";

    const auto * reset_change = rebuilt.changes.tryGet("max_execution_time");
    ASSERT_NE(nullptr, reset_change) << "the core reset was dropped instead of being resolved";
    EXPECT_EQ(Settings{}.get("max_execution_time"), *reset_change)
        << "the forwarded value is not the declared default a reset would produce";

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
