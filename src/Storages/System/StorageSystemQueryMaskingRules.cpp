#include <Storages/System/StorageSystemQueryMaskingRules.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <Access/AccessControl.h>
#include <Access/Common/AccessFlags.h>
#include <Access/QueryMaskingRule.h>
#include <Backups/BackupEntriesCollector.h>
#include <Backups/RestorerFromBackup.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/HiddenSecret.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <Interpreters/Context.h>
#include <Interpreters/formatWithPossiblyHidingSecrets.h>


namespace DB
{

ColumnsDescription StorageSystemQueryMaskingRules::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Rule name."},
        {"id", std::make_shared<DataTypeUUID>(), "Rule ID."},
        {"storage", std::make_shared<DataTypeString>(), "Name of the access storage the rule is stored in."},
        {"regexp", std::make_shared<DataTypeString>(), "The regular expression. Shown as `[HIDDEN]` unless the display of secrets is enabled."},
        {"replace", std::make_shared<DataTypeString>(), "The substitution string for the matches of the regular expression."},
    };
}


void StorageSystemQueryMaskingRules::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    /// If "select_from_system_db_requires_grant" is enabled the access rights were already checked in InterpreterSelectQuery.
    const auto & access_control = context->getAccessControl();
    if (!access_control.doesSelectFromSystemDatabaseRequireGrant())
        context->checkAccess(AccessType::QUERY_MASKING_RULE_ADMIN);

    const bool show_secrets = canDisplaySecrets(context);
    std::vector<UUID> ids = access_control.findAll<QueryMaskingRule>();

    size_t column_index = 0;
    auto & column_name = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_id = assert_cast<ColumnUUID &>(*res_columns[column_index++]).getData();
    auto & column_storage = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_regexp = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_replace = assert_cast<ColumnString &>(*res_columns[column_index++]);

    for (const auto & id : ids)
    {
        auto rule = access_control.tryRead<QueryMaskingRule>(id);
        if (!rule)
            continue;
        auto storage = access_control.findStorage(id);
        if (!storage)
            continue;

        column_name.insertData(rule->getName().data(), rule->getName().length());
        column_id.push_back(id.toUnderType());
        column_storage.insertData(storage->getStorageName().data(), storage->getStorageName().length());
        if (show_secrets)
            column_regexp.insertData(rule->regexp.data(), rule->regexp.length());
        else
            column_regexp.insertData(HIDDEN_SECRET.data(), HIDDEN_SECRET.length());
        column_replace.insertData(rule->replace.data(), rule->replace.length());
    }
}

void StorageSystemQueryMaskingRules::backupData(
    BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & /* partitions */)
{
    const auto & access_control = backup_entries_collector.getContext()->getAccessControl();
    access_control.backup(backup_entries_collector, data_path_in_backup, AccessEntityType::QUERY_MASKING_RULE);
}

void StorageSystemQueryMaskingRules::restoreDataFromBackup(
    RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & /* partitions */)
{
    auto & access_control = restorer.getContext()->getAccessControl();
    access_control.restoreFromBackup(restorer, data_path_in_backup);
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemQueryMaskingRules) }
