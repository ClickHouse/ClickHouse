#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/** Implements the `query_masking_rules` system table, which contains the query masking rules created by SQL.
  */
class StorageSystemQueryMaskingRules final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemQueryMaskingRules"; }
    static ColumnsDescription getColumnsDescription();

    void backupData(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & partitions) override;
    void restoreDataFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & partitions) override;

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
