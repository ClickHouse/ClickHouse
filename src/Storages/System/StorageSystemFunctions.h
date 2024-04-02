#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements `functions`system table, which allows you to get a list
  * all normal and aggregate functions.
  */
class StorageSystemFunctions final : public IStorageSystemOneBlock<StorageSystemFunctions>
{
public:
    std::string getName() const override { return "SystemFunctions"; }

    static NamesAndTypesList getNamesAndTypes();

    void backupData(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & partitions) override;
    void restoreDataFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & partitions) override;

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
