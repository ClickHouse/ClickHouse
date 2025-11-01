#pragma once

#include <Storages/IStorage.h>


namespace DB
{

class StorageFilesystem final : public IStorage
{
public:
    std::string getName() const override { return "Filesystem"; }

    StorageFilesystem(
        const StorageID & table_id_,
        const ColumnsDescription & columns_description_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        bool local_mode_,
        String path_,
        String user_files_absolute_path_string_);

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & /* queryInfo */,
        ContextPtr /* context */,
        QueryProcessingStage::Enum /* processed_stage */,
        size_t max_block_size,
        size_t num_streams) override;

    bool storesDataOnDisk() const override { return false; }

    Strings getDataPaths() const override;

    NamesAndTypesList getVirtuals() const override;

protected:
    friend class StorageFilesystemSource;

private:
    bool local_mode;
    String path;
    String user_files_absolute_path_string;
};

}
