#pragma once

#include <QueryPipeline/Pipe.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>

namespace DB
{

class StorageDirectory final : public shared_ptr_helper<StorageDirectory>, public IStorage
{
    friend struct shared_ptr_helper<StorageDirectory>;

public:
    std::string getName() const override { return "Directory"; }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & /* queryInfo */,
        ContextPtr /* context */,
        QueryProcessingStage::Enum /* processed_stage */,
        size_t max_block_size,
        unsigned num_streams) override;

    bool storesDataOnDisk() const override { return true; }

    Strings getDataPaths() const override;

    NamesAndTypesList getVirtuals() const override;

protected:
    friend class StorageDirectorySource;

private:
    String path;

    StorageDirectory(
        const StorageID & table_id_,
        ColumnsDescription columns_description_,
        String path_,
        ConstraintsDescription constraints_,
        const String & comment);
};
}
