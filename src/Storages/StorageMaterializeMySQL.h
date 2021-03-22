#pragma once

#include "config_core.h"

#if USE_MYSQL

#include <Storages/IStorage.h>
#include <Databases/MySQL/DatabaseMaterializeMySQL.h>

namespace DB
{

class StorageMaterializeMySQL final : public ext::shared_ptr_helper<StorageMaterializeMySQL>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageMaterializeMySQL>;
public:
    String getName() const override { return "MaterializeMySQL"; }

    bool supportsFinal() const override { return nested_storage->supportsFinal(); }
    bool supportsSampling() const override { return nested_storage->supportsSampling(); }

    StorageMaterializeMySQL(const StoragePtr & nested_storage_, const DatabaseMaterializeMySQL * database_);

    Pipe read(
        const Names & column_names, const StorageMetadataPtr & metadata_snapshot, const SelectQueryInfo & query_info,
        const Context & context, QueryProcessingStage::Enum processed_stage, size_t max_block_size, unsigned num_streams) override;

    NamesAndTypesList getVirtuals() const override;

private:
    StoragePtr nested_storage;
    const DatabaseMaterializeMySQL * database;
};

}

#endif
