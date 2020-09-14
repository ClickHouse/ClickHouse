#pragma once

#include "config_core.h"

#if USE_MYSQL

#include <Storages/IStorage.h>

namespace DB
{

template<typename DatabaseT>
class StorageMaterializeMySQL final : public ext::shared_ptr_helper<StorageMaterializeMySQL<DatabaseT>>, public IStorage
{
    //static_assert(std::is_same_v<DatabaseT, DatabaseMaterializeMySQL<DatabaseOrdinary>> ||
    //              std::is_same_v<DatabaseT, DatabaseMaterializeMySQL<DatabaseAtomic>>);
    friend struct ext::shared_ptr_helper<StorageMaterializeMySQL<DatabaseT>>;
public:
    String getName() const override { return "MaterializeMySQL"; }

    bool supportsFinal() const override { return nested_storage->supportsFinal(); }
    bool supportsSampling() const override { return nested_storage->supportsSampling(); }

    StorageMaterializeMySQL(const StoragePtr & nested_storage_, const DatabaseT * database_);

    Pipe read(
        const Names & column_names, const StorageMetadataPtr & metadata_snapshot, const SelectQueryInfo & query_info,
        const Context & context, QueryProcessingStage::Enum processed_stage, size_t max_block_size, unsigned num_streams) override;

    NamesAndTypesList getVirtuals() const override;

    StoragePtr getNested() const { return nested_storage; }

    void drop() override { nested_storage->drop(); }

private:
    StoragePtr nested_storage;
    const DatabaseT * database;
};

}

#endif
