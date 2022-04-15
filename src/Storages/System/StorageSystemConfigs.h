#pragma once

#include <base/shared_ptr_helper.h>
#include <Storages/IStorage.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeObject.h>


namespace DB
{

class Context;


/** Implements storage for the system table configs
  * The table contains a single column of Object and a single row which represents the current configuration.
  */
class StorageSystemConfigs final : public shared_ptr_helper<StorageSystemConfigs>, public IStorage
{
    friend struct shared_ptr_helper<StorageSystemConfigs>;
public:
    std::string getName() const override { return "SystemConfigs"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    bool isSystemStorage() const override { return true; }

protected:
    explicit StorageSystemConfigs(const StorageID & table_id_);

    inline static const NamesAndTypesList names_and_types_list = {
        {"name", std::make_shared<DataTypeString>()},
        // {"config", std::make_shared<DataTypeObject>("json", false)},
        {"config", std::make_shared<DataTypeString>()},
    };
};

}
