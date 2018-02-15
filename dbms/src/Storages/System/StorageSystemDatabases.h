#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/IStorage.h>


namespace DB
{

class Context;


/** Implements `databases` system table, which allows you to get information about all databases.
  */
class StorageSystemDatabases : public ext::shared_ptr_helper<StorageSystemDatabases>, public IStorage
{
public:
    std::string getName() const override { return "SystemDatabases"; }
    std::string getTableName() const override { return name; }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

private:
    const std::string name;

protected:
    StorageSystemDatabases(const std::string & name_);
};

}
