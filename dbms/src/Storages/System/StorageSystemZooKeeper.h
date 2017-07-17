#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/IStorage.h>


namespace DB
{

class Context;


/** Implements `zookeeper` system table, which allows you to view the data in ZooKeeper for debugging purposes.
  */
class StorageSystemZooKeeper : public ext::shared_ptr_helper<StorageSystemZooKeeper>, public IStorage
{
friend class ext::shared_ptr_helper<StorageSystemZooKeeper>;
public:
    std::string getName() const override { return "SystemZooKeeper"; }
    std::string getTableName() const override { return name; }

    const NamesAndTypesList & getColumnsListImpl() const override { return columns; }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

private:
    const std::string name;
    NamesAndTypesList columns;

    StorageSystemZooKeeper(const std::string & name_);
};

}
