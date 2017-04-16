#pragma once

#include <ext/shared_ptr_helper.hpp>
#include <Storages/IStorage.h>


namespace DB
{

class Context;


/** Implements `zookeeper` system table, which allows you to view the data in ZooKeeper for debugging purposes.
  */
class StorageSystemZooKeeper : private ext::shared_ptr_helper<StorageSystemZooKeeper>, public IStorage
{
friend class ext::shared_ptr_helper<StorageSystemZooKeeper>;

public:
    static StoragePtr create(const std::string & name_);

    std::string getName() const override { return "SystemZooKeeper"; }
    std::string getTableName() const override { return name; }

    const NamesAndTypesList & getColumnsListImpl() const override { return columns; }

    BlockInputStreams read(
        const Names & column_names,
        ASTPtr query,
        const Context & context,
        const Settings & settings,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size = DEFAULT_BLOCK_SIZE,
        unsigned threads = 1) override;

private:
    const std::string name;
    NamesAndTypesList columns;

    StorageSystemZooKeeper(const std::string & name_);
};

}
