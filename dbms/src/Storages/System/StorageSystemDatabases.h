#pragma once

#include <ext/shared_ptr_helper.hpp>
#include <Storages/IStorage.h>


namespace DB
{

class Context;


/** Implements `databases` system table, which allows you to get information about all databases.
  */
class StorageSystemDatabases : private ext::shared_ptr_helper<StorageSystemDatabases>, public IStorage
{
friend class ext::shared_ptr_helper<StorageSystemDatabases>;

public:
    static StoragePtr create(const std::string & name_);

    std::string getName() const override { return "SystemDatabases"; }
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

    StorageSystemDatabases(const std::string & name_);
};

}
