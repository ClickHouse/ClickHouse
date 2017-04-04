#pragma once

#include <ext/shared_ptr_helper.hpp>
#include <Storages/IStorage.h>


namespace DB
{

class Context;


class StorageSystemMerges : private ext::shared_ptr_helper<StorageSystemMerges>, public IStorage
{
friend class ext::shared_ptr_helper<StorageSystemMerges>;

public:
    static StoragePtr create(const std::string & name);

    std::string getName() const override { return "SystemMerges"; }
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

    StorageSystemMerges(const std::string & name);
};

}
