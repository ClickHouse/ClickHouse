#pragma once

#include <ext/shared_ptr_helper.hpp>
#include <Storages/IStorage.h>


namespace DB
{

class Context;

/** Implements system table 'columns', that allows to get information about columns for every table.
  */
class StorageSystemColumns : private ext::shared_ptr_helper<StorageSystemColumns>, public IStorage
{
friend class ext::shared_ptr_helper<StorageSystemColumns>;
public:
    static StoragePtr create(const std::string & name_);

    std::string getName() const override { return "SystemColumns"; }
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
    StorageSystemColumns(const std::string & name_);

private:
    const std::string name;
    NamesAndTypesList columns;
};

}
