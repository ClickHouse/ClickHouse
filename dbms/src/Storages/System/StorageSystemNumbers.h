#pragma once

#include <ext/shared_ptr_helper.hpp>
#include <Storages/IStorage.h>


namespace DB
{

class Context;


/** Implements a repository for the system table Numbers.
  * The table contains the only column number UInt64.
  * From this table, you can read all natural numbers, starting from 0 (to 2^64 - 1, and then again).
  */
class StorageSystemNumbers : private ext::shared_ptr_helper<StorageSystemNumbers>, public IStorage
{
friend class ext::shared_ptr_helper<StorageSystemNumbers>;

public:
    static StoragePtr create(const std::string & name_, bool multithreaded_ = false);

    std::string getName() const override { return "SystemNumbers"; }
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
    bool multithreaded;

    StorageSystemNumbers(const std::string & name_, bool multithreaded_);
};

}
