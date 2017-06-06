#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/IStorage.h>


namespace DB
{

class Context;


/** Implements a repository for the system table Numbers.
  * The table contains the only column number UInt64.
  * From this table, you can read all natural numbers, starting from 0 (to 2^64 - 1, and then again).
  */
class StorageSystemNumbers : public ext::shared_ptr_helper<StorageSystemNumbers>, public IStorage
{
friend class ext::shared_ptr_helper<StorageSystemNumbers>;
public:
    std::string getName() const override { return "SystemNumbers"; }
    std::string getTableName() const override { return name; }

    const NamesAndTypesList & getColumnsListImpl() const override { return columns; }

    BlockInputStreams read(
        const Names & column_names,
        const ASTPtr & query,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

private:
    const std::string name;
    NamesAndTypesList columns;
    bool multithreaded;

    StorageSystemNumbers(const std::string & name_, bool multithreaded_);
};

}
