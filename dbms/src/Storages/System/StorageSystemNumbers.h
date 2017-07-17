#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/IStorage.h>


namespace DB
{

class Context;


/** Implements a table engine for the system table "numbers".
  * The table contains the only column number UInt64.
  * From this table, you can read all natural numbers, starting from 0 (to 2^64 - 1, and then again).
  *
  * You could also specify a limit (how many numbers to give).
  * If multithreaded is specified, numbers will be generated in several streams
  *  (and result could be out of order). If both multithreaded and limit are specified,
  *  the table could give you not exactly 1..limit range, but some arbitary 'limit' numbers.
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
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

private:
    const std::string name;
    NamesAndTypesList columns;
    bool multithreaded;
    size_t limit;

    /// limit: 0 means unlimited.
    StorageSystemNumbers(const std::string & name_, bool multithreaded_, size_t limit_ = 0);
};

}
