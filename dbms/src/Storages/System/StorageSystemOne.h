#pragma once

#include <ext/shared_ptr_helper.hpp>
#include <Storages/IStorage.h>


namespace DB
{

class Context;


/** Implements storage for the system table One.
  * The table contains a single column of dummy UInt8 and a single row with a value of 0.
  * Used when the table is not specified in the query.
  * Analog of the DUAL table in Oracle and MySQL.
  */
class StorageSystemOne : private ext::shared_ptr_helper<StorageSystemOne>, public IStorage
{
friend class ext::shared_ptr_helper<StorageSystemOne>;

public:
    static StoragePtr create(const std::string & name_);

    std::string getName() const override { return "SystemOne"; }
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

    StorageSystemOne(const std::string & name_);
};

}
