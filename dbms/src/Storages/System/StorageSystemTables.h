#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/IStorage.h>


namespace DB
{

class Context;


/** Implements the system table `tables`, which allows you to get information about all tables.
  */
class StorageSystemTables : public ext::shared_ptr_helper<StorageSystemTables>, public IStorage
{
public:
    std::string getName() const override { return "SystemTables"; }
    std::string getTableName() const override { return name; }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        UInt64 max_block_size,
        unsigned num_streams) override;

private:
    const std::string name;

protected:
    StorageSystemTables(const std::string & name_);
};

}
