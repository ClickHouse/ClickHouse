#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/IStorage.h>


namespace DB
{

class Context;


/** Implements `functions`system table, which allows you to get a list
  * all normal and aggregate functions.
  */
class StorageSystemFunctions : public ext::shared_ptr_helper<StorageSystemFunctions>, public IStorage
{
public:
    std::string getName() const override { return "SystemFunctions"; }
    std::string getTableName() const override { return name; }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

protected:
    StorageSystemFunctions(const std::string & name_);

private:
    const std::string name;
};

}
