#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/IStorage.h>


namespace DB
{

class Context;


/** Information about macros for introspection.
  */
class StorageSystemMacros : public ext::shared_ptr_helper<StorageSystemMacros>, public IStorage
{
public:
    std::string getName() const override { return "SystemMacros"; }
    std::string getTableName() const override { return name; }

    BlockInputStreams read(
            const Names & column_names,
            const SelectQueryInfo & query_info,
            const Context & context,
            QueryProcessingStage::Enum & processed_stage,
            size_t max_block_size,
            unsigned num_streams) override;

private:
    const std::string name;

protected:
    StorageSystemMacros(const std::string & name_);
};

}
