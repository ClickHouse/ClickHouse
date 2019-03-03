#pragma once

#include <Storages/IStorage.h>
#include <ext/shared_ptr_helper.h>

namespace DB
{

class StorageSystemFormatSchemas : public ext::shared_ptr_helper<StorageSystemFormatSchemas>, public IStorage
{
public:
    StorageSystemFormatSchemas(const String & name_);

    std::string getTableName() const override { return name; }
    std::string getName() const override { return "SystemFormatSchemas"; }

    BlockInputStreams read(const Names & column_names,
                           const SelectQueryInfo & query_info,
                           const Context & context,
                           QueryProcessingStage::Enum /*processed_stage*/,
                           size_t /*max_block_size*/,
                           unsigned /*num_streams*/) override;

private:
    String name;
};

}
