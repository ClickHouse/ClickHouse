#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/IStorage.h>


namespace DB
{

class Context;


/** System table "build_options" with many params used for clickhouse building
  */
class StorageSystemBuildOptions : public ext::shared_ptr_helper<StorageSystemBuildOptions>, public IStorage
{
friend class ext::shared_ptr_helper<StorageSystemBuildOptions>;

public:
    std::string getName() const override { return "SystemBuildOptions"; }
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

    StorageSystemBuildOptions(const std::string & name_);
};

}
