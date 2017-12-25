#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/IStorage.h>


namespace DB
{

class Context;


class StorageSystemModels : public ext::shared_ptr_helper<StorageSystemModels>, public IStorage
{
public:
    std::string getName() const override { return "SystemModels"; }
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
    const NamesAndTypesList columns;

    const NamesAndTypesList & getColumnsListImpl() const override { return columns; }

protected:
    StorageSystemModels(const std::string & name);
};

}
