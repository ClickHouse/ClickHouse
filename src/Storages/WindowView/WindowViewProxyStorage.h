#pragma once

#include <DataTypes/DataTypeDateTime.h>
#include <Processors/Pipe.h>
#include <Storages/IStorage.h>

namespace DB
{

class WindowViewProxyStorage : public IStorage
{
public:
    WindowViewProxyStorage(const StorageID & table_id_, ColumnsDescription columns_, Pipes pipes_, QueryProcessingStage::Enum to_stage_)
    : IStorage(table_id_)
    , pipes(std::move(pipes_))
    , to_stage(to_stage_)
    {
        columns_.add({"____timestamp", std::make_shared<DataTypeDateTime>(), false});
        setColumns(std::move(columns_));
    }

public:
    std::string getName() const override { return "WindowViewProxy"; }

    QueryProcessingStage::Enum getQueryProcessingStage(const Context &, QueryProcessingStage::Enum /*to_stage*/, const ASTPtr &) const override { return to_stage; }

    Pipes read(
            const Names & /*column_names*/,
            const SelectQueryInfo & /*query_info*/,
            const Context & /*context*/,
            QueryProcessingStage::Enum /*processed_stage*/,
            size_t /*max_block_size*/,
            unsigned /*num_streams*/) override
    {
        return std::move(pipes);
    }

private:
    Pipes pipes;
    QueryProcessingStage::Enum to_stage;
};
}
