#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class Context;

struct ReplicatedPartitionExportInfo
{
    String destination_database;
    String destination_table;
    String partition_id;
    String transaction_id;
    String query_id;
    time_t create_time;
    String source_replica;
    size_t parts_count;
    size_t parts_to_do;
    std::vector<String> parts;
    String status;
    std::string exception_replica;
    std::string last_exception;
    std::string exception_part;
    size_t exception_count;
};

class StorageSystemReplicatedPartitionExports final : public IStorageSystemOneBlock
{
public:

    std::string getName() const override { return "SystemReplicatedPartitionExports"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
