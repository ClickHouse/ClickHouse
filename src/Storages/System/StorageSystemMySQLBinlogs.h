#pragma once

#include <Storages/IStorage.h>

namespace DB
{

class StorageSystemMySQLBinlogs final : public IStorage
{
public:
    explicit StorageSystemMySQLBinlogs(const StorageID & storage_id_);

    std::string getName() const override { return "MySQLBinlogs"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    bool isSystemStorage() const override { return true; }

    static NamesAndTypesList getNamesAndTypes();
};

}
