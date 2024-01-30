#pragma once

#include <optional>
#include <Parsers/ASTIdentifier.h>
#include <Storages/IStorage.h>


namespace DB
{

class Context;

class   StorageSystemNumbers final : public IStorage
{
public:
    /// Otherwise, streams concurrently increment atomic.
    StorageSystemNumbers(const StorageID & table_id, bool multithreaded_, const std::string& column_name, std::optional<UInt64> limit_ = std::nullopt, UInt64 offset_ = 0, UInt64 step_ = 1);

    std::string getName() const override { return "SystemNumbers"; }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t max_block_size,
        size_t num_streams) override;

    bool hasEvenlyDistributedRead() const override { return true; }
    bool isSystemStorage() const override { return true; }

    bool supportsTransactions() const override { return true; }

private:
    friend class ReadFromSystemNumbersStep;

    bool multithreaded;
    std::optional<UInt64> limit;
    UInt64 offset;
    std::string column_name;
    
    UInt64 step;

};

}
