#pragma once

#include <ext/shared_ptr_helper.h>
#include <optional>
#include <Storages/IStorage.h>

namespace DB
{

/** Implements a table engine for the system table "zeros".
  * The table contains the only column zero UInt8.
  * From this table, you can read non-materialized zeros.
  *
  * You could also specify a limit (how many zeros to give).
  * If multithreaded is specified, zeros will be generated in several streams.
  */
class StorageSystemZeros final : public ext::shared_ptr_helper<StorageSystemZeros>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageSystemZeros>;
public:
    std::string getName() const override { return "SystemZeros"; }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    bool hasEvenlyDistributedRead() const override { return true; }

private:
    bool multithreaded;
    std::optional<UInt64> limit;

protected:
    /// If even_distribution is true, numbers are distributed evenly between streams.
    /// Otherwise, streams concurrently increment atomic.
    StorageSystemZeros(const StorageID & table_id_, bool multithreaded_, std::optional<UInt64> limit_ = std::nullopt);
};

}
