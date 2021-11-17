#pragma once

#include <base/shared_ptr_helper.h>
#include <optional>
#include <Storages/IStorage.h>


namespace DB
{

class Context;


/** Implements a table engine for the system table "numbers".
  * The table contains the only column number UInt64.
  * From this table, you can read all natural numbers, starting from 0 (to 2^64 - 1, and then again).
  *
  * You could also specify a limit (how many numbers to give).
  * If multithreaded is specified, numbers will be generated in several streams
  *  (and result could be out of order). If both multithreaded and limit are specified,
  *  the table could give you not exactly 1..limit range, but some arbitrary 'limit' numbers.
  *
  *  In multithreaded case, if even_distributed is False, implementation with atomic is used,
  *     and result is always in [0 ... limit - 1] range.
  */
class StorageSystemNumbers final : public shared_ptr_helper<StorageSystemNumbers>, public IStorage
{
    friend struct shared_ptr_helper<StorageSystemNumbers>;
public:
    std::string getName() const override { return "SystemNumbers"; }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    bool hasEvenlyDistributedRead() const override { return true; }

private:
    bool multithreaded;
    bool even_distribution;
    std::optional<UInt64> limit;
    UInt64 offset;

protected:
    /// If even_distribution is true, numbers are distributed evenly between streams.
    /// Otherwise, streams concurrently increment atomic.
    StorageSystemNumbers(const StorageID & table_id, bool multithreaded_, std::optional<UInt64> limit_ = std::nullopt, UInt64 offset_ = 0, bool even_distribution_ = true);
};

}
