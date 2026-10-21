#pragma once

#include <Storages/IStorage.h>

#include <optional>

namespace DB
{

/** Implements a table engine for the system table `primes`.
  * The table contains the only column `prime` UInt64.
  * From this table, we can read all prime numbers, starting from 2.
  *
  * Parameters:
  *   limit  - how many rows (primes) to produce
  *   offset - how many primes to skip from the beginning (0-based in prime-index space)
  *   step   - step in prime-index space (1 means every prime)
  *
  * In text, after skipping `offset` primes, take every `step`-th prime until `limit` primes are taken.
  */
class StorageSystemPrimes final : public IStorage
{
public:
    StorageSystemPrimes(
        const StorageID & table_id,
        const std::string & column_name,
        std::optional<UInt64> limit_ = std::nullopt,
        UInt64 offset_ = 0,
        UInt64 step_ = 1);

    std::string getName() const override { return "SystemPrimes"; }

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
    friend class ReadFromSystemPrimesStep;

    std::optional<UInt64> limit;
    UInt64 offset;
    std::string column_name;
    UInt64 step;
};

}
