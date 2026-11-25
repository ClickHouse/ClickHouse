#pragma once

#include <Common/HashTable/HashTableGrowthBudget.h>

#include <atomic>

namespace DB
{

/** Tells the buckets of the hash tables of a `HashJoin` build to stay smaller, in the Robin Hood mode,
  * when growing would take the join over the spill threshold of `SpillingHashJoin` or over the memory limit of the query.
  *
  * The buckets fill at about the same rate, so they overflow at about the same time. The first bucket to ask therefore
  * judges the growth of the whole join, and the answer is kept: every later bucket of the build also takes the smaller table.
  *
  * The step is judged against two limits:
  *  - the spill threshold: `SpillingHashJoin` switches to `GraceHashJoin` once the bytes of the join reach half of
  *    `max_bytes_before_external_join`;
  *  - the memory that the query can still allocate. `realloc` charges the new buffer before it releases the old one,
  *    and one bucket per build slot can be in it at once, so the check also covers that.
  */
class JoinGrowthBudget final : public HashTableGrowthBudget
{
public:
    /// The budget keeps pointers to the counters and reads them during the build. `total_bytes` is the byte count of the join,
    /// and `stored_bytes` is the part of it held by the stored right blocks, which have `stored_rows` rows. `buckets` is the
    /// number of buckets of all the tables of the join, `num_slots` the number of build slots.
    ///
    /// `expected_rows` is the number of right rows that the hash table statistics of an earlier run give, 0 if unknown.
    /// Such a join sizes its hash tables at the first insert, when most of the right blocks are still to come. The spill check
    /// adds their bytes, which it can know from the blocks stored so far. The memory check does not add them: blocks that share
    /// their columns with their source are not allocated by the query.
    ///
    /// Call it before the first insert.
    void configure(
        const std::atomic<size_t> & total_bytes_,
        const std::atomic<size_t> & stored_bytes_,
        const std::atomic<size_t> & stored_rows_,
        size_t expected_rows_,
        size_t cell_size_,
        size_t buckets_,
        size_t num_slots_);

    /// The threshold of `SpillingHashJoin`, 0 if the join cannot spill. Call it before the first insert.
    void setSpillThreshold(size_t max_bytes_before_external_join_) { max_bytes_before_external_join = max_bytes_before_external_join_; }

    bool preferSmallerTable(size_t growth_cells, size_t bucket_cells) override;

    [[nodiscard]] bool pressureLatched() const override { return latched.load(std::memory_order_relaxed); }

    /// How many times a bucket took the smaller table, or was sized for it.
    [[nodiscard]] size_t switches() const { return switch_count.load(std::memory_order_relaxed); }

private:
    [[nodiscard]] size_t futureStoredBytes() const;

    const std::atomic<size_t> * total_bytes = nullptr;
    const std::atomic<size_t> * stored_bytes = nullptr;
    const std::atomic<size_t> * stored_rows = nullptr;
    size_t expected_rows = 0;
    size_t cell_size = 0;
    size_t buckets = 0;
    size_t num_slots = 0;
    size_t max_bytes_before_external_join = 0;

    std::atomic<bool> latched = false;
    std::atomic<size_t> switch_count = 0;
};

}
