#pragma once

#include <Common/DequeWithMemoryTracking.h>
#include <Common/HashTable/HashTableGrowthBudget.h>

#include <array>
#include <atomic>

namespace DB
{

/// Before it adds a block, `SpillingHashJoin` switches to `GraceHashJoin` once the bytes of the join reach
/// `1 / SPILL_THRESHOLD_DIVISOR` of `max_bytes_before_external_join`, so that a hash table buffer, which holds 3X while it
/// grows from X to 2X, still fits. After the last block it switches only at the full threshold.
inline constexpr size_t SPILL_THRESHOLD_DIVISOR = 2;

[[nodiscard]] inline bool reachesSpillThreshold(size_t join_bytes, size_t max_bytes_before_external_join)
{
    return join_bytes * SPILL_THRESHOLD_DIVISOR >= max_bytes_before_external_join;
}

/** Tells the buckets of the hash tables of a `HashJoin` build to stay smaller, in the Robin Hood mode,
  * when growing would take the join over the spill threshold of `SpillingHashJoin` or over the memory limit of the query.
  *
  * The buckets of a hash table fill at about the same rate, so they overflow at about the same time. A bucket therefore
  * judges the growth of all the buckets that have not grown from its size yet, and a yes is kept: every later bucket of the
  * build also takes the smaller table. The buckets that already grew are not counted again: their growth joins the byte
  * counts when their insert ends. The hash tables of a join with several disjuncts grow at their own rates, so a hash table
  * counts only once one of its buckets has reached that size: a hash table that grows with this one has reached it when
  * half full, and one whose keys stay few never does.
  *
  * The step is judged against two limits:
  *  - the spill threshold, see `SPILL_THRESHOLD_DIVISOR`;
  *  - the memory that the query can still allocate. `realloc` charges the new buffer before it releases the old one,
  *    and one bucket per build slot can be in it at once, so the check also covers that.
  */
class JoinGrowthBudget
{
public:
    /// The budget of the buckets of one hash table of the join.
    class Table final : public HashTableGrowthBudget
    {
    public:
        explicit Table(JoinGrowthBudget & join_) : join(join_) {}

        bool preferSmallerTable(size_t growth_cells, size_t bucket_cells) override;

    private:
        friend class JoinGrowthBudget;

        JoinGrowthBudget & join;
        /// log2 of the cells of the largest bucket that asked or was told to grow.
        std::atomic<size_t> largest_size_degree = 0;
        /// How many buckets were told to grow from each size, indexed by log2 of their cells.
        std::array<std::atomic<size_t>, 64> grown_from_size{};
    };

    /// The budget of the next hash table of the join. Call it before the first insert.
    Table & addTable() { return tables.emplace_back(*this); }

    /// The budget keeps pointers to the counters and reads them during the build. `total_bytes` is the byte count of the join,
    /// and `stored_bytes` is the part of it held by the stored right blocks, which have `stored_rows` rows. `buckets` is the
    /// number of buckets of each hash table, `cell_size` the size of their cells, `num_slots` the number of build slots.
    ///
    /// `expected_rows` is the number of right rows that the hash table statistics of an earlier run give, 0 if unknown.
    /// Such a join sizes its hash tables at the first insert, when most of the right blocks are still to come. The spill check
    /// adds their bytes, which it estimates from the bytes per row of the blocks stored so far. The memory check does not add them: blocks that share
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

    /// Answer yes from now on, as after the first yes of `Table::preferSmallerTable`.
    void latch() { latched.store(true, std::memory_order_relaxed); }

private:
    [[nodiscard]] size_t futureStoredBytes() const;
    /// The buckets that are about to grow from a size with log2 `size_degree`.
    [[nodiscard]] size_t bucketsToGrowFrom(size_t size_degree) const;
    /// Whether a growth that adds `step_bytes`, with `transient_bytes` of old buffers still charged, crosses a limit. Latches on yes.
    bool crossesLimits(size_t step_bytes, size_t transient_bytes);

    const std::atomic<size_t> * total_bytes = nullptr;
    const std::atomic<size_t> * stored_bytes = nullptr;
    const std::atomic<size_t> * stored_rows = nullptr;
    size_t expected_rows = 0;
    size_t cell_size = 0;
    size_t buckets = 0;
    size_t num_slots = 0;
    size_t max_bytes_before_external_join = 0;

    std::atomic<bool> latched = false;
    DequeWithMemoryTracking<Table> tables;
};

}
