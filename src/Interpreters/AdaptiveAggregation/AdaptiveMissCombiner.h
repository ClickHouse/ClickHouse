#pragma once

#include <limits>

#include <base/defines.h>
#include <base/types.h>
#include <base/unaligned.h>
#include <Common/ProfileEvents.h>
#include <Common/assert_cast.h>
#include <Interpreters/AdaptiveAggregation/AdaptiveAggregationImpl.h>
#include <Interpreters/AdaptiveAggregation/AdaptivePartitionTails.h>
#include <Interpreters/AdaptiveAggregation/AdaptiveRowSelection.h>

/// How the frozen kernels of the adaptive aggregation stage their misses (see
/// `Aggregator::executeFrozenImpl`): through a combiner, which folds the misses of a key into the tail of its
/// partition when the aggregation's records allow it, or through a stager, which stages every miss as a
/// record of its own.

namespace ProfileEvents
{
    extern const Event AdaptiveAggregationCombinedRows;
}

namespace DB
{

/// How the misses of a frozen kernel combine into the tails of their partitions (see
/// `AdaptivePartitionTails`), by the records that the aggregation stages.
enum class AdaptiveMissCombine
{
    /// Key-only records: a miss of the tail's key adds nothing.
    keys,
    /// Count records: a miss adds its rows to the tail's count.
    counts,
    /// The records of a row selection (see `AdaptiveRowSelection`): the record keeps the best row and, when
    /// the selection counts rows, the number of rows.
    selection,
};

/// Stages the misses of a frozen kernel whose records combine. A miss combines into the tail of its partition
/// when the tail is a record of its key. Otherwise it becomes a descriptor of the batch, which turns into the
/// partition's tail. A miss of a constant-key block stands for all the block's rows. The rules:
///
/// - A key-only miss adds nothing to the tail.
/// - A count miss adds its rows to the tail's count: the descriptor's multiplicity while the tail is pending,
///   the record's count once it is written.
/// - A row-selection miss whose row is not better adds its rows to the tail's multiplicity when the selection
///   counts rows. A row is better by a strictly larger rank, so the earlier row stays on a tie, as in
///   `argMax`. A better row takes a pending tail over by moving its descriptor's source row. A written record
///   cannot take another row, so a better row rolls the record back, which is its partition's last, and
///   stages a descriptor of its own that carries the record's multiplicity.
/// - A count or multiplicity never passes the `UInt32` maximum: a miss that would take it past stages a
///   record of its own.
///
/// For example, under `argMax(x, t), count()` the misses A(t=5), A(t=9) of a block stage one descriptor, of
/// A(t=9) with multiplicity 2. A miss A(t=4) of the next block makes the written record count 3, and a miss
/// A(t=12) then replaces the record with a descriptor of A(t=12) with multiplicity 4.
///
/// The kernel calls `finish` once, at the end of its block, with the rows of the block that missed the frozen
/// table.
template <typename RecordKey, AdaptiveMissCombine kind, typename KeepKeyBytes>
class AdaptiveMissCombiner
{
public:
    /// `selection` is the aggregation's row selection, which a combiner of row selections requires and the
    /// other kinds ignore. The ranks of the rows are the producer's ranks of the current block (see
    /// `fillRowSelectionRanks`).
    AdaptiveMissCombiner(
        AdaptiveAggregationProducer & adaptive_, KeepKeyBytes keep_key_bytes_, const AdaptiveRowSelection * selection = nullptr)
        : adaptive(adaptive_)
        , misses(adaptive_.misses)
        , tails(tailsOf(adaptive_))
        , layout(adaptive_.session->layout)
        , ranks(adaptive_.row_selection_ranks.data())
        , selection_counts_rows(selection && selection->countsRows())
        , keep_key_bytes(keep_key_bytes_)
    {
        chassert(kind != AdaptiveMissCombine::selection || selection);
    }

    /// Stages the miss of `key` with `hash`, whose record takes the arguments of `row`. The miss stands for `rows`
    /// rows: one, or all the rows of a constant-key block.
    ///
    /// Whatever the previous miss did, the tail of its partition is a record of its key afterwards, so a miss
    /// of the same key goes into that tail without a lookup. On data sorted by the key most repeats are
    /// consecutive, and comparing with the previous miss costs less than the lookup. The repeat of a key-only
    /// miss adds nothing, and that of a count miss into a pending tail adds its rows through a pointer to the
    /// descriptor's multiplicity, so either keeps the loop as small as a comparison with the previous key. A
    /// kernel whose key views do not outlive their row keeps no previous key, as it keeps no pending keys.
    void ALWAYS_INLINE add(UInt32 row, UInt64 hash, const RecordKey & key, UInt32 rows = 1)
    {
        if (repeatsPrevious(hash, key))
        {
            if constexpr (kind == AdaptiveMissCombine::counts)
                *previous_count += rows;
            else if constexpr (kind == AdaptiveMissCombine::selection)
                stage(*previous_tail, previous_partition, previous_pending, row, hash, key, rows);
            return;
        }
        const size_t partition = layout.partitionOf(hash);
        bool pending = true;
        if (Tail * tail = tails.find(partition, hash, key, !keep_key_bytes))
        {
            pending = tails.isPending(partition);
            stage(*tail, partition, pending, row, hash, key, rows);
        }
        else
            start(partition, row, hash, key, rows);
        if (!keep_key_bytes)
            remember(partition, pending, hash, key);
    }

    /// Reports the rows that combined into earlier records, of the `missed_rows` rows of the block that
    /// missed the frozen table: every missed row combines or starts a record. Counting the starts alone keeps
    /// the repeats free of the count.
    void finish(size_t missed_rows) const
    {
        ProfileEvents::increment(ProfileEvents::AdaptiveAggregationCombinedRows, missed_rows - started_rows);
    }

private:
    using Tails = AdaptivePartitionTails<RecordKey>;
    using Tail = typename Tails::Tail;

    static constexpr UInt32 max_rows = std::numeric_limits<UInt32>::max();

    static Tails & tailsOf(AdaptiveAggregationProducer & adaptive)
    {
        if (!adaptive.tails)
            adaptive.tails = std::make_unique<Tails>(adaptive.session->layout.numPartitions());
        return assert_cast<Tails &>(*adaptive.tails);
    }

    /// Whether the miss of `key` with `hash` repeats the previous miss. A fixed-width key determines its
    /// hash, so only a string-like key compares the hash first, as a cheap filter.
    bool ALWAYS_INLINE repeatsPrevious([[maybe_unused]] UInt64 hash, const RecordKey & key) const
    {
        if constexpr (kind == AdaptiveMissCombine::counts)
        {
            if (!previous_count)
                return false;
        }
        else if (!previous_tail)
            return false;
        if constexpr (adaptive_key_stages_bytes<RecordKey>)
        {
            if (hash != previous_hash)
                return false;
        }
        return key == previous_key;
    }

    /// Keeps the miss of `key` with `hash`, whose tail is that of `partition`, as the previous miss. A count
    /// kernel keeps only a pending tail's multiplicity: the batch adds no descriptor until the next miss that
    /// is not a repeat, so the pointer stays valid, and a written tail's repeats go through the lookup.
    void ALWAYS_INLINE remember(size_t partition, bool pending, [[maybe_unused]] UInt64 hash, const RecordKey & key)
    {
        previous_key = key;
        if constexpr (adaptive_key_stages_bytes<RecordKey>)
            previous_hash = hash;
        if constexpr (kind == AdaptiveMissCombine::counts)
            previous_count = pending ? &misses.multiplicityOf(tails.at(partition).descriptor) : nullptr;
        else
        {
            previous_tail = &tails.at(partition);
            previous_partition = partition;
            previous_pending = pending;
        }
    }

    /// Combines the miss into `tail`, the tail of `partition` and a record of its key, or starts a record of
    /// its own. `pending` tells whether the tail is pending, and becomes whether the partition's tail is
    /// pending afterwards.
    void ALWAYS_INLINE stage(
        Tail & tail, size_t partition, bool & pending, UInt32 row, UInt64 hash, const RecordKey & key, UInt32 rows)
    {
        if (!combine(tail, partition, pending, row, hash, key, rows))
        {
            start(partition, row, hash, key, rows);
            pending = true;
        }
    }

    /// Whether the records count their rows: every count record, and the records of a row selection with a
    /// `count`.
    bool ALWAYS_INLINE countsRows() const
    {
        if constexpr (kind == AdaptiveMissCombine::selection)
            return selection_counts_rows;
        else
            return kind == AdaptiveMissCombine::counts;
    }

    /// Combines the miss into `tail`, a record of its key that is pending when `pending`, and returns whether
    /// it did. A replacement of a written record leaves a pending tail, which `pending` then tells.
    bool ALWAYS_INLINE combine(
        Tail & tail, size_t partition, bool & pending, UInt32 row, UInt64 hash, const RecordKey & key, UInt32 rows)
    {
        if constexpr (kind == AdaptiveMissCombine::keys)
            return true;
        else if constexpr (kind == AdaptiveMissCombine::counts)
            return addRows(tail, pending, hash, rows);
        else
        {
            if (ranks[row] <= tail.rank)
                return !countsRows() || addRows(tail, pending, hash, rows);
            if (pending)
            {
                if (countsRows() && !addRows(tail, pending, hash, rows))
                    return false;
                misses.setSourceRow(tail.descriptor, row);
                tail.rank = ranks[row];
                return true;
            }
            const UInt32 multiplicity = countsRows() ? unalignedLoad<UInt32>(tail.counter) : 0;
            if (multiplicity > max_rows - rows)
                return false;
            adaptive.partitions->rollback(partition, tail.bytes);
            misses.addReplaced({.key_bytes = keyBytes(key), .record_bytes = tail.bytes});
            addDescriptor(partition, row, hash, key, multiplicity + rows);
            pending = true;
            return true;
        }
    }

    /// Adds `rows` rows to the count or the multiplicity of `tail`, which is pending when `pending`, unless
    /// they would take it past the maximum.
    bool ALWAYS_INLINE addRows(Tail & tail, bool pending, [[maybe_unused]] UInt64 hash, UInt32 rows)
    {
        if (pending)
        {
            /// A pending count record takes only rows of the current block, whose rows are numbered in 32
            /// bits, so its count cannot pass the maximum. The record of a row selection may carry the rows
            /// of a written record it replaced.
            UInt32 & multiplicity = misses.multiplicityOf(tail.descriptor);
            if constexpr (kind == AdaptiveMissCombine::selection)
            {
                if (multiplicity > max_rows - rows)
                    return false;
            }
            multiplicity += rows;
            return true;
        }
        const UInt32 multiplicity = unalignedLoad<UInt32>(tail.counter);
        if (multiplicity > max_rows - rows)
            return false;
        unalignedStore<UInt32>(tail.counter, multiplicity + rows);
        /// The rows of a descriptor reach the count bins with its multiplicity when its batch is accounted,
        /// and the rows combined into a written record reach them here.
        if constexpr (kind == AdaptiveMissCombine::counts)
        {
            if (adaptive.count_bins.isAllocated())
                adaptive.count_bins.add(hash, rows);
        }
        return true;
    }

    /// Stages the miss as a record of its own.
    void ALWAYS_INLINE start(size_t partition, UInt32 row, UInt64 hash, const RecordKey & key, UInt32 rows)
    {
        addDescriptor(partition, row, hash, key, rows);
        started_rows += rows;
    }

    void ALWAYS_INLINE addDescriptor(size_t partition, UInt32 row, UInt64 hash, const RecordKey & key, UInt32 rows)
    {
        const auto descriptor = static_cast<UInt32>(misses.size());
        if (countsRows())
            misses.addCount<RecordKey>(row, hash, key, rows, keep_key_bytes);
        else
            misses.add<RecordKey>(row, hash, key, keep_key_bytes);
        tails.setPending(partition, descriptor, hash, key, kind == AdaptiveMissCombine::selection ? ranks[row] : 0);
    }

    /// The key bytes that the writer counts for a record of `key` (see `AdaptiveStagedBytes`).
    static size_t keyBytes(const RecordKey & key)
    {
        if constexpr (adaptive_key_stages_bytes<RecordKey>)
            return static_cast<std::string_view>(key).size();
        else
            return sizeof(RecordKey);
    }

    AdaptiveAggregationProducer & adaptive;
    AdaptiveMissBatch & misses;
    Tails & tails;
    const AdaptivePartitionLayout layout;
    const Int64 * const ranks;
    const bool selection_counts_rows;
    const KeepKeyBytes keep_key_bytes;
    /// The rows of the misses that started records of their own.
    size_t started_rows = 0;

    /// The previous miss, which `remember` keeps for a kernel whose key views outlive their row: its key, and
    /// the hash of a string-like key. A count kernel keeps the multiplicity of its tail while the tail is
    /// pending, the other kernels the tail itself, with its partition and whether it is pending. No batch is
    /// staged between two misses of such a kernel, so the state stays as the previous miss left it. Null
    /// pointers mean that no previous miss is kept.
    RecordKey previous_key{};
    UInt64 previous_hash = 0;
    UInt32 * previous_count = nullptr;
    Tail * previous_tail = nullptr;
    size_t previous_partition = 0;
    bool previous_pending = false;
};

/// Stages every miss as a record of its own, for an aggregation whose records carry the arguments of their
/// rows.
template <typename RecordKey, typename KeepKeyBytes>
struct AdaptiveMissStager
{
    AdaptiveMissBatch & misses;
    const KeepKeyBytes keep_key_bytes;

    void ALWAYS_INLINE add(UInt32 row, UInt64 hash, const RecordKey & key)
    {
        misses.add<RecordKey>(row, hash, key, keep_key_bytes);
    }
};

}
