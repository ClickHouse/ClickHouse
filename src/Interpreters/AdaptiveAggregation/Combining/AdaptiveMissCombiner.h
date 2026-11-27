#pragma once

#include <algorithm>

#include <base/defines.h>
#include <base/types.h>
#include <Common/ProfileEvents.h>
#include <Common/assert_cast.h>
#include <Interpreters/AdaptiveAggregation/AdaptiveAggregationImpl.h>
#include <Interpreters/AdaptiveAggregation/Combining/AdaptivePartitionTails.h>
#include <Interpreters/AdaptiveAggregation/Combining/AdaptiveRowSelection.h>

/// How the frozen kernels of the adaptive aggregation stage their misses (see
/// `Aggregator::executeFrozenImpl`). When the aggregation's records combine, a kernel stages them through
/// one of two combiners, as the gate of the tails decides (see `AdaptiveTailsGate`): one folds the misses of
/// a key into the tail of its partition, and the other only the repeats of the previous miss. A driver,
/// `combineRowMisses`, runs the kernel's probe loop through them. Otherwise a stager stages every miss as a
/// record of its own.

namespace ProfileEvents
{
    extern const Event AdaptiveAggregationCombinedRows;
    extern const Event AdaptiveAggregationTailClosures;
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

/// What the two combiners share (see `AdaptiveMissCombiner` and `AdaptiveRunCombiner`): the batch of misses,
/// the rules for the rows of a record of `kind`, the key of the previous miss, and the rows of the misses
/// that started records of their own, for the report of the rows that combined.
template <typename RecordKey, AdaptiveMissCombine kind, typename KeepKeyBytes>
class AdaptiveCombinerBase
{
public:
    /// Ends the combiner's pass, of `missed_rows` misses: reports the rows that combined into earlier
    /// records. Every missed row combines or starts a record, and counting the starts alone keeps the repeats
    /// free of the count. Inlined: a call that took the combiner's address would keep its fields out of the
    /// registers of the kernel's loop.
    void ALWAYS_INLINE finish(size_t missed_rows) const
    {
        ProfileEvents::increment(ProfileEvents::AdaptiveAggregationCombinedRows, missed_rows - started_rows);
    }

protected:
    /// `selection` is the aggregation's row selection, which a combiner of row selections requires and the
    /// other kinds ignore.
    AdaptiveCombinerBase(AdaptiveMissBatch & misses_, KeepKeyBytes keep_key_bytes_, const AdaptiveRowSelection * selection)
        : misses(misses_)
        , selection_counts_rows(selection && selection->countsRows())
        , keep_key_bytes(keep_key_bytes_)
    {
        chassert(kind != AdaptiveMissCombine::selection || selection);
    }

    /// Whether the records count their rows: every count record, and the records of a row selection with a
    /// `count`.
    [[nodiscard]] bool ALWAYS_INLINE countsRows() const
    {
        if constexpr (kind == AdaptiveMissCombine::selection)
            return selection_counts_rows;
        else
            return kind == AdaptiveMissCombine::counts;
    }

    /// Whether a row of rank `rank` is better than the record's row, of rank `kept_rank`: by a strictly
    /// larger rank, so the earlier row stays on a tie, as in `argMax`.
    [[nodiscard]] static bool ALWAYS_INLINE isBetterRow(Int64 rank, Int64 kept_rank) { return rank > kept_rank; }

    /// Adds `rows` rows to `multiplicity`, that of a pending record, and returns whether it did. The record
    /// of a row selection takes at most `staged_multiplicity_max` rows (see `stagedMultiplicityTakes`). A
    /// count takes only rows of the current block, whose rows are numbered in 32 bits, so it cannot pass the
    /// `UInt32` maximum, and the writer stores a count of any size (see `Aggregator::appendDelayedRecords`).
    [[nodiscard]] static bool ALWAYS_INLINE addToPendingRows(UInt32 & multiplicity, UInt32 rows)
    {
        if constexpr (kind == AdaptiveMissCombine::selection)
        {
            if (!stagedMultiplicityTakes(multiplicity, rows))
                return false;
        }
        multiplicity += rows;
        return true;
    }

    /// Whether the miss of `key` with `hash` has the key of the previous miss, which `rememberKey` kept. A
    /// fixed-width key determines its hash, so only a string-like key compares the hash first, as a cheap
    /// filter. The caller guarantees that it kept a previous miss.
    [[nodiscard]] bool ALWAYS_INLINE hasPreviousKey([[maybe_unused]] UInt64 hash, const RecordKey & key) const
    {
        if constexpr (adaptive_key_stages_bytes<RecordKey>)
        {
            if (hash != previous_hash)
                return false;
        }
        return key == previous_key;
    }

    void ALWAYS_INLINE rememberKey([[maybe_unused]] UInt64 hash, const RecordKey & key)
    {
        previous_key = key;
        if constexpr (adaptive_key_stages_bytes<RecordKey>)
            previous_hash = hash;
    }

    AdaptiveMissBatch & misses;
    const bool selection_counts_rows;
    const KeepKeyBytes keep_key_bytes;
    /// The rows of the misses that started records of their own.
    size_t started_rows = 0;

private:
    /// The key of the previous miss, and the hash of a string-like key.
    RecordKey previous_key{};
    UInt64 previous_hash = 0;
};

/// Stages the misses of a frozen kernel whose records combine, while the tails are open. A miss combines into
/// the tail of its partition when the tail is a record of its key. Otherwise it becomes a descriptor of the
/// batch, which turns into the partition's tail. A miss of a constant-key block stands for rows of the block.
/// The rules:
///
/// - A key-only miss adds nothing to the tail.
/// - A count miss adds its rows to the tail's count: the descriptor's multiplicity while the tail is pending,
///   the record's count once it is written, while that count keeps its stored width (see
///   `AdaptivePartitionTails::addToCount`).
/// - A row-selection miss whose row is not better adds its rows to the tail's multiplicity when the selection
///   counts rows. A row is better by a strictly larger rank, so the earlier row stays on a tie, as in
///   `argMax`. A better row takes a pending tail over by moving its descriptor's source row. A written record
///   cannot take another row, so a better row rolls the record back, which is its partition's last, and
///   stages a descriptor of its own that carries the record's multiplicity.
/// - The record of a row selection that counts rows never stands for more than `staged_multiplicity_max`
///   rows, and the count of a written count record never passes what its stored width holds: a miss that
///   would take either past stages a record of its own, which becomes the tail.
///
/// For example, under `argMax(x, t), count()` the misses A(t=5), A(t=9) of a block stage one descriptor, of
/// A(t=9) with multiplicity 2. A miss A(t=4) of the next block makes the written record count 3, and a miss
/// A(t=12) then replaces the record with a descriptor of A(t=12) with multiplicity 4.
///
/// The driver of the kernel owns the protocol with the gate (see `combineRowMisses`): it takes the lookups
/// of its chunks of rows from the combiner (see `takeChunkLookups`), and calls `finish` once, at the end of
/// the combiner's pass.
template <typename RecordKey, AdaptiveMissCombine kind, typename KeepKeyBytes>
class AdaptiveMissCombiner : public AdaptiveCombinerBase<RecordKey, kind, KeepKeyBytes>
{
    using Base = AdaptiveCombinerBase<RecordKey, kind, KeepKeyBytes>;
    using Base::countsRows;
    using Base::isBetterRow;
    using Base::addToPendingRows;
    using Base::hasPreviousKey;
    using Base::rememberKey;
    using Base::misses;
    using Base::keep_key_bytes;
    using Base::started_rows;

public:
    /// `selection` is the aggregation's row selection, which a combiner of row selections requires and the
    /// other kinds ignore. Such a combiner reads the rank of every row that it takes from the producer's
    /// ranks of the current block, so the caller ranks a row before it adds its miss (see
    /// `AdaptiveRowSelectionRanks`). The caller guarantees that the tails are open.
    AdaptiveMissCombiner(
        AdaptiveAggregationProducer & adaptive_,
        AdaptivePartitionTails<RecordKey> & tails_,
        KeepKeyBytes keep_key_bytes_,
        const AdaptiveRowSelection * selection = nullptr)
        : Base(adaptive_.misses, keep_key_bytes_, selection)
        , adaptive(adaptive_)
        , tails(tails_)
        , layout(adaptive_.session->layout)
        , ranks(selection ? adaptive_.row_selection_ranks.data() : nullptr)
    {
        chassert(tails.gate.isOpen());
    }

    /// Stages the miss of `key` with `hash`, whose record takes the arguments of `row`. The miss stands for
    /// `rows` rows: one, or rows of a constant-key block. The caller guarantees that a miss of a row
    /// selection that counts rows stands for at most `staged_multiplicity_max` rows.
    ///
    /// Whatever the previous miss did, the tail of its partition is a record of its key afterwards, so a miss
    /// of the same key goes into that tail without a lookup. On data sorted by the key most repeats are
    /// consecutive, and comparing with the previous miss costs less than the lookup. The repeat of a key-only
    /// miss adds nothing, and that of a count miss into a pending tail adds its rows through a pointer to the
    /// descriptor's multiplicity, so either keeps the loop as small as a comparison with the previous key. A
    /// kernel whose key views do not outlive their row keeps no previous key, as it keeps no pending keys.
    void ALWAYS_INLINE add(UInt32 row, UInt64 hash, const RecordKey & key, UInt32 rows = 1)
    {
        chassert(!(kind == AdaptiveMissCombine::selection && countsRows()) || rows <= staged_multiplicity_max);
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
            ++tail_hits;
            pending = tails.isPending(partition);
            stage(*tail, partition, pending, row, hash, key, rows);
        }
        else
            start(partition, row, hash, key, rows);
        if (!keep_key_bytes)
            remember(partition, pending, hash, key);
    }

    /// What the lookups since the last call did, for the gate (see `AdaptiveTailsGate::addLookups`). A miss of
    /// one row that does not repeat the previous miss looks its partition's tail up once: it finds a record of
    /// its key, a hit, or starts a record of one row. So the hits and the started rows add up to the lookups,
    /// and the kernel's loop needs no counter of lookups, which would cost it a register. A full record that
    /// refuses rows adds a started record without a lookup of its own, but a record fills only after it took
    /// more than a hundred rows. A constant-key block would count all its rows as lookups, so its probe
    /// reports none (see `ConstantKeyMissProbe`). Nothing here calls out of line: a call between two chunks
    /// would keep whatever lives across it out of the registers of the kernel's loop.
    [[nodiscard]] AdaptiveTailLookups ALWAYS_INLINE takeChunkLookups()
    {
        const size_t lookups = tail_hits + started_rows;
        const AdaptiveTailLookups chunk{.lookups = lookups - reported.lookups, .hits = tail_hits - reported.hits};
        reported = {.lookups = lookups, .hits = tail_hits};
        return chunk;
    }

private:
    using Tails = AdaptivePartitionTails<RecordKey>;
    using Tail = typename Tails::Tail;

    /// Whether the miss of `key` with `hash` repeats the previous miss, which `remember` kept.
    [[nodiscard]] bool ALWAYS_INLINE repeatsPrevious(UInt64 hash, const RecordKey & key) const
    {
        if constexpr (kind == AdaptiveMissCombine::counts)
        {
            if (!previous_count)
                return false;
        }
        else if (!previous_tail)
            return false;
        return hasPreviousKey(hash, key);
    }

    /// Keeps the miss of `key` with `hash`, whose tail is that of `partition`, as the previous miss. A count
    /// kernel keeps only a pending tail's multiplicity: the batch adds no descriptor until the next miss that
    /// is not a repeat, so the pointer stays valid, and a written tail's repeats go through the lookup.
    void ALWAYS_INLINE remember(size_t partition, bool pending, UInt64 hash, const RecordKey & key)
    {
        rememberKey(hash, key);
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
            if (!isBetterRow(ranks[row], tail.rank))
                return !countsRows() || addRows(tail, pending, hash, rows);
            if (pending)
            {
                if (countsRows() && !addRows(tail, pending, hash, rows))
                    return false;
                misses.setSourceRow(tail.descriptor, row);
                tail.rank = ranks[row];
                return true;
            }
            UInt32 multiplicity = 0;
            if (countsRows())
            {
                multiplicity = readStagedMultiplicity(tail.counter);
                if (!stagedMultiplicityTakes(multiplicity, rows))
                    return false;
            }
            adaptive.partitions->rollback(partition, tail.record_bytes);
            misses.addReplaced({.key_bytes = tail.key_bytes, .record_bytes = tail.record_bytes});
            /// The rows of the rolled-back record leave the count bins with it, and the descriptor that
            /// replaces it brings them back with its multiplicity when its batch is accounted.
            if (adaptive.count_bins.isAllocated())
                adaptive.count_bins.remove(hash, multiplicity);
            addDescriptor(partition, row, hash, key, multiplicity + rows);
            pending = true;
            return true;
        }
    }

    /// Adds `rows` rows to the count or the multiplicity of `tail`, which is pending when `pending`, unless
    /// the record of a row selection takes no more rows (see `stagedMultiplicityTakes`) or they would take
    /// the count of a written count record past its stored width.
    bool ALWAYS_INLINE addRows(Tail & tail, bool pending, UInt64 hash, UInt32 rows)
    {
        if (pending)
            return addToPendingRows(misses.multiplicityOf(tail.descriptor), rows);
        if constexpr (kind == AdaptiveMissCombine::counts)
        {
            if (!tails.addToCount(tail, rows))
                return false;
        }
        else
        {
            if (!addToStagedMultiplicity(tail.counter, rows))
                return false;
        }
        /// The rows of a descriptor reach the count bins with its multiplicity when its batch is accounted,
        /// and the rows that grow a written record reach them here.
        if (adaptive.count_bins.isAllocated())
            adaptive.count_bins.add(hash, rows);
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
            misses.template addCount<RecordKey>(row, hash, key, rows, keep_key_bytes);
        else
            misses.template add<RecordKey>(row, hash, key, keep_key_bytes);
        tails.setPending(partition, descriptor, hash, key, kind == AdaptiveMissCombine::selection ? ranks[row] : 0);
    }

    AdaptiveAggregationProducer & adaptive;
    Tails & tails;
    const AdaptivePartitionLayout layout;
    const Int64 * const ranks;
    /// The lookups of the tails that found their key, and what `takeChunkLookups` reported.
    size_t tail_hits = 0;
    AdaptiveTailLookups reported;

    /// The tail of the previous miss, which `remember` keeps for a kernel whose key views outlive their row:
    /// a count kernel keeps the multiplicity of the tail while the tail is pending, the other kernels the
    /// tail itself, with its partition and whether it is pending. No batch is staged between two misses of
    /// such a kernel, so the state stays as the previous miss left it. Null pointers mean that no previous
    /// miss is kept.
    UInt32 * previous_count = nullptr;
    Tail * previous_tail = nullptr;
    size_t previous_partition = 0;
    bool previous_pending = false;
};

/// Stages the misses of a frozen kernel whose records combine while the tails are closed (see
/// `AdaptiveTailsGate`). Every miss is a record of its own, except the repeats of the previous miss, which
/// combine into its record by the rules of `AdaptiveMissCombiner` for a pending tail. That record is the
/// last descriptor of the batch: a kernel that keeps the previous miss stages no batch between two misses.
/// A repeat that the record cannot take stages a record of its own, which the next repeats then combine
/// into.
///
/// The combiner keeps only the previous key, so its loop is as small as that of an aggregation whose records
/// do not combine. A kernel whose key views do not outlive their row keeps no previous key. The batch of a
/// row selection that counts rows carries no multiplicities until a record takes a second row, so that a
/// block without repeats stages as the block of an aggregation whose records do not combine; that of
/// `AdaptiveMissCombiner` carries them from its first record, because open tails are there to combine.
template <typename RecordKey, AdaptiveMissCombine kind, typename KeepKeyBytes>
class AdaptiveRunCombiner : public AdaptiveCombinerBase<RecordKey, kind, KeepKeyBytes>
{
    using Base = AdaptiveCombinerBase<RecordKey, kind, KeepKeyBytes>;
    using Base::countsRows;
    using Base::isBetterRow;
    using Base::addToPendingRows;
    using Base::hasPreviousKey;
    using Base::rememberKey;
    using Base::misses;
    using Base::keep_key_bytes;
    using Base::started_rows;

public:
    /// `selection` is the aggregation's row selection, which a combiner of row selections requires and the
    /// other kinds ignore. Such a combiner ranks the block's rows itself, when its first repeat compares two
    /// rows (see `AdaptiveRowSelectionRanks`).
    AdaptiveRunCombiner(
        AdaptiveAggregationProducer & adaptive_, KeepKeyBytes keep_key_bytes_, const AdaptiveRowSelection * selection = nullptr)
        : Base(adaptive_.misses, keep_key_bytes_, selection)
        , lazy_ranks(adaptive_.row_selection_ranks)
        , with_multiplicities(kind == AdaptiveMissCombine::counts || adaptive_.misses.hasMultiplicities())
    {
    }

    /// Stages the miss of `key` with `hash`, as `AdaptiveMissCombiner::add` does, with the same guarantee of
    /// the caller about `rows`.
    void ALWAYS_INLINE add(UInt32 row, UInt64 hash, const RecordKey & key, UInt32 rows = 1)
    {
        chassert(!(kind == AdaptiveMissCombine::selection && countsRows()) || rows <= staged_multiplicity_max);
        if (has_previous && hasPreviousKey(hash, key))
        {
            /// A repeat that the last record cannot take starts a record of the same key here, and not on the
            /// path of the other misses below: what that path keeps in registers would otherwise have to
            /// outlive the calls of this branch.
            if (!combineIntoLast(row, rows))
            {
                misses.template addCount<RecordKey>(row, hash, key, rows, keep_key_bytes);
                started_rows += rows;
            }
            return;
        }
        if (countsRows() && rows != 1 && !with_multiplicities)
            materializeMultiplicities();
        if (with_multiplicities)
            misses.template addCount<RecordKey>(row, hash, key, rows, keep_key_bytes);
        else
            misses.template add<RecordKey>(row, hash, key, keep_key_bytes);
        started_rows += rows;
        if (!keep_key_bytes)
        {
            rememberKey(hash, key);
            has_previous = true;
        }
    }

private:
    /// Combines the miss, a repeat of the previous one, into the last record of the batch, and returns
    /// whether it did. Only the record of a row selection refuses rows (see `addToPendingRows`), and the
    /// batch then carries multiplicities, so the caller stages the miss with `AdaptiveMissBatch::addCount`.
    [[nodiscard]] bool ALWAYS_INLINE combineIntoLast(UInt32 row, UInt32 rows)
    {
        const size_t record = misses.size() - 1;
        if (countsRows())
        {
            if (!with_multiplicities)
                materializeMultiplicities();
            if (!addToPendingRows(misses.multiplicityOf(record), rows))
                return false;
        }
        if constexpr (kind == AdaptiveMissCombine::selection)
        {
            if (!ranks)
            {
                lazy_ranks.rankAll();
                ranks = lazy_ranks.data();
            }
            if (isBetterRow(ranks[row], ranks[misses.getSourceRows()[record]]))
                misses.setSourceRow(record, row);
        }
        return true;
    }

    /// Inlined, like every member: a call that took the combiner's address would keep its fields out of the
    /// registers of the kernel's loop.
    void ALWAYS_INLINE materializeMultiplicities()
    {
        misses.materializeMultiplicities();
        with_multiplicities = true;
    }

    /// The ranks of the block's rows under the row selection, which the first repeat that compares two rows
    /// fills and takes.
    AdaptiveRowSelectionRanks & lazy_ranks;
    const Int64 * ranks = nullptr;
    /// Whether the records of the batch carry multiplicities: those of a count always, and those of a row
    /// selection that counts rows once one of them stands for several rows.
    bool with_multiplicities;
    /// Whether the combiner keeps a previous miss (see `AdaptiveCombinerBase::rememberKey`).
    bool has_previous = false;
};

/// The tails of the producer's partitions, which the first frozen block of a phase creates.
template <typename RecordKey>
AdaptivePartitionTails<RecordKey> & adaptivePartitionTailsOf(AdaptiveAggregationProducer & adaptive)
{
    if (!adaptive.tails)
        adaptive.tails = std::make_unique<AdaptivePartitionTails<RecordKey>>(adaptive.session->layout);
    return assert_cast<AdaptivePartitionTails<RecordKey> &>(*adaptive.tails);
}

/// Empties the tails that their gate has closed: the records staged past closed tails are no tails.
template <typename RecordKey>
void ALWAYS_INLINE emptyClosedTails(AdaptivePartitionTails<RecordKey> & tails)
{
    chassert(!tails.gate.isOpen());
    tails.clear();
    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationTailClosures);
}

/// Stages the misses of the rows from `row_begin` to `row_end` of a kernel whose records combine. `probe` is
/// the kernel's probe loop: `probe.probeRows(stager, begin, end)` probes the rows from `begin` to `end`,
/// stages their misses through `stager`, and returns the hits and the rows of the NULL key. While the tails
/// are open, the loop runs through an `AdaptiveMissCombiner`, up to `Probe::chunk_rows` rows at a time, and
/// the gate of the tails takes the lookups of each chunk (see `AdaptiveTailsGate`), unless the probe reports
/// none (`Probe::reports_lookups`). The rest of the block, or a whole block that starts with the tails
/// closed, runs through an `AdaptiveRunCombiner`, in a loop of its own, so that it carries none of the state
/// of the tails, and its misses count toward the gate's next try.
template <typename RecordKey, AdaptiveMissCombine kind, typename KeepKeyBytes, typename Probe>
AdaptiveLocalProbes ALWAYS_INLINE combineRowMisses(
    AdaptiveAggregationProducer & adaptive,
    KeepKeyBytes keep_key_bytes,
    const AdaptiveRowSelection * selection,
    size_t row_begin,
    size_t row_end,
    const Probe & probe)
{
    auto & tails = adaptivePartitionTailsOf<RecordKey>(adaptive);
    AdaptiveLocalProbes probes;
    size_t row = row_begin;
    if (tails.gate.startBlock())
    {
        AdaptiveMissCombiner<RecordKey, kind, KeepKeyBytes> combiner(adaptive, tails, keep_key_bytes, selection);
        bool open = true;
        while (open && row != row_end)
        {
            const size_t chunk_end = row + std::min(row_end - row, Probe::chunk_rows);
            /// The combiner reads the rank of every row that it takes, so the chunk's rows are ranked first.
            if constexpr (kind == AdaptiveMissCombine::selection)
                adaptive.row_selection_ranks.rankUpTo(chunk_end);
            const AdaptiveLocalProbes chunk = probe.probeRows(combiner, row, chunk_end);
            probes.hits += chunk.hits;
            probes.null_rows += chunk.null_rows;
            row = chunk_end;
            if constexpr (Probe::reports_lookups)
                open = tails.gate.addLookups(combiner.takeChunkLookups());
        }
        combiner.finish(row - row_begin - probes.hits - probes.null_rows);
        if (!open)
            emptyClosedTails(tails);
    }
    if (row != row_end)
    {
        AdaptiveRunCombiner<RecordKey, kind, KeepKeyBytes> combiner(adaptive, keep_key_bytes, selection);
        const AdaptiveLocalProbes rest = probe.probeRows(combiner, row, row_end);
        const size_t missed_rows = row_end - row - rest.hits - rest.null_rows;
        combiner.finish(missed_rows);
        tails.gate.addClosedMisses(missed_rows);
        probes.hits += rest.hits;
        probes.null_rows += rest.null_rows;
    }
    return probes;
}

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
