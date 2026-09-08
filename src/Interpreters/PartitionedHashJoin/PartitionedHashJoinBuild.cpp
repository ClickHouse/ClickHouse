#include <Columns/ColumnsScatter.h>
#include <Interpreters/HashJoin/HashJoinMethodsImpl.h>
#include <Interpreters/HashJoin/JoinUsedFlags.h>
#include <Interpreters/HashJoin/KeyGetter.h>
#include <Interpreters/PartitionedHashJoin/AmacRing.h>
#include <Interpreters/PartitionedHashJoin/PartitionedHashJoin.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/joinDispatch.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadPool.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>

#include <algorithm>
#include <cmath>
#include <deque>

namespace ProfileEvents
{
extern const Event PartitionedHashJoinBuildMicroseconds;
extern const Event PartitionedHashJoinBuildHistogramMicroseconds;
extern const Event PartitionedHashJoinBuildScatterMicroseconds;
extern const Event PartitionedHashJoinBuildLeafMicroseconds;
extern const Event PartitionedHashJoinLeafRows;
extern const Event PartitionedHashJoinHashTableBytes;
extern const Event PartitionedHashJoinOverflowRows;
extern const Event PartitionedHashJoinDuplicateRunBytes;
extern const Event PartitionedHashJoinScatterGroups;
extern const Event PartitionedHashJoinTeardownMicroseconds;
}

namespace CurrentMetrics
{
extern const Metric PartitionedHashJoinPoolThreads;
extern const Metric PartitionedHashJoinPoolThreadsActive;
extern const Metric PartitionedHashJoinPoolThreadsScheduled;
}

namespace DB
{

namespace ErrorCodes
{
extern const int LIMIT_EXCEEDED;
extern const int LOGICAL_ERROR;
extern const int UNSUPPORTED_JOIN_KEYS;
}

namespace
{

constexpr size_t locator_piece_rows = 32768; /// locator synthesis scratch stays L2-resident

/** Per-worker scratch for the duplicate rows of the pass in progress. A key's first duplicate of the pass
  * turns its cell word into a scratch marker naming a pass-local key id; every later row of the key appends
  * (key id, ref) here. The pass's finish counting-sorts the refs by key id into `staged` and hands each
  * key's run to the `DuplicateRunWriter`, which writes the final word back into the cell. No marker
  * survives a finish, so nothing probes a marker.
  */
struct TailScratch
{
    /// The pass-local key ids and the offsets into `staged` are 32-bit, so one pass (one partition's rows
    /// of one group, or the whole build on the single-partition plan, or all drained overflow rows) must
    /// stay below 2^32 rows; `checkPassRowLimit` throws before such a pass writes anything.
    static constexpr size_t MAX_PASS_ROWS = 1uz << 32;

    PaddedPODArray<UInt32> tail_key; /// per duplicate row: its pass-local key id
    PaddedPODArray<UInt64> tail_ref; /// per duplicate row: its ref word
    PaddedPODArray<UInt32> tail_count; /// per key id: rows of this pass
    PaddedPODArray<UInt64> tail_cell; /// per key id: the address of the cell's mapped value
    PaddedPODArray<UInt64> tail_prev_word; /// per key id: the word the cell held before this pass
    PaddedPODArray<UInt64> staged; /// the refs grouped by key id, in insertion order
    PaddedPODArray<UInt32> starts; /// per key id: its first ref in `staged`

    size_t keys() const { return tail_count.size(); }
    size_t rows() const { return tail_ref.size(); }

    void clear()
    {
        tail_key.clear();
        tail_ref.clear();
        tail_count.clear();
        tail_cell.clear();
        tail_prev_word.clear();
    }
};

/** Rows an owner's walk reached its range end with: the key (persisted, so the chunk can be freed), its
  * hash and its ref. One buffer per partition, written by the partition's owner, read by the serial
  * drain after the barrier.
  */
struct OverflowBuffer
{
    PaddedPODArray<char> keys;
    PaddedPODArray<UInt64> hashes;
    PaddedPODArray<UInt64> refs;

    size_t rows() const { return refs.size(); }

    template <typename Key>
    void push(const Key & key, UInt64 hash, UInt64 ref)
    {
        static_assert(std::is_trivially_copyable_v<Key>);
        const size_t offset = keys.size();
        keys.resize(offset + sizeof(Key));
        memcpy(keys.data() + offset, &key, sizeof(Key));
        hashes.push_back(hash);
        refs.push_back(ref);
    }

    template <typename Key>
    Key keyAt(size_t i) const
    {
        Key key;
        memcpy(&key, keys.data() + i * sizeof(Key), sizeof(Key));
        return key;
    }

    void clear()
    {
        keys.clear();
        hashes.clear();
        refs.clear();
    }
};

/// The first row of a key: `RowRef` and `RowRefList` both start as the inline ref word.
template <typename Mapped>
ALWAYS_INLINE void initMapped(Mapped & mapped, UInt64 ref)
{
    if constexpr (std::is_same_v<Mapped, RowRef>)
        new (&mapped) RowRef(RowRef::fromWord(ref));
    else
    {
        static_assert(std::is_same_v<Mapped, RowRefList>);
        new (&mapped) RowRefList(RowRefList::fromWord(ref));
    }
}

[[noreturn]] void throwPassRowLimit(size_t rows, const char * what)
{
    throw Exception(
        ErrorCodes::LIMIT_EXCEEDED,
        "PartitionedHashJoin: {} of {} rows exceeds the {} rows one build pass can hold",
        what,
        rows,
        TailScratch::MAX_PASS_ROWS);
}

/// Once per pass, before any row of it is inserted.
ALWAYS_INLINE void checkPassRowLimit(size_t rows, const char * what)
{
    if (unlikely(rows >= TailScratch::MAX_PASS_ROWS))
        throwPassRowLimit(rows, what);
}

/// A later row of a key: `RowRef` keeps the first row, or the last under `any_take_last_row`;
/// `RowRefList` defers to the pass's finish through the scratch.
template <typename Mapped>
ALWAYS_INLINE void appendRowToMapped(Mapped & mapped, UInt64 ref, TailScratch & scratch, bool any_take_last_row, bool & all_unique)
{
    all_unique = false;
    if constexpr (std::is_same_v<Mapped, RowRef>)
    {
        if (any_take_last_row)
            mapped = RowRef::fromWord(ref);
    }
    else
    {
        static_assert(std::is_same_v<Mapped, RowRefList>);
        UInt32 key_id;
        if (mapped.isScratchMarker())
        {
            key_id = static_cast<UInt32>(mapped.scratchKeyId());
            ++scratch.tail_count[key_id];
        }
        else
        {
            key_id = static_cast<UInt32>(scratch.tail_count.size());
            scratch.tail_count.push_back(1);
            scratch.tail_cell.push_back(reinterpret_cast<UInt64>(&mapped));
            scratch.tail_prev_word.push_back(mapped.word);
            mapped = RowRefList::makeScratchMarker(key_id);
        }
        scratch.tail_key.push_back(key_id);
        scratch.tail_ref.push_back(ref);
    }
}

/// The pass's finish: every key's refs of this pass become one contiguous run appended to the key's
/// word (a pair, a run, or one more node of a list), and the scratch marker leaves the cell. Cells are
/// prefetched in batches before the write-back, as the reference does.
void finishTailScratch(TailScratch & scratch, DuplicateRunWriter & writer)
{
    const size_t keys = scratch.keys();
    if (keys == 0)
        return;
    /// Guaranteed by `checkPassRowLimit` on every pass; the offsets below are 32-bit.
    if (scratch.rows() >= TailScratch::MAX_PASS_ROWS)
        throwPassRowLimit(scratch.rows(), "the duplicate rows of a build pass");

    scratch.starts.resize(keys + 1);
    UInt32 running = 0;
    for (size_t key = 0; key < keys; ++key)
    {
        scratch.starts[key] = running;
        running += scratch.tail_count[key];
    }
    scratch.starts[keys] = running;

    /// `tail_count` doubles as the placement cursor, then holds the end offsets; `starts` keeps the
    /// beginnings.
    scratch.staged.resize(running);
    for (size_t key = 0; key < keys; ++key)
        scratch.tail_count[key] = scratch.starts[key];
    const size_t rows = scratch.rows();
    for (size_t row = 0; row < rows; ++row)
        scratch.staged[scratch.tail_count[scratch.tail_key[row]]++] = scratch.tail_ref[row];

    static constexpr size_t batch_size = 32;
    for (size_t base = 0; base < keys; base += batch_size)
    {
        const size_t batch = std::min(batch_size, keys - base);
        for (size_t index = 0; index < batch; ++index)
            __builtin_prefetch(reinterpret_cast<const void *>(scratch.tail_cell[base + index]), 1, 3); /// NOLINT(performance-no-int-to-ptr)
        for (size_t index = 0; index < batch; ++index)
        {
            const size_t key = base + index;
            auto & mapped = *reinterpret_cast<RowRefList *>(scratch.tail_cell[key]); /// NOLINT(performance-no-int-to-ptr)
            RowRefList word = RowRefList::fromWord(scratch.tail_prev_word[key]);
            writer.append(word, scratch.staged.data() + scratch.starts[key], scratch.tail_count[key] - scratch.starts[key]);
            mapped = word;
        }
    }
    scratch.clear();
}

/// What one section insert into the shared table works with, shared by the sequential loop, the AMAC
/// policy and the drain so the three cannot diverge on the state machine.
template <typename Table>
struct InsertTarget
{
    using Cell = typename Table::cell_type;
    using Mapped = typename Table::mapped_type;
    static constexpr bool mapped_asof = std::is_same_v<Mapped, AsofRowRefs>;

    Table & table;
    Cell * cells;
    TailScratch & scratch;
    OverflowBuffer & overflow;
    /// The range this pass may write, and whether the walk wraps at the buffer end (single partition,
    /// one writer) or hands rows at `range_end` to the overflow (parallel owners).
    size_t range_begin;
    size_t range_end;
    bool wrap;
    bool any_take_last_row;
    /// ASOF: the inequality column of the stored block being inserted, and its number.
    const IColumn * asof_column = nullptr;
    UInt32 asof_block_no = 0;
    const HashJoin * join = nullptr;

    UInt64 claimed = 0;
    bool all_unique = true;

    ALWAYS_INLINE void claimed_one()
    {
        ++claimed;
        /// Single-writer wrapping walks have no barrier before which the capacity guard could run, so they
        /// check per claim: the table must keep an empty cell for every walk to terminate.
        if (wrap && claimed > table.maxFill()) [[unlikely]]
            throwCapacity();
    }

    [[noreturn]] void throwCapacity() const
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "PartitionedHashJoin: the shared hash table of {} cells received more than {} distinct keys; the size estimate that "
            "created it was too low",
            table.cellCount(),
            table.maxFill());
    }

    ALWAYS_INLINE void initFirst(Mapped & mapped, UInt64 ref, size_t row)
    {
        if constexpr (mapped_asof)
        {
            new (&mapped) AsofRowRefs(createAsofRowRef(*join->getAsofType(), join->getAsofInequality()));
            mapped->insert(*asof_column, asof_block_no, row);
        }
        else
            initMapped(mapped, ref);
    }

    ALWAYS_INLINE void appendLater(Mapped & mapped, UInt64 ref, size_t row)
    {
        if constexpr (mapped_asof)
        {
            all_unique = false;
            mapped->insert(*asof_column, asof_block_no, row);
        }
        else
            appendRowToMapped(mapped, ref, scratch, any_take_last_row, all_unique);
    }

    /// The zero key has one cell outside the buffer, and by construction only one partition's chunk
    /// carries zero-key rows, so this needs no ownership check. The zero cell is not a claimed buffer
    /// cell: the capacity guards and the published distinct count account for it through `hasZero`.
    template <typename Key>
    ALWAYS_INLINE void insertZero(const Key & key, UInt64 ref, size_t row)
    {
        const size_t hash = table.hash(key);
        if (!table.hasZero())
        {
            Cell * cell = table.claimZero(hash);
            initFirst(cell->getMapped(), ref, row);
        }
        else
            appendLater(table.zeroValue()->getMapped(), ref, row);
    }

    /// The owner walk from the home cell. Returns false when the row reached the range end and was
    /// handed to the overflow.
    template <typename KeyHolder>
    ALWAYS_INLINE bool insertFromHome(KeyHolder && key_holder, size_t hash, UInt64 ref, size_t row)
    {
        const auto & key = keyHolderGetKey(key_holder);
        size_t pos = table.place(hash);
        chassert(pos >= range_begin && pos < range_end);
        while (true)
        {
            Cell * cell = cells + pos;
            if (table.isEmptyCell(cell))
            {
                table.claim(pos, key_holder, hash);
                initFirst(cell->getMapped(), ref, row);
                claimed_one();
                return true;
            }
            if (table.keyEquals(cell, key, hash))
            {
                appendLater(cell->getMapped(), ref, row);
                return true;
            }
            if (wrap)
                pos = table.next(pos);
            else if (++pos == range_end)
                break;
        }
        keyHolderPersistKey(key_holder);
        overflow.push(keyHolderGetKey(key_holder), hash, ref);
        return false;
    }
};

/** The AMAC insert policy over one partition's range. `start` computes the map hash - whose latency
  * overlaps the other slots' outstanding cell misses - and prefetches the home cell for writing. `step`
  * is the one fused read-then-act the ring requires: claim an empty cell, append a duplicate, or advance
  * and prefetch; a row whose next cell would be the range end goes to the overflow instead. Zero-sentinel
  * keys and skipped rows never enter the ring.
  */
template <typename KeyGetter, typename Table>
struct OwnerAmacInsertPolicy
{
    using Cell = typename Table::cell_type;
    using Mapped = typename Table::mapped_type;
    static constexpr bool store_hash = cell_stores_hash<Cell>;
    /// The frame copy needs a copyable key getter, and the `KeysFixed` one is not - it owns a
    /// prepared-keys buffer and shuffle masks - so it stays by reference.
    static constexpr bool copy_into_frame = std::is_copy_constructible_v<KeyGetter>;

    template <size_t ring_size>
    struct RingBase
    {
        std::array<size_t, ring_size> pos{};
        std::array<UInt32, ring_size> row; /// `amac_inactive_row` == inactive

        RingBase() { row.fill(amac_inactive_row); }
        bool isActive(size_t s) const { return row[s] != amac_inactive_row; }
        void deactivate(size_t s) { row[s] = amac_inactive_row; }
        UInt32 rowAt(size_t s) const { return row[s]; }
    };
    template <size_t ring_size>
    struct RingWithHash : public RingBase<ring_size>
    {
        std::array<size_t, ring_size> hash{};
    };
    template <size_t ring_size>
    using Ring = std::conditional_t<store_hash, RingWithHash<ring_size>, RingBase<ring_size>>;

    InsertTarget<Table> & target;
    std::conditional_t<copy_into_frame, KeyGetter, KeyGetter &> key_getter;
    const UInt64 * locators = nullptr;
    const UInt32 * narrow_locators = nullptr;
    const UInt8 * skip_bytes = nullptr;
    UInt32 block_no = 0;
    Arena & pool;

    ALWAYS_INLINE UInt64 refWordAt(size_t row) const
    {
        if (locators)
            return locators[row];
        if (narrow_locators)
            return RowRef(narrow_locators[row] >> 16, narrow_locators[row] & 0xFFFFu).encode();
        return RowRef(block_no, static_cast<UInt32>(row)).encode();
    }

    template <typename RingT>
    ALWAYS_INLINE bool start(RingT & ring, size_t s, size_t row)
    {
        if (skip_bytes && skip_bytes[row])
            return false;
        auto && key_holder = key_getter.getKeyHolder(row, pool);
        const auto & key = keyHolderGetKey(key_holder);
        if (unlikely(Table::isZeroKey(key)))
        {
            target.insertZero(key, refWordAt(row), row);
            return false;
        }
        const size_t hash = target.table.hash(key);
        const size_t pos = target.table.place(hash);
        ring.pos[s] = pos;
        ring.row[s] = static_cast<UInt32>(row);
        if constexpr (store_hash)
            ring.hash[s] = hash;
        __builtin_prefetch(target.cells + pos, 1, 3);
        return true;
    }

    template <typename RingT>
    ALWAYS_INLINE AmacStepResult step(RingT & ring, size_t s)
    {
        const size_t row = ring.row[s];
        auto && key_holder = key_getter.getKeyHolder(row, pool);
        const auto & key = keyHolderGetKey(key_holder);
        size_t hash = 0;
        if constexpr (store_hash)
            hash = ring.hash[s];
        else
            hash = target.table.hash(key);
        const size_t pos = ring.pos[s];
        Cell * cell = target.cells + pos;
        if (target.table.isEmptyCell(cell))
        {
            /// Claim and write in the same visit, so no other in-flight row can also see this cell
            /// empty.
            target.table.claim(pos, key_holder, hash);
            target.initFirst(cell->getMapped(), refWordAt(row), row);
            target.claimed_one();
            return AmacStepResult::Done;
        }
        if (target.table.keyEquals(cell, key, hash))
        {
            target.appendLater(cell->getMapped(), refWordAt(row), row);
            return AmacStepResult::Done;
        }
        size_t next_pos;
        if (target.wrap)
            next_pos = target.table.next(pos);
        else
        {
            next_pos = pos + 1;
            if (next_pos == target.range_end)
            {
                keyHolderPersistKey(key_holder);
                target.overflow.push(keyHolderGetKey(key_holder), hash, refWordAt(row));
                return AmacStepResult::Done;
            }
        }
        ring.pos[s] = next_pos;
        __builtin_prefetch(target.cells + next_pos, 1, 3);
        return AmacStepResult::Advance;
    }
};

/// Inserts one compact section into the shared table on behalf of the owner of `target.range_*`, with the
/// semantics of `insertFromBlockImplTypeCase`: one hash per build row, then the value shape's own append.
/// The recorded ref comes from the scattered locator column - 8-byte encoded or 4-byte packed - or, on
/// the single-partition path, from `RowRef(block_no, i)` with `skip_bytes` excluding the rows that must
/// not be inserted.
template <typename KeyGetter, typename Table>
void insertSectionShared(
    InsertTarget<Table> & target,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    size_t rows,
    const UInt64 * locators,
    const UInt32 * narrow_locators,
    UInt32 block_no,
    const UInt8 * skip_bytes,
    Arena & pool,
    bool enable_prefetch,
    bool use_amac)
{
    constexpr bool mapped_asof = InsertTarget<Table>::mapped_asof;

    /// The ASOF value sits at the row's own index in the trailing key column, so this only works
    /// where the compact index is the stored row - which is why ASOF plans stay single-partition.
    if constexpr (mapped_asof)
    {
        if (locators || narrow_locators || !target.wrap)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ASOF inserts require the single-partition build plan");
        target.asof_column = key_columns.back();
        target.asof_block_no = block_no;
    }

    /// As in `createKeyGetter`: the ASOF getter excludes the inequality column.
    auto key_getter = [&]
    {
        if constexpr (mapped_asof)
        {
            ColumnRawPtrs equi_columns(key_columns.begin(), key_columns.end() - 1);
            Sizes equi_sizes(key_sizes.begin(), key_sizes.end() - 1);
            return KeyGetter(equi_columns, equi_sizes, nullptr);
        }
        else
        {
            return KeyGetter(key_columns, key_sizes, nullptr);
        }
    }();

    /// The ring replaces the sequential loop once the caller has decided the cell misses dominate
    /// and the section is long enough to amortize prime and drain. ASOF stays sequential: appending
    /// to a per-key sorted lookup is not a one-cell fused action.
    if constexpr (!mapped_asof && amac_join_supported<KeyGetter, Table>)
    {
        if (use_amac && rows >= amac_min_rows && rows < amac_inactive_row)
        {
            OwnerAmacInsertPolicy<KeyGetter, Table> policy{
                .target = target,
                .key_getter = key_getter,
                .locators = locators,
                .narrow_locators = narrow_locators,
                .skip_bytes = skip_bytes,
                .block_no = block_no,
                .pool = pool};
            amacRun(policy, rows);
            return;
        }
    }

    constexpr bool can_prefetch = join_prefetch_supported<KeyGetter, Table>;
    bool use_prefetch = false;
    if constexpr (can_prefetch)
        use_prefetch = enable_prefetch && target.table.reservedBytes() > getMinBytesForPrefetchInJoin();

    auto prefetcher = makeJoinPrefetcher(
        use_prefetch,
        rows,
        [&](size_t k) __attribute__((always_inline))
        {
            if constexpr (can_prefetch)
                target.table.prefetch(key_getter.getKeyHolder(k, pool));
        });

    for (size_t i = 0; i < rows; ++i)
    {
        if constexpr (can_prefetch)
            prefetcher.prefetchAt(i);

        if (skip_bytes && skip_bytes[i])
            continue;

        UInt64 ref = 0;
        if (locators)
            ref = locators[i];
        else if (narrow_locators)
            ref = RowRef(narrow_locators[i] >> 16, narrow_locators[i] & 0xFFFFu).encode();
        else
            ref = RowRef(block_no, i).encode();

        auto && key_holder = key_getter.getKeyHolder(i, pool);
        const auto & key = keyHolderGetKey(key_holder);
        if (unlikely(Table::isZeroKey(key)))
        {
            target.insertZero(key, ref, i);
            continue;
        }
        target.insertFromHome(key_holder, target.table.hash(key), ref, i);
    }
}

/// The direct-index maps (`key8`, `key16`) have no ranges and no collisions: the standard `emplace`
/// applies, and only the duplicate layout is shared with the partitioned build. ASOF keys of these widths
/// (`HashJoin` picks the map by the equi-key width, so `UInt16` keys of an ASOF join land here) append to
/// the per-key sorted lookup exactly as on the shared path.
template <typename KeyGetter, typename Table>
void insertSectionFixed(
    Table & table,
    const HashJoin & join,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    size_t rows,
    const UInt64 * locators,
    const UInt32 * narrow_locators,
    UInt32 block_no,
    const UInt8 * skip_bytes,
    Arena & pool,
    TailScratch & scratch,
    bool any_take_last_row,
    UInt64 & claimed,
    bool & all_unique)
{
    using Mapped = typename Table::mapped_type;
    constexpr bool mapped_asof = std::is_same_v<Mapped, AsofRowRefs>;

    /// The ASOF value is read at the row's own index in the trailing key column, so the stored row must be
    /// the compact index (the single-partition plan), and the getter excludes the inequality column.
    const IColumn * asof_column [[maybe_unused]] = nullptr;
    if constexpr (mapped_asof)
    {
        if (locators || narrow_locators)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ASOF inserts require the single-partition build plan");
        asof_column = key_columns.back();
    }
    auto key_getter = [&]
    {
        if constexpr (mapped_asof)
        {
            ColumnRawPtrs equi_columns(key_columns.begin(), key_columns.end() - 1);
            Sizes equi_sizes(key_sizes.begin(), key_sizes.end() - 1);
            return KeyGetter(equi_columns, equi_sizes, nullptr);
        }
        else
        {
            return KeyGetter(key_columns, key_sizes, nullptr);
        }
    }();

    for (size_t i = 0; i < rows; ++i)
    {
        if (skip_bytes && skip_bytes[i])
            continue;

        auto emplace_result = key_getter.emplaceKey(table, i, pool);
        Mapped & mapped = emplace_result.getMapped();
        if constexpr (mapped_asof)
        {
            if (emplace_result.isInserted())
            {
                new (&mapped) AsofRowRefs(createAsofRowRef(*join.getAsofType(), join.getAsofInequality()));
                ++claimed;
            }
            else
                all_unique = false;
            mapped->insert(*asof_column, block_no, i);
        }
        else
        {
            UInt64 ref = 0;
            if (locators)
                ref = locators[i];
            else if (narrow_locators)
                ref = RowRef(narrow_locators[i] >> 16, narrow_locators[i] & 0xFFFFu).encode();
            else
                ref = RowRef(block_no, i).encode();

            if (emplace_result.isInserted())
            {
                initMapped(mapped, ref);
                ++claimed;
            }
            else
                appendRowToMapped(mapped, ref, scratch, any_take_last_row, all_unique);
        }
    }
}

/// The serial drain of one partition's overflow: the walk from the home cell with the global mask. A row
/// here has no cell of its key inside its owner's range (otherwise its owner would have appended to it),
/// so it either claims the first empty cell beyond the range or appends to a cell an earlier group or an
/// earlier drained row created.
template <typename Table>
void drainPartitionOverflow(InsertTarget<Table> & target, UInt64 & appended)
{
    using Key = typename Table::key_type;
    OverflowBuffer & overflow = target.overflow;
    const size_t rows = overflow.rows();
    for (size_t i = 0; i < rows; ++i)
    {
        const Key key = overflow.template keyAt<Key>(i);
        const size_t hash = overflow.hashes[i];
        const UInt64 ref = overflow.refs[i];
        size_t pos = target.table.place(hash);
        while (true)
        {
            auto * cell = target.cells + pos;
            if (target.table.isEmptyCell(cell))
            {
                /// The key was persisted when it was handed off; a plain key needs no holder.
                target.table.claim(pos, key, hash);
                target.initFirst(cell->getMapped(), ref, 0);
                target.claimed_one();
                break;
            }
            if (target.table.keyEquals(cell, key, hash))
            {
                target.appendLater(cell->getMapped(), ref, 0);
                ++appended;
                break;
            }
            pos = target.table.next(pos);
        }
    }
    overflow.clear();
}

/// From the saved routes, taking the MSB-first slice this pass owns. Skipped rows go to the drop
/// bucket.
void deriveBucketIds(const PaddedPODArray<UInt16> & routes, const UInt8 * skip_bytes, size_t bits, size_t partitions, UInt16 * bucket_ids)
{
    const size_t rows = routes.size();
    const UInt32 shift = static_cast<UInt32>(16 - bits);
    if (skip_bytes)
    {
        for (size_t i = 0; i < rows; ++i)
            bucket_ids[i] = skip_bytes[i] ? static_cast<UInt16>(partitions) : static_cast<UInt16>(routes[i] >> shift);
    }
    else
    {
        for (size_t i = 0; i < rows; ++i)
            bucket_ids[i] = static_cast<UInt16>(routes[i] >> shift);
    }
}

template <typename Stage>
void runPostBuildWave(ThreadPool & pool, size_t workers, Stage && stage, std::atomic<UInt64> & stage_thread_us)
{
    try
    {
        for (size_t w = 0; w < workers; ++w)
            pool.scheduleOrThrow(
                [&stage, &stage_thread_us, w, thread_group = CurrentThread::getGroup()]
                {
                    ThreadGroupSwitcher switcher(thread_group, ThreadName::PARTITIONED_JOIN);
                    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::PartitionedHashJoinBuildMicroseconds);
                    Stopwatch stage_watch;
                    stage(w);
                    stage_thread_us.fetch_add(stage_watch.elapsedMicroseconds(), std::memory_order_relaxed);
                });
        pool.wait();
    }
    catch (...)
    {
        pool.wait();
        throw;
    }
}

void emplaceSizedBuildArena(std::deque<Arena> & arenas, size_t predicted_bytes)
{
    /// Below 1 MiB the default 4 KiB doubling is cheaper than a first chunk a small worker would
    /// not fill. Above that, the first allocation should cover the prediction so Arena does not
    /// leave a last exponential chunk about as large as the runs themselves.
    constexpr size_t min_sized = 1uz << 20;
    if (predicted_bytes < min_sized)
    {
        arenas.emplace_back();
        return;
    }
    arenas.emplace_back(predicted_bytes, /*growth_factor_=*/2, predicted_bytes);
}

void accumulate(DuplicateRunWriter::Stats & into, const DuplicateRunWriter::Stats & from)
{
    into.pairs += from.pairs;
    into.runs += from.runs;
    into.descriptors += from.descriptors;
    into.appended_nodes += from.appended_nodes;
    into.small_blocks += from.small_blocks;
    into.in_place_fills += from.in_place_fills;
    into.moved_refs += from.moved_refs;
    into.arena_bytes += from.arena_bytes;
    into.slack_slots += from.slack_slots;
}

}

/// The stages communicate through exact per-bucket offsets: bucket `p` holds worker `w`'s stripe at
/// `[starts[p * workers + w], + worker_hist[w][p])`, in worker order. The last bucket collects
/// null-key rows; it is scattered like any other and dropped before the inserts, so a partition only
/// ever sees insertable rows.
struct PartitionedHashJoin::PostBuildContext
{
    size_t workers = 0;
    size_t fanout = 0; /// pass-1 partitions + 1 (the null bucket); == partitions + 1 on single-pass plans
    size_t num_key_columns = 0;
    bool generic_mode = false;

    /// The first pass also scatters the saved route words, so a refine pass can derive its
    /// sub-bucket ids without touching the key columns. Once `refined`, every per-bucket container is
    /// final-partition-indexed and has no drop bucket.
    size_t route_bits = 0; /// pass-1 bits (== total bits on single-pass plans)
    bool multi_pass = false;
    bool refined = false;
    size_t current_buckets = 0; /// buckets refine passes operate on (drop bucket excluded)
    std::vector<PaddedPODArray<UInt16>> routes;

    /// Generic mode after a refine pass: one self-contained piece per (key column, partition).
    std::vector<MutableColumns> refined_pieces;

    PaddedPODArray<UInt64> worker_hist; /// workers x fanout
    std::vector<UInt64> bucket_rows; /// per bucket
    PaddedPODArray<UInt64> starts; /// fanout x workers

    /// Fixed mode: one exact uninitialized column per (key column, bucket), written by all workers.
    std::vector<MutableColumns> fixed_out;
    std::vector<std::vector<char *>> fixed_base;
    std::vector<size_t> fixed_widths;

    /// Generic mode: self-contained per-(key column, worker, bucket) pieces from `ColumnsScatter`.
    std::vector<std::vector<MutableColumns>> pieces;

    /// Always scattered cooperatively: 8-byte encoded `RowRef` words, or the packed 4-byte form.
    std::vector<PaddedPODArray<UInt64>> locators;
    std::vector<PaddedPODArray<UInt32>> locators32;

    struct WorkerState
    {
        std::vector<ColumnsScatter::ScatterScratch> key_scratch;
        ColumnsScatter::ScatterScratch locator_scratch;
        ColumnsScatter::ScatterScratch route_scratch;
        PaddedPODArray<UInt64> locator_piece;
        PaddedPODArray<UInt32> locator_piece32;
        /// The owner's duplicate writer over its own arena, and its pass scratch.
        std::optional<DuplicateRunWriter> writer;
        TailScratch tail_scratch;
        bool all_values_unique = true;
        UInt64 inserted_rows = 0;
    };
    std::deque<WorkerState> worker_state;

    /// Per partition: the overflow of the current group's owner pass, the distinct keys claimed so far
    /// (by owners and the drain), and whether the range has been committed.
    std::vector<OverflowBuffer> overflow;
    std::vector<UInt64> claimed_per_partition;
    std::vector<UInt8> range_committed;
    std::vector<UInt32> partition_order; /// largest first
    std::atomic<UInt32> partition_claim{0};

    /// The drain's writer (over the last arena), scratch and counters.
    std::optional<DuplicateRunWriter> drain_writer;
    TailScratch drain_scratch;
    UInt64 drain_claimed = 0;
    UInt64 drain_appended = 0;
    bool drain_all_unique = true;

    /// Set for the range currently being scattered. `blockStripe` divides this span among workers.
    size_t block_begin = 0;
    size_t block_end = 0;

    /// Empty clones of the prepared key columns, taken before any range is scattered. The chunk
    /// allocation only needs each column's type and width, and a consumed range has already
    /// dropped its own key columns.
    Columns key_samples;

    std::pair<size_t, size_t> blockStripe(size_t worker) const
    {
        const size_t n = block_end - block_begin;
        return {block_begin + worker * n / workers, block_begin + (worker + 1) * n / workers};
    }
};

void PartitionedHashJoin::decideAmacEngagement()
{
    /// The same heuristics that enable the standard loops' software prefetch: the user toggle plus
    /// the table size past the L2 threshold, below which the cell reads hit anyway and pipelining them
    /// costs more than it saves.
    amac_build_engaged = amac_enabled && leaf_join->enableSoftwarePrefetch() && ht_total_bytes > getMinBytesForPrefetchInJoin();
}

void PartitionedHashJoin::insertPartitionSection(
    PostBuildContext & ctx,
    size_t worker,
    size_t partition,
    const ColumnRawPtrs & key_columns,
    size_t rows,
    const UInt64 * locators,
    const UInt32 * narrow_locators_data,
    UInt32 block_no,
    const UInt8 * skip_bytes)
{
    const Sizes & key_sizes = leaf_join->key_sizes[0];
    const bool enable_prefetch = leaf_join->enableSoftwarePrefetch();
    auto & state = ctx.worker_state[worker];
    Arena & arena = build_arenas[worker];
    OverflowBuffer & overflow = ctx.overflow[partition == single_partition ? 0 : partition];
    UInt64 & claimed = ctx.claimed_per_partition[partition == single_partition ? 0 : partition];

    std::visit(
        [&](auto & shape_maps)
        {
            switch (leaf_join->data->type)
            {
#define M(TYPE) \
    case HashJoin::Type::TYPE: { \
        using Table = typename decltype(shape_maps.TYPE)::element_type; \
        using KeyGetter = typename KeyGetterForType<HashJoin::Type::TYPE, Table>::Type; \
        Table & table = *shape_maps.TYPE; \
        if constexpr (is_shared_join_table<Table>) \
        { \
            const bool wrap = partition == single_partition; \
            InsertTarget<Table> target{ \
                .table = table, \
                .cells = table.cells(), \
                .scratch = state.tail_scratch, \
                .overflow = overflow, \
                .range_begin = wrap ? 0 : table.rangeBegin(partition), \
                .range_end = wrap ? table.cellCount() : table.rangeEnd(partition), \
                .wrap = wrap, \
                .any_take_last_row = any_take_last_row, \
                .join = leaf_join.get()}; \
            target.claimed = claimed; \
            insertSectionShared<KeyGetter, Table>( \
                target, key_columns, key_sizes, rows, locators, narrow_locators_data, block_no, skip_bytes, arena, enable_prefetch, amac_build_engaged); \
            claimed = target.claimed; \
            state.all_values_unique = state.all_values_unique && target.all_unique; \
        } \
        else \
        { \
            insertSectionFixed<KeyGetter, Table>( \
                table, *leaf_join, key_columns, key_sizes, rows, locators, narrow_locators_data, block_no, skip_bytes, arena, \
                state.tail_scratch, any_take_last_row, claimed, state.all_values_unique); \
        } \
        break; \
    }
                APPLY_FOR_PARTITIONED_JOIN_VARIANTS(M)
#undef M
                default:
                    throw Exception(
                        ErrorCodes::UNSUPPORTED_JOIN_KEYS,
                        "Unsupported JOIN keys for the partitioned join (type: {})",
                        leaf_join->data->type);
            }
        },
        shared_maps->maps);
}

void PartitionedHashJoin::runPostBuildPhase()
{
    chassert(!build_phase_finished);

    if (delegate_mode)
    {
        /// Already built during the fill and the barrier. Its single-map post-build optimizations
        /// stay off, as they do on the partitioned path.
        build_phase_finished = true;
        return;
    }

    bool all_values_unique = true;
    if (bits == 0)
    {
        /// Single-partition has no histogram or scatter stage - every row is inserted straight from the
        /// stored blocks - so all of it charges to the insert sub-phase.
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::PartitionedHashJoinBuildMicroseconds);
        ProfileEventTimeIncrement<Microseconds> leaf_watch(ProfileEvents::PartitionedHashJoinBuildLeafMicroseconds);
        all_values_unique = postBuildSinglePartition();
    }
    else
    {
        if (!post_build_ctx)
            preparePostBuildContext();
        all_values_unique = postBuildPartitioned();
    }

    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::PartitionedHashJoinBuildMicroseconds);

    /// The routes and prepared key columns were already dropped as the scatter consumed them; this
    /// is the block shells and the lane bookkeeping, freed before the probe starts.
    build_blocks.clear();
    build_blocks.shrink_to_fit();
    /// From here the byte count tracks only the stored blocks.
    accumulated_bytes.store(leaf_join->data->allocated_size, std::memory_order_relaxed);

    ProfileEvents::increment(ProfileEvents::PartitionedHashJoinHashTableBytes, ht_total_bytes);
    ProfileEvents::increment(ProfileEvents::PartitionedHashJoinOverflowRows, stats.overflow_rows);
    ProfileEvents::increment(
        ProfileEvents::PartitionedHashJoinDuplicateRunBytes, stats.owner_duplicates.arena_bytes + stats.drain_duplicates.arena_bytes);

    /// For the next run of this query and for the planner's consumers. Published, never consumed for
    /// sizing: the table cannot grow, and a cached count would be data-independent.
    if (stats_collecting_params.isCollectionAndUseEnabled())
    {
        PartitionedHashJoinEntry entry;
        entry.bits = bits;
        entry.total_distinct = stats.distinct_keys;
        if (post_build_ctx)
            entry.per_partition.assign(post_build_ctx->claimed_per_partition.begin(), post_build_ctx->claimed_per_partition.end());
        else
            entry.per_partition.assign(1, stats.distinct_keys);
        getHashTablesStatistics<PartitionedHashJoinEntry>().update(entry, stats_collecting_params);

        /// Join reordering, `rhs_size_estimation` and the runtime-filter sizing all read
        /// `HashJoinEntry` and none of them cares which algorithm produced it, so publish that
        /// shape too - `total_distinct` is the exact key count `ht_size` means. `leaf_join` holds no
        /// stats params, so nothing else writes this key for this join.
        if (entry.total_distinct)
            getHashTablesStatistics<HashJoinEntry>().update(
                {.ht_size = entry.total_distinct, .source_rows = leaf_join->data->rows_to_join}, stats_collecting_params);
    }

    post_build_ctx.reset();
    post_build_pool.reset();

    finishBuildPhase(all_values_unique);

    LOG_TRACE(
        log,
        "Built one shared hash table of {} cells in {} partitions: {} keys from {} rows, {} of right-table data including the table "
        "({} committed, {} overflow rows drained, {} bytes of duplicate runs)",
        stats.table_cells,
        partitions,
        stats.distinct_keys,
        stats.inserted_rows,
        ReadableSize(getTotalByteCount()),
        ReadableSize(ht_total_bytes),
        stats.overflow_rows,
        ReadableSize(stats.owner_duplicates.arena_bytes + stats.drain_duplicates.arena_bytes));
}

void PartitionedHashJoin::finishBuildPhase(bool all_values_unique)
{
    /// The leaf join's own barrier: used-flags init over its empty map, the ALL -> RightAny promotion
    /// when every build key turned out unique - the probe dispatches on the promoted strictness - and
    /// the non-joined status. The flags are then resized to span the whole table.
    leaf_join->all_values_unique = all_values_unique;
    leaf_join->onBuildPhaseFinish();
    reinitUsedFlags();
    leaf_join->data->keys_to_join = getTotalRowCount();
    build_phase_finished = true;
}

void PartitionedHashJoin::reinitUsedFlags()
{
    /// One per-offset space of `cells + 1`, offset 0 being the zero-value cell, exactly the
    /// `getBufferSizeInCells() + 1` the standard join sizes. `reinit` only grows, and does nothing for
    /// shapes without right-side flags. It has to run after the leaf join's barrier, which sized the
    /// flags to its own empty map.
    const size_t flags = shared_maps->getBufferSizeInCells(leaf_join->data->type) + 1;
    joinDispatch(
        leaf_join->getKind(),
        leaf_join->getStrictness(),
        leaf_join->data->maps.front(),
        leaf_join->getMapsKind(),
        [&](auto kind_, auto strictness_, auto & map_)
        {
            leaf_join->used_flags->reinit<kind_, strictness_, mapsKindOf<decltype(map_)>()>(flags);
        });
}

void PartitionedHashJoin::createSharedTable()
{
    const HashJoin::Type type = leaf_join->data->type;
    const size_t insertable_rows = accumulated_rows.load(std::memory_order_relaxed);
    const size_t reserve = reserveFor(insertable_rows, hll_estimate);
    ht_total_bytes = SharedJoinMaps::predictedBufferBytes(maps_variant_index, type, reserve);

    shared_maps = std::make_unique<SharedJoinMaps>(maps_variant_index);
    shared_maps->create(type, size_degree, bits);

    stats.table_size_degree = size_degree;
    stats.table_cells = shared_maps->getBufferSizeInCells(type);
    stats.predictions_exact = shared_maps->getReservedBufferBytes(type) == ht_total_bytes;
    decideAmacEngagement();
}

size_t PartitionedHashJoin::reserveFor(size_t rows, double distinct_estimate) const
{
    /// The safety factor covers the sketch's error; the row clamp says a table cannot hold more keys
    /// than rows. Above 2^31 estimated words the 32-bit sketch is saturating, so the exact upper bound
    /// takes over: at most a 2x over-reservation, only for builds already holding 64 GiB of cells.
    const double scaled = std::ceil(std::max(distinct_estimate, 1.0) * reserve_safety);
    const size_t rows_bound = std::max<size_t>(rows, 1);
    if (scaled >= 2147483648.0)
        return rows_bound;
    return std::clamp<size_t>(static_cast<size_t>(scaled), 1, rows_bound);
}

bool PartitionedHashJoin::postBuildSinglePartition()
{
    const HashJoin::Type type = leaf_join->data->type;

    /// One partition over the whole build, with no scatter: rows go in straight from the stored blocks
    /// with plain `RowRef(block_no, row)` refs, the walk wraps at the buffer end, and nothing overflows.
    chassert(bits == 0 && !post_build_ctx);
    post_build_ctx.reset(new PostBuildContext);
    auto & ctx = *post_build_ctx;
    ctx.workers = 1;
    ctx.worker_state.resize(1);
    ctx.overflow.resize(1);
    ctx.claimed_per_partition.assign(1, 0);
    ctx.range_committed.assign(1, 0);

    createSharedTable();
    measureGenericKeyBytes();
    const size_t insertable_rows = accumulated_rows.load(std::memory_order_relaxed);
    chassert(build_arenas.empty());
    emplaceSizedBuildArena(build_arenas, predictedArenaBytes(insertable_rows, post_build_plan == PostBuildPlan::Grouped));
    emplaceSizedBuildArena(build_arenas, 0);
    const bool grouped = false;
    ctx.worker_state[0].writer.emplace(build_arenas[0], grouped);
    ctx.drain_writer.emplace(build_arenas[1], grouped);

    std::visit(
        [&](auto & shape_maps)
        {
            switch (type)
            {
#define M(TYPE) \
    case HashJoin::Type::TYPE: { \
        if constexpr (is_shared_join_table<typename decltype(shape_maps.TYPE)::element_type>) \
            shape_maps.TYPE->commitAll(); \
        break; \
    }
                APPLY_FOR_PARTITIONED_JOIN_VARIANTS(M)
#undef M
                default:
                    break;
            }
        },
        shared_maps->maps);
    ctx.range_committed[0] = 1;

    for (auto & fill : build_blocks)
    {
        insertPartitionSection(
            ctx,
            /*worker=*/0,
            single_partition,
            fill.key_columns,
            fill.rows,
            /*locators=*/nullptr,
            /*narrow_locators_data=*/nullptr,
            fill.block_no,
            fill.skipData());
        ProfileEvents::increment(ProfileEvents::PartitionedHashJoinLeafRows, fill.rows);
        ctx.worker_state[0].inserted_rows += fill.rows;

        /// Consumed - drop this block's prepared keys and routes.
        fill.keys_holder.clear();
        fill.key_columns.clear();
        fill.null_map_holder.reset();
        fill.null_map = nullptr;
        fill.join_mask = JoinCommon::JoinMask();
        fill.skip_bytes = {};
        fill.routes = {};
    }
    finishTailScratch(ctx.worker_state[0].tail_scratch, *ctx.worker_state[0].writer);
    chassert(ctx.overflow[0].rows() == 0);

    stats.inserted_rows = ctx.worker_state[0].inserted_rows;
    stats.owner_duplicates = ctx.worker_state[0].writer->stats();
    publishTableSize(ctx);
    return ctx.worker_state[0].all_values_unique;
}

void PartitionedHashJoin::publishTableSize(const PostBuildContext & ctx)
{
    const HashJoin::Type type = leaf_join->data->type;
    UInt64 distinct = 0;
    for (UInt64 claimed : ctx.claimed_per_partition)
        distinct += claimed;
    std::visit(
        [&](auto & shape_maps)
        {
            switch (type)
            {
#define M(TYPE) \
    case HashJoin::Type::TYPE: { \
        using Table = typename decltype(shape_maps.TYPE)::element_type; \
        if constexpr (is_shared_join_table<Table>) \
        { \
            if (shape_maps.TYPE->hasZero()) \
                ++distinct; \
            shape_maps.TYPE->setSize(distinct); \
            verifyPublishedTable(*shape_maps.TYPE); \
        } \
        else \
            distinct = shape_maps.TYPE->size(); \
        break; \
    }
                APPLY_FOR_PARTITIONED_JOIN_VARIANTS(M)
#undef M
                default:
                    break;
            }
        },
        shared_maps->maps);
    stats.distinct_keys = distinct;
}

/// Debug and sanitizer builds only: the published table must carry no scratch marker (every pass finished
/// its tail) and its duplicate layout must account for every inserted row. A leaked marker would otherwise
/// read as an empty key in the release build - silent row loss.
template <typename Table>
void PartitionedHashJoin::verifyPublishedTable(const Table & table) const
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    using Mapped = typename Table::mapped_type;
    if constexpr (std::is_same_v<Mapped, RowRefList>)
    {
        UInt64 rows = 0;
        const auto visit_cell = [&](const auto * cell, size_t position)
        {
            const RowRefList & mapped = cell->getMapped();
            if (mapped.isScratchMarker())
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "PartitionedHashJoin: a scratch marker survived publication in cell {}", position);
            rows += mapped.rows();
        };
        if (table.hasZero())
            visit_cell(table.zeroValue(), 0);
        for (size_t position = 0, cells = table.cellCount(); position < cells; ++position)
        {
            const auto * cell = table.cellAt(position);
            if (!table.isEmptyCell(cell))
                visit_cell(cell, position + 1);
        }
        if (rows != stats.inserted_rows)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "PartitionedHashJoin: the published table holds {} rows in its duplicate layout but {} rows were inserted",
                rows,
                stats.inserted_rows);
    }
#else
    (void)table;
#endif
}

size_t PartitionedHashJoin::predictedTableAndArenaBytes(size_t rows, size_t distinct, bool grouped) const
{
    const size_t distinct_keys = std::max(distinct, 1uz);
    const size_t reserve = reserveFor(rows, static_cast<double>(distinct_keys));
    size_t bytes = SharedJoinMaps::predictedBufferBytes(maps_variant_index, leaf_join->data->type, reserve);

    /// `maps_variant_index == 1` is `MapsAll` (`RowRefList`). Unique keys stay inline in the cell
    /// word; only this shape keeps duplicate runs in the arena, at 8 bytes per row of a duplicated
    /// key. `preferUseMapsAll` is still false at the gate - the ALL-to-RightAny promotion has not run -
    /// so the variant index is what actually keeps the runs. LEFT/INNER Any/Semi/Anti use `MapsOne`
    /// and hold no run.
    ///
    /// Multiplicity inside `reserve_safety` is treated as unique for the arena term. A fill-phase
    /// distinct estimate that lags the row count by a sixteenth, or a HyperLogLog that undershoots
    /// by a percent, would otherwise look like `m > 1` and charge every row - enough to spill a unique
    /// build that fits. Real duplicate builds (m=5, m=8) sit far above the band. The factor already
    /// covers sketch error for the table reserve; reusing it here keeps the unique/duplicate decision on
    /// the same inputs.
    if (maps_variant_index == 1)
    {
        const double multiplicity = static_cast<double>(rows) / static_cast<double>(distinct_keys);
        if (multiplicity > reserve_safety)
        {
            bytes += sizeof(UInt64) * rows;
            /// Grouped scatter appends each group's rows of a key as its own run, so a key whose rows arrive
            /// in several groups costs more than its refs: up to 8 rows it holds them in one 64-byte block
            /// (the unfilled slots are the cost; two rows are a pair, exact), beyond that it chains a
            /// descriptor, a link word and one partially filled block (plan section 6, at most 88 bytes).
            /// The number of duplicated keys bounds the keys that can span groups.
            if (grouped)
            {
                const size_t duplicated_keys = std::min(distinct_keys, rows > distinct_keys ? rows - distinct_keys : 0);
                size_t extra_per_key = 88;
                if (multiplicity <= 2.0)
                    extra_per_key = 0;
                else if (multiplicity < 8.0)
                    extra_per_key = 64 - 8 * static_cast<size_t>(multiplicity);
                bytes += extra_per_key * duplicated_keys;
            }
        }
    }
    return bytes;
}

size_t PartitionedHashJoin::predictedArenaBytes(size_t insertable_rows, bool grouped) const
{
    /// Duplicate-run bytes come from the shared helper so the fill-phase prediction and the gate cannot
    /// drift. Variable-length keys are copied into the arena; that total is measured once before the
    /// first range is scattered, because a consumed range has dropped its key columns.
    const size_t distinct = std::max(static_cast<size_t>(std::llround(hll_estimate)), 1uz);
    const size_t tables_and_runs = predictedTableAndArenaBytes(insertable_rows, distinct, grouped);
    const size_t tables = SharedJoinMaps::predictedBufferBytes(maps_variant_index, leaf_join->data->type, reserveFor(insertable_rows, static_cast<double>(distinct)));
    chassert(tables_and_runs >= tables);
    return tables_and_runs - tables + generic_key_bytes;
}

/// Total bytes of the prepared key columns across the whole build. Measured while every block still
/// holds its keys, so the gate and the group-boundary re-checks agree on the arena term.
void PartitionedHashJoin::measureGenericKeyBytes()
{
    generic_key_bytes = 0;
    if (build_blocks.empty())
        return;
    for (const auto * column : build_blocks.front().key_columns)
        if (!column->isFixedAndContiguous())
        {
            for (const auto & fill : build_blocks)
                for (const auto * key_column : fill.key_columns)
                    generic_key_bytes += key_column->byteSize();
            return;
        }
}

size_t PartitionedHashJoin::chunkBytesForBlockRange(size_t b0, size_t b1) const
{
    chassert(post_build_ctx);
    const auto & ctx = *post_build_ctx;
    const size_t locator_width = narrow_locators ? sizeof(UInt32) : sizeof(UInt64);
    size_t bytes = 0;
    size_t rows_in_range = 0;
    for (size_t b = b0; b < b1; ++b)
    {
        const FillBlock & fill = build_blocks[b];
        rows_in_range += fill.rows;
        if (ctx.generic_mode)
        {
            size_t key_bytes = 0;
            for (const auto * column : fill.key_columns)
                key_bytes += column->byteSize();
            bytes += key_bytes + fill.rows * (sizeof(UInt64) * ctx.num_key_columns + locator_width);
        }
        else
        {
            size_t key_width = 0;
            for (size_t w : ctx.fixed_widths)
                key_width += w;
            bytes += fill.rows * (key_width + locator_width);
        }
        if (pass_bits.size() > 1)
            bytes += fill.rows * sizeof(UInt16);
    }

    /// The pass scratch of the duplicate rows this range brings: `tail_key` and `tail_ref` per row plus
    /// the per-key arrays, amortised to 16 bytes per duplicate row, estimated from the build's ratio.
    const size_t total_rows = accumulated_rows.load(std::memory_order_relaxed);
    if (total_rows > 0 && hll_estimate < static_cast<double>(total_rows))
    {
        const double duplicate_fraction = 1.0 - hll_estimate / static_cast<double>(total_rows);
        bytes += static_cast<size_t>(16.0 * duplicate_fraction * static_cast<double>(rows_in_range));
    }
    return bytes;
}

void PartitionedHashJoin::reduceWorkerHistogram()
{
    auto & ctx = *post_build_ctx;
    ctx.bucket_rows.assign(ctx.fanout, 0);
    for (size_t w = 0; w < ctx.workers; ++w)
        for (size_t p = 0; p < ctx.fanout; ++p)
            ctx.bucket_rows[p] += ctx.worker_hist[w * ctx.fanout + p];
}

void PartitionedHashJoin::resetWorkerHistogram(PostBuildContext & ctx)
{
    /// `resize_fill` only fills what it grows, so a reused same-sized array would histogram
    /// on top of the previous range's counts.
    ctx.worker_hist.clear();
    ctx.worker_hist.resize_fill(ctx.workers * ctx.fanout, 0);
}

void PartitionedHashJoin::preparePostBuildContext()
{
    if (post_build_ctx)
        return;

    post_build_ctx.reset(new PostBuildContext);
    auto & ctx = *post_build_ctx;
    ctx.workers = std::max<size_t>(1, std::min(num_threads, build_blocks.size()));
    chassert(!pass_bits.empty());
    ctx.multi_pass = pass_bits.size() > 1;
    ctx.route_bits = pass_bits.front();
    chassert(ctx.route_bits <= 15); /// the bucket ids are UInt16 and the drop bucket needs one more
    ctx.fanout = (1uz << ctx.route_bits) + 1;
    ctx.num_key_columns = build_blocks.front().key_columns.size();

    ctx.key_samples.reserve(ctx.num_key_columns);
    for (const auto * column : build_blocks.front().key_columns)
        ctx.key_samples.push_back(column->cloneEmpty());

    ctx.generic_mode = false;
    ctx.fixed_widths.resize(ctx.num_key_columns);
    for (size_t c = 0; c < ctx.num_key_columns; ++c)
    {
        const IColumn & column = *build_blocks.front().key_columns[c];
        if (column.isFixedAndContiguous())
            ctx.fixed_widths[c] = column.sizeOfValueIfFixed();
        else
            ctx.generic_mode = true;
    }

    ctx.starts.resize(ctx.fanout * ctx.workers);
    if (narrow_locators)
        ctx.locators32.resize(ctx.fanout);
    else
        ctx.locators.resize(ctx.fanout);
    if (ctx.multi_pass)
        ctx.routes.resize(ctx.fanout);
    if (ctx.generic_mode)
    {
        ctx.pieces.resize(ctx.num_key_columns);
        for (auto & column_pieces : ctx.pieces)
            column_pieces.resize(ctx.workers);
    }
    else
    {
        ctx.fixed_out.resize(ctx.num_key_columns);
        for (auto & column_out : ctx.fixed_out)
            column_out.resize(ctx.fanout);
        ctx.fixed_base.assign(ctx.num_key_columns, std::vector<char *>(ctx.fanout, nullptr));
    }
    ctx.worker_state.resize(ctx.workers);
    ctx.overflow.resize(partitions);
    ctx.claimed_per_partition.assign(partitions, 0);
    ctx.range_committed.assign(partitions, 0);

    post_build_pool = std::make_unique<ThreadPool>(
        CurrentMetrics::PartitionedHashJoinPoolThreads,
        CurrentMetrics::PartitionedHashJoinPoolThreadsActive,
        CurrentMetrics::PartitionedHashJoinPoolThreadsScheduled,
        /*max_threads_*/ ctx.workers,
        /*max_free_threads_*/ 0,
        /*queue_size_*/ ctx.workers);

    std::atomic<UInt64> hist_thread_us{0};
    if (ctx.multi_pass)
    {
        /// Exact per-partition counts need the full `bits` width, which is not the pass-1 histogram the
        /// scatter uses.
        const size_t saved_route_bits = ctx.route_bits;
        const size_t saved_fanout = ctx.fanout;
        ctx.route_bits = bits;
        ctx.fanout = partitions + 1;
        ctx.block_begin = 0;
        ctx.block_end = build_blocks.size();
        resetWorkerHistogram(ctx);
        runPostBuildWave(*post_build_pool, ctx.workers, [this, &ctx](size_t w) { histogramWorker(ctx, w); }, hist_thread_us);
        reduceWorkerHistogram();
        total_bucket_rows.assign(ctx.bucket_rows.begin(), ctx.bucket_rows.begin() + partitions);
        ctx.route_bits = saved_route_bits;
        ctx.fanout = saved_fanout;
        resetWorkerHistogram(ctx);
        ctx.bucket_rows.assign(ctx.fanout, 0);
        histogram_covers_full_build = false;
    }
    else
    {
        ctx.block_begin = 0;
        ctx.block_end = build_blocks.size();
        resetWorkerHistogram(ctx);
        runPostBuildWave(*post_build_pool, ctx.workers, [this, &ctx](size_t w) { histogramWorker(ctx, w); }, hist_thread_us);
        reduceWorkerHistogram();
        total_bucket_rows.assign(ctx.bucket_rows.begin(), ctx.bucket_rows.begin() + partitions);
        histogram_covers_full_build = true;
    }
    ProfileEvents::increment(ProfileEvents::PartitionedHashJoinBuildHistogramMicroseconds, hist_thread_us.load(std::memory_order_relaxed));

    stats.partition_row_counts = total_bucket_rows;
    ctx.partition_order.resize(partitions);
    for (size_t partition = 0; partition < partitions; ++partition)
        ctx.partition_order[partition] = static_cast<UInt32>(partition);
    std::sort(
        ctx.partition_order.begin(),
        ctx.partition_order.end(),
        [&](UInt32 a, UInt32 b) { return total_bucket_rows[a] > total_bucket_rows[b]; });

    /// Nothing is committed here; the owner that claims a partition commits its range.
    createSharedTable();

    measureGenericKeyBytes();
    UInt64 insertable = 0;
    for (UInt64 rows : total_bucket_rows)
        insertable += rows;
    const size_t arena_pred = predictedArenaBytes(insertable, post_build_plan == PostBuildPlan::Grouped);
    const size_t per_worker = arena_pred / std::max(ctx.workers, 1uz);
    chassert(build_arenas.empty());
    for (size_t w = 0; w < ctx.workers; ++w)
        emplaceSizedBuildArena(build_arenas, per_worker);
    emplaceSizedBuildArena(build_arenas, 0); /// the drain's
}

PartitionedHashJoin::PostBuildPlan PartitionedHashJoin::planPostBuild()
{
    if (max_bytes_before_external_join == 0 || delegate_mode)
    {
        post_build_plan = PostBuildPlan::Fits;
        return post_build_plan;
    }

    const size_t row_store = leaf_join->data->allocated_size + leaf_join->data->nullmaps_allocated_size;
    size_t routes = 0;
    for (const auto & fill : build_blocks)
        routes += fill.routes.allocated_bytes();
    measureGenericKeyBytes();

    if (bits == 0)
    {
        const size_t insertable = accumulated_rows.load(std::memory_order_relaxed);
        const size_t distinct = std::max(static_cast<size_t>(std::llround(hll_estimate)), 1uz);
        /// The single-partition path inserts straight from the stored blocks, so there is no transient
        /// to bound and grouping has nothing to do. Table and duplicate runs go through the shared
        /// helper so this verdict cannot drift from the fill-phase prediction.
        const size_t resident
            = row_store + routes + predictedTableAndArenaBytes(insertable, distinct, /*grouped=*/false) + generic_key_bytes;
        post_build_plan = resident <= max_bytes_before_external_join ? PostBuildPlan::Fits : PostBuildPlan::MustSpill;
        return post_build_plan;
    }

    preparePostBuildContext();

    UInt64 insertable = 0;
    for (UInt64 rows : total_bucket_rows)
        insertable += rows;

    /// What must be resident whatever the scatter schedule is.
    const size_t floor_bytes = row_store + routes + predictedArenaBytes(insertable, /*grouped=*/false);
    const size_t floor_bytes_grouped = row_store + routes + predictedArenaBytes(insertable, /*grouped=*/true);
    const size_t tables = ht_total_bytes;
    const size_t chunk_all = build_blocks.empty() ? 0 : chunkBytesForBlockRange(0, build_blocks.size());

    /// The ungrouped scatter does not hold the whole chunk alongside the whole table: the owner of a
    /// partition commits its range and frees that partition's chunk in the same claim, so the two trade
    /// off range by range and the peak sits at one end of the wave.
    const size_t leaves = std::max<size_t>(partitions, 1);
    const size_t peak_ungrouped = floor_bytes + std::max(chunk_all + tables / leaves, tables + chunk_all / leaves);

    /// Grouping holds the full table from the first range that touches every partition - which every
    /// realistic build does - and one range's chunk at a time. So it lowers the peak only while the chunk
    /// dominates the table; where the table dominates, grouping would ADD `chunk / g` on top of it and be
    /// strictly worse than the ungrouped scatter. The floor as the ranges get finer is one block's chunk.
    const size_t grouped_floor = floor_bytes_grouped + tables + (build_blocks.empty() ? 0 : chunkBytesForBlockRange(0, 1));

    if (peak_ungrouped <= max_bytes_before_external_join)
        post_build_plan = PostBuildPlan::Fits;
    else if (grouped_floor <= max_bytes_before_external_join && grouped_floor < peak_ungrouped)
        post_build_plan = PostBuildPlan::Grouped;
    else
        post_build_plan = PostBuildPlan::MustSpill;

    LOG_TRACE(
        log,
        "Post-build gate: budget {}, row store + routes + arena {}, table {}, full chunk {}; predicted peak without grouping "
        "{}, floor with grouping {} -> {}",
        ReadableSize(max_bytes_before_external_join),
        ReadableSize(floor_bytes),
        ReadableSize(tables),
        ReadableSize(chunk_all),
        ReadableSize(peak_ungrouped),
        ReadableSize(grouped_floor),
        post_build_plan == PostBuildPlan::Fits ? "ungrouped scatter"
            : post_build_plan == PostBuildPlan::Grouped ? "grouped scatter"
                                                        : "switch to grace");
    return post_build_plan;
}

void PartitionedHashJoin::runGroupStages(size_t block_begin, size_t block_end)
{
    auto & ctx = *post_build_ctx;
    ctx.block_begin = block_begin;
    ctx.block_end = block_end;
    ctx.refined = false;
    ctx.current_buckets = 0;
    ctx.refined_pieces.clear();
    ctx.partition_claim.store(0, std::memory_order_relaxed);

    /// A refine pass resizes the scatter containers to the final partition count. The next range's
    /// histogram / allocate / scatter stages expect the pass-1 layout again (`fanout` buckets,
    /// including the drop bucket).
    if (narrow_locators)
    {
        ctx.locators32.clear();
        ctx.locators32.resize(ctx.fanout);
    }
    else
    {
        ctx.locators.clear();
        ctx.locators.resize(ctx.fanout);
    }
    if (ctx.multi_pass)
    {
        ctx.routes.clear();
        ctx.routes.resize(ctx.fanout);
    }
    if (ctx.generic_mode)
    {
        ctx.pieces.clear();
        ctx.pieces.resize(ctx.num_key_columns);
        for (auto & column_pieces : ctx.pieces)
            column_pieces.resize(ctx.workers);
    }
    else
    {
        ctx.fixed_out.clear();
        ctx.fixed_out.resize(ctx.num_key_columns);
        for (auto & column_out : ctx.fixed_out)
            column_out.resize(ctx.fanout);
        ctx.fixed_base.assign(ctx.num_key_columns, std::vector<char *>(ctx.fanout, nullptr));
    }

    const bool reuse_histogram = histogram_covers_full_build && block_begin == 0 && block_end == build_blocks.size();
    histogram_covers_full_build = false;

    std::atomic<UInt64> hist_thread_us{0};
    std::atomic<UInt64> alloc_thread_us{0};
    std::atomic<UInt64> scatter_thread_us{0};
    std::atomic<UInt64> insert_thread_us{0};

    Stopwatch stage_watch;
    if (!reuse_histogram)
    {
        resetWorkerHistogram(ctx);
        ctx.bucket_rows.assign(ctx.fanout, 0);
        runPostBuildWave(*post_build_pool, ctx.workers, [this, &ctx](size_t w) { histogramWorker(ctx, w); }, hist_thread_us);
    }
    const UInt64 hist_wall_us = stage_watch.elapsedMicroseconds();

    stage_watch.restart();
    runPostBuildWave(*post_build_pool, ctx.workers, [this, &ctx](size_t w) { allocateWorker(ctx, w); }, alloc_thread_us);
    const UInt64 alloc_wall_us = stage_watch.elapsedMicroseconds();

    stage_watch.restart();
    runPostBuildWave(*post_build_pool, ctx.workers, [this, &ctx](size_t w) { scatterWorker(ctx, w); }, scatter_thread_us);
    const UInt64 scatter_wall_us = stage_watch.elapsedMicroseconds();

    std::atomic<UInt64> refine_thread_us{0};
    stage_watch.restart();
    if (ctx.multi_pass)
    {
        /// Freed before the refine passes, so those rows are neither scattered again nor held.
        const size_t drop = ctx.fanout - 1;
        if (narrow_locators)
            ctx.locators32[drop] = {};
        else
            ctx.locators[drop] = {};
        ctx.routes[drop] = {};
        if (!ctx.generic_mode)
            for (size_t c = 0; c < ctx.num_key_columns; ++c)
                ctx.fixed_out[c][drop].reset();

        ctx.current_buckets = drop;
        size_t bits_done = ctx.route_bits;
        for (size_t k = 1; k < pass_bits.size(); ++k)
        {
            refinePassWave(ctx, pass_bits[k], bits_done, refine_thread_us);
            bits_done += pass_bits[k];
        }
        chassert(bits_done == bits);
        chassert(ctx.current_buckets == partitions);
    }
    const UInt64 refine_wall_us = stage_watch.elapsedMicroseconds();

    stage_watch.restart();
    {
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::PartitionedHashJoinBuildMicroseconds);
        /// Null-key rows are never inserted. A refined build has no drop bucket left - it was freed
        /// before the refine passes.
        if (!ctx.refined)
        {
            if (narrow_locators)
                ctx.locators32[partitions] = {};
            else
                ctx.locators[partitions] = {};
            if (!ctx.generic_mode)
                for (size_t c = 0; c < ctx.num_key_columns; ++c)
                    ctx.fixed_out[c][partitions].reset();
        }
    }
    const UInt64 plan_wall_us = stage_watch.elapsedMicroseconds();

    /// The owner wave, then the barrier, the capacity guard and the serial drain. The drain must finish
    /// before the next group's wave: it writes wherever a walk wraps to, which is any owner's range.
    stage_watch.restart();
    runPostBuildWave(*post_build_pool, ctx.workers, [this, &ctx](size_t w) { ownerWaveWorker(ctx, w); }, insert_thread_us);
    const UInt64 insert_wall_us = stage_watch.elapsedMicroseconds();

    stage_watch.restart();
    UInt64 group_overflow = 0;
    for (const auto & overflow : ctx.overflow)
        group_overflow += overflow.rows();
    drainOverflow(ctx, checkCapacityGuard(ctx));
    const UInt64 drain_wall_us = stage_watch.elapsedMicroseconds();

    const auto to_ms = [](UInt64 us) { return static_cast<double>(us) / 1000.0; };
    LOG_TRACE(
        log,
        "Post-build stages for blocks [{}, {}), wall/thread ms: histogram {:.1f}/{:.1f}, chunk allocation {:.1f}/{:.1f}, scatter "
        "{:.1f}/{:.1f}, refine passes {:.1f}/{:.1f}, plan {:.1f}, owner inserts {:.1f}/{:.1f} (AMAC {}), drain {:.1f} for {} "
        "overflow rows",
        block_begin,
        block_end,
        to_ms(hist_wall_us),
        to_ms(hist_thread_us.load(std::memory_order_relaxed)),
        to_ms(alloc_wall_us),
        to_ms(alloc_thread_us.load(std::memory_order_relaxed)),
        to_ms(scatter_wall_us),
        to_ms(scatter_thread_us.load(std::memory_order_relaxed)),
        to_ms(refine_wall_us),
        to_ms(refine_thread_us.load(std::memory_order_relaxed)),
        to_ms(plan_wall_us),
        to_ms(insert_wall_us),
        to_ms(insert_thread_us.load(std::memory_order_relaxed)),
        amac_build_engaged ? "engaged" : "off",
        to_ms(drain_wall_us),
        group_overflow);

    ProfileEvents::increment(
        ProfileEvents::PartitionedHashJoinBuildHistogramMicroseconds,
        hist_thread_us.load(std::memory_order_relaxed) + alloc_thread_us.load(std::memory_order_relaxed));
    ProfileEvents::increment(
        ProfileEvents::PartitionedHashJoinBuildScatterMicroseconds,
        scatter_thread_us.load(std::memory_order_relaxed) + refine_thread_us.load(std::memory_order_relaxed));
    ProfileEvents::increment(
        ProfileEvents::PartitionedHashJoinBuildLeafMicroseconds,
        plan_wall_us + insert_thread_us.load(std::memory_order_relaxed) + drain_wall_us);
}

bool PartitionedHashJoin::postBuildPartitioned()
{
    if (!post_build_ctx)
        preparePostBuildContext();

    auto & ctx = *post_build_ctx;
    const bool grouped = post_build_plan == PostBuildPlan::Grouped;
    for (size_t w = 0; w < ctx.workers; ++w)
        ctx.worker_state[w].writer.emplace(build_arenas[w], grouped);
    ctx.drain_writer.emplace(build_arenas[ctx.workers], grouped);

    size_t groups = 0;
    size_t b = 0;
    while (b < build_blocks.size())
    {
        size_t end = b + 1;
        if (max_bytes_before_external_join == 0 || post_build_plan == PostBuildPlan::Fits)
        {
            end = build_blocks.size();
        }
        else
        {
            /// `getTotalByteCount` is actuals (row store, remaining routes, committed table, arenas).
            /// Uncommitted ranges and the still-unallocated duplicate runs are charged from the gate's
            /// predictions so the first range is not sized as if those bytes were free. They are
            /// allocated during the range, not before it.
            size_t used = getTotalByteCount();
            const size_t committed = shared_maps->getBufferSizeInBytes(leaf_join->data->type);
            if (ht_total_bytes > committed)
                used += ht_total_bytes - committed;
            size_t arena_actual = 0;
            for (const auto & arena : build_arenas)
                arena_actual += arena.allocatedBytes();
            UInt64 insertable = 0;
            for (UInt64 rows : total_bucket_rows)
                insertable += rows;
            const size_t arena_pred = predictedArenaBytes(insertable, post_build_plan == PostBuildPlan::Grouped);
            if (arena_pred > arena_actual)
                used += arena_pred - arena_actual;

            const size_t headroom = used < max_bytes_before_external_join ? max_bytes_before_external_join - used : 0;
            while (end < build_blocks.size() && chunkBytesForBlockRange(b, end + 1) <= headroom)
                ++end;
            /// A range is never empty: the loop has to make progress, and a single block's chunk is
            /// bounded by its row count, so the overshoot is at most that block. The threshold
            /// triggers spilling; `max_memory_usage` is the cap. This path is only for when the
            /// actuals drifted past the gate's prediction.
            const size_t chunk = chunkBytesForBlockRange(b, end);
            if (chunk > headroom)
                LOG_DEBUG(
                    log,
                    "Grouped scatter: one block's chunk ({}) exceeds the remaining headroom ({}); scattering it anyway, because a "
                    "range cannot be empty",
                    ReadableSize(chunk),
                    ReadableSize(headroom));
        }
        runGroupStages(b, end);
        b = end;
        ++groups;
    }

    /// Ranges no group touched are committed now, so the whole table is accounted and the probe never
    /// reads an uncommitted page.
    for (size_t partition = 0; partition < partitions; ++partition)
        if (!ctx.range_committed[partition])
            commitRange(partition);

    stats.scatter_groups = std::max<size_t>(groups, 1);
    ProfileEvents::increment(ProfileEvents::PartitionedHashJoinScatterGroups, stats.scatter_groups);

    post_build_pool.reset();

    bool all_values_unique = ctx.drain_all_unique;
    for (const auto & worker : ctx.worker_state)
    {
        all_values_unique &= worker.all_values_unique;
        stats.inserted_rows += worker.inserted_rows;
        accumulate(stats.owner_duplicates, worker.writer->stats());
    }
    stats.drain_duplicates = ctx.drain_writer->stats();
    stats.drain_claimed_keys = ctx.drain_claimed;
    stats.drain_appended_rows = ctx.drain_appended;
    publishTableSize(ctx);
    return all_values_unique;
}

void PartitionedHashJoin::commitRange(size_t partition)
{
    std::visit(
        [&](auto & shape_maps)
        {
            switch (leaf_join->data->type)
            {
#define M(TYPE) \
    case HashJoin::Type::TYPE: { \
        if constexpr (is_shared_join_table<typename decltype(shape_maps.TYPE)::element_type>) \
            shape_maps.TYPE->commitRange(partition); \
        break; \
    }
                APPLY_FOR_PARTITIONED_JOIN_VARIANTS(M)
#undef M
                default:
                    break;
            }
        },
        shared_maps->maps);
    post_build_ctx->range_committed[partition] = 1;
}

UInt64 PartitionedHashJoin::checkCapacityGuard(const PostBuildContext & ctx) const
{
    /// The cells the owners claimed plus the zero cell are distinct keys, so they are bounded by the
    /// sketch margin that sized the table (or by the exact row count above the saturation zone): a trip
    /// here is a derivation bug and must surface as an exception rather than a spinning drain. The
    /// overflow rows are NOT counted: a duplicated key whose walk reached its range end hands every one
    /// of its rows to the overflow, so their number bounds nothing. The drain guards each cell it
    /// claims instead (`InsertTarget::claimed_one` on a wrapping target), starting from this total.
    UInt64 claimed = 0;
    for (UInt64 c : ctx.claimed_per_partition)
        claimed += c;
    bool has_zero = false;
    std::visit(
        [&](auto & shape_maps)
        {
            switch (leaf_join->data->type)
            {
#define M(TYPE) \
    case HashJoin::Type::TYPE: { \
        using Table = typename decltype(shape_maps.TYPE)::element_type; \
        if constexpr (is_shared_join_table<Table>) \
            has_zero = shape_maps.TYPE->hasZero(); \
        break; \
    }
                APPLY_FOR_PARTITIONED_JOIN_VARIANTS(M)
#undef M
                default:
                    break;
            }
        },
        shared_maps->maps);
    const UInt64 total = claimed + (has_zero ? 1 : 0);
    const size_t max_fill = SharedJoinTable<UInt64, HashMapCell<UInt64, RowRef, HashCRC32<UInt64>>, HashCRC32<UInt64>, HashTableGrowerWithPrecalculation<>>::maxFillFor(size_degree);
    if (total > max_fill)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "PartitionedHashJoin: the shared hash table of {} cells received {} distinct keys, more than its max fill of {}; the "
            "size estimate that created it was too low",
            1uz << size_degree,
            total,
            max_fill);
    return total;
}

void PartitionedHashJoin::drainOverflow(PostBuildContext & ctx, UInt64 claimed_with_zero)
{
    const HashJoin::Type type = leaf_join->data->type;
    UInt64 drained = 0;
    size_t overflow_total = 0;
    for (const auto & buffer : ctx.overflow)
        overflow_total += buffer.rows();
    checkPassRowLimit(overflow_total, "the overflow rows of a build group");
    for (size_t partition = 0; partition < partitions; ++partition)
    {
        OverflowBuffer & overflow = ctx.overflow[partition];
        if (overflow.rows() == 0)
            continue;
        drained += overflow.rows();
        std::visit(
            [&](auto & shape_maps)
            {
                switch (type)
                {
#define M(TYPE) \
    case HashJoin::Type::TYPE: { \
        using Table = typename decltype(shape_maps.TYPE)::element_type; \
        if constexpr (is_shared_join_table<Table>) \
        { \
            Table & table = *shape_maps.TYPE; \
            InsertTarget<Table> target{ \
                .table = table, \
                .cells = table.cells(), \
                .scratch = ctx.drain_scratch, \
                .overflow = overflow, \
                .range_begin = 0, \
                .range_end = table.cellCount(), \
                .wrap = true, \
                .any_take_last_row = any_take_last_row, \
                .join = leaf_join.get()}; \
            /* The running distinct total, so `claimed_one` guards every drained claim against max fill. */ \
            target.claimed = claimed_with_zero; \
            drainPartitionOverflow(target, ctx.drain_appended); \
            const UInt64 newly_claimed = target.claimed - claimed_with_zero; \
            claimed_with_zero = target.claimed; \
            ctx.claimed_per_partition[partition] += newly_claimed; \
            ctx.drain_claimed += newly_claimed; \
            ctx.drain_all_unique = ctx.drain_all_unique && target.all_unique; \
        } \
        break; \
    }
                    APPLY_FOR_PARTITIONED_JOIN_VARIANTS(M)
#undef M
                    default:
                        break;
                }
            },
            shared_maps->maps);
    }
    finishTailScratch(ctx.drain_scratch, *ctx.drain_writer);
    stats.overflow_rows += drained;
}

void PartitionedHashJoin::histogramWorker(PostBuildContext & ctx, size_t worker) const
{
    UInt64 * hist = ctx.worker_hist.data() + worker * ctx.fanout;

    PaddedPODArray<UInt64> hist_lanes_mem;
    UInt64 * hist_lanes = nullptr;
    if (ctx.fanout <= ColumnsScatter::HIST_INTERLEAVE_MAX_FANOUT)
    {
        hist_lanes_mem.resize_fill(4 * ctx.fanout, 0);
        hist_lanes = hist_lanes_mem.data();
    }

    PaddedPODArray<UInt16> bucket_ids;
    const auto [begin, end] = ctx.blockStripe(worker);
    for (size_t b = begin; b < end; ++b)
    {
        const FillBlock & fill = build_blocks[b];
        bucket_ids.resize(fill.rows);
        deriveBucketIds(fill.routes, fill.skipData(), ctx.route_bits, ctx.fanout - 1, bucket_ids.data());
        ColumnsScatter::histogramPidChunk(bucket_ids.data(), fill.rows, hist, hist_lanes, ctx.fanout);
    }
    if (hist_lanes)
        ColumnsScatter::reduceHistogramLanes(hist, hist_lanes, ctx.fanout);
}

void PartitionedHashJoin::allocateWorker(PostBuildContext & ctx, size_t worker) const
{
    /// Fuses the parallel prefix sum over the per-worker histograms with one exact uninitialized
    /// allocation per (bucket, scattered column), leaving the scatter writes to first-touch the pages.
    const size_t buckets_begin = worker * ctx.fanout / ctx.workers;
    const size_t buckets_end = (worker + 1) * ctx.fanout / ctx.workers;
    for (size_t p = buckets_begin; p < buckets_end; ++p)
    {
        UInt64 running = 0;
        for (size_t w = 0; w < ctx.workers; ++w)
        {
            ctx.starts[p * ctx.workers + w] = running;
            running += ctx.worker_hist[w * ctx.fanout + p];
        }
        ctx.bucket_rows[p] = running;
        if (narrow_locators)
            ctx.locators32[p].resize_exact(running);
        else
            ctx.locators[p].resize_exact(running);
        if (ctx.multi_pass)
            ctx.routes[p].resize_exact(running);
        if (!ctx.generic_mode)
        {
            for (size_t c = 0; c < ctx.num_key_columns; ++c)
            {
                auto [column, raw] = ColumnsScatter::allocateUninitializedFixed(*ctx.key_samples[c], running);
                ctx.fixed_out[c][p] = std::move(column);
                ctx.fixed_base[c][p] = raw.data();
            }
        }
    }
}

void PartitionedHashJoin::scatterWorker(PostBuildContext & ctx, size_t worker)
{
    auto & state = ctx.worker_state[worker];
    const auto [begin, end] = ctx.blockStripe(worker);

    const size_t locator_width = narrow_locators ? sizeof(UInt32) : sizeof(UInt64);
    const bool locator_swwc = ctx.fanout >= ColumnsScatter::SWWC_MIN_FANOUT;
    state.locator_scratch.init(ctx.fanout, locator_swwc);
    if (narrow_locators)
        state.locator_piece32.resize(locator_piece_rows);
    else
        state.locator_piece.resize(locator_piece_rows);
    for (size_t p = 0; p < ctx.fanout; ++p)
    {
        const UInt64 start = ctx.starts[p * ctx.workers + worker];
        char * cursor = narrow_locators ? reinterpret_cast<char *>(ctx.locators32[p].data() + start)
                                        : reinterpret_cast<char *>(ctx.locators[p].data() + start);
        state.locator_scratch.seed(p, cursor);
    }

    /// Same layout as the locators, so a refine pass can derive its sub-bucket ids from them.
    const bool route_swwc = ctx.multi_pass && ctx.fanout >= ColumnsScatter::SWWC_MIN_FANOUT;
    if (ctx.multi_pass)
    {
        state.route_scratch.init(ctx.fanout, route_swwc);
        for (size_t p = 0; p < ctx.fanout; ++p)
        {
            const UInt64 start = ctx.starts[p * ctx.workers + worker];
            state.route_scratch.seed(p, reinterpret_cast<char *>(ctx.routes[p].data() + start));
        }
    }
    auto scatter_routes = [&](const FillBlock & fill, const UInt16 * bucket_ids)
    {
        ColumnsScatter::scatterPidChunk(
            sizeof(UInt16),
            bucket_ids,
            reinterpret_cast<const char *>(fill.routes.data()),
            fill.rows,
            route_swwc,
            state.route_scratch);
    };

    /// Derived once per block and shared by every scattered column of it, so every column's rows land
    /// in the per-bucket positions the histogram assigned.
    auto scatter_locators = [&](const FillBlock & fill, const UInt16 * bucket_ids)
    {
        for (size_t offset = 0; offset < fill.rows; offset += locator_piece_rows)
        {
            const size_t piece = std::min(locator_piece_rows, fill.rows - offset);
            const char * piece_data = nullptr;
            if (narrow_locators)
            {
                for (size_t j = 0; j < piece; ++j)
                    state.locator_piece32[j] = static_cast<UInt32>((fill.block_no << 16) | (offset + j));
                piece_data = reinterpret_cast<const char *>(state.locator_piece32.data());
            }
            else
            {
                for (size_t j = 0; j < piece; ++j)
                    state.locator_piece[j] = RowRef(fill.block_no, offset + j).encode();
                piece_data = reinterpret_cast<const char *>(state.locator_piece.data());
            }
            ColumnsScatter::scatterPidChunk(locator_width, bucket_ids + offset, piece_data, piece, locator_swwc, state.locator_scratch);
        }
    };

    auto release_block_inputs = [this](FillBlock & fill)
    {
        const size_t freed_route_bytes = fill.routes.allocated_bytes();
        fill.keys_holder.clear();
        fill.key_columns.clear();
        fill.null_map_holder.reset();
        fill.null_map = nullptr;
        fill.join_mask = JoinCommon::JoinMask();
        fill.skip_bytes = {};
        fill.routes = {};
        accumulated_bytes.fetch_sub(freed_route_bytes, std::memory_order_relaxed);
    };

    if (!ctx.generic_mode)
    {
        state.key_scratch.resize(ctx.num_key_columns);
        std::vector<bool> key_swwc(ctx.num_key_columns);
        for (size_t c = 0; c < ctx.num_key_columns; ++c)
        {
            key_swwc[c] = ctx.fanout >= ColumnsScatter::SWWC_MIN_FANOUT && ColumnsScatter::widthSupportsSwwc(ctx.fixed_widths[c]);
            state.key_scratch[c].init(ctx.fanout, key_swwc[c]);
            for (size_t p = 0; p < ctx.fanout; ++p)
                state.key_scratch[c].seed(p, ctx.fixed_base[c][p] + ctx.starts[p * ctx.workers + worker] * ctx.fixed_widths[c]);
        }

        /// Whole-block batches sized by `scatterBatchRowsTarget`, with the per-(column, bucket)
        /// cursors persisting across them and each batch's inputs dropped as soon as its last column
        /// is scattered, so the scattered side cycles memory instead of doubling it.
        const size_t batch_rows_target = ColumnsScatter::scatterBatchRowsTarget(ctx.fanout);
        std::vector<PaddedPODArray<UInt16>> batch_bucket_ids;
        size_t b = begin;
        while (b < end)
        {
            const size_t batch_begin = b;
            size_t batch_rows = 0;
            while (b < end && batch_rows < batch_rows_target)
            {
                batch_rows += build_blocks[b].rows;
                ++b;
            }
            batch_bucket_ids.resize(b - batch_begin);
            for (size_t i = batch_begin; i < b; ++i)
            {
                const FillBlock & fill = build_blocks[i];
                batch_bucket_ids[i - batch_begin].resize(fill.rows);
                deriveBucketIds(fill.routes, fill.skipData(), ctx.route_bits, ctx.fanout - 1, batch_bucket_ids[i - batch_begin].data());
            }
            for (size_t c = 0; c < ctx.num_key_columns; ++c)
                for (size_t i = batch_begin; i < b; ++i)
                    /// The kernel consumes `rows * width` bytes, which `getRawData` spans by
                    /// contract; the view's own `.size()` is never read.
                    ColumnsScatter::scatterPidChunk(
                        ctx.fixed_widths[c],
                        batch_bucket_ids[i - batch_begin].data(),
                        build_blocks[i].key_columns[c]->getRawData().data(), /// NOLINT(bugprone-suspicious-stringview-data-usage)
                        build_blocks[i].rows,
                        key_swwc[c],
                        state.key_scratch[c]);
            for (size_t i = batch_begin; i < b; ++i)
                scatter_locators(build_blocks[i], batch_bucket_ids[i - batch_begin].data());
            if (ctx.multi_pass)
                for (size_t i = batch_begin; i < b; ++i)
                    scatter_routes(build_blocks[i], batch_bucket_ids[i - batch_begin].data());
            for (size_t i = batch_begin; i < b; ++i)
                release_block_inputs(build_blocks[i]);
        }

        for (auto & scratch : state.key_scratch)
            scratch.drain();
        state.locator_scratch.drain();
        state.route_scratch.drain();
        return;
    }

    /// String, LowCardinality and exotic key columns: each worker scatters its stripe through
    /// `ColumnsScatter::scatter` into self-contained per-bucket pieces. Per-piece allocation is what
    /// satisfies the String kernel's overflow-15 contract, and worker-private pieces are what make
    /// the parallelism safe. The owner inserts consume them in worker order, matching the locator layout.
    std::vector<PaddedPODArray<UInt16>> stripe_bucket_ids(end - begin);
    std::vector<std::span<const UInt16>> bucket_id_spans(end - begin);
    for (size_t i = begin; i < end; ++i)
    {
        const FillBlock & fill = build_blocks[i];
        stripe_bucket_ids[i - begin].resize(fill.rows);
        deriveBucketIds(fill.routes, fill.skipData(), ctx.route_bits, ctx.fanout - 1, stripe_bucket_ids[i - begin].data());
        bucket_id_spans[i - begin] = {stripe_bucket_ids[i - begin].data(), fill.rows};
        scatter_locators(fill, stripe_bucket_ids[i - begin].data());
        if (ctx.multi_pass)
            scatter_routes(fill, stripe_bucket_ids[i - begin].data());
    }
    state.locator_scratch.drain();
    state.route_scratch.drain();

    std::vector<const IColumn *> sources(end - begin);
    for (size_t c = 0; c < ctx.num_key_columns; ++c)
    {
        if (begin == end)
        {
            /// A group with fewer blocks than workers leaves this worker nothing to scatter; the consumers
            /// still index its pieces by bucket, so they get empty columns of the right type.
            MutableColumns & pieces = ctx.pieces[c][worker];
            pieces.resize(ctx.fanout);
            for (size_t p = 0; p + 1 < ctx.fanout; ++p)
                pieces[p] = ctx.key_samples[c]->cloneEmpty();
            continue;
        }
        for (size_t i = begin; i < end; ++i)
            sources[i - begin] = build_blocks[i].key_columns[c];
        ctx.pieces[c][worker] = ColumnsScatter::scatter(sources, bucket_id_spans, ctx.fanout);
        /// Those rows are never inserted.
        ctx.pieces[c][worker][ctx.fanout - 1].reset();
    }
    for (size_t i = begin; i < end; ++i)
        release_block_inputs(build_blocks[i]);
}

void PartitionedHashJoin::refinePassWave(
    PostBuildContext & ctx, size_t refine_bits, size_t bits_done, std::atomic<UInt64> & stage_thread_us)
{
    /// Splits every group into `2^refine_bits` sub-buckets by the next MSB-first slice of its
    /// scattered route words, group-major, so after the last pass a row's partition is
    /// `route >> (16 - bits)` - the same partition a single-pass plan would give it. Groups are claimed
    /// dynamically because their sizes can be skewed, and each group's inputs are freed as they are
    /// consumed so the pass cycles memory rather than doubling the scattered side.
    const size_t groups = ctx.current_buckets;
    const size_t sub_fanout = 1uz << refine_bits;
    const size_t new_buckets = groups * sub_fanout;
    const bool last_pass = bits_done + refine_bits == bits;
    chassert(bits_done + refine_bits <= 16);
    const auto shift = static_cast<UInt32>(16 - bits_done - refine_bits);
    const auto mask = static_cast<UInt32>(sub_fanout - 1);

    std::vector<PaddedPODArray<UInt64>> new_locators;
    std::vector<PaddedPODArray<UInt32>> new_locators32;
    if (narrow_locators)
        new_locators32.resize(new_buckets);
    else
        new_locators.resize(new_buckets);
    std::vector<PaddedPODArray<UInt16>> new_routes;
    if (!last_pass)
        new_routes.resize(new_buckets);
    std::vector<MutableColumns> new_fixed;
    std::vector<MutableColumns> new_pieces;
    for (size_t c = 0; c < ctx.num_key_columns; ++c)
        (ctx.generic_mode ? new_pieces : new_fixed).emplace_back(new_buckets);
    std::vector<UInt64> new_bucket_rows(new_buckets, 0);

    std::atomic<size_t> next_group{0};

    auto worker_body = [&]
    {
        const size_t locator_width = narrow_locators ? sizeof(UInt32) : sizeof(UInt64);
        const bool swwc_fanout = sub_fanout >= ColumnsScatter::SWWC_MIN_FANOUT;
        ColumnsScatter::ScatterScratch scratch;
        scratch.init(sub_fanout, swwc_fanout);
        PaddedPODArray<UInt16> pids;
        PaddedPODArray<UInt32> hist(sub_fanout);
        std::vector<const IColumn *> sources;
        std::vector<std::span<const UInt16>> pid_spans;

        for (size_t g = next_group.fetch_add(1, std::memory_order_relaxed); g < groups;
             g = next_group.fetch_add(1, std::memory_order_relaxed))
        {
            const size_t n = ctx.bucket_rows[g];
            const UInt16 * group_routes = ctx.routes[g].data();
            const size_t out_base = g * sub_fanout;

            pids.resize(n);
            memset(hist.data(), 0, sub_fanout * sizeof(UInt32));
            for (size_t i = 0; i < n; ++i)
            {
                const auto p = static_cast<UInt16>((group_routes[i] >> shift) & mask);
                pids[i] = p;
                ++hist[p];
            }
            for (size_t p = 0; p < sub_fanout; ++p)
                new_bucket_rows[out_base + p] = hist[p];

            /// Both locator widths support write combining.
            scratch.setUseSwwc(swwc_fanout);
            for (size_t p = 0; p < sub_fanout; ++p)
            {
                char * cursor = nullptr;
                if (narrow_locators)
                {
                    new_locators32[out_base + p].resize_exact(hist[p]);
                    cursor = reinterpret_cast<char *>(new_locators32[out_base + p].data());
                }
                else
                {
                    new_locators[out_base + p].resize_exact(hist[p]);
                    cursor = reinterpret_cast<char *>(new_locators[out_base + p].data());
                }
                scratch.seed(p, cursor);
            }
            {
                const char * data = narrow_locators ? reinterpret_cast<const char *>(ctx.locators32[g].data())
                                                    : reinterpret_cast<const char *>(ctx.locators[g].data());
                ColumnsScatter::scatterPidChunk(locator_width, pids.data(), data, n, swwc_fanout, scratch);
                scratch.drain();
            }
            if (narrow_locators)
                ctx.locators32[g] = {};
            else
                ctx.locators[g] = {};

            /// Only needed when another refine pass follows.
            if (!last_pass)
            {
                scratch.setUseSwwc(swwc_fanout);
                for (size_t p = 0; p < sub_fanout; ++p)
                {
                    new_routes[out_base + p].resize_exact(hist[p]);
                    scratch.seed(p, reinterpret_cast<char *>(new_routes[out_base + p].data()));
                }
                ColumnsScatter::scatterPidChunk(
                    sizeof(UInt16), pids.data(), reinterpret_cast<const char *>(group_routes), n, swwc_fanout, scratch);
                scratch.drain();
            }
            ctx.routes[g] = {};

            if (!ctx.generic_mode)
            {
                for (size_t c = 0; c < ctx.num_key_columns; ++c)
                {
                    const size_t width = ctx.fixed_widths[c];
                    const bool use_swwc = swwc_fanout && ColumnsScatter::widthSupportsSwwc(width);
                    scratch.setUseSwwc(use_swwc);
                    const IColumn & sample = *ctx.fixed_out[c][g];
                    for (size_t p = 0; p < sub_fanout; ++p)
                    {
                        auto [column, raw] = ColumnsScatter::allocateUninitializedFixed(sample, hist[p]);
                        new_fixed[c][out_base + p] = std::move(column);
                        scratch.seed(p, raw.data());
                    }
                    ColumnsScatter::scatterPidChunk(
                        width,
                        pids.data(),
                        ctx.fixed_out[c][g]->getRawData().data(), /// NOLINT(bugprone-suspicious-stringview-data-usage)
                        n,
                        use_swwc,
                        scratch);
                    scratch.drain();
                    ctx.fixed_out[c][g].reset();
                }
            }
            else
            {
                /// The per-worker pieces on the first refine pass, the single refined piece
                /// afterwards. The pid spans slice the group's pid array worker-major, the same way
                /// the locator layout is built, so a row lands where its locator does.
                for (size_t c = 0; c < ctx.num_key_columns; ++c)
                {
                    sources.clear();
                    pid_spans.clear();
                    if (!ctx.refined)
                    {
                        for (size_t w = 0; w < ctx.workers; ++w)
                        {
                            sources.push_back(ctx.pieces[c][w][g].get());
                            pid_spans.emplace_back(pids.data() + ctx.starts[g * ctx.workers + w], ctx.worker_hist[w * ctx.fanout + g]);
                        }
                    }
                    else
                    {
                        sources.push_back(ctx.refined_pieces[c][g].get());
                        pid_spans.emplace_back(pids.data(), n);
                    }
                    MutableColumns outs = ColumnsScatter::scatter(sources, pid_spans, sub_fanout, {hist.data(), sub_fanout});
                    for (size_t p = 0; p < sub_fanout; ++p)
                        new_pieces[c][out_base + p] = std::move(outs[p]);
                    if (!ctx.refined)
                        for (size_t w = 0; w < ctx.workers; ++w)
                            ctx.pieces[c][w][g].reset();
                    else
                        ctx.refined_pieces[c][g].reset();
                }
            }
        }
    };

    try
    {
        for (size_t w = 0; w < ctx.workers; ++w)
            post_build_pool->scheduleOrThrow(
                [&worker_body, &stage_thread_us, thread_group = CurrentThread::getGroup()]
                {
                    ThreadGroupSwitcher switcher(thread_group, ThreadName::PARTITIONED_JOIN);
                    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::PartitionedHashJoinBuildMicroseconds);
                    Stopwatch stage_watch;
                    worker_body();
                    stage_thread_us.fetch_add(stage_watch.elapsedMicroseconds(), std::memory_order_relaxed);
                });
        post_build_pool->wait();
    }
    catch (...)
    {
        post_build_pool->wait();
        throw;
    }

    if (narrow_locators)
        ctx.locators32 = std::move(new_locators32);
    else
        ctx.locators = std::move(new_locators);
    ctx.routes = std::move(new_routes);
    if (!ctx.generic_mode)
        ctx.fixed_out = std::move(new_fixed);
    else
    {
        ctx.refined_pieces = std::move(new_pieces);
        ctx.pieces.clear();
    }
    ctx.bucket_rows = std::move(new_bucket_rows);
    ctx.current_buckets = new_buckets;
    ctx.refined = true;
}

void PartitionedHashJoin::ownerWaveWorker(PostBuildContext & ctx, size_t worker)
{
    auto & state = ctx.worker_state[worker];

    ColumnRawPtrs section_columns(ctx.num_key_columns);

    /// Largest first, and claimed dynamically, so skew cannot serialize the build behind a
    /// worker-to-partition affinity. Whatever a partition's rows, its owner commits its range on the
    /// first group that claims it, so the whole table is accounted by the time the build ends.
    while (true)
    {
        const UInt32 claim = ctx.partition_claim.fetch_add(1, std::memory_order_relaxed);
        if (claim >= partitions)
            break;
        const UInt32 partition = ctx.partition_order[claim];

        if (!ctx.range_committed[partition])
            commitRange(partition);

        const UInt64 partition_rows = ctx.bucket_rows[partition];
        checkPassRowLimit(partition_rows, "a partition's rows in one build group");
        auto release_chunk = [&]
        {
            if (narrow_locators)
                ctx.locators32[partition] = {};
            else
                ctx.locators[partition] = {};
            if (!ctx.generic_mode)
            {
                for (size_t c = 0; c < ctx.num_key_columns; ++c)
                    ctx.fixed_out[c][partition].reset();
            }
            else if (ctx.refined)
            {
                for (size_t c = 0; c < ctx.num_key_columns; ++c)
                    ctx.refined_pieces[c][partition].reset();
            }
            else
            {
                for (size_t c = 0; c < ctx.num_key_columns; ++c)
                    for (size_t piece_worker = 0; piece_worker < ctx.workers; ++piece_worker)
                        ctx.pieces[c][piece_worker][partition].reset();
            }
        };

        if (partition_rows == 0)
        {
            release_chunk();
            continue;
        }

        if (!ctx.generic_mode)
        {
            for (size_t c = 0; c < ctx.num_key_columns; ++c)
                section_columns[c] = ctx.fixed_out[c][partition].get();
            insertPartitionSection(
                ctx,
                worker,
                partition,
                section_columns,
                partition_rows,
                narrow_locators ? nullptr : ctx.locators[partition].data(),
                narrow_locators ? ctx.locators32[partition].data() : nullptr,
                /*block_no=*/0,
                /*skip_bytes=*/nullptr);
        }
        else if (ctx.refined)
        {
            /// After the refine passes there is one piece per key column, aligned with the
            /// partition's whole locator array.
            for (size_t c = 0; c < ctx.num_key_columns; ++c)
                section_columns[c] = ctx.refined_pieces[c][partition].get();
            insertPartitionSection(
                ctx,
                worker,
                partition,
                section_columns,
                partition_rows,
                narrow_locators ? nullptr : ctx.locators[partition].data(),
                narrow_locators ? ctx.locators32[partition].data() : nullptr,
                /*block_no=*/0,
                /*skip_bytes=*/nullptr);
        }
        else
        {
            /// A partition's pieces in worker order are exactly its locator layout.
            for (size_t piece_worker = 0; piece_worker < ctx.workers; ++piece_worker)
            {
                const size_t piece_rows = ctx.worker_hist[piece_worker * ctx.fanout + partition];
                if (piece_rows == 0)
                    continue;
                for (size_t c = 0; c < ctx.num_key_columns; ++c)
                    section_columns[c] = ctx.pieces[c][piece_worker][partition].get();
                const UInt64 piece_start = ctx.starts[partition * ctx.workers + piece_worker];
                insertPartitionSection(
                    ctx,
                    worker,
                    partition,
                    section_columns,
                    piece_rows,
                    narrow_locators ? nullptr : ctx.locators[partition].data() + piece_start,
                    narrow_locators ? ctx.locators32[partition].data() + piece_start : nullptr,
                    /*block_no=*/0,
                    /*skip_bytes=*/nullptr);
            }
        }

        /// The pass's duplicates become runs while the partition's cells are still warm.
        finishTailScratch(state.tail_scratch, *state.writer);
        state.inserted_rows += partition_rows;
        ProfileEvents::increment(ProfileEvents::PartitionedHashJoinLeafRows, partition_rows);

        /// Released as soon as they are consumed, so the table replaces the chunks rather than
        /// coexisting with them.
        release_chunk();
    }
}

void PartitionedHashJoin::PostBuildContextDeleter::operator()(PostBuildContext * ctx) const
{
    delete ctx;
}

PartitionedHashJoin::~PartitionedHashJoin()
{
    /// Defined here because `post_build_ctx` holds a `PostBuildContext` that is complete only in
    /// this translation unit.
    /// Explicit, because members are otherwise destroyed after the body and outside the timer.
    /// Order matters: cells point into the arenas and the row store, so the table goes first.
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::PartitionedHashJoinTeardownMicroseconds);

    post_build_ctx.reset();
    post_build_pool.reset();
    shared_maps.reset();
    build_arenas.clear();
    leaf_join.reset();
    probe_scratch_pool.clear();
    for (auto & slot : probe_scratch_slots)
        delete slot.load(std::memory_order_acquire);
}

}
