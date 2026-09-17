#include <Interpreters/PartitionedHashJoin/HashJoinClause.h>

#include <Columns/ColumnsScatter.h>
#include <Interpreters/HashJoin/HashJoinMethodsImpl.h>
#include <Interpreters/HashJoin/JoinUsedFlags.h>
#include <Interpreters/HashJoin/KeyGetter.h>
#include <Interpreters/PartitionedHashJoin/AmacRing.h>
#include <Interpreters/PartitionedHashJoin/JoinRouteHashing.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/joinDispatch.h>
#include <base/getL1CacheSize.h>
#include <base/getL2CacheSize.h>
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
#include <base/unaligned.h>

#include <fmt/ranges.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <deque>
#include <limits>
#include <new>
#include <vector>

namespace ProfileEvents
{
extern const Event HashJoinPartitionedBuildMicroseconds;
extern const Event HashJoinPartitionedBuildHistogramMicroseconds;
extern const Event HashJoinPartitionedBuildScatterMicroseconds;
extern const Event HashJoinPartitionedBuildInsertMicroseconds;
extern const Event HashJoinInsertedRows;
extern const Event HashJoinScatterGroups;
extern const Event HashJoinTableResizes;
}

namespace CurrentMetrics
{
extern const Metric HashJoinPostBuildThreads;
extern const Metric HashJoinPostBuildThreadsActive;
extern const Metric HashJoinPostBuildThreadsScheduled;
}

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LIMIT_EXCEEDED;
extern const int LOGICAL_ERROR;
extern const int UNSUPPORTED_JOIN_KEYS;
}

namespace
{

/// The saved routes are 16 bits. The per-row bucket ids of the scatter are 16 bits too.
/// One extra drop bucket sits past the partitions. A plan can address at most 2^15 partitions.
constexpr size_t max_plan_bits = 15;

constexpr size_t locator_piece_rows = 32768; /// locator synthesis scratch stays L2-resident

size_t ceilDiv(size_t a, size_t b)
{
    chassert(b > 0);
    return a / b + (a % b != 0);
}

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

    size_t allocatedBytes() const { return keys.allocated_bytes() + hashes.allocated_bytes() + refs.allocated_bytes(); }

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
        return unalignedLoad<Key>(keys.data() + i * sizeof(Key));
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

/// A later row of a key: `RowRef` keeps the first row, or the last under `any_take_last_row`;
/// `RowRefList` defers to the pass's finish through the scratch.
template <typename Mapped>
ALWAYS_INLINE void
appendRowToMapped(Mapped & mapped, UInt64 ref, PassScratch & scratch, bool any_take_last_row, bool & all_unique, UInt32 bucket)
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
        appendRow(mapped, ref, bucket, scratch);
    }
}

/// What one section insert into the shared table works with. Shared by the sequential loop, the AMAC
/// policy and the drain, so the three cannot diverge on the state machine.
template <typename Table>
struct InsertTarget
{
    using Cell = typename Table::cell_type;
    using Mapped = typename Table::mapped_type;
    static constexpr bool mapped_asof = std::is_same_v<Mapped, AsofRowRefs>;

    Table & table;
    Cell * cells;
    PassScratch & scratch;
    OverflowBuffer & overflow;
    /// Where this pass stops writing, and whether the walk wraps at the buffer end (single partition,
    /// one writer) or hands rows at `range_end` to the overflow (parallel owners).
    size_t range_end = 0;
    bool wrap = false;
    bool any_take_last_row = false;
    /// ASOF: the inequality column of the stored block being inserted, and its number.
    const IColumn * asof_column = nullptr;
    UInt32 asof_block_no = 0;
    const HashJoin * join = nullptr;

    HashJoinClause * owner = nullptr;
    /// Distinct keys claimed so far, per partition. `foldClaimed` adds this target's new claims to
    /// slot `partition`, and to `drain_claimed` when the target is the drain.
    std::vector<UInt64> * claimed_per_partition = nullptr;
    UInt64 * drain_claimed = nullptr;
    SpanWriter * writer = nullptr;
    size_t * scratch_high_water = nullptr;
    size_t partition = 0;
    UInt64 fold_base = 0;
    UInt64 claimed = 0;

    bool all_unique = true;
    /// The single fill thread sized its table from a hint that may be low. It grows at the load-factor
    /// bound as `hash` does. The post-build inserts grow only when the last free cell is claimed.
    bool grow_at_max_fill = false;
    /// `growBound()`, cached so the per-row grow check below stays a register compare; refreshed by
    /// `growBeforeLastFreeCell` after a grow. A stale smaller value only costs an extra exact re-check there.
    size_t grow_threshold = 0;

    size_t growBound() const { return grow_at_max_fill ? table.maxFill() : table.cellCount() - 1; }

    ALWAYS_INLINE void claimOne() { ++claimed; }

    /// The cheap form of the grow check; `growBeforeLastFreeCell` re-checks exactly and grows.
    ALWAYS_INLINE void maybeGrowBeforeClaim()
    {
        if (wrap && owner && claimed >= grow_threshold)
            owner->growBeforeLastFreeCell(*this);
    }

    void foldClaimed()
    {
        if (!claimed_per_partition)
            return;
        const UInt64 delta = claimed - fold_base;
        (*claimed_per_partition)[partition] += delta;
        if (drain_claimed)
            *drain_claimed += delta;
        fold_base = claimed;
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

    ALWAYS_INLINE void appendLater(Mapped & mapped, UInt64 ref, size_t row, UInt32 bucket)
    {
        if constexpr (mapped_asof)
        {
            all_unique = false;
            mapped->insert(*asof_column, asof_block_no, row);
        }
        else
            appendRowToMapped(mapped, ref, scratch, any_take_last_row, all_unique, bucket);
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
        else if constexpr (std::is_same_v<Mapped, RowRefList>)
        {
            all_unique = false;
            appendRowZero(table.zeroValue()->getMapped(), ref, scratch);
        }
        else
            appendLater(table.zeroValue()->getMapped(), ref, row, /*bucket=*/0);
    }

    /// The owner walk from the home cell. Returns false when the row reached the range end and was
    /// handed to the overflow. Wrapping targets check for the last free cell before this walk.
    template <typename KeyHolder>
    ALWAYS_INLINE bool insertFromHome(KeyHolder && key_holder, size_t hash, UInt64 ref, size_t row)
    {
        const auto & key = keyHolderGetKey(key_holder);
        maybeGrowBeforeClaim();
        size_t pos = table.place(hash);
        chassert(pos < range_end && (wrap || pos >= table.rangeBegin(partition)));
        while (true)
        {
            Cell * cell = cells + pos;
            if (table.isEmptyCell(cell))
            {
                table.claim(pos, key_holder, hash);
                initFirst(cell->getMapped(), ref, row);
                claimOne();
                return true;
            }
            if (table.keyEquals(cell, key, hash))
            {
                appendLater(cell->getMapped(), ref, row, static_cast<UInt32>(pos));
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
            target.claimOne();
            return AmacStepResult::Done;
        }
        if (target.table.keyEquals(cell, key, hash))
        {
            target.appendLater(cell->getMapped(), refWordAt(row), row, static_cast<UInt32>(pos));
            return AmacStepResult::Done;
        }
        size_t next_pos = 0;
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

/// As in `createKeyGetter`: the ASOF getter excludes the inequality column, which is the trailing key
/// column.
template <typename KeyGetter, bool mapped_asof>
KeyGetter makeSectionKeyGetter(const ColumnRawPtrs & key_columns, const Sizes & key_sizes)
{
    if constexpr (mapped_asof)
    {
        ColumnRawPtrs equi_columns(key_columns.begin(), key_columns.end() - 1);
        Sizes equi_sizes(key_sizes.begin(), key_sizes.end() - 1);
        return KeyGetter(equi_columns, equi_sizes, nullptr);
    }
    else
        return KeyGetter(key_columns, key_sizes, nullptr);
}

/// Inserts one compact section into the shared table on behalf of the owner of `target.range_end`.
/// Semantics match `insertFromBlockImplTypeCase`: one hash per build row, then the value shape's own append.
/// The recorded ref comes from the scattered locator column, 8-byte encoded or 4-byte packed.
/// On the single-partition path it is `RowRef(block_no, i)`, with `skip_bytes` excluding rows that
/// must not be inserted.
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

    /// The ASOF value sits at the row's own index in the trailing key column. This only works where
    /// the compact index is the stored row. That is why ASOF plans stay single-partition.
    if constexpr (mapped_asof)
    {
        if (locators || narrow_locators || !target.wrap)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ASOF inserts require the single-partition build plan");
        target.asof_column = key_columns.back();
        target.asof_block_no = block_no;
    }

    auto key_getter = makeSectionKeyGetter<KeyGetter, mapped_asof>(key_columns, key_sizes);

    /// The ring replaces the sequential loop once the caller has decided the cell misses dominate
    /// and the section is long enough to amortize prime and drain. ASOF stays sequential: appending
    /// to a per-key sorted lookup is not a one-cell fused action.
    if constexpr (!mapped_asof && amac_join_supported<KeyGetter, Table>)
    {
        if (use_amac && !target.wrap && rows >= amac_min_rows && rows < amac_inactive_row)
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

/// The direct-index maps (`key8`, `key16`) have no ranges and no collisions. The standard `emplace`
/// applies. Only the duplicate layout is shared with the partitioned build. ASOF keys of these widths
/// append to the per-key sorted lookup exactly as on the shared path. `HashJoin` picks the map by the
/// equi-key width, so `UInt16` keys of an ASOF join land here.
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
    PassScratch & scratch,
    bool any_take_last_row,
    UInt64 & claimed,
    bool & all_unique)
{
    using Mapped = typename Table::mapped_type;
    constexpr bool mapped_asof = std::is_same_v<Mapped, AsofRowRefs>;

    /// The ASOF value is read at the row's own index in the trailing key column. The stored row must
    /// therefore be the compact index (the single-partition plan). The getter excludes the inequality column.
    const IColumn * asof_column [[maybe_unused]] = nullptr;
    if constexpr (mapped_asof)
    {
        if (locators || narrow_locators)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ASOF inserts require the single-partition build plan");
        asof_column = key_columns.back();
    }
    auto key_getter = makeSectionKeyGetter<KeyGetter, mapped_asof>(key_columns, key_sizes);

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
                appendRowToMapped(mapped, ref, scratch, any_take_last_row, all_unique, static_cast<UInt32>(emplace_result.getKey()));
        }
    }
}

/// Serial drain of one partition's overflow. The walk starts at the home cell and uses the global mask.
/// A row here has no cell of its key inside its owner's range. Otherwise its owner would have appended
/// to that cell. The row either claims the first empty cell beyond the range, or appends to a cell
/// an earlier group or an earlier drained row created.
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
        target.maybeGrowBeforeClaim();
        size_t pos = target.table.place(hash);
        while (true)
        {
            auto * cell = target.cells + pos;
            if (target.table.isEmptyCell(cell))
            {
                /// The key was persisted when it was handed off; a plain key needs no holder.
                target.table.claim(pos, key, hash);
                target.initFirst(cell->getMapped(), ref, 0);
                target.claimOne();
                break;
            }
            if (target.table.keyEquals(cell, key, hash))
            {
                target.appendLater(cell->getMapped(), ref, 0, static_cast<UInt32>(pos));
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
                    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::HashJoinPartitionedBuildMicroseconds);
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

/// Runs `f(table)` on the shared table of the active map type. The direct-index maps (`key8`, `key16`)
/// have no shared table and are skipped.
template <typename F>
void forHashJoinTable(HashJoinTableMaps & maps, HashJoin::Type type, F && f)
{
    std::visit(
        [&](auto & shape_maps)
        {
            switch (type)
            {
#define M(TYPE) \
    case HashJoin::Type::TYPE: \
        if constexpr (is_hash_join_table<typename decltype(shape_maps.TYPE)::element_type>) \
            f(*shape_maps.TYPE); \
        break;
                APPLY_FOR_PARTITIONED_JOIN_VARIANTS(M)
#undef M
                default:
                    break;
            }
        },
        maps.maps);
}
}

/// The stages communicate through exact per-bucket offsets. Bucket `p` holds worker `w`'s stripe at
/// `[starts[p * workers + w], + worker_hist[w][p])`, in worker order. The last bucket is the drop
/// bucket. Null-key and ON-filtered rows land there. They are scattered like any other rows and are
/// freed before the inserts. A partition only ever sees insertable rows.
struct HashJoinClause::PostBuildContext
{
    size_t workers = 0;
    size_t fanout = 0; /// pass-1 partitions + 1 (the drop bucket); == partitions + 1 on single-pass plans
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
        std::optional<SpanWriter> writer;
        PassScratch scratch;
        size_t scratch_used_high_water = 0;
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
    std::optional<SpanWriter> drain_writer;
    PassScratch drain_scratch;
    size_t drain_scratch_used_high_water = 0;
    UInt64 drain_claimed = 0;
    UInt64 drain_appended = 0;
    bool drain_all_unique = true;
    UInt64 rehash_listed = 0;

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

    UInt64 insertedRows() const
    {
        UInt64 rows = 0;
        for (const auto & worker : worker_state)
            rows += worker.inserted_rows;
        return rows;
    }

    /// The pass-1 layout of the per-bucket containers: `fanout` buckets including the drop bucket. A
    /// refine pass leaves them indexed by final partition, so every block range starts by resetting them.
    void resetScatterContainers(bool narrow)
    {
        if (narrow)
        {
            locators32.clear();
            locators32.resize(fanout);
        }
        else
        {
            locators.clear();
            locators.resize(fanout);
        }
        if (multi_pass)
        {
            routes.clear();
            routes.resize(fanout);
        }
        if (generic_mode)
        {
            pieces.clear();
            pieces.resize(num_key_columns);
            for (auto & column_pieces : pieces)
                column_pieces.resize(workers);
        }
        else
        {
            fixed_out.clear();
            fixed_out.resize(num_key_columns);
            for (auto & column_out : fixed_out)
                column_out.resize(fanout);
            fixed_base.assign(num_key_columns, std::vector<char *>(fanout, nullptr));
        }
    }
};

HashJoinClause::HashJoinClause(
    HashJoin & hash_join_,
    const TableJoin & table_join,
    bool any_take_last_row_,
    size_t num_threads_,
    size_t max_bytes_before_external_join_,
    std::vector<FillBlock> & build_blocks_,
    std::atomic<size_t> & accumulated_bytes_,
    LoggerPtr log_)
    : hash_join(hash_join_)
    , any_take_last_row(any_take_last_row_)
    , num_threads(num_threads_)
    , max_bytes_before_external_join(max_bytes_before_external_join_)
    , grow_budget(max_bytes_before_external_join_)
    , build_blocks(build_blocks_)
    , accumulated_bytes(accumulated_bytes_)
    , maps_variant_index(hash_join.data->maps.empty() ? 1 : hash_join.data->maps.front().index())
    , max_fanout_per_pass(table_join.partitionedHashJoinMaxFanoutPerPass())
    , cap_partitions_by_l1_descriptors(table_join.partitionedHashJoinCapPartitionsByL1Descriptors())
    , parallel_hash_join_threshold(table_join.parallelHashJoinThreshold())
    , log(std::move(log_))
{
    /// A ceiling above 2^15 would let a 16-bit plan wrap the drop bucket onto partition 0 and insert
    /// the skipped rows there; see `max_plan_bits`.
    if (max_fanout_per_pass < 2 || max_fanout_per_pass > 32768)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Setting partitioned_hash_join_max_fanout_per_pass must be between 2 and 32768, got {}",
            max_fanout_per_pass);
}

HashJoinClause::~HashJoinClause() = default;

void HashJoinClause::computeRoutes(FillBlock & fill, DenseHyperLogLog & sketch) const
{
    /// Skipped rows are not inserted and do not reach the sketch, but their routes are still written:
    /// the scatter reads them. ASOF hashes the equi-key prefix only.
    const size_t rows = fill.rows;
    fill.routes.resize_exact(rows);
    const Sizes & key_sizes = hash_join.key_sizes[0];
    if (hash_join.getStrictness() == JoinStrictness::Asof)
    {
        ColumnRawPtrs equi_columns(fill.key_columns.begin(), fill.key_columns.end() - 1);
        Sizes equi_sizes(key_sizes.begin(), key_sizes.end() - 1);
        computeJoinRoutesForFill(hash_join.data->type, equi_columns, equi_sizes, rows, fill.skipData(), fill.routes.data(), sketch);
    }
    else
        computeJoinRoutesForFill(hash_join.data->type, fill.key_columns, key_sizes, rows, fill.skipData(), fill.routes.data(), sketch);
}

bool HashJoinClause::postBuild(size_t rows)
{
    if (bits == 0)
    {
        /// Single-partition has no histogram or scatter stage - every row is inserted straight from the
        /// stored blocks - so all of it charges to the insert sub-phase.
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::HashJoinPartitionedBuildMicroseconds);
        ProfileEventTimeIncrement<Microseconds> leaf_watch(ProfileEvents::HashJoinPartitionedBuildInsertMicroseconds);
        return postBuildSinglePartition(rows);
    }
    return postBuildPartitioned();
}

void HashJoinClause::releaseBuildScratch()
{
    post_build_ctx.reset();
    post_build_pool.reset();
}

void HashJoinClause::releaseTable()
{
    releaseBuildScratch();
    table_maps.reset();
    build_arenas.clear();
}

size_t HashJoinClause::tableAndArenaBytes() const
{
    size_t res = 0;
    if (table_maps)
        res += table_maps->getBufferSizeInBytes(hash_join.data->type);
    for (const auto & arena : build_arenas)
        res += arena.allocatedBytes();
    return res;
}

HashJoinClause::BuildStats HashJoinClause::buildStats() const
{
    BuildStats res = stats;
    res.bits = bits;
    res.partitions = partitions;
    res.pass_bits = pass_bits;
    res.hll_estimate = hll_estimate;
    res.ht_total_bytes = ht_total_bytes;
    res.amac_build_engaged = amac_build_engaged;
    return res;
}

void HashJoinClause::decideAmacEngagement()
{
    /// The same heuristics that enable the standard loops' software prefetch: the user toggle, plus
    /// a table larger than L2. Below that threshold the cell reads hit anyway. Pipelining them then
    /// costs more than it saves.
    amac_build_engaged = amac_enabled && bits > 0 && hash_join.enableSoftwarePrefetch() && ht_total_bytes > getMinBytesForPrefetchInJoin();
}

void HashJoinClause::insertPartitionSection(
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
    const Sizes & key_sizes = hash_join.key_sizes[0];
    const bool enable_prefetch = hash_join.enableSoftwarePrefetch();
    auto & state = ctx.worker_state[worker];
    Arena & arena = build_arenas[worker];
    const bool wrap = partition == single_partition;
    const size_t slot = wrap ? 0 : partition;
    OverflowBuffer & overflow = ctx.overflow[slot];
    UInt64 & claimed = ctx.claimed_per_partition[slot];

    std::visit(
        [&](auto & shape_maps)
        {
            switch (hash_join.data->type)
            {
#define M(TYPE) \
    case HashJoin::Type::TYPE: { \
        using Table = typename decltype(shape_maps.TYPE)::element_type; \
        using KeyGetter = typename KeyGetterForType<HashJoin::Type::TYPE, Table, /*use_offset=*/false>::Type; \
        Table & table = *shape_maps.TYPE; \
        if constexpr (is_hash_join_table<Table>) \
        { \
            InsertTarget<Table> target{ \
                .table = table, \
                .cells = table.cells(), \
                .scratch = state.scratch, \
                .overflow = overflow, \
                .range_end = wrap ? table.cellCount() : table.rangeEnd(partition), \
                .wrap = wrap, \
                .any_take_last_row = any_take_last_row, \
                .join = &hash_join, \
                .owner = this, \
                .claimed_per_partition = &ctx.claimed_per_partition, \
                .drain_claimed = nullptr, \
                .writer = &*state.writer, \
                .scratch_high_water = &state.scratch_used_high_water, \
                .partition = slot, \
                .fold_base = claimed, \
                .claimed = claimed, \
                .grow_at_max_fill = grow_at_max_fill}; \
            target.grow_threshold = target.growBound(); \
            insertSectionShared<KeyGetter, Table>( \
                target, \
                key_columns, \
                key_sizes, \
                rows, \
                locators, \
                narrow_locators_data, \
                block_no, \
                skip_bytes, \
                arena, \
                enable_prefetch, \
                amac_build_engaged && !wrap); \
            claimed = target.claimed; \
            state.all_values_unique = state.all_values_unique && target.all_unique; \
        } \
        else \
        { \
            insertSectionFixed<KeyGetter, Table>( \
                table, \
                hash_join, \
                key_columns, \
                key_sizes, \
                rows, \
                locators, \
                narrow_locators_data, \
                block_no, \
                skip_bytes, \
                arena, \
                state.scratch, \
                any_take_last_row, \
                claimed, \
                state.all_values_unique); \
        } \
        break; \
    }
                APPLY_FOR_PARTITIONED_JOIN_VARIANTS(M)
#undef M
                default:
                    throw Exception(
                        ErrorCodes::UNSUPPORTED_JOIN_KEYS,
                        "Unsupported JOIN keys for the partitioned join (type: {})",
                        hash_join.data->type);
            }
        },
        table_maps->maps);
}

size_t HashJoinClause::finishPassScratch(PassScratch & scratch, SpanWriter & writer)
{
    if (scratch.empty())
        return 0;
    const size_t used = scratch.usedBytes();
    const HashJoin::Type type = hash_join.data->type;
    std::visit(
        [&](auto & shape_maps)
        {
            switch (type)
            {
#define M(TYPE) \
    case HashJoin::Type::TYPE: { \
        using Table = typename decltype(shape_maps.TYPE)::element_type; \
        Table & table = *shape_maps.TYPE; \
        if constexpr (std::is_same_v<typename Table::mapped_type, RowRefList>) \
        { \
            if constexpr (is_hash_join_table<Table>) \
            { \
                RowRefList * zero = scratch.zero_items.empty() ? nullptr : &table.zeroValue()->getMapped(); \
                writer.finish(scratch, [&](UInt32 bucket) -> RowRefList & { return table.cells()[bucket].getMapped(); }, zero); \
            } \
            else \
            { \
                writer.finish(scratch, [&](UInt32 bucket) -> RowRefList & { return table.data()[bucket].getMapped(); }); \
            } \
        } \
        else \
        { \
            throw Exception(ErrorCodes::LOGICAL_ERROR, "PartitionedHashJoin: duplicate scratch on a map that does not store lists"); \
        } \
        break; \
    }
                APPLY_FOR_PARTITIONED_JOIN_VARIANTS(M)
#undef M
                default:
                    throw Exception(
                        ErrorCodes::UNSUPPORTED_JOIN_KEYS,
                        "Unsupported JOIN keys for the partitioned join (type: {})",
                        hash_join.data->type);
            }
        },
        table_maps->maps);
    return used;
}

template <typename Target>
bool HashJoinClause::growBeforeLastFreeCell(Target & target)
{
    if (target.claimed < target.growBound())
        return false;
    chassert(target.writer);
    const size_t used = finishPassScratch(target.scratch, *target.writer);
    if (target.scratch_high_water)
        *target.scratch_high_water = std::max(*target.scratch_high_water, used);
    target.foldClaimed();
    /// At the last free cell the grow must happen. At the load-factor bound it may be refused under a
    /// tight budget. The walk then goes on until the table is full.
    const size_t cells_before = target.table.cellCount();
    const bool table_full = target.claimed >= cells_before - 1;
    grow(target.claimed, target.claimed + 1, table_full ? GrowReason::LastFreeCell : GrowReason::LoadFactor);
    target.cells = target.table.cells();
    target.range_end = target.table.cellCount();
    target.grow_threshold = target.table.cellCount() == cells_before ? cells_before - 1 : target.growBound();
    return true;
}

/// Read only between block ranges and at a grow, when every scattered chunk has been released, so the
/// chunks are not counted here.
size_t HashJoinClause::residentBytes() const
{
    /// The join's byte count: the stored blocks and routes, the null maps, the table and the arenas.
    size_t bytes = accumulated_bytes.load(std::memory_order_relaxed) + hash_join.data->nullmaps_allocated_size + tableAndArenaBytes();
    if (!post_build_ctx)
        return bytes;
    const auto & ctx = *post_build_ctx;
    for (const auto & overflow : ctx.overflow)
        bytes += overflow.allocatedBytes();
    for (const auto & worker : ctx.worker_state)
        bytes += worker.scratch.allocatedBytes();
    bytes += ctx.drain_scratch.allocatedBytes();
    return bytes;
}

UInt64 HashJoinClause::boundaryProjection(
    UInt64 claimed_total, UInt64 rows_inserted, UInt64 insertable, double hll_estimate, double reserve_safety)
{
    /// The sketch saw every build row, so it is the projection while the exact count is inside its safety
    /// band. Once keys repeat, a linear extrapolation of the exact count overshoots badly. With 8 rows per
    /// key, every key has appeared after the first half of the rows, and the linear term would double a
    /// table whose sketch was right. So the linear term is used only after the exact count has refuted the
    /// sketch; then nothing better is known.
    const UInt64 sketch = static_cast<UInt64>(std::ceil(hll_estimate * reserve_safety));
    if (claimed_total <= sketch)
        return sketch;
    const UInt64 extrapolated = rows_inserted > 0 ? claimed_total * insertable / rows_inserted : 0;
    return std::max(claimed_total, extrapolated);
}

namespace
{

template <typename Mapped>
void placeMapped(Mapped & dest, Mapped && src)
{
    new (&dest) Mapped(std::forward<Mapped>(src));
}

}

template <typename Table>
void HashJoinClause::growHashJoinTable(Table & table, UInt64 occupied, UInt64 projected, GrowReason reason, size_t extra_reserved)
{
    using Cell = typename Table::cell_type;
    using Key = typename Table::key_type;
    using Mapped = typename Table::mapped_type;

    auto & ctx = *post_build_ctx;
    size_t new_degree = table.sizeDegree() + 1;
    if (reason == GrowReason::LoadFactor)
    {
        while (new_degree <= 32 && Table::maxFillFor(new_degree) < projected)
            ++new_degree;
    }

    const size_t need = (1uz << new_degree) * sizeof(Cell);
    const size_t entry_bytes = sizeof(Key) + sizeof(size_t) + sizeof(Mapped);
    /// The rehash lists hold the keys outside their partition's range (every drain claim, an upper bound)
    /// plus the crossings of the rehash walks. Real builds overflow a few hundred rows; 64 per partition
    /// is a loose allowance that costs 2 MiB of budget at 1024 partitions.
    const size_t allowance = entry_bytes * (ctx.drain_claimed + ctx.rehash_listed + 64 * partitions);
    const bool refused
        = new_degree > 32 || (grow_budget != 0 && residentBytes() + need + allowance + extra_reserved > grow_budget);
    if (refused)
    {
        if (reason == GrowReason::LastFreeCell)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "PartitionedHashJoin: the shared hash table of {} cells cannot grow to hold a projection of {} distinct keys "
                "(need {}, resident {}); the size estimate that created it was too low",
                table.cellCount(),
                projected,
                ReadableSize(need),
                ReadableSize(residentBytes()));
        /// A skipped quality grow is expected under a tight budget; it is not a user-facing warning.
        LOG_DEBUG(
            log,
            "PartitionedHashJoin: skipping a load-factor grow; projection {}, current fill {}/{}, need {}",
            projected,
            occupied,
            table.cellCount(),
            ReadableSize(need));
        ++stats.load_factor_grow_skipped;
        return;
    }

    struct RehashEntry
    {
        Key key;
        size_t hash;
        Mapped mapped;
    };
    std::vector<std::vector<RehashEntry>> lists(ctx.workers);

    table.beginRehash(new_degree);

    auto rehash_partition = [&](size_t p, std::vector<RehashEntry> & list)
    {
        table.commitNewRange(p);
        const size_t begin = table.rangeBegin(p);
        const size_t end = table.rangeEnd(p);
        const size_t new_end = table.newRangeEnd(p);
        for (size_t pos = begin; pos < end; ++pos)
        {
            Cell * cell = table.cellAt(pos);
            if (table.isEmptyCell(cell))
                continue;
            const size_t hash = table.cellHash(cell);
            const Key key = Cell::getKey(cell->getValue());
            Mapped mapped = std::move(cell->getMapped());
            if (table.partitionOf(hash) != p)
            {
                list.push_back(RehashEntry{key, hash, std::move(mapped)});
                continue;
            }
            size_t np = table.newPlace(hash);
            while (np < new_end && !table.isEmptyCell(table.newCellAt(np)))
                ++np;
            if (np == new_end)
            {
                list.push_back(RehashEntry{key, hash, std::move(mapped)});
                continue;
            }
            Cell * nc = table.newCellAt(np);
            table.claimPersisted(nc, key, hash);
            placeMapped(nc->getMapped(), std::move(mapped));
        }
    };

    if (partitions == 1)
        rehash_partition(0, lists[0]);
    else
    {
        std::atomic<UInt32> claim{0};
        std::atomic<UInt64> unused_us{0};
        runPostBuildWave(
            *post_build_pool,
            ctx.workers,
            [&](size_t w)
            {
                while (true)
                {
                    const UInt32 i = claim.fetch_add(1, std::memory_order_relaxed);
                    if (i >= partitions)
                        break;
                    const size_t p = ctx.partition_order[i];
                    rehash_partition(p, lists[w]);
                }
            },
            unused_us);
    }

    for (const auto & list : lists)
        ctx.rehash_listed += list.size();

    for (auto & list : lists)
    {
        for (auto & entry : list)
        {
            size_t np = table.newPlace(entry.hash);
            while (!table.isEmptyCell(table.newCellAt(np)))
                np = table.newNext(np);
            Cell * nc = table.newCellAt(np);
            table.claimPersisted(nc, entry.key, entry.hash);
            placeMapped(nc->getMapped(), std::move(entry.mapped));
        }
    }

    for (size_t p = 0; p < partitions; ++p)
        if (!table.newRangeIsCommitted(p))
            table.commitNewRange(p);

#ifdef DEBUG_OR_SANITIZER_BUILD
    UInt64 seen = 0;
    for (size_t pos = 0; pos < table.newCellCount(); ++pos)
        seen += !table.isEmptyCell(table.newCellAt(pos));
    if (seen != occupied)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PartitionedHashJoin: rehash wrote {} occupied cells, expected {}", seen, occupied);
#endif

    table.adoptRehash();
    ht_total_bytes = need;
    size_degree = new_degree;
    stats.table_size_degree = new_degree;
    stats.table_cells = table.cellCount();
    stats.predictions_exact = false;
    ++stats.table_resizes;
    ProfileEvents::increment(ProfileEvents::HashJoinTableResizes);
    ctx.range_committed.assign(partitions, 1);
    decideAmacEngagement();
}

void HashJoinClause::grow(UInt64 occupied, UInt64 projected, GrowReason reason, size_t extra_reserved)
{
    if (!table_maps || !post_build_ctx)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PartitionedHashJoin: grow called without a table");

    forHashJoinTable(
        *table_maps, hash_join.data->type, [&](auto & table) { growHashJoinTable(table, occupied, projected, reason, extra_reserved); });
}

void HashJoinClause::maybeGrowForLoadFactor(UInt64 projected, size_t extra_reserved)
{
    const UInt64 occupied = claimedBufferCells();
    forHashJoinTable(
        *table_maps,
        hash_join.data->type,
        [&](auto & table)
        {
            if (projected > table.maxFill())
                grow(occupied, projected, GrowReason::LoadFactor, extra_reserved);
        });
}

void HashJoinClause::decidePartitionPlan(size_t rows)
{
    const HashJoin::Type type = hash_join.data->type;
    total_rows = rows;

    /// Typical pipelines deliver blocks under 65536 rows, so the packed encoding usually applies and
    /// halves the locator transient. Decided here because the memory gate and the partition floor's
    /// guard size the scatter chunk from it before the post-build phase.
    narrow_locators = build_blocks.size() <= (1uz << 16);
    for (const auto & fill : build_blocks)
        narrow_locators = narrow_locators && fill.block_no < (1u << 16) && fill.rows <= (1uz << 16);

    /// The table is sized from the sketch over the whole input at the standard 50% max fill; it may
    /// grow during post-build when the estimate was low.
    size_degree = sizeDegreeFor(reserveFor(rows, hll_estimate));

    /// ASOF stays single-partition. Its mapped values are per-key sorted vectors. Insert wants the
    /// original row order, and that sorting dominates the build. Partitioning the equi-key table
    /// would pay a scattered insert order for nothing. The fixed-size maps have no ranges to split.
    bits = 0;
    if (!HashJoinTableMaps::isFixedSizeType(type) && hash_join.getStrictness() != JoinStrictness::Asof)
    {
        /// The fewest bits whose range - `2^(size_degree - bits)` cells - fits the private L2 budget, so
        /// an owner's inserts stay cache-resident while it streams its chunk.
        const size_t cell_bytes = HashJoinTableMaps::cellBytes(maps_variant_index, type);
        const size_t l2_bytes = std::max<size_t>(getL2CacheSize(), 1 << 20);
        const auto budget_bytes = static_cast<size_t>(0.8 * static_cast<double>(l2_bytes));
        size_t range_bits = 0;
        while ((2uz << range_bits) * cell_bytes <= budget_bytes)
            ++range_bits;
        bits = size_degree > range_bits ? size_degree - range_bits : 0;

        /// A cap on the partition count at one partition per 64 bytes of L1 (1024 on a 64 KiB L1). It
        /// dates from a layout with a 16-byte descriptor per partition that had to fit a quarter of L1.
        /// The shared table has no descriptors, but deeper fanouts measured no better, and the cap
        /// bounds the per-partition overflow and scratch buffers.
        std::optional<size_t> descriptor_cap_bits;
        if (cap_partitions_by_l1_descriptors)
        {
            constexpr size_t bytes_per_partition = 16;
            const size_t l1_bytes = l1_cache_bytes_for_tests.value_or(std::max<size_t>(getL1CacheSize(), 32 << 10));
            const size_t max_partitions = std::max<size_t>(1, l1_bytes / 4 / bytes_per_partition);
            descriptor_cap_bits = static_cast<size_t>(std::bit_width(max_partitions) - 1);
            bits = std::min(bits, *descriptor_cap_bits);
        }

        const auto parallelism_floor = static_cast<size_t>(std::bit_width(std::bit_ceil(num_threads) - 1));
        if (bits > 0)
        {
            /// Once partitioning pays for itself, at least one range per worker, so the owner wave
            /// parallelizes. A small build stays single-partition.
            bits = std::max(bits, parallelism_floor);
        }
        else if (rows >= parallel_hash_join_threshold)
        {
            /// A large build over few distinct keys: the table is small, so the L2 rule wants one
            /// partition, and one worker would insert every row after the barrier. Above the threshold
            /// the planner reserves for parallel builds. Give the insert one partition per worker, as
            /// `parallel_hash` has one table per slot. Never more partitions than distinct keys.
            const size_t distinct = distinctEstimate();
            const size_t partitions_wanted = std::min(std::bit_ceil(num_threads), std::bit_ceil(distinct));
            size_t floor_bits = static_cast<size_t>(std::bit_width(partitions_wanted) - 1);
            if (descriptor_cap_bits)
                floor_bits = std::min(floor_bits, *descriptor_cap_bits);
            /// Every range keeps at least 2^10 cells. The table widens for that floor when the
            /// estimate alone sized it smaller (2^12 cells over 1024 keys become 2^13 for 8 workers).
            /// The scatter the floor introduces holds a locator and the keys of every row at once; a
            /// memory budget that cannot absorb that keeps the serial insert instead.
            constexpr size_t min_range_bits = 10;
            const size_t floor_degree = std::max(size_degree, floor_bits + min_range_bits);
            if (partitionFloorFitsMemory(floor_bits, floor_degree, rows))
            {
                bits = floor_bits;
                size_degree = floor_degree;
            }
        }

        if (forced_bits_for_tests)
            bits = *forced_bits_for_tests;

        /// Every range holds at least one cell; see `max_plan_bits`.
        bits = std::min({bits, size_degree, max_plan_bits});
    }

    partitions = 1uz << bits;

    /// When the plan wants a wider fanout than one scatter pass sustains, the bits split into MSB-first
    /// passes rather than the fanout being capped. Empty for a single partition.
    pass_bits = ColumnsScatter::computePassBits(partitions, max_fanout_per_pass);

    LOG_TRACE(
        log,
        "Partition plan: table of 2^{} cells, bits = {}, partitions = {}, {} scatter pass(es) (bits per pass [{}]), {} rows in {} "
        "blocks, estimated {} distinct keys",
        size_degree,
        bits,
        partitions,
        std::max<size_t>(pass_bits.size(), 1),
        fmt::join(pass_bits, ", "),
        rows,
        build_blocks.size(),
        static_cast<size_t>(hll_estimate));
}

void HashJoinClause::createHashJoinTable()
{
    const HashJoin::Type type = hash_join.data->type;
    /// From the degree, not the reserve. The partition floor may have widened the table past what the
    /// reserve alone asks for. The exactness check compares against what was actually created.
    ht_total_bytes = HashJoinTableMaps::bufferBytesForDegree(maps_variant_index, type, size_degree);

    table_maps = std::make_unique<HashJoinTableMaps>(maps_variant_index);
    table_maps->create(type, size_degree, bits);

    stats.table_size_degree = size_degree;
    stats.table_cells = table_maps->getBufferSizeInCells(type);
    stats.predictions_exact = table_maps->getReservedBufferBytes(type) == ht_total_bytes;
    decideAmacEngagement();
}

bool HashJoinClause::partitionFloorFitsMemory(size_t floor_bits, size_t floor_degree, size_t rows) const
{
    /// Without a memory budget nothing bounds the peak but the query's own memory limit, as for
    /// `parallel_hash`, whose per-slot tables are never budgeted either.
    if (max_bytes_before_external_join == 0)
        return true;

    const HashJoin::Type type = hash_join.data->type;
    const size_t tables = HashJoinTableMaps::bufferBytesForDegree(maps_variant_index, type, floor_degree);

    /// The ungrouped scatter chunk as `chunkBytesForBlockRange` will size it: the scattered key width,
    /// one locator per row, the variable-length key bytes and the duplicate scratch.
    const KeyLayout layout = keyLayout();
    const size_t locator_width = narrow_locators ? sizeof(UInt32) : sizeof(UInt64);
    const size_t generic_key_bytes_est = layout.generic ? keyColumnBytes() : 0;
    const size_t transient = rows * (layout.scatteredKeyWidth() + locator_width) + generic_key_bytes_est
        + duplicateScratchBytesForRows(rows, /*first_group=*/true);

    /// What is resident whatever the plan: the stored blocks, their null maps and routes, and the
    /// duplicate spans. Then the post-build gate's ungrouped peak, where the chunk and the table trade
    /// off range by range.
    const size_t distinct = distinctEstimate();
    const size_t tables_and_spans = predictedTableAndArenaBytes(rows, distinct, /*grouped=*/false);
    const size_t predicted_tables = HashJoinTableMaps::predictedBufferBytes(maps_variant_index, type, reserveFor(rows, static_cast<double>(distinct)));
    const size_t spans = tables_and_spans > predicted_tables ? tables_and_spans - predicted_tables : 0;
    const size_t floor_bytes = hash_join.data->allocated_size + hash_join.data->nullmaps_allocated_size + routeBytes() + spans + generic_key_bytes_est;
    const size_t floor_partitions = 1uz << floor_bits;
    const size_t peak = floor_bytes + std::max(transient + tables / floor_partitions, tables + transient / floor_partitions);
    return peak <= max_bytes_before_external_join;
}

size_t HashJoinClause::sizeDegreeFor(size_t reserve) const
{
    const size_t degree = HashJoinTableMaps::sizeDegree(maps_variant_index, hash_join.data->type, reserve);
    if (degree > 32)
        throw Exception(
            ErrorCodes::LIMIT_EXCEEDED,
            "PartitionedHashJoin: a table of degree {} would exceed the 2^31 distinct-key cap (size_degree <= 32)",
            degree);
    return degree;
}

UInt64 HashJoinClause::insertableRows() const
{
    UInt64 rows = 0;
    for (UInt64 partition_rows : total_bucket_rows)
        rows += partition_rows;
    return rows;
}

size_t HashJoinClause::routeBytes() const
{
    size_t bytes = 0;
    for (const auto & fill : build_blocks)
        bytes += fill.routes.allocated_bytes();
    return bytes;
}

size_t HashJoinClause::KeyLayout::scatteredKeyWidth() const
{
    if (generic)
        return sizeof(UInt64) * fixed_widths.size();
    size_t width = 0;
    for (size_t w : fixed_widths)
        width += w;
    return width;
}

HashJoinClause::KeyLayout HashJoinClause::keyLayout() const
{
    KeyLayout layout;
    if (build_blocks.empty())
        return layout;
    const ColumnRawPtrs & key_columns = build_blocks.front().key_columns;
    layout.fixed_widths.resize(key_columns.size());
    for (size_t c = 0; c < key_columns.size(); ++c)
    {
        if (key_columns[c]->isFixedAndContiguous())
            layout.fixed_widths[c] = key_columns[c]->sizeOfValueIfFixed();
        else
            layout.generic = true;
    }
    return layout;
}

size_t HashJoinClause::keyColumnBytes() const
{
    size_t bytes = 0;
    for (const auto & fill : build_blocks)
        for (const auto * key_column : fill.key_columns)
            bytes += key_column->byteSize();
    return bytes;
}

std::unique_ptr<ThreadPool> HashJoinClause::makePostBuildPool(size_t workers)
{
    return std::make_unique<ThreadPool>(
        CurrentMetrics::HashJoinPostBuildThreads,
        CurrentMetrics::HashJoinPostBuildThreadsActive,
        CurrentMetrics::HashJoinPostBuildThreadsScheduled,
        /*max_threads_*/ workers,
        /*max_free_threads_*/ 0,
        /*queue_size_*/ workers);
}

size_t HashJoinClause::reserveFor(size_t rows, double distinct_estimate) const
{
    /// The safety factor covers the sketch's error. The row clamp says a table cannot hold more keys
    /// than rows. Above 2^31 estimated words the 32-bit sketch is saturating. The exact upper bound
    /// then takes over: at most a 2x over-reservation, only for builds already holding 64 GiB of cells.
    if (reserve_override_for_tests)
        return *reserve_override_for_tests;
    const double scaled = std::ceil(std::max(distinct_estimate, 1.0) * reserve_safety);
    const size_t rows_bound = std::max<size_t>(rows, 1);
    if (scaled >= 2147483648.0)
        return rows_bound;
    return std::clamp<size_t>(static_cast<size_t>(scaled), 1, rows_bound);
}

bool HashJoinClause::postBuildSinglePartition(size_t rows)
{
    /// One partition over the whole build, with no scatter. Rows go in straight from the stored blocks
    /// with plain `RowRef(block_no, row)` refs. The walk wraps at the buffer end. Nothing overflows.
    chassert(bits == 0 && !post_build_ctx);
    beginSinglePartitionInsert(reserveFor(rows, hll_estimate), rows, /*grow_at_max_fill_=*/false);
    for (auto & fill : build_blocks)
        insertSingleLaneBlock(fill);
    return finishSinglePartitionInsert();
}

void HashJoinClause::beginSinglePartitionInsert(size_t reserve, size_t rows, bool grow_at_max_fill_)
{
    grow_at_max_fill = grow_at_max_fill_;
    total_rows = rows;
    /// The barrier's plan already derived these for the post-build path. The single fill thread has no
    /// plan and derives them here, from the hint, before its first block.
    size_degree = sizeDegreeFor(reserve);
    bits = 0;
    partitions = 1;
    pass_bits.clear();

    chassert(!post_build_ctx);
    post_build_ctx.reset(new PostBuildContext);
    auto & ctx = *post_build_ctx;
    ctx.workers = 1;
    ctx.worker_state.resize(1);
    ctx.overflow.resize(1);
    ctx.claimed_per_partition.assign(1, 0);
    ctx.range_committed.assign(1, 0);

    createHashJoinTable();
    measureGenericKeyBytes();
    chassert(build_arenas.empty());
    emplaceSizedBuildArena(build_arenas, predictedArenaBytes(rows, post_build_plan == PostBuildPlan::Grouped));
    emplaceSizedBuildArena(build_arenas, 0);
    ctx.worker_state[0].writer.emplace(build_arenas[0]);
    ctx.drain_writer.emplace(build_arenas[1]);

    forHashJoinTable(*table_maps, hash_join.data->type, [](auto & table) { table.commitAll(); });
    ctx.range_committed[0] = 1;
}

void HashJoinClause::insertSingleLaneBlock(FillBlock & fill)
{
    auto & ctx = *post_build_ctx;
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
    ProfileEvents::increment(ProfileEvents::HashJoinInsertedRows, fill.rows);
    ctx.worker_state[0].inserted_rows += fill.rows;
    fill.releaseInputs();
}

bool HashJoinClause::finishSinglePartitionInsert()
{
    auto & ctx = *post_build_ctx;
    auto & worker0 = ctx.worker_state[0];
    worker0.scratch_used_high_water = std::max(worker0.scratch_used_high_water, finishPassScratch(worker0.scratch, *worker0.writer));
    stats.scratch_used_high_water = worker0.scratch_used_high_water;
    chassert(ctx.overflow[0].rows() == 0);

    stats.inserted_rows = worker0.inserted_rows;
    stats.owner_duplicates = worker0.writer->stats();
    maybeGrowForLoadFactor(claimedTotal());
    publishTableSize(ctx);
    return worker0.all_values_unique;
}

void HashJoinClause::publishTableSize(const PostBuildContext & ctx)
{
    const HashJoin::Type type = hash_join.data->type;
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
        if constexpr (is_hash_join_table<Table>) \
        { \
            if (!shape_maps.TYPE->fullyCommitted()) \
                throw Exception(ErrorCodes::LOGICAL_ERROR, "PartitionedHashJoin: the shared hash table is published with uncommitted ranges"); \
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
        table_maps->maps);
    stats.distinct_keys = distinct;
    stats.claimed_per_partition = ctx.claimed_per_partition;
}

/// Debug and sanitizer builds only. The published table must carry no build-time word (every pass
/// finished its scratch). Its duplicate layout must account for every inserted row. A leaked
/// `TAG_COUNT` / `TAG_FILL*` would otherwise read as an empty key in the release build - silent row loss.
template <typename Table>
void HashJoinClause::verifyPublishedTable(const Table & table) const
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    using Mapped = typename Table::mapped_type;
    if constexpr (std::is_same_v<Mapped, RowRefList>)
    {
        UInt64 rows = 0;
        const auto visit_cell = [&](const auto * cell, size_t position)
        {
            const RowRefList & mapped = cell->getMapped();
            if (mapped.isCount() || mapped.isFill())
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "PartitionedHashJoin: a build-time word survived publication in cell {}", position);
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

size_t HashJoinClause::predictedTableAndArenaBytes(size_t rows, size_t distinct, bool grouped, size_t groups_est_) const
{
    const size_t distinct_keys = std::max(distinct, 1uz);
    const size_t reserve = reserveFor(rows, static_cast<double>(distinct_keys));
    size_t bytes = HashJoinTableMaps::predictedBufferBytes(maps_variant_index, hash_join.data->type, reserve);

    /// `maps_variant_index == 1` is `MapsAll` (`RowRefList`). Unique keys stay inline in the cell
    /// word; only this shape keeps duplicate spans in the arena, at 8 bytes per row of a duplicated
    /// key. `preferUseMapsAll` is still false at the gate - the ALL-to-RightAny promotion has not run -
    /// so the variant index is what actually keeps the runs. LEFT/INNER Any/Semi/Anti use `MapsOne`
    /// and hold no run.
    ///
    /// Multiplicity inside `reserve_safety` is treated as unique for the arena term. A fill-phase
    /// distinct estimate that lags the row count by a sixteenth, or a HyperLogLog that undershoots
    /// by a percent, would otherwise look like `m > 1`. That would charge every row of a unique build
    /// to the arena, enough to spill a build that fits. Real duplicate builds (m=5, m=8) sit far above
    /// the band. The factor already
    /// covers sketch error for the table reserve; reusing it here keeps the unique/duplicate decision on
    /// the same inputs.
    if (maps_variant_index == 1)
    {
        const double multiplicity = static_cast<double>(rows) / static_cast<double>(distinct_keys);
        if (multiplicity > reserve_safety)
        {
            /// Every row of a duplicated key lives in the arena, the once-inline row included.
            bytes += sizeof(UInt64) * rows;
            /// A grouped scatter writes a span header for every key appended in a later group.
            /// `groups_est_` is computed from the ungrouped floor so this term cannot feed back into itself.
            if (grouped && groups_est_ > 1)
            {
                const size_t dup_rows = rows > distinct_keys ? rows - distinct_keys : 0;
                const size_t dup_keys = std::min(distinct_keys, dup_rows);
                const size_t extra_groups = groups_est_ - 1;
                size_t headers = dup_rows;
                if (dup_keys != 0 && extra_groups <= std::numeric_limits<size_t>::max() / dup_keys)
                    headers = std::min(dup_rows, dup_keys * extra_groups);
                bytes += SpanWriter::span_header_bytes * headers;
            }
        }
    }
    return bytes;
}

size_t HashJoinClause::predictedArenaBytes(size_t insertable_rows, bool grouped) const
{
    /// Duplicate-span bytes come from the shared helper so the fill-phase prediction and the gate cannot
    /// drift. Variable-length keys are copied into the arena; that total is measured once before the
    /// first range is scattered, because a consumed range has dropped its key columns.
    const size_t distinct = distinctEstimate();
    const size_t tables_and_spans = predictedTableAndArenaBytes(insertable_rows, distinct, grouped, grouped ? groups_est : 1uz);
    const size_t tables = HashJoinTableMaps::predictedBufferBytes(maps_variant_index, hash_join.data->type, reserveFor(insertable_rows, static_cast<double>(distinct)));
    chassert(tables_and_spans >= tables);
    return tables_and_spans - tables + generic_key_bytes;
}

size_t HashJoinClause::duplicateScratchBytesForRows(size_t rows_in_range, bool first_group) const
{
    if (total_rows == 0 || hll_estimate >= static_cast<double>(total_rows) || rows_in_range == 0)
        return 0;
    const double f = 1.0 - hll_estimate / static_cast<double>(total_rows);
    const size_t rows_dup = static_cast<size_t>(std::ceil(std::min(1.0, 2.0 * f) * static_cast<double>(rows_in_range)));
    /// The keys a range appends to are at most the build's distinct keys, which the sketch bounds; the
    /// duplicate rows do not. Charging every duplicate row as a new key overcharges a range by up to a
    /// quarter and cuts the groups short.
    const size_t distinct = static_cast<size_t>(std::ceil(hll_estimate * reserve_safety));
    if (first_group)
    {
        const size_t dup_keys = static_cast<size_t>(std::ceil(f * static_cast<double>(rows_in_range)));
        return PassScratch::bytes_per_item * rows_dup + PassScratch::bytes_per_key * std::min(distinct, dup_keys);
    }
    /// In a later group every appended key already has a span, so its first duplicate also stores the
    /// previous word as an item.
    return PassScratch::bytes_per_item * rows_dup + (PassScratch::bytes_per_item + PassScratch::bytes_per_key) * std::min(distinct, rows_dup);
}

size_t HashJoinClause::predictedArenaBytesForTests(bool grouped) const
{
    return predictedArenaBytes(total_rows, grouped);
}

size_t HashJoinClause::predictedDuplicateScratchBytesForTests(size_t rows_in_range, bool first_group) const
{
    return duplicateScratchBytesForRows(rows_in_range, first_group);
}

/// Variable-length keys are copied into the arena, so their bytes join the arena term. Measured while
/// every block still holds its keys, so the gate and the group-boundary re-checks agree on it.
void HashJoinClause::measureGenericKeyBytes()
{
    generic_key_bytes = keyLayout().generic ? keyColumnBytes() : 0;
}

size_t HashJoinClause::chunkBytesForBlockRange(size_t b0, size_t b1) const
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
        if (ctx.multi_pass)
            bytes += fill.rows * sizeof(UInt16);
    }

    bytes += duplicateScratchBytesForRange(rows_in_range, /*first_group=*/b0 == 0);
    return bytes;
}

size_t HashJoinClause::duplicateScratchBytesForRange(size_t rows_in_range, bool first_group) const
{
    if (rows_in_range == 0)
        return 0;

    /// An owner finishes its scratch after every partition it claims, so at most `workers` partitions of
    /// scratch are live at once: the largest ones, in the worst case. A partition's rows in this range are
    /// estimated from its share of the whole build (`total_bucket_rows`, sorted largest first in
    /// `partition_order`) at twice the range's row fraction, capped by its total. Charging the whole range's
    /// duplicate rows overcharges by an order of magnitude; charging `workers` times the largest partition's
    /// total makes a Zipf build plan dozens of groups.
    const auto & ctx = *post_build_ctx;
    const UInt64 insertable = insertableRows();
    UInt64 top_rows = 0;
    const size_t live = std::min<size_t>(ctx.workers, ctx.partition_order.size());
    for (size_t i = 0; i < live; ++i)
        top_rows += total_bucket_rows[ctx.partition_order[i]];
    size_t live_rows = top_rows;
    if (insertable > 0)
    {
        const double share = 2.0 * static_cast<double>(rows_in_range) / static_cast<double>(insertable);
        live_rows = std::min<size_t>(top_rows, static_cast<size_t>(std::ceil(static_cast<double>(top_rows) * share)));
    }
    size_t bytes = duplicateScratchBytesForRows(std::min(live_rows, live * rows_in_range), first_group);

    /// The drain's scratch is finished once per group and holds the rows whose owner walk reached its range
    /// end. Charged at twice the rate seen so far; one row in 1024 before anything was inserted.
    const UInt64 rows_so_far = ctx.insertedRows();
    const UInt64 overflow_so_far = ctx.drain_claimed + ctx.drain_appended;
    size_t drain_rows = rows_in_range / 1024;
    if (rows_so_far > 0)
        drain_rows = static_cast<size_t>(std::ceil(
            2.0 * static_cast<double>(overflow_so_far) / static_cast<double>(rows_so_far) * static_cast<double>(rows_in_range)));
    bytes += duplicateScratchBytesForRows(std::min(drain_rows, rows_in_range), /*first_group=*/false);
    return bytes;
}

void HashJoinClause::reduceWorkerHistogram()
{
    auto & ctx = *post_build_ctx;
    ctx.bucket_rows.assign(ctx.fanout, 0);
    for (size_t w = 0; w < ctx.workers; ++w)
        for (size_t p = 0; p < ctx.fanout; ++p)
            ctx.bucket_rows[p] += ctx.worker_hist[w * ctx.fanout + p];
}

void HashJoinClause::resetWorkerHistogram(PostBuildContext & ctx)
{
    /// `resize_fill` only fills what it grows, so a reused same-sized array would histogram
    /// on top of the previous range's counts.
    ctx.worker_hist.clear();
    ctx.worker_hist.resize_fill(ctx.workers * ctx.fanout, 0);
}

void HashJoinClause::preparePostBuildContext()
{
    if (post_build_ctx)
        return;

    chassert(!build_blocks.empty());
    post_build_ctx.reset(new PostBuildContext);
    auto & ctx = *post_build_ctx;
    ctx.workers = std::max<size_t>(1, std::min(num_threads, build_blocks.size()));
    chassert(!pass_bits.empty());
    ctx.multi_pass = pass_bits.size() > 1;
    ctx.route_bits = pass_bits.front();
    chassert(ctx.route_bits <= 15); /// `max_plan_bits`: the bucket ids are UInt16 and the drop bucket needs one more
    ctx.fanout = (1uz << ctx.route_bits) + 1;
    ctx.num_key_columns = build_blocks.front().key_columns.size();

    ctx.key_samples.reserve(ctx.num_key_columns);
    for (const auto * column : build_blocks.front().key_columns)
        ctx.key_samples.push_back(column->cloneEmpty());

    KeyLayout layout = keyLayout();
    ctx.generic_mode = layout.generic;
    ctx.fixed_widths = std::move(layout.fixed_widths);

    ctx.starts.resize(ctx.fanout * ctx.workers);
    ctx.resetScatterContainers(narrow_locators);
    ctx.worker_state.resize(ctx.workers);
    ctx.overflow.resize(partitions);
    ctx.claimed_per_partition.assign(partitions, 0);
    ctx.range_committed.assign(partitions, 0);

    post_build_pool = makePostBuildPool(ctx.workers);

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
    ProfileEvents::increment(ProfileEvents::HashJoinPartitionedBuildHistogramMicroseconds, hist_thread_us.load(std::memory_order_relaxed));

    stats.partition_row_counts = total_bucket_rows;
    ctx.partition_order.resize(partitions);
    for (size_t partition = 0; partition < partitions; ++partition)
        ctx.partition_order[partition] = static_cast<UInt32>(partition);
    std::sort(
        ctx.partition_order.begin(),
        ctx.partition_order.end(),
        [&](UInt32 a, UInt32 b) { return total_bucket_rows[a] > total_bucket_rows[b]; });

    /// Nothing is committed here; the owner that claims a partition commits its range.
    createHashJoinTable();

    measureGenericKeyBytes();
    const size_t arena_pred = predictedArenaBytes(insertableRows(), post_build_plan == PostBuildPlan::Grouped);
    const size_t per_worker = arena_pred / ctx.workers;
    chassert(build_arenas.empty());
    for (size_t w = 0; w < ctx.workers; ++w)
        emplaceSizedBuildArena(build_arenas, per_worker);
    emplaceSizedBuildArena(build_arenas, /*predicted_bytes=*/0); /// the drain's arena
}

HashJoinClause::PostBuildPlan HashJoinClause::planPostBuild(size_t rows)
{
    if (max_bytes_before_external_join == 0)
    {
        post_build_plan = PostBuildPlan::Fits;
        return post_build_plan;
    }

    const size_t row_store = hash_join.data->allocated_size + hash_join.data->nullmaps_allocated_size;
    const size_t routes = routeBytes();
    measureGenericKeyBytes();

    if (bits == 0)
    {
        const size_t insertable = rows;
        const size_t distinct = distinctEstimate();
        /// The single-partition path inserts straight from the stored blocks, so there is no transient
        /// to bound and grouping has nothing to do. Table and duplicate runs go through the shared
        /// helper so this verdict cannot drift from the fill-phase prediction.
        const size_t resident
            = row_store + routes + predictedTableAndArenaBytes(insertable, distinct, /*grouped=*/false) + generic_key_bytes;
        post_build_plan = resident <= max_bytes_before_external_join ? PostBuildPlan::Fits : PostBuildPlan::MustSpill;
        return post_build_plan;
    }

    preparePostBuildContext();
    const UInt64 insertable = insertableRows();

    /// What must be resident whatever the scatter schedule is. The grouped arena term needs `groups_est`;
    /// it is computed from this ungrouped floor, so the header charge cannot feed back into itself.
    const size_t floor_bytes = row_store + routes + predictedArenaBytes(insertable, /*grouped=*/false);
    const size_t tables = ht_total_bytes;
    const size_t chunk_all = chunkBytesForBlockRange(0, build_blocks.size());
    const size_t headroom_for_groups
        = max_bytes_before_external_join > floor_bytes + tables ? max_bytes_before_external_join - floor_bytes - tables : 1;
    groups_est = std::max(1uz, ceilDiv(chunk_all, headroom_for_groups));
    const size_t floor_bytes_grouped = row_store + routes + predictedArenaBytes(insertable, /*grouped=*/true);

    /// The ungrouped scatter never holds the whole chunk next to the whole table: an owner commits its
    /// range and frees that partition's chunk in the same claim, so the peak sits at one end of the wave.
    const size_t peak_ungrouped = floor_bytes + std::max(chunk_all + tables / partitions, tables + chunk_all / partitions);

    /// Grouping holds the full table from the first range (every realistic range touches every partition)
    /// plus one range's chunk. It lowers the peak only while the chunk dominates the table; when the table
    /// dominates, grouping adds `chunk / g` on top and is strictly worse. Its floor, as ranges get finer,
    /// is one block's chunk.
    const size_t grouped_floor = floor_bytes_grouped + tables + chunkBytesForBlockRange(0, 1);

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
                                                        : "over budget");
    return post_build_plan;
}

void HashJoinClause::runGroupStages(size_t block_begin, size_t block_end)
{
    auto & ctx = *post_build_ctx;
    ctx.block_begin = block_begin;
    ctx.block_end = block_end;
    ctx.refined = false;
    ctx.current_buckets = 0;
    ctx.refined_pieces.clear();
    ctx.partition_claim.store(0, std::memory_order_relaxed);

    ctx.resetScatterContainers(narrow_locators);

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

    {
        /// The drop bucket holds the null-key and ON-filtered rows. They are never inserted and must not
        /// be refined again, so the bucket is freed before the refine passes.
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::HashJoinPartitionedBuildMicroseconds);
        const size_t drop = ctx.fanout - 1;
        if (narrow_locators)
            ctx.locators32[drop] = {};
        else
            ctx.locators[drop] = {};
        if (ctx.multi_pass)
            ctx.routes[drop] = {};
        if (!ctx.generic_mode)
            for (size_t c = 0; c < ctx.num_key_columns; ++c)
                ctx.fixed_out[c][drop].reset();
    }

    std::atomic<UInt64> refine_thread_us{0};
    stage_watch.restart();
    if (ctx.multi_pass)
    {
        ctx.current_buckets = ctx.fanout - 1;
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

    /// The owner wave, then the barrier, the capacity guard and the serial drain. The drain must finish
    /// before the next group's wave: it writes wherever a walk wraps to, which is any owner's range.
    stage_watch.restart();
    runPostBuildWave(*post_build_pool, ctx.workers, [this, &ctx](size_t w) { ownerWaveWorker(ctx, w); }, insert_thread_us);
    const UInt64 insert_wall_us = stage_watch.elapsedMicroseconds();

    stage_watch.restart();
    UInt64 group_overflow = 0;
    for (const auto & overflow : ctx.overflow)
        group_overflow += overflow.rows();
    maybeGrowForLoadFactor(
        claimedTotal() + group_overflow,
        ctx.block_end < build_blocks.size() ? chunkBytesForBlockRange(ctx.block_end, ctx.block_end + 1) : 0);
    drainOverflow(ctx);
    const UInt64 drain_wall_us = stage_watch.elapsedMicroseconds();

    const auto to_ms = [](UInt64 us) { return static_cast<double>(us) / 1000.0; };
    LOG_TRACE(
        log,
        "Post-build stages for blocks [{}, {}), wall/thread ms: histogram {:.1f}/{:.1f}, chunk allocation {:.1f}/{:.1f}, scatter "
        "{:.1f}/{:.1f}, refine passes {:.1f}/{:.1f}, owner inserts {:.1f}/{:.1f} (AMAC {}), drain {:.1f} for {} "
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
        to_ms(insert_wall_us),
        to_ms(insert_thread_us.load(std::memory_order_relaxed)),
        amac_build_engaged ? "engaged" : "off",
        to_ms(drain_wall_us),
        group_overflow);

    ProfileEvents::increment(
        ProfileEvents::HashJoinPartitionedBuildHistogramMicroseconds,
        hist_thread_us.load(std::memory_order_relaxed) + alloc_thread_us.load(std::memory_order_relaxed));
    ProfileEvents::increment(
        ProfileEvents::HashJoinPartitionedBuildScatterMicroseconds,
        scatter_thread_us.load(std::memory_order_relaxed) + refine_thread_us.load(std::memory_order_relaxed));
    ProfileEvents::increment(
        ProfileEvents::HashJoinPartitionedBuildInsertMicroseconds, insert_thread_us.load(std::memory_order_relaxed) + drain_wall_us);
}

bool HashJoinClause::postBuildPartitioned()
{
    preparePostBuildContext();
    auto & ctx = *post_build_ctx;
    for (size_t w = 0; w < ctx.workers; ++w)
        ctx.worker_state[w].writer.emplace(build_arenas[w]);
    ctx.drain_writer.emplace(build_arenas[ctx.workers]);

    size_t groups = 0;
    size_t b = 0;
    while (b < build_blocks.size())
    {
        if (groups > 0)
        {
            const UInt64 projected
                = boundaryProjection(claimedTotal(), ctx.insertedRows(), insertableRows(), hll_estimate, reserve_safety);
            maybeGrowForLoadFactor(projected, chunkBytesForBlockRange(b, b + 1));
        }

        size_t end = b + 1;
        size_t chunk = 0;
        size_t one_block = 0;
        size_t resident_at_plan = 0;
        if (max_bytes_before_external_join == 0 || post_build_plan == PostBuildPlan::Fits)
        {
            end = build_blocks.size();
        }
        else
        {
            /// `residentBytes` is actuals (row store, remaining routes, committed table, arenas,
            /// overflow buffers, scratch capacity). Uncommitted ranges and the still-unallocated
            /// duplicate runs are charged from the gate's predictions so the first range is not sized
            /// as if those bytes were free.
            resident_at_plan = residentBytes();
            size_t used = resident_at_plan;
            const size_t committed = table_maps->getBufferSizeInBytes(hash_join.data->type);
            if (ht_total_bytes > committed)
                used += ht_total_bytes - committed;
            size_t arena_actual = 0;
            for (const auto & arena : build_arenas)
                arena_actual += arena.allocatedBytes();
            const size_t arena_pred = predictedArenaBytes(insertableRows(), post_build_plan == PostBuildPlan::Grouped);
            if (arena_pred > arena_actual)
                used += arena_pred - arena_actual;

            const size_t headroom = used < max_bytes_before_external_join ? max_bytes_before_external_join - used : 0;
            while (end < build_blocks.size() && chunkBytesForBlockRange(b, end + 1) <= headroom)
                ++end;
            /// A range is never empty: the loop has to make progress, and a single block's chunk is
            /// bounded by its row count, so the overshoot is at most that block. The budget is a
            /// target, `max_memory_usage` is the cap. This path is only for when the actuals drifted
            /// past the gate's prediction.
            chunk = chunkBytesForBlockRange(b, end);
            one_block = chunkBytesForBlockRange(b, b + 1);
            if (chunk > headroom)
                LOG_DEBUG(
                    log,
                    "Grouped scatter: one block's chunk ({}) exceeds the remaining headroom ({}); scattering it anyway, because a "
                    "range cannot be empty",
                    ReadableSize(chunk),
                    ReadableSize(headroom));
        }
        runGroupStages(b, end);
        stats.scatter_group_ranges.push_back(
            {.begin = b, .end = end, .chunk_bytes = chunk, .one_block_chunk_bytes = one_block, .resident_bytes = resident_at_plan});
        b = end;
        ++groups;
    }

    /// Ranges no group touched are committed now, so the whole table is accounted and the probe never
    /// reads an uncommitted page.
    for (size_t partition = 0; partition < partitions; ++partition)
        if (!ctx.range_committed[partition])
            commitRange(partition);

    stats.scatter_groups = std::max<size_t>(groups, 1);
    ProfileEvents::increment(ProfileEvents::HashJoinScatterGroups, stats.scatter_groups);

    bool all_values_unique = ctx.drain_all_unique;
    for (const auto & worker : ctx.worker_state)
    {
        all_values_unique &= worker.all_values_unique;
        stats.inserted_rows += worker.inserted_rows;
        stats.owner_duplicates += worker.writer->stats();
        stats.scratch_used_high_water = std::max(stats.scratch_used_high_water, worker.scratch_used_high_water);
    }
    stats.scratch_used_high_water = std::max(stats.scratch_used_high_water, ctx.drain_scratch_used_high_water);
    stats.drain_duplicates = ctx.drain_writer->stats();
    stats.drain_claimed_keys = ctx.drain_claimed;
    stats.drain_appended_rows = ctx.drain_appended;
    /// A load-factor grow before publication rehashes on the pool, so the pool has to outlive it.
    maybeGrowForLoadFactor(claimedTotal());
    post_build_pool.reset();
    publishTableSize(ctx);
    return all_values_unique;
}

void HashJoinClause::commitRange(size_t partition)
{
    forHashJoinTable(*table_maps, hash_join.data->type, [&](auto & table) { table.commitRange(partition); });
    post_build_ctx->range_committed[partition] = 1;
}

UInt64 HashJoinClause::claimedBufferCells() const
{
    UInt64 claimed = 0;
    for (UInt64 c : post_build_ctx->claimed_per_partition)
        claimed += c;
    return claimed;
}

bool HashJoinClause::tableHasZero() const
{
    bool has_zero = false;
    forHashJoinTable(*table_maps, hash_join.data->type, [&](const auto & table) { has_zero = table.hasZero(); });
    return has_zero;
}

UInt64 HashJoinClause::claimedTotal() const
{
    return claimedBufferCells() + (tableHasZero() ? 1 : 0);
}

void HashJoinClause::drainOverflow(PostBuildContext & ctx)
{
    if (grow_budget_for_drain_for_tests)
        grow_budget = *grow_budget_for_drain_for_tests;

    const HashJoin::Type type = hash_join.data->type;
    UInt64 drained = 0;
    for (size_t partition = 0; partition < partitions; ++partition)
    {
        OverflowBuffer & overflow = ctx.overflow[partition];
        if (overflow.rows() == 0)
            continue;
        drained += overflow.rows();
        forHashJoinTable(
            *table_maps,
            type,
            [&](auto & table)
            {
                using Table = std::remove_reference_t<decltype(table)>;
                const UInt64 seed = claimedBufferCells();
                InsertTarget<Table> target{
                    .table = table,
                    .cells = table.cells(),
                    .scratch = ctx.drain_scratch,
                    .overflow = overflow,
                    .range_end = table.cellCount(),
                    .wrap = true,
                    .any_take_last_row = any_take_last_row,
                    .join = &hash_join,
                    .owner = this,
                    .claimed_per_partition = &ctx.claimed_per_partition,
                    .drain_claimed = &ctx.drain_claimed,
                    .writer = &*ctx.drain_writer,
                    .scratch_high_water = &ctx.drain_scratch_used_high_water,
                    .partition = partition,
                    .fold_base = seed,
                    .claimed = seed,
                    .grow_threshold = table.cellCount() - 1};
                drainPartitionOverflow(target, ctx.drain_appended);
                target.foldClaimed();
                ctx.drain_all_unique = ctx.drain_all_unique && target.all_unique;
            });
    }
    ctx.drain_scratch_used_high_water
        = std::max(ctx.drain_scratch_used_high_water, finishPassScratch(ctx.drain_scratch, *ctx.drain_writer));
    stats.overflow_rows += drained;
}

void HashJoinClause::histogramWorker(PostBuildContext & ctx, size_t worker) const
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

void HashJoinClause::allocateWorker(PostBuildContext & ctx, size_t worker) const
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

void HashJoinClause::scatterWorker(PostBuildContext & ctx, size_t worker)
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

    auto release_block_inputs = [this](FillBlock & fill) { accumulated_bytes.fetch_sub(fill.releaseInputs(), std::memory_order_relaxed); };

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

        /// Whole-block batches sized by `scatterBatchRowsTarget`. The per-(column, bucket)
        /// cursors persist across them. Each batch's inputs are dropped as soon as its last column
        /// is scattered. The scattered side then cycles memory instead of doubling it.
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
            /// A group with fewer blocks than workers leaves this worker nothing to scatter. The consumers
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

void HashJoinClause::refinePassWave(
    PostBuildContext & ctx, size_t refine_bits, size_t bits_done, std::atomic<UInt64> & stage_thread_us)
{
    /// Buckets are claimed dynamically because their sizes can be skewed. Each bucket's inputs are
    /// freed as they are consumed. The pass then cycles memory instead of doubling the scattered side.
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

    runPostBuildWave(*post_build_pool, ctx.workers, [&](size_t) { worker_body(); }, stage_thread_us);

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

void HashJoinClause::ownerWaveWorker(PostBuildContext & ctx, size_t worker)
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

        if (!ctx.generic_mode || ctx.refined)
        {
            /// Fixed-width keys are one column per partition; generic keys are one piece per partition
            /// once the refine passes have run. Either way the section is the partition's whole locator array.
            for (size_t c = 0; c < ctx.num_key_columns; ++c)
                section_columns[c] = ctx.generic_mode ? ctx.refined_pieces[c][partition].get() : ctx.fixed_out[c][partition].get();
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

        /// The pass's duplicates become spans while the partition's cells are still warm.
        state.scratch_used_high_water = std::max(state.scratch_used_high_water, finishPassScratch(state.scratch, *state.writer));
        state.inserted_rows += partition_rows;
        ProfileEvents::increment(ProfileEvents::HashJoinInsertedRows, partition_rows);

        /// Released as soon as they are consumed, so the table replaces the chunks rather than
        /// coexisting with them.
        release_chunk();
    }
}

void HashJoinClause::PostBuildContextDeleter::operator()(PostBuildContext * ctx) const
{
    delete ctx;
}

}
