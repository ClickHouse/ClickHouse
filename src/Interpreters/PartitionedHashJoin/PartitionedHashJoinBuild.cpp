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
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>

#include <algorithm>

namespace ProfileEvents
{
extern const Event PartitionedHashJoinBuildMicroseconds;
extern const Event PartitionedHashJoinBuildHistogramMicroseconds;
extern const Event PartitionedHashJoinBuildScatterMicroseconds;
extern const Event PartitionedHashJoinBuildLeafMicroseconds;
extern const Event PartitionedHashJoinLeafRows;
extern const Event PartitionedHashJoinHashTableBytes;
extern const Event PartitionedHashJoinHashTableGrowths;
extern const Event PartitionedHashJoinAmacRingGrowths;
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
extern const int LOGICAL_ERROR;
extern const int UNSUPPORTED_JOIN_KEYS;
}

namespace
{

constexpr size_t locator_piece_rows = 32768; /// locator synthesis scratch stays L2-resident

/// Shared by the sequential path and the fused AMAC step so the two cannot diverge: `RowRefList`
/// keeps the first ref inline and appends duplicates to the arena list, `RowRef` keeps the first row
/// per key, or the last under `any_take_last_row`.
template <typename Mapped>
ALWAYS_INLINE void
applyBuildRowToMapped(Mapped & mapped, bool inserted, UInt64 ref, Arena & pool, bool any_take_last_row, bool & all_unique)
{
    if constexpr (std::is_same_v<Mapped, RowRef>)
    {
        if (inserted || any_take_last_row)
            new (&mapped) RowRef(refWordBlockNo(ref), refWordRowNo(ref));
    }
    else
    {
        static_assert(std::is_same_v<Mapped, RowRefList>);
        if (inserted)
        {
            new (&mapped) RowRefList(RowRefList::fromWord(ref));
        }
        else
        {
            mapped.insert(ref, pool);
            all_unique = false;
        }
    }
}

/** The AMAC insert policy. `start` computes the map hash - whose latency overlaps the other slots'
  * outstanding cell misses - and prefetches the home cell for writing. `step` is the one fused
  * read-then-act the ring requires: claim an empty cell, append a duplicate, or advance and prefetch.
  * A zero-sentinel key goes through the standard `emplace` synchronously; it has no walk to overlap.
  */
template <typename KeyGetter, typename Map, typename PosT = size_t>
struct AmacBuildInsertPolicy
{
    using Cell = Map::cell_type;
    using Mapped = Map::mapped_type;
    static constexpr bool store_hash = cell_stores_hash<Cell>;
    static constexpr bool may_grow = true;
    /// The frame copy needs a copyable key getter, and the `KeysFixed` one is not - it owns a
    /// prepared-keys buffer and shuffle masks - so it stays by reference.
    static constexpr bool copy_into_frame = std::is_copy_constructible_v<KeyGetter>;
    /// The cursor position, the row, and the hash where the cell stores one. `PosT` is UInt32 when
    /// the caller can prove the buffer index fits 32 bits for the whole run, growths included, which
    /// halves the position array. The inactive sentinel lives in the row array here, so it has to be
    /// filled at construction - the probe ring gets it from value-initialization instead.
    template <size_t ring_size>
    struct RingBase
    {
        std::array<PosT, ring_size> pos{};
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

    Map & map;
    /// Cached so a visit's cell address is one add off a register rather than a load chain through
    /// the map. `grow` refreshes it; the zero-sentinel `emplace` in `start` never resizes, so it
    /// cannot invalidate it.
    Cell * cells;
    /// By value where the getter is a cheap pointer bundle, which keeps the key-column bases in
    /// registers through the steady loop.
    std::conditional_t<copy_into_frame, KeyGetter, KeyGetter &> key_getter;
    const UInt64 * locators = nullptr;
    const UInt32 * narrow_locators = nullptr;
    const UInt8 * skip_bytes = nullptr;
    UInt32 block_no = 0;
    bool any_take_last_row = false;
    Arena & pool;
    bool all_unique = true;
    UInt64 growths = 0;

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
        if (unlikely(map.isZeroKey(key)))
        {
            typename Map::LookupResult it;
            bool inserted = false;
            map.emplace(key_holder, it, inserted);
            applyBuildRowToMapped(it->getMapped(), inserted, refWordAt(row), pool, any_take_last_row, all_unique);
            return false;
        }
        const size_t hash = map.hash(key);
        const size_t pos = map.cursorPlace(hash);
        ring.pos[s] = static_cast<PosT>(pos);
        ring.row[s] = static_cast<UInt32>(row);
        if constexpr (store_hash)
            ring.hash[s] = hash;
        __builtin_prefetch(cells + pos, 1, 3);
        return true;
    }

    /// Re-admit a row after a growth cancelled it. `start`'s synchronous cases - skipped rows and
    /// the zero key - never entered the ring, so this cannot fail.
    template <typename RingT>
    ALWAYS_INLINE void reseed(RingT & ring, size_t s, size_t row)
    {
        auto && key_holder = key_getter.getKeyHolder(row, pool);
        const size_t hash = map.hash(keyHolderGetKey(key_holder));
        const size_t pos = map.cursorPlace(hash);
        ring.pos[s] = static_cast<PosT>(pos);
        ring.row[s] = static_cast<UInt32>(row);
        if constexpr (store_hash)
            ring.hash[s] = hash;
        __builtin_prefetch(cells + pos, 1, 3);
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
            hash = map.hash(key);
        Cell * cell = cells + ring.pos[s];
        if (map.cursorCellIsEmpty(cell))
        {
            /// Claim and write in the same visit, so no other in-flight row can also see this cell
            /// empty.
            const bool needs_grow = map.cursorClaim(cell, key_holder, hash);
            applyBuildRowToMapped(cell->getMapped(), /*inserted=*/true, refWordAt(row), pool, any_take_last_row, all_unique);
            return needs_grow ? AmacStepResult::DoneNeedsGrow : AmacStepResult::Done;
        }
        if (map.cursorKeyEquals(cell, key, hash))
        {
            applyBuildRowToMapped(cell->getMapped(), /*inserted=*/false, refWordAt(row), pool, any_take_last_row, all_unique);
            return AmacStepResult::Done;
        }
        const size_t next_pos = map.cursorNext(ring.pos[s]);
        ring.pos[s] = static_cast<PosT>(next_pos);
        __builtin_prefetch(cells + next_pos, 1, 3);
        return AmacStepResult::Advance;
    }

    void grow()
    {
        ++growths;
        map.cursorGrow();
        cells = map.cursorCells();
    }

    /// The driver runs on a frame-local copy, and these are the only fields the caller reads back.
    void writeBackTo(AmacBuildInsertPolicy & original) const
    {
        original.all_unique = all_unique;
        original.growths = growths;
    }
};

/// Inserts one compact section with the semantics of `insertFromBlockImplTypeCase` and the
/// `Inserter` family: one hash per build row inside `emplaceKey`, then the value shape's own append.
/// The recorded ref comes from the scattered locator column - 8-byte encoded or 4-byte packed - or,
/// on the single-leaf path, from `RowRef(block_no, i)` with `skip_bytes` excluding the rows that must
/// not be inserted.
template <typename KeyGetter, typename Map>
void insertSectionImpl(
    const HashJoin & join,
    Map & map,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    size_t rows,
    const UInt64 * locators,
    const UInt32 * narrow_locators,
    UInt32 block_no,
    const UInt8 * skip_bytes,
    Arena & pool,
    bool & all_values_unique,
    bool enable_prefetch,
    bool use_amac,
    UInt64 & amac_ring_growths)
{
    using Mapped = Map::mapped_type;
    constexpr bool mapped_asof = std::is_same_v<Mapped, AsofRowRefs>;

    /// The ASOF value sits at the row's own index in the trailing key column, so this only works
    /// where the compact index is the stored row - which is why ASOF plans stay single-leaf.
    const IColumn * asof_column [[maybe_unused]] = nullptr;
    if constexpr (mapped_asof)
    {
        if (locators || narrow_locators)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ASOF leaf inserts require the single-leaf build plan");
        asof_column = key_columns.back();
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

    const bool any_take_last_row = join.anyTakeLastRow();

    /// The ring replaces the sequential loop once the caller has decided the cell misses dominate
    /// and the section is long enough to amortize prime and drain. ASOF stays sequential: appending
    /// to a per-key sorted lookup is not a one-cell fused action.
    if constexpr (!mapped_asof && amac_join_supported<KeyGetter, Map>)
    {
        if (use_amac && rows >= amac_min_rows && rows < amac_inactive_row)
        {
            auto run_ring = [&]<typename PosT>()
            {
                AmacBuildInsertPolicy<KeyGetter, Map, PosT> policy{
                    .map = map,
                    .cells = map.cursorCells(),
                    .key_getter = key_getter,
                    .locators = locators,
                    .narrow_locators = narrow_locators,
                    .skip_bytes = skip_bytes,
                    .block_no = block_no,
                    .any_take_last_row = any_take_last_row,
                    .pool = pool};
                amacRun(policy, rows);
                all_values_unique = all_values_unique && policy.all_unique;
                amac_ring_growths += policy.growths;
            };
            /// The narrow slot needs the cell index to fit 32 bits for the whole run, growths
            /// included: a growth fires above half fill and doubles past degree 23, so a buffer only
            /// outgrows 2^32 cells past 2^31 keys.
            if (map.getBufferSizeInCells() <= (1uz << 32) && map.size() + rows <= (1uz << 30))
                run_ring.template operator()<UInt32>();
            else
                run_ring.template operator()<size_t>();
            return;
        }
    }

    constexpr bool can_prefetch = join_prefetch_supported<KeyGetter, Map>;
    bool use_prefetch = false;
    if constexpr (can_prefetch)
        use_prefetch = enable_prefetch && map.getBufferSizeInBytes() > getMinBytesForPrefetchInJoin();

    auto prefetcher = makeJoinPrefetcher(
        use_prefetch,
        rows,
        [&](size_t k) __attribute__((always_inline))
        {
            if constexpr (can_prefetch)
                map.prefetch(key_getter.getKeyHolder(k, pool));
        });

    bool all_unique = all_values_unique;
    for (size_t i = 0; i < rows; ++i)
    {
        if constexpr (can_prefetch)
            prefetcher.prefetchAt(i);

        if (skip_bytes && skip_bytes[i])
            continue;

        auto emplace_result = key_getter.emplaceKey(map, i, pool);

        if constexpr (mapped_asof)
        {
            Mapped * time_series_map = &emplace_result.getMapped();
            if (emplace_result.isInserted())
                time_series_map = new (time_series_map) Mapped(createAsofRowRef(*join.getAsofType(), join.getAsofInequality()));
            (*time_series_map)->insert(*asof_column, block_no, i);
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

            applyBuildRowToMapped(emplace_result.getMapped(), emplace_result.isInserted(), ref, pool, any_take_last_row, all_unique);
        }
    }
    all_values_unique = all_unique;
}

}

/// The stages communicate through exact per-bucket offsets: bucket `p` holds worker `w`'s stripe at
/// `[starts[p * workers + w], + worker_hist[w][p])`, in worker order. The last bucket collects
/// null-key rows; it is scattered like any other and dropped before the leaf builds, so a leaf only
/// ever sees insertable rows.
struct PartitionedHashJoin::PostBuildContext
{
    size_t workers = 0;
    size_t fanout = 0; /// pass-1 partitions + 1 (the null bucket); == partitions + 1 on single-pass plans
    size_t num_key_columns = 0;
    bool generic_mode = false;

    /// The first pass also scatters the saved route words, so a refine pass can derive its
    /// sub-bucket ids without touching the key columns. Once `refined`, every per-bucket container is
    /// final-leaf-indexed and has no drop bucket.
    size_t route_bits = 0; /// pass-1 bits (== total bits on single-pass plans)
    bool multi_pass = false;
    bool refined = false;
    size_t current_buckets = 0; /// buckets refine passes operate on (drop bucket excluded)
    std::vector<PaddedPODArray<UInt16>> routes;

    /// Generic mode after a refine pass: one self-contained piece per (key column, leaf).
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
        bool all_values_unique = true;
        bool predictions_exact = true;
        UInt64 leaf_rows = 0;
        UInt64 leaf_growths = 0;
    };
    std::deque<WorkerState> worker_state;

    std::vector<UInt64> leaf_reserve;
    std::vector<UInt64> leaf_bytes;
    std::vector<UInt32> leaf_order; /// largest first
    std::atomic<UInt32> leaf_claim{0};

    std::pair<size_t, size_t> blockStripe(size_t worker, size_t num_blocks) const
    {
        return {worker * num_blocks / workers, (worker + 1) * num_blocks / workers};
    }
};

namespace
{

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

}

void PartitionedHashJoin::decideAmacEngagement()
{
    /// The same heuristics that enable the standard loops' software prefetch: the user toggle plus
    /// the aggregate table size past the L2 threshold, below which the cell reads hit anyway and
    /// pipelining them costs more than it saves. Aggregate, not per-leaf, because the build streams
    /// scattered chunks through the cache alongside its leaf and the probe misses across all leaves.
    amac_build_engaged = amac_enabled && leaf_join->enableSoftwarePrefetch() && ht_total_bytes > getMinBytesForPrefetchInJoin();
}

void PartitionedHashJoin::collectLeafMapPointers()
{
    leaf_map_ptrs.resize(leaf_maps.size());
    leaf_map_descs.assign(leaf_maps.size(), LeafMapDesc{});
    for (size_t leaf = 0; leaf < leaf_maps.size(); ++leaf)
    {
        std::visit(
            [&](auto & shape_maps)
            {
                switch (leaf_join->data->type)
                {
#define M(TYPE) \
    case HashJoin::Type::TYPE: { \
        const auto & map = *shape_maps.TYPE; \
        leaf_map_ptrs[leaf] = &map; \
        if constexpr (AmacResumableMap<std::remove_cvref_t<decltype(map)>>) \
        { \
            leaf_map_descs[leaf] = LeafMapDesc{map.cursorCell(0), map.cursorMask()}; \
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
            leaf_maps[leaf].maps);
    }
}

void PartitionedHashJoin::insertLeafSection(
    PartitionedJoinMaps & maps,
    const ColumnRawPtrs & key_columns,
    size_t rows,
    const UInt64 * locators,
    const UInt32 * narrow_locators_data,
    UInt32 block_no,
    const UInt8 * skip_bytes,
    Arena & pool,
    bool & all_values_unique)
{
    const Sizes & key_sizes = leaf_join->key_sizes[0];
    const bool enable_prefetch = leaf_join->enableSoftwarePrefetch();
    UInt64 ring_growths = 0;

    std::visit(
        [&](auto & shape_maps)
        {
            switch (leaf_join->data->type)
            {
#define M(TYPE) \
    case HashJoin::Type::TYPE: { \
        using Map = typename decltype(shape_maps.TYPE)::element_type; \
        using KeyGetter = typename KeyGetterForType<HashJoin::Type::TYPE, Map>::Type; \
        insertSectionImpl<KeyGetter>( \
            *leaf_join, \
            *shape_maps.TYPE, \
            key_columns, \
            key_sizes, \
            rows, \
            locators, \
            narrow_locators_data, \
            block_no, \
            skip_bytes, \
            pool, \
            all_values_unique, \
            enable_prefetch, \
            amac_build_engaged, \
            ring_growths); \
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
        maps.maps);

    if (ring_growths)
        amac_ring_growths.fetch_add(ring_growths, std::memory_order_relaxed);
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
        /// Single-leaf has no histogram or scatter stage - every row is inserted straight from the
        /// stored blocks - so all of it charges to the leaf-build sub-phase.
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::PartitionedHashJoinBuildMicroseconds);
        ProfileEventTimeIncrement<Microseconds> leaf_watch(ProfileEvents::PartitionedHashJoinBuildLeafMicroseconds);
        all_values_unique = postBuildSingleLeaf();
    }
    else
    {
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
    if (stats.leaf_growths)
        ProfileEvents::increment(ProfileEvents::PartitionedHashJoinHashTableGrowths, stats.leaf_growths);
    if (const UInt64 growths = amac_ring_growths.load(std::memory_order_relaxed))
        ProfileEvents::increment(ProfileEvents::PartitionedHashJoinAmacRingGrowths, growths);

    /// For the next run of this query. A leaf map's size is the exact number of distinct keys it
    /// holds - one per cell, duplicates chaining inside - so this is better than re-publishing an
    /// estimate.
    if (stats_collecting_params.isCollectionAndUseEnabled())
    {
        const HashJoin::Type type = leaf_join->data->type;
        PartitionedHashJoinEntry entry;
        entry.bits = bits;
        entry.per_partition.resize(leaf_maps.size());
        for (size_t leaf = 0; leaf < leaf_maps.size(); ++leaf)
        {
            const size_t distinct = leaf_maps[leaf].getTotalRowCount(type);
            entry.per_partition[leaf] = distinct;
            entry.total_distinct += distinct;
        }
        getHashTablesStatistics<PartitionedHashJoinEntry>().update(entry, stats_collecting_params);
    }

    finishBuildPhase(all_values_unique);

    LOG_TRACE(
        log,
        "Built {} leaf hash tables: {} keys, {} of right-table data including the hash tables "
        "({} predicted for the exact-reserved buffers, {} leaf growths, {} ring growths)",
        partitions,
        getTotalRowCount(),
        ReadableSize(getTotalByteCount()),
        ReadableSize(ht_total_bytes),
        stats.leaf_growths,
        amac_ring_growths.load(std::memory_order_relaxed));
}

void PartitionedHashJoin::finishBuildPhase(bool all_values_unique)
{
    /// The leaf join's own barrier: used-flags init over its empty map, the ALL -> RightAny promotion
    /// when every build key turned out unique - the probe dispatches on the promoted strictness - and
    /// the non-joined status. The flags are resized to span every leaf afterwards, once the bucket
    /// counts are final.
    leaf_join->all_values_unique = all_values_unique;
    leaf_join->onBuildPhaseFinish();
    computeFlagBaseAndReinitUsedFlags();
    collectLeafMapPointers();
    leaf_join->data->keys_to_join = getTotalRowCount();
    build_phase_finished = true;
}

void PartitionedHashJoin::computeFlagBaseAndReinitUsedFlags()
{
    /// Leaf L's flags start at `flag_base[L]` and span its bucket count plus one, the extra slot
    /// covering the map's zero-value cell as the standard `getBufferSizeInCells() + 1` does. The
    /// probe shifts every `FindResult` offset by its leaf's base, which is what lets `JoinUsedFlags`
    /// and the non-joined iteration keep their single-map semantics.
    const HashJoin::Type type = leaf_join->data->type;
    flag_base.assign(1, 0);
    flag_base.reserve(leaf_maps.size() + 1);
    for (const auto & maps : leaf_maps)
        flag_base.push_back(flag_base.back() + maps.getBufferSizeInCells(type) + 1);

    /// `reinit` only grows, and does nothing for shapes without right-side flags. It has to run
    /// after the leaf join's barrier, which sized the flags to its own empty map.
    const bool prefer_use_maps_all = leaf_join->preferUseMapsAll();
    joinDispatch(
        leaf_join->getKind(),
        leaf_join->getStrictness(),
        leaf_join->data->maps.front(),
        prefer_use_maps_all,
        [&](auto kind_, auto strictness_, auto & map_)
        {
            leaf_join->used_flags->reinit<kind_, strictness_, std::is_same_v<std::decay_t<decltype(map_)>, HashJoin::MapsAll>>(
                flag_base.back());
        });

    /// Left empty for the shapes that never consult right-side flags, which the tests assert on.
    if (!leaf_join->used_flags->need_flags)
        flag_base.clear();
}

bool PartitionedHashJoin::postBuildSingleLeaf()
{
    const HashJoin::Type type = leaf_join->data->type;

    /// One leaf over the whole build, exact-reserved from the sketch and with no scatter: rows go in
    /// straight from the stored blocks with plain `RowRef(block_no, row)` refs.
    const size_t insertable_rows = accumulated_rows.load(std::memory_order_relaxed);
    const auto reserve
        = std::clamp<size_t>(static_cast<size_t>(std::ceil(hll_estimate * reserve_safety)), 1, std::max<size_t>(insertable_rows, 1));
    const size_t predicted_bytes = PartitionedJoinMaps::predictedBufferBytes(maps_variant_index, type, reserve);

    ht_total_bytes = predicted_bytes;
    decideAmacEngagement();

    leaf_maps.assign(1, PartitionedJoinMaps(maps_variant_index));
    build_arenas.emplace_back();

    leaf_maps[0].create(type, reserve);
    const size_t created_bytes = leaf_maps[0].getBufferSizeInBytes(type);
    stats.predictions_exact = created_bytes == predicted_bytes;

    bool all_values_unique = true;
    for (auto & fill : build_blocks)
    {
        insertLeafSection(
            leaf_maps[0],
            fill.key_columns,
            fill.rows,
            /*locators=*/nullptr,
            /*narrow_locators_data=*/nullptr,
            fill.block_no,
            fill.skipData(),
            build_arenas.front(),
            all_values_unique);
        ProfileEvents::increment(ProfileEvents::PartitionedHashJoinLeafRows, fill.rows);
        stats.leaf_rows += fill.rows;

        /// Consumed - drop this block's prepared keys and routes.
        fill.keys_holder.clear();
        fill.key_columns.clear();
        fill.null_map_holder.reset();
        fill.null_map = nullptr;
        fill.join_mask = JoinCommon::JoinMask();
        fill.skip_bytes = {};
        fill.routes = {};
    }
    if (leaf_maps[0].getBufferSizeInBytes(type) != created_bytes)
        ++stats.leaf_growths;
    return all_values_unique;
}

bool PartitionedHashJoin::postBuildPartitioned()
{
    PostBuildContext ctx;
    ctx.workers = std::max<size_t>(1, std::min(num_threads, build_blocks.size()));
    chassert(!pass_bits.empty());
    ctx.multi_pass = pass_bits.size() > 1;
    ctx.route_bits = pass_bits.front();
    ctx.fanout = (1uz << ctx.route_bits) + 1;
    ctx.num_key_columns = build_blocks.front().key_columns.size();

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

    ctx.worker_hist.resize_fill(ctx.workers * ctx.fanout, 0);
    ctx.bucket_rows.assign(ctx.fanout, 0);
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
    for (size_t w = 0; w < ctx.workers; ++w)
        build_arenas.emplace_back();

    post_build_pool = std::make_unique<ThreadPool>(
        CurrentMetrics::PartitionedHashJoinPoolThreads,
        CurrentMetrics::PartitionedHashJoinPoolThreadsActive,
        CurrentMetrics::PartitionedHashJoinPoolThreadsScheduled,
        /*max_threads_*/ ctx.workers,
        /*max_free_threads_*/ 0,
        /*queue_size_*/ ctx.workers);

    /// One wave of jobs per stage, with `post_build_pool->wait()` as the barrier. The build event
    /// accumulates per-worker thread time inside the jobs, so its unit matches the summed thread time
    /// `parallel_hash`'s build event reports.
    auto run_wave = [&](auto && stage, std::atomic<UInt64> & stage_thread_us)
    {
        try
        {
            for (size_t w = 0; w < ctx.workers; ++w)
                post_build_pool->scheduleOrThrow(
                    [&stage, &stage_thread_us, w, thread_group = CurrentThread::getGroup()]
                    {
                        ThreadGroupSwitcher switcher(thread_group, ThreadName::PARTITIONED_JOIN);
                        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::PartitionedHashJoinBuildMicroseconds);
                        Stopwatch stage_watch;
                        stage(w);
                        stage_thread_us.fetch_add(stage_watch.elapsedMicroseconds(), std::memory_order_relaxed);
                    });
            post_build_pool->wait();
        }
        catch (...)
        {
            post_build_pool->wait();
            throw;
        }
    };

    std::atomic<UInt64> hist_thread_us{0};
    std::atomic<UInt64> alloc_thread_us{0};
    std::atomic<UInt64> scatter_thread_us{0};
    std::atomic<UInt64> insert_thread_us{0};

    Stopwatch stage_watch;
    run_wave([&](size_t w) { histogramWorker(ctx, w); }, hist_thread_us);
    const UInt64 hist_wall_us = stage_watch.elapsedMicroseconds();

    stage_watch.restart();
    run_wave([&](size_t w) { allocateWorker(ctx, w); }, alloc_thread_us);
    const UInt64 alloc_wall_us = stage_watch.elapsedMicroseconds();

    stage_watch.restart();
    run_wave([&](size_t w) { scatterWorker(ctx, w); }, scatter_thread_us);
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
        /// Caller-thread work, counted like any other build thread time.
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::PartitionedHashJoinBuildMicroseconds);
        planHashTables(ctx);
    }
    const UInt64 plan_wall_us = stage_watch.elapsedMicroseconds();

    stage_watch.restart();
    run_wave([&](size_t w) { leafBuildWorker(ctx, w); }, insert_thread_us);
    const UInt64 insert_wall_us = stage_watch.elapsedMicroseconds();

    const auto to_ms = [](UInt64 us) { return static_cast<double>(us) / 1000.0; };
    LOG_TRACE(
        log,
        "Post-build stages, wall/thread ms: histogram {:.1f}/{:.1f}, chunk allocation {:.1f}/{:.1f}, scatter {:.1f}/{:.1f}, "
        "refine passes {:.1f}/{:.1f}, hash-table plan {:.1f}, leaf inserts {:.1f}/{:.1f} (AMAC {})",
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
        amac_build_engaged ? "engaged" : "off");

    /// Split the thread time collected above into the three sub-phase events. Each is a subset of the
    /// overall build event the waves already charged per worker.
    ProfileEvents::increment(
        ProfileEvents::PartitionedHashJoinBuildHistogramMicroseconds,
        hist_thread_us.load(std::memory_order_relaxed) + alloc_thread_us.load(std::memory_order_relaxed));
    ProfileEvents::increment(
        ProfileEvents::PartitionedHashJoinBuildScatterMicroseconds,
        scatter_thread_us.load(std::memory_order_relaxed) + refine_thread_us.load(std::memory_order_relaxed));
    ProfileEvents::increment(
        ProfileEvents::PartitionedHashJoinBuildLeafMicroseconds, plan_wall_us + insert_thread_us.load(std::memory_order_relaxed));

    post_build_pool.reset();

    bool all_values_unique = true;
    for (const auto & worker : ctx.worker_state)
    {
        all_values_unique &= worker.all_values_unique;
        stats.predictions_exact = stats.predictions_exact && worker.predictions_exact;
        stats.leaf_rows += worker.leaf_rows;
        stats.leaf_growths += worker.leaf_growths;
    }
    return all_values_unique;
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
    const auto [begin, end] = ctx.blockStripe(worker, build_blocks.size());
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
                auto [column, raw] = ColumnsScatter::allocateUninitializedFixed(*build_blocks.front().key_columns[c], running);
                ctx.fixed_out[c][p] = std::move(column);
                ctx.fixed_base[c][p] = raw.data();
            }
        }
    }
}

void PartitionedHashJoin::scatterWorker(PostBuildContext & ctx, size_t worker)
{
    auto & state = ctx.worker_state[worker];
    const auto [begin, end] = ctx.blockStripe(worker, build_blocks.size());

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

    auto release_block_inputs = [](FillBlock & fill)
    {
        fill.keys_holder.clear();
        fill.key_columns.clear();
        fill.null_map_holder.reset();
        fill.null_map = nullptr;
        fill.join_mask = JoinCommon::JoinMask();
        fill.skip_bytes = {};
        fill.routes = {};
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
    /// the parallelism safe. The leaf builds consume them in worker order, matching the locator
    /// layout.
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
    /// scattered route words, group-major, so after the last pass a row's leaf is `route >> (16 -
    /// bits)` - the same leaf a single-pass plan would give it, and the one the probe derives. Groups
    /// are claimed dynamically because their sizes can be skewed, and each group's inputs are freed
    /// as they are consumed so the pass cycles memory rather than doubling the scattered side.
    const size_t groups = ctx.current_buckets;
    const size_t sub_fanout = 1uz << refine_bits;
    const size_t new_buckets = groups * sub_fanout;
    const bool last_pass = bits_done + refine_bits == bits;
    chassert(bits_done + refine_bits <= 16);
    chassert(sub_fanout <= ColumnsScatter::MAX_FANOUT_PER_PASS);
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

void PartitionedHashJoin::planHashTables(PostBuildContext & ctx)
{
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

    stats.leaf_row_counts.assign(ctx.bucket_rows.begin(), ctx.bucket_rows.begin() + partitions);

    const HashJoin::Type type = leaf_join->data->type;

    /// A previous run's per-partition breakdown is folded or split to this build's partition count -
    /// the two leaf ranges always nest, both being MSB-first partitions of the same route space, so a
    /// coarser cache sums and a finer one splits uniformly. Without one, the single estimate is
    /// rescaled uniformly. Either way the clamp just below bounds each leaf by its exact row count,
    /// so a stale estimate can only mis-size a reserve.
    std::vector<UInt64> per_leaf_distinct;
    if (cached_stats && !cached_stats->per_partition.empty())
    {
        const size_t cached_bits = cached_stats->bits;
        chassert(cached_stats->per_partition.size() == (1uz << cached_bits));
        per_leaf_distinct.assign(partitions, 0);
        if (cached_bits == bits)
        {
            for (size_t leaf = 0; leaf < partitions; ++leaf)
                per_leaf_distinct[leaf] = cached_stats->per_partition[leaf];
        }
        else if (cached_bits > bits)
        {
            const size_t group = 1uz << (cached_bits - bits);
            for (size_t i = 0; i < cached_stats->per_partition.size(); ++i)
                per_leaf_distinct[i / group] += cached_stats->per_partition[i];
        }
        else
        {
            const size_t group = 1uz << (bits - cached_bits);
            for (size_t j = 0; j < cached_stats->per_partition.size(); ++j)
            {
                const UInt64 split = cached_stats->per_partition[j] / group;
                for (size_t k = 0; k < group; ++k)
                    per_leaf_distinct[j * group + k] = split;
            }
        }
    }

    const auto per_leaf_estimate
        = std::max<UInt64>(1, static_cast<UInt64>(std::ceil(hll_estimate * reserve_safety / static_cast<double>(partitions))));

    ctx.leaf_reserve.resize(partitions);
    ctx.leaf_bytes.resize(partitions);
    UInt64 running = 0;
    for (size_t leaf = 0; leaf < partitions; ++leaf)
    {
        const UInt64 leaf_hint = per_leaf_distinct.empty()
            ? per_leaf_estimate
            : std::max<UInt64>(1, static_cast<UInt64>(std::ceil(static_cast<double>(per_leaf_distinct[leaf]) * reserve_safety)));
        /// An estimate may shrink a leaf below its row count but never inflate it past it.
        ctx.leaf_reserve[leaf] = std::clamp<UInt64>(leaf_hint, 1, std::max<UInt64>(ctx.bucket_rows[leaf], 1));
        ctx.leaf_bytes[leaf] = PartitionedJoinMaps::predictedBufferBytes(maps_variant_index, type, ctx.leaf_reserve[leaf]);
        running += ctx.leaf_bytes[leaf];
    }

    /// Nothing is allocated here; the worker that claims a leaf allocates its buffer.
    ht_total_bytes = running;
    decideAmacEngagement();

    leaf_maps.assign(partitions, PartitionedJoinMaps(maps_variant_index));
    ctx.leaf_order.resize(partitions);
    for (size_t leaf = 0; leaf < partitions; ++leaf)
        ctx.leaf_order[leaf] = static_cast<UInt32>(leaf);
    std::sort(ctx.leaf_order.begin(), ctx.leaf_order.end(), [&](UInt32 a, UInt32 b) { return ctx.bucket_rows[a] > ctx.bucket_rows[b]; });
}

void PartitionedHashJoin::leafBuildWorker(PostBuildContext & ctx, size_t worker)
{
    const HashJoin::Type type = leaf_join->data->type;
    auto & state = ctx.worker_state[worker];
    Arena & arena = build_arenas[worker];

    ColumnRawPtrs section_columns(ctx.num_key_columns);

    /// Largest first, and claimed dynamically, so skew cannot serialize the build behind a
    /// worker-to-partition affinity.
    while (true)
    {
        const UInt32 claim = ctx.leaf_claim.fetch_add(1, std::memory_order_relaxed);
        if (claim >= partitions)
            break;
        const UInt32 leaf = ctx.leaf_order[claim];

        /// Allocates the leaf's buffer on this worker; see `ZeroingHashTableAllocator` for why that
        /// matters.
        leaf_maps[leaf].create(type, ctx.leaf_reserve[leaf]);
        const size_t created_bytes = leaf_maps[leaf].getBufferSizeInBytes(type);
        state.predictions_exact = state.predictions_exact && created_bytes == ctx.leaf_bytes[leaf];

        const UInt64 leaf_rows = ctx.bucket_rows[leaf];
        if (!ctx.generic_mode)
        {
            for (size_t c = 0; c < ctx.num_key_columns; ++c)
                section_columns[c] = ctx.fixed_out[c][leaf].get();
            insertLeafSection(
                leaf_maps[leaf],
                section_columns,
                leaf_rows,
                narrow_locators ? nullptr : ctx.locators[leaf].data(),
                narrow_locators ? ctx.locators32[leaf].data() : nullptr,
                /*block_no=*/0,
                /*skip_bytes=*/nullptr,
                arena,
                state.all_values_unique);
        }
        else if (ctx.refined)
        {
            /// After the refine passes there is one piece per key column, aligned with the leaf's
            /// whole locator array.
            for (size_t c = 0; c < ctx.num_key_columns; ++c)
                section_columns[c] = ctx.refined_pieces[c][leaf].get();
            insertLeafSection(
                leaf_maps[leaf],
                section_columns,
                leaf_rows,
                narrow_locators ? nullptr : ctx.locators[leaf].data(),
                narrow_locators ? ctx.locators32[leaf].data() : nullptr,
                /*block_no=*/0,
                /*skip_bytes=*/nullptr,
                arena,
                state.all_values_unique);
        }
        else
        {
            /// A leaf's pieces in worker order are exactly its locator layout.
            for (size_t piece_worker = 0; piece_worker < ctx.workers; ++piece_worker)
            {
                const size_t piece_rows = ctx.worker_hist[piece_worker * ctx.fanout + leaf];
                if (piece_rows == 0)
                    continue;
                for (size_t c = 0; c < ctx.num_key_columns; ++c)
                    section_columns[c] = ctx.pieces[c][piece_worker][leaf].get();
                const UInt64 piece_start = ctx.starts[leaf * ctx.workers + piece_worker];
                insertLeafSection(
                    leaf_maps[leaf],
                    section_columns,
                    piece_rows,
                    narrow_locators ? nullptr : ctx.locators[leaf].data() + piece_start,
                    narrow_locators ? ctx.locators32[leaf].data() + piece_start : nullptr,
                    /*block_no=*/0,
                    /*skip_bytes=*/nullptr,
                    arena,
                    state.all_values_unique);
            }
        }
        state.leaf_rows += leaf_rows;
        ProfileEvents::increment(ProfileEvents::PartitionedHashJoinLeafRows, leaf_rows);

        if (leaf_maps[leaf].getBufferSizeInBytes(type) != created_bytes)
            ++state.leaf_growths;

        /// Released as soon as they are consumed, so the tables replace the chunks rather than
        /// coexisting with them.
        if (narrow_locators)
            ctx.locators32[leaf] = {};
        else
            ctx.locators[leaf] = {};
        if (!ctx.generic_mode)
        {
            for (size_t c = 0; c < ctx.num_key_columns; ++c)
                ctx.fixed_out[c][leaf].reset();
        }
        else if (ctx.refined)
        {
            for (size_t c = 0; c < ctx.num_key_columns; ++c)
                ctx.refined_pieces[c][leaf].reset();
        }
        else
        {
            for (size_t c = 0; c < ctx.num_key_columns; ++c)
                for (size_t piece_worker = 0; piece_worker < ctx.workers; ++piece_worker)
                    ctx.pieces[c][piece_worker][leaf].reset();
        }
    }
}

}
