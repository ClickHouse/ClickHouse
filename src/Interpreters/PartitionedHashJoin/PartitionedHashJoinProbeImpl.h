#pragma once

#include <Interpreters/HashJoin/AddedColumns.h>
#include <Interpreters/HashJoin/HashJoinMethodsImpl.h>
#include <Interpreters/HashJoin/HashJoinResult.h>
#include <Interpreters/HashJoin/JoinUsedFlags.h>
#include <Interpreters/HashJoin/KeyGetter.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/PartitionedHashJoin/AmacRing.h>
#include <Interpreters/PartitionedHashJoin/JoinRouteHashing.h>
#include <Interpreters/PartitionedHashJoin/PartitionedHashJoin.h>
#include <Interpreters/RowRefs.h>
#include <Interpreters/TableJoin.h>
#include <base/scope_guard.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/HashTable/HashTable.h>
#include <Common/PODArray.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
extern const Event PartitionedHashJoinProbeLookupMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNSUPPORTED_JOIN_KEYS;
}

/// Mapped values the find pass can record by value. Both are 8-byte words - a `RowRef` encodes to
/// its ref word, a `RowRefList` is one - that are never 0 for a built cell, since a `RowRef` always
/// carries `INLINE_FLAG` in bit 63 and a `RowRefList` word is either an inline ref or a non-null
/// node pointer, so 0 is free to encode a miss. Everything the second pass does with a match reads
/// only the word, and the probe maps are immutable, so the copy is the cell.
template <typename Mapped>
inline constexpr bool amac_mapped_fits_word = std::is_same_v<Mapped, RowRef> || std::is_same_v<Mapped, RowRefList>;

template <typename Mapped>
requires amac_mapped_fits_word<Mapped>
ALWAYS_INLINE UInt64 mappedWordOf(const Mapped & mapped)
{
    if constexpr (std::is_same_v<Mapped, RowRefList>)
        return mapped.word;
    else
        return mapped.encode();
}

template <typename Mapped>
requires amac_mapped_fits_word<Mapped>
ALWAYS_INLINE Mapped mappedFromWord(UInt64 word)
{
    if constexpr (std::is_same_v<Mapped, RowRefList>)
        return RowRefList::fromWord(word);
    else
        return RowRef::fromWord(word);
}

/// What the descriptor-based lookups below assume of a grower: the home cell is `hash & mask` and
/// the chain steps by one, so a 16-byte {buffer, mask} descriptor is all a lookup needs. A grower
/// without it must keep the map-resolved paths.
template <typename Grower>
concept LinearProbingGrower = Grower::performs_linear_probing_with_single_step;

/** The find pass of the two-phase probe: out-of-order lookups that emit nothing and only fill the
  * per-row result arrays - `found_word`, and for the flagged shapes the used-flags offset already
  * shifted into the shared space. Recording the word in the same visit that reads the cell is what
  * keeps the second pass from touching the cell again: by the time an in-order loop reaches that row,
  * a block later, the line has usually left the cache, and re-reading it through a recorded pointer
  * cost a second random miss per row. ASOF does not fit a word and keeps the pointer scheme.
  *
  * One ring serves as many maps as there are leaves. A row's leaf is resolved once at admit and the
  * slot carries the resolved cell pointer, so a steady visit dereferences nothing but the cell and
  * the key - the map headers, scattered across one heap object per partition, would otherwise add two
  * or three dependent loads to every visit. The selector variant is a template parameter for the same
  * reason: it used to be a per-visit branch.
  */
template <typename KeyGetter, typename Map, bool need_flags, bool selector_is_range>
struct RoutedAmacFindPolicy
{
    using MapNonConst = std::remove_const_t<Map>;
    using Cell = MapNonConst::cell_type;
    static constexpr bool store_hash = cell_stores_hash<Cell>;
    static constexpr bool may_grow = false;
    static constexpr bool copy_into_frame = true; /// results live in the arrays; no state survives the run
    static constexpr bool mapped_by_value = amac_mapped_fits_word<typename MapNonConst::mapped_type>;

    /// The walk below is `HashMapTable::find` only under `FlatLookupMap`'s contract: a linear
    /// grower, and stateless cells whose zero-check and key-compare read nothing through the map.
    /// Every map the AMAC gate admits satisfies it.
    static_assert(LinearProbingGrower<typename MapNonConst::grower_type>);
    static_assert(std::is_same_v<typename Cell::State, HashTableNoState>);
    static constexpr HashTableNoState no_state{};

    /// The key exactly as the map compares it: fixed keys by value, string keys as a view into the
    /// probe column. Trivially copyable across the whole admitted getter set - the serialized getter
    /// is gated out, and the arena-backed string holder persists nothing on the find path.
    using KeyHolder = std::remove_reference_t<decltype(std::declval<KeyGetter &>().getKeyHolder(0uz, std::declval<Arena &>()))>;
    using StoredKey = std::decay_t<decltype(keyHolderGetKey(std::declval<KeyHolder &>()))>;
    static_assert(std::is_trivially_copyable_v<StoredKey>);

    /** The find-ring state. The resolved cell pointer stands in for a {buffer, mask, position}
      * triple, and `cell == nullptr` is the inactive sentinel - value-initialization means
      * all-inactive - which frees `row` for the full 16-bit range of the driver's chunks. The leaf id
      * stays because the record path needs the leaf's used-flags base, and the hit position is
      * recovered as `cell - buf` once per matched row. The key is packed at admit and re-read per
      * visit: re-fetching it through `getKeyHolder` re-packed the wide fixed keys from the column
      * pointers on every visit, which measured as the dominant per-visit cost of the wide-key ring.
      */
    template <size_t ring_size>
    struct RingBase
    {
        std::array<const Cell *, ring_size> cell{}; /// the cell the next visit reads; nullptr == inactive
        std::array<UInt16, ring_size> row{}; /// chunk-local probe row
        std::array<UInt16, ring_size> leaf{};
        alignas(64) std::array<StoredKey, ring_size> key{};

        bool isActive(size_t s) const { return cell[s] != nullptr; }
        void deactivate(size_t s) { cell[s] = nullptr; }
        UInt32 rowAt(size_t s) const { return row[s]; }
    };
    template <size_t ring_size>
    struct RingWithHash : public RingBase<ring_size>
    {
        std::array<size_t, ring_size> hash{};
    };
    template <size_t ring_size>
    using Ring = std::conditional_t<store_hash, RingWithHash<ring_size>, RingBase<ring_size>>;

    /// Chunked so the ring's row index fits 16 bits. The default probe block is one chunk.
    static constexpr size_t chunk_rows_max = 1uz << 13;

    /// By value where possible, so the key-column pointer is a field of the frame-local policy
    /// rather than two dependent loads behind a reference.
    std::conditional_t<std::is_trivially_copyable_v<KeyGetter>, KeyGetter, KeyGetter &> key_getter;
    /// Reads nothing through the object - the hash functor is an empty base and the cells are
    /// stateless - so any leaf serves as the provider.
    const MapNonConst & map0;
    const void * const * leaf_maps_data = nullptr; /// the zero-key sentinel path only
    const LeafMapDesc * leaf_descs = nullptr;
    const UInt16 * leaf_ids = nullptr; /// null at the single-leaf plan
    size_t selector_base = 0; /// the first row of a continuous-range selector
    const UInt64 * selector_indexes = nullptr; /// the data of an explicit-indexes selector
    const UInt8 * skip_data = nullptr; /// null on the fast path
    const UInt64 * flag_base_data = nullptr;
    Arena & pool;
    UInt64 * found_word = nullptr;
    UInt64 * found_offset = nullptr; /// null unless `need_flags`

    ALWAYS_INLINE size_t indexAt(size_t i) const
    {
        if constexpr (selector_is_range)
            return selector_base + i;
        else
            return selector_indexes[i];
    }

    ALWAYS_INLINE const MapNonConst & mapAt(size_t leaf) const { return *static_cast<Map *>(leaf_maps_data[leaf]); }

    /// `start`'s synchronous zero-key path: the cell came from the map object, so its used-flags
    /// offset has to as well.
    ALWAYS_INLINE void record(size_t row, size_t leaf [[maybe_unused]], const Cell * cell, const MapNonConst & map [[maybe_unused]])
    {
        if (!cell)
        {
            found_word[row] = 0;
            return;
        }
        if constexpr (mapped_by_value)
            found_word[row] = mappedWordOf(cell->getMapped());
        else
            found_word[row] = reinterpret_cast<UInt64>(&cell->getMapped());
        if constexpr (need_flags)
            found_offset[row] = map.offsetInternal(cell) + flag_base_data[leaf];
    }

    /// The cell is known non-zero, so its used-flags offset is its buffer position + 1 - what
    /// `offsetInternal` would return, without touching the map. Recovering the position costs one
    /// descriptor load, but only here, once per matched row, and only for the flagged shapes.
    ALWAYS_INLINE void recordHit(size_t row, size_t leaf [[maybe_unused]], const Cell * cell)
    {
        if constexpr (mapped_by_value)
            found_word[row] = mappedWordOf(cell->getMapped());
        else
            found_word[row] = reinterpret_cast<UInt64>(&cell->getMapped());
        if constexpr (need_flags)
        {
            const auto pos = static_cast<size_t>(cell - static_cast<const Cell *>(leaf_descs[leaf].buf));
            found_offset[row] = pos + 1 + flag_base_data[leaf];
        }
    }

    template <typename RingT>
    ALWAYS_INLINE bool start(RingT & ring, size_t s, size_t i)
    {
        const size_t ind = indexAt(i);
        if (skip_data && skip_data[ind])
        {
            found_word[i] = 0;
            return false;
        }
        auto && key_holder = key_getter.getKeyHolder(ind, pool);
        const auto & key = keyHolderGetKey(key_holder);
        const size_t leaf = leaf_ids ? leaf_ids[ind] : 0;
        if (unlikely(map0.isZeroKey(key)))
        {
            /// The zero-value cell has no walk to overlap.
            const MapNonConst & map = mapAt(leaf);
            record(i, leaf, map.find(key), map);
            return false;
        }
        const size_t hash = map0.hash(key);
        ring.key[s] = key;
        const LeafMapDesc & desc = leaf_descs[leaf];
        const Cell * cell = static_cast<const Cell *>(desc.buf) + (hash & desc.mask);
        ring.cell[s] = cell;
        ring.row[s] = static_cast<UInt16>(i);
        ring.leaf[s] = static_cast<UInt16>(leaf);
        if constexpr (store_hash)
            ring.hash[s] = hash;
        prefetchCell(cell);
        return true;
    }

    /// Locality 3, not 1, and the whole cell rather than its first line. Locality 1 - "the cell is
    /// not revisited" - compiles to `prfm pldl3keep` on AArch64, which stages the line in L3 only and
    /// leaves the visit's demand load paying the full L1-miss latency; that measured as the ring's
    /// dominant stall on wide keys. Not revisiting a line makes L1 pollution cheap, but it does not
    /// make an L3-resident load fast. Cells past 24 bytes straddle two lines often enough - a 40-byte
    /// one does on roughly 61% of positions - that the second line has to be prefetched too, or its
    /// limb compares stall the same way.
    static ALWAYS_INLINE void prefetchCell(const Cell * cell)
    {
        __builtin_prefetch(cell, 0, 3);
        if constexpr (sizeof(Cell) > 24)
            __builtin_prefetch(reinterpret_cast<const char *>(cell) + sizeof(Cell) - 1, 0, 3);
    }

    template <typename RingT>
    ALWAYS_INLINE AmacStepResult step(RingT & ring, size_t s)
    {
        const Cell * cell = ring.cell[s];
        if (cell->isZero(no_state))
        {
            found_word[ring.row[s]] = 0;
            return AmacStepResult::Done;
        }
        const StoredKey & key = ring.key[s];
        /// Only the saved-hash cells (the string keys) read the hash at all - as the compare
        /// prefilter. Every other cell ignores the argument, so passing a literal beats
        /// recomputing a value nothing looks at, once per visit.
        size_t hash = 0;
        if constexpr (store_hash)
            hash = ring.hash[s];
        if (cell->keyEquals(key, hash, no_state))
        {
            recordHit(ring.row[s], ring.leaf[s], cell);
            return AmacStepResult::Done;
        }
        /// The descriptor is only read here, on a collision - at load factor 0.5 the vast majority
        /// of lookups end at the home cell or an empty one and never touch it.
        const LeafMapDesc & desc = leaf_descs[ring.leaf[s]];
        const Cell * buf = static_cast<const Cell *>(desc.buf);
        if (++cell == buf + desc.mask + 1) [[unlikely]]
            cell = buf;
        ring.cell[s] = cell;
        prefetchCell(cell);
        return AmacStepResult::Advance;
    }
};

/** A map whose find needs nothing from the map object: a linear grower, so the home cell and the
  * walk are computable from the 16-byte leaf descriptor alone, and stateless cells, whose
  * `isZero`/`keyEquals` read only the cell and the key. The fixed-size maps have no cursor API, and
  * the string, `hashed` and LowCardinality getters are excluded by the caller's cheap-key gate.
  */
template <typename Map>
concept FlatLookupMap = AmacResumableMap<Map> && requires {
    requires LinearProbingGrower<typename Map::grower_type>;
    requires std::is_same_v<typename Map::cell_type::State, HashTableNoState>;
};

/** The routed probe: the single-map `joinRightColumns` loop with one difference - a row's map is the
  * leaf its recomputed route word points at. Probe blocks are never scattered, buffered or
  * materialized, and everything around the lookup is the standard `HashJoin` machinery over the
  * shared row store.
  *
  * Above the engagement threshold the lookups run as two passes per block: a find ring completing
  * rows out of order into the reused scratch, then an in-order pass over its results. On the
  * flagless word-mapped lazy shapes that pass degenerates to `word_loop`; the rest run the same
  * sequential loop with the lookup replaced by the precomputed result. Either way the replication
  * offsets, used-flags semantics and per-kind logic are untouched.
  *
  * `MapsShape` is the standard shape driving `JoinFeatures` and `processMatch`; `Map` is the
  * partitioned leaf map holding identical cells. A found cell's offset is shifted by its leaf's
  * `flag_base` before `processMatch` sees it, which is what keeps `JoinUsedFlags` single-map.
  */
template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsShape, typename KeyGetter, typename Map, typename AddedColumnsType>
size_t PartitionedHashJoin::routedJoinRightColumns(AddedColumnsType & added_columns, const ScatteredBlock & block, size_t lane)
{
    constexpr JoinFeatures<KIND, STRICTNESS, MapsShape> join_features;
    /// The per-row-flags shapes take the delegated standard path instead.
    constexpr bool flag_per_row = false;

    if (added_columns.additional_filter_expression)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Additional filter expression is not supported for PartitionedHashJoin");

    const auto & join_keys = added_columns.join_on_keys.at(0);
    const auto & selector = block.getSelector();
    const size_t rows = selector.size();
    JoinStuff::JoinUsedFlags & used_flags = *leaf_join->used_flags;
    const UInt64 * flag_base_data [[maybe_unused]] = flag_base.data();

    /// The entries were stored by the same `data->type` and maps-variant dispatch that selected this
    /// template, so the cast is a round trip.
    const void * const * leaf_maps_data = leaf_map_ptrs.data();
    auto map_at = [&](size_t leaf) -> Map & { return *static_cast<Map *>(leaf_maps_data[leaf]); };

    /// Acquired only where it is needed - routing above zero bits, the find pass's result arrays -
    /// so a single-leaf plan pays nothing for it.
    std::unique_ptr<ProbeScratch> scratch;
    SCOPE_EXIT({
        if (scratch)
            releaseProbeScratch(std::move(scratch), lane);
    });
    auto ensure_scratch = [&]() -> ProbeScratch &
    {
        if (!scratch)
            scratch = acquireProbeScratch(lane);
        return *scratch;
    };

    /// One per probe row of the whole source block, so continuation chunks share it.
    const size_t source_rows = block.getSourceBlock().rows();
    const UInt16 * leaf_ids = nullptr;
    if (bits > 0 && source_rows > 0)
    {
        chassert(!join_features.is_asof_join);
        auto & routing = ensure_scratch();
        routing.leaf_ids.resize(source_rows);
        computeJoinLeafIds(join_keys.key_columns, source_rows, bits, routing.leaf_ids.data());
        leaf_ids = routing.leaf_ids.data();
    }

    /// As in `createKeyGetter`: the ASOF getter excludes the inequality column.
    auto key_getter = [&]
    {
        if constexpr (join_features.is_asof_join)
        {
            ColumnRawPtrs equi_columns(join_keys.key_columns.begin(), join_keys.key_columns.end() - 1);
            Sizes equi_sizes(join_keys.key_sizes.begin(), join_keys.key_sizes.end() - 1);
            return KeyGetter(equi_columns, equi_sizes, nullptr);
        }
        else
        {
            return KeyGetter(join_keys.key_columns, join_keys.key_sizes, nullptr);
        }
    }();

    /// One byte merging the null map and the ON mask, as in the single-map loop; where neither
    /// applies the check compiles out.
    const bool fast_path = !join_keys.null_map && join_keys.join_mask_column.getKind() == JoinCommon::JoinMask::Kind::AllTrue;

    if constexpr (!flag_per_row && (STRICTNESS == JoinStrictness::All || (STRICTNESS == JoinStrictness::Semi && KIND == JoinKind::Right)))
        added_columns.lazy_output.output_by_row_list = true;

    if constexpr (join_features.need_replication)
        added_columns.offsets_to_replicate = IColumn::Offsets(rows);

    Arena pool;

    const UInt8 * skip_data = nullptr;
    IColumn::Filter skip_buffer;
    if (!fast_path)
    {
        if (selector.isContinuousRange())
            skip_data = join_keys.buildRowSkipData(skip_buffer, selector.getRange().first, rows);
        else
            skip_data = join_keys.buildRowSkipData(skip_buffer, selector.getIndexes());
    }

    /// Above the threshold the ring is the only engaged probe path for every capable shape. On the
    /// string-key maps it is also the only mechanism that overlaps the cell misses at all - the
    /// look-ahead prefetcher cannot run there, `getKeyHolder` per look-ahead being too expensive for
    /// its heuristic - and measured ~20% faster end-to-end than the plain loop. On the cheap-key
    /// getters it beats the flat loop's adaptive look-ahead too, ~11% of probe thread time at 16
    /// threads. The conditions mirror the software-prefetch heuristics: the user toggle, the
    /// aggregate table size past L2, and a row floor below which prime and drain dominate.
    using MapNonConst = std::remove_const_t<Map>;
    constexpr bool amac_supported = amac_join_supported<KeyGetter, MapNonConst>;
    constexpr bool prefetch_supported = join_prefetch_supported<KeyGetter, Map>;
    /// The cheap-key open-addressing shapes take the flat-descriptor loop rather than the plain one.
    constexpr bool flat_lookup_supported = prefetch_supported && FlatLookupMap<MapNonConst>;
    bool use_amac = false;
    if constexpr (amac_supported)
    {
        use_amac = amac_enabled && added_columns.enable_prefetch && ht_total_bytes > getMinBytesForPrefetchInJoin()
            && rows >= amac_min_rows;
        /// The wide fixed keys used to be excluded here unless the leaves were DRAM-deep, because
        /// the ring re-packed the key per visit and its prefetch only staged the cell in L3, which
        /// lost to the look-ahead-prefetched flat loop while the leaves stayed cache-resident. With
        /// the key packed once at admit and `prefetchCell` staging every line into L1, the ring's
        /// lookup measured 35% faster than the flat loop on the former worst shape, so the exclusion
        /// is gone.
    }

    /// Mutually exclusive with the find pass, on the same threshold: the leaf tables are cache-sized
    /// by design, so both only fire once the aggregate size outgrows it.
    constexpr bool can_prefetch = prefetch_supported;
    bool use_prefetch = false;
    if constexpr (can_prefetch)
        use_prefetch = !use_amac && added_columns.enable_prefetch && ht_total_bytes > getMinBytesForPrefetchInJoin();

    auto prefetcher = makeJoinPrefetcher(
        use_prefetch,
        rows,
        [&](size_t k) __attribute__((always_inline))
        {
            if constexpr (can_prefetch)
            {
                const size_t ind = selector[k];
                map_at(leaf_ids ? leaf_ids[ind] : 0).prefetch(key_getter.getKeyHolder(ind, pool));
            }
        });

    /// Serves as both the in-order second pass and the plain loop. With `precomputed` the lookup is
    /// the find pass's result, and the skip check compiles out because skipped rows were recorded as
    /// misses there. Everything downstream is the standard machinery.
    auto loop = [&]<bool need_filter, bool with_skip, bool precomputed>(const ProbeScratch * results)
    {
        if constexpr (need_filter)
        {
            added_columns.filter = IColumn::Filter(rows, 0);
            added_columns.matched_rows.reserve(rows);
        }

        /// Const-qualified: probe maps are immutable.
        using Mapped = std::remove_reference_t<decltype(std::declval<typename KeyGetter::FindResult &>().getMapped())>;

        IColumn::Offset current_offset = 0;
        for (size_t i = 0; i < rows; ++i)
        {
            if constexpr (can_prefetch && !precomputed)
                prefetcher.prefetchAt(i);

            const size_t ind = selector[i];

            bool right_row_found = false;
            KnownRowsHolder<flag_per_row> dummy_known_rows;

            if constexpr (precomputed)
            {
                if (const UInt64 word = results->found_word[i])
                {
                    right_row_found = true;
                    size_t offset = 0;
                    if constexpr (join_features.need_flags)
                        offset = results->found_offset[i];
                    /// The find pass decided by-value recording from the map's mapped type and this
                    /// side decides from the `FindResult`'s. If they ever differ, a word would be
                    /// reinterpreted as a pointer.
                    static_assert(std::is_same_v<std::remove_const_t<Mapped>, typename std::remove_const_t<Map>::mapped_type>);
                    if constexpr (amac_mapped_fits_word<std::remove_const_t<Mapped>>)
                    {
                        /// Rebuilt on the stack from the recorded word; the cell is not touched.
                        auto mapped_value = mappedFromWord<std::remove_const_t<Mapped>>(word);
                        typename KeyGetter::FindResult find_result(&mapped_value, true, offset);
                        processMatch<KIND, STRICTNESS, need_filter, flag_per_row, MapsShape, Map, KeyGetter>(
                            find_result, added_columns, used_flags, i, ind, current_offset, dummy_known_rows, /*is_last_disjunct=*/ true);
                    }
                    else
                    {
                        typename KeyGetter::FindResult find_result(
                            reinterpret_cast<Mapped *>(word), true, offset); /// NOLINT(performance-no-int-to-ptr)
                        processMatch<KIND, STRICTNESS, need_filter, flag_per_row, MapsShape, Map, KeyGetter>(
                            find_result, added_columns, used_flags, i, ind, current_offset, dummy_known_rows, /*is_last_disjunct=*/ true);
                    }
                }
            }
            else
            {
                bool skip_row = false;
                if constexpr (with_skip)
                    skip_row = skip_data && skip_data[ind];

                if (!skip_row)
                {
                    const size_t leaf = leaf_ids ? leaf_ids[ind] : 0;
                    auto find_result = key_getter.findKey(map_at(leaf), ind, pool);
                    if (find_result.isFound())
                    {
                        right_row_found = true;
                        if constexpr (join_features.need_flags)
                        {
                            /// Into the shared flag space, before the standard machinery reads it.
                            find_result = typename KeyGetter::FindResult(
                                &find_result.getMapped(), true, find_result.getOffset() + flag_base_data[leaf]);
                        }
                        processMatch<KIND, STRICTNESS, need_filter, flag_per_row, MapsShape, Map, KeyGetter>(
                            find_result, added_columns, used_flags, i, ind, current_offset, dummy_known_rows, /*is_last_disjunct=*/ true);
                    }
                }
            }

            if (!right_row_found)
            {
                if constexpr (join_features.is_anti_join && join_features.left)
                    setUsed<need_filter>(added_columns.filter, i, added_columns.matched_rows);
                addNotFoundRow<join_features.add_missing, join_features.need_replication>(added_columns, current_offset);
            }

            if constexpr (join_features.need_replication)
                added_columns.offsets_to_replicate[i] = current_offset;
        }
    };

    /// Whether the second pass can degenerate to `word_loop`: the recorded word has to be the mapped
    /// value itself, the emit has to be the lazy ref-word append, and the shape must consume no
    /// per-row state beyond the filter, the appended words and the replication offsets. The flagged
    /// shapes and ASOF keep the full loop.
    constexpr bool degenerate_phase_b = AddedColumnsType::isLazy() && amac_mapped_fits_word<typename MapNonConst::mapped_type>
        && !join_features.need_flags && !join_features.is_asof_join && !join_features.is_any_join;

    /// On the shapes gated above, `processMatch` reduces to marking the row matched and appending one
    /// word - the list word for ALL, advancing the replication offset by its row count, or its first
    /// ref - so this pass reads `found_word` directly instead of rebuilding a `FindResult` per row and
    /// dispatching through an outlined `appendFromBlock` that forced the loop-carried state to spill.
    /// At most one append per row means the cursors write into pre-sized arrays with no capacity
    /// check. Row order, filter, offsets and `row_count` match the full loop, which the parity tests
    /// pin.
    auto word_loop = [&]<bool need_filter, bool with_refs>(const ProbeScratch & results [[maybe_unused]])
    {
        if constexpr (degenerate_phase_b)
        {
            using Mapped = MapNonConst::mapped_type;

            if constexpr (need_filter)
            {
                added_columns.filter = IColumn::Filter(rows, 0);
                added_columns.matched_rows.resize(rows);
            }

            const UInt64 * const words = results.found_word.data();
            [[maybe_unused]] UInt8 * filter_data = nullptr;
            [[maybe_unused]] IColumn::Offset * matched_cur = nullptr;
            if constexpr (need_filter)
            {
                filter_data = added_columns.filter.data();
                matched_cur = added_columns.matched_rows.data();
            }
            [[maybe_unused]] UInt64 * ref_cur = nullptr;
            if constexpr (with_refs)
            {
                auto & row_refs = added_columns.lazy_output.row_refs;
                const size_t refs_begin = row_refs.size();
                row_refs.resize(refs_begin + rows);
                ref_cur = row_refs.data() + refs_begin;
            }
            [[maybe_unused]] IColumn::Offset * offsets = nullptr;
            if constexpr (join_features.need_replication)
                offsets = added_columns.offsets_to_replicate.data();

            [[maybe_unused]] IColumn::Offset current_offset = 0;
            [[maybe_unused]] UInt64 appended_row_count = 0;
            /// Copied out: the filter's byte stores may alias whatever the closure points at, so the
            /// bound would otherwise be reloaded every iteration.
            const size_t rows_local = rows;
            for (size_t i = 0; i < rows_local; ++i)
            {
                const UInt64 word = words[i];
                if (word)
                {
                    /// A flagless anti match only leaves the row unmatched in the filter.
                    if constexpr (!join_features.is_anti_join)
                    {
                        if constexpr (need_filter)
                        {
                            filter_data[i] = 1;
                            *matched_cur++ = i;
                        }
                        if constexpr (join_features.is_all_join)
                        {
                            const UInt32 match_rows = refWordRows(word);
                            current_offset += match_rows;
                            if constexpr (with_refs)
                            {
                                *ref_cur++ = word;
                                appended_row_count += match_rows;
                            }
                        }
                        else if constexpr (with_refs)
                        {
                            *ref_cur++ = firstRefWord(mappedFromWord<Mapped>(word));
                            ++appended_row_count;
                        }
                    }
                }
                else
                {
                    if constexpr (join_features.is_anti_join && join_features.left && need_filter)
                    {
                        filter_data[i] = 1;
                        *matched_cur++ = i;
                    }
                    if constexpr (join_features.add_missing)
                    {
                        if constexpr (with_refs)
                        {
                            *ref_cur++ = 0;
                            ++appended_row_count;
                        }
                        if constexpr (join_features.need_replication)
                            ++current_offset;
                    }
                }
                if constexpr (join_features.need_replication)
                    offsets[i] = current_offset;
            }

            if constexpr (need_filter)
                added_columns.matched_rows.resize(matched_cur - added_columns.matched_rows.data());
            if constexpr (with_refs)
            {
                auto & row_refs = added_columns.lazy_output.row_refs;
                row_refs.resize(ref_cur - row_refs.data());
                added_columns.lazy_output.row_count += appended_row_count;
            }
        }
    };

    /// The flat loop for the cheap-key open-addressing maps, which is the hot shape. Two structural
    /// differences from the plain loop, both from reading the probe's disassembly: a row's cell
    /// address comes from the contiguous descriptor array in one L1 load, instead of
    /// `leaf_map_ptrs[leaf]` and then the map header - three dependent loads on the
    /// address-generation critical path; and every loop invariant is snapshotted into a local,
    /// because the closure's fields sit behind a pointer the compiler must conservatively reload
    /// after each opaque call, which showed up as roughly ten loads per row. The selector variant is
    /// a template parameter for the same reason. The lookup itself is `HashMapTable::find` with
    /// identical offset semantics, zero-sentinel keys going through the map object.
    auto flat_loop = [&]<bool need_filter, bool with_skip, bool selector_is_range>()
    {
        /// The call sites are gated on the same constant, but instantiating the enclosing function
        /// substitutes into this body whether the lambda is called or not, and the lookup below is
        /// only well-formed for the gated map types.
        if constexpr (flat_lookup_supported)
        {
            using Cell = typename MapNonConst::cell_type;

            /// The snapshots below touch the leaf tables before the first row, and an empty probe
            /// block may legally arrive with no leaf maps at all.
            if (rows == 0)
                return;

            if constexpr (need_filter)
            {
                added_columns.filter = IColumn::Filter(rows, 0);
                added_columns.matched_rows.reserve(rows);
            }

            [[maybe_unused]] size_t selector_base = 0;
            [[maybe_unused]] const UInt64 * selector_indexes = nullptr;
            if constexpr (selector_is_range)
                selector_base = selector.getRange().first;
            else
                selector_indexes = selector.getIndexes().getData().data();
            auto index_at = [&](size_t k) __attribute__((always_inline))
            {
                if constexpr (selector_is_range)
                    return selector_base + k;
                else
                    return static_cast<size_t>(selector_indexes[k]);
            };

            const UInt16 * const leaf_ids_local = leaf_ids;
            [[maybe_unused]] const UInt8 * const skip_local = skip_data;
            [[maybe_unused]] const UInt64 * const flag_base_local = flag_base_data;
            const LeafMapDesc * const descs = leaf_map_descs.data();
            /// The gate guarantees the zero-check and key-compare read no map state.
            const HashTableNoState no_state{};
            /// Reads nothing through the object; the hash functor is an empty base.
            const MapNonConst & map0 = map_at(0);
            /// A private copy keeps the key getter's column pointer in a register.
            std::conditional_t<std::is_trivially_copyable_v<KeyGetter>, KeyGetter, KeyGetter &> keys = key_getter;

            auto flat_prefetcher = makeJoinPrefetcher(
                use_prefetch,
                rows,
                [&](size_t k) __attribute__((always_inline))
                {
                    const size_t ind = index_at(k);
                    const auto & desc = descs[leaf_ids_local ? leaf_ids_local[ind] : 0];
                    auto && key_holder = keys.getKeyHolder(ind, pool);
                    const size_t hash = map0.hash(keyHolderGetKey(key_holder));
                    __builtin_prefetch(static_cast<const Cell *>(desc.buf) + (hash & desc.mask));
                });

            IColumn::Offset current_offset = 0;
            for (size_t i = 0; i < rows; ++i)
            {
                flat_prefetcher.prefetchAt(i);

                const size_t ind = index_at(i);

                bool right_row_found = false;
                KnownRowsHolder<flag_per_row> dummy_known_rows;

                bool skip_row = false;
                if constexpr (with_skip)
                    skip_row = skip_local && skip_local[ind];

                if (!skip_row)
                {
                    const size_t leaf = leaf_ids_local ? leaf_ids_local[ind] : 0;
                    auto && key_holder = keys.getKeyHolder(ind, pool);
                    const auto & key = keyHolderGetKey(key_holder);
                    const Cell * cell = nullptr;
                    size_t offset = 0;
                    if (unlikely(Cell::isZero(key, no_state)))
                    {
                        /// The zero-value cell's `offsetInternal` is 0.
                        cell = map_at(leaf).find(key);
                    }
                    else
                    {
                        const auto & desc = descs[leaf];
                        const size_t hash = map0.hash(key);
                        const Cell * buf = static_cast<const Cell *>(desc.buf);
                        size_t pos = hash & desc.mask;
                        while (!buf[pos].isZero(no_state) && !buf[pos].keyEquals(key, hash, no_state))
                            pos = (pos + 1) & desc.mask;
                        if (!buf[pos].isZero(no_state))
                        {
                            cell = buf + pos;
                            offset = pos + 1;
                        }
                    }
                    if (cell)
                    {
                        right_row_found = true;
                        if constexpr (join_features.need_flags)
                            offset += flag_base_local[leaf];
                        typename KeyGetter::FindResult find_result(&cell->getMapped(), true, offset);
                        processMatch<KIND, STRICTNESS, need_filter, flag_per_row, MapsShape, Map, KeyGetter>(
                            find_result, added_columns, used_flags, i, ind, current_offset, dummy_known_rows, /*is_last_disjunct=*/ true);
                    }
                }

                if (!right_row_found)
                {
                    if constexpr (join_features.is_anti_join && join_features.left)
                        setUsed<need_filter>(added_columns.filter, i, added_columns.matched_rows);
                    addNotFoundRow<join_features.add_missing, join_features.need_replication>(added_columns, current_offset);
                }

                if constexpr (join_features.need_replication)
                    added_columns.offsets_to_replicate[i] = current_offset;
            }
        }
    };

    bool amac_ran = false;
    if constexpr (amac_supported)
    {
        if (use_amac)
        {
            /// Every row gets a result - `start` records skipped and zero-key rows synchronously,
            /// `step` records hits and misses - so the arrays need no pre-fill and the second pass
            /// needs no skip logic. Offsets are recorded, and sized, only for the flagged shapes.
            auto & results = ensure_scratch();
            results.found_word.resize(rows);
            UInt64 * found_offset_data = nullptr;
            if constexpr (join_features.need_flags)
            {
                results.found_offset.resize(rows);
                found_offset_data = results.found_offset.data();
            }
            /// Chunked so the compact slot's row index fits 16 bits, with the selector view and the
            /// result arrays re-based per chunk. The row-indexed side arrays are indexed by the
            /// selector's global row and need no re-basing. The default probe block is one chunk.
            auto amac_find = [&]<bool selector_is_range>()
            {
                using Policy = RoutedAmacFindPolicy<KeyGetter, Map, join_features.need_flags, selector_is_range>;
                size_t selector_base = 0;
                const UInt64 * selector_indexes = nullptr;
                if constexpr (selector_is_range)
                    selector_base = selector.getRange().first;
                else
                    selector_indexes = selector.getIndexes().getData().data();
                for (size_t chunk_begin = 0; chunk_begin < rows; chunk_begin += Policy::chunk_rows_max)
                {
                    const size_t chunk_rows = std::min(Policy::chunk_rows_max, rows - chunk_begin);
                    Policy policy{
                        .key_getter = key_getter,
                        .map0 = map_at(0),
                        .leaf_maps_data = leaf_maps_data,
                        .leaf_descs = leaf_map_descs.data(),
                        .leaf_ids = leaf_ids,
                        .selector_base = selector_base + chunk_begin,
                        .selector_indexes = selector_indexes ? selector_indexes + chunk_begin : nullptr,
                        .skip_data = skip_data,
                        .flag_base_data = flag_base_data,
                        .pool = pool,
                        .found_word = results.found_word.data() + chunk_begin,
                        .found_offset = found_offset_data ? found_offset_data + chunk_begin : nullptr};
                    amacRun(policy, chunk_rows);
                }
            };
            if (selector.isContinuousRange())
                amac_find.template operator()<true>();
            else
                amac_find.template operator()<false>();

            if constexpr (degenerate_phase_b)
            {
                auto word_dispatch = [&]<bool need_filter>()
                {
                    if (added_columns.record_row_refs)
                        word_loop.template operator()<need_filter, true>(results);
                    else
                        word_loop.template operator()<need_filter, false>(results);
                };
                if (added_columns.need_filter)
                    word_dispatch.template operator()<true>();
                else
                    word_dispatch.template operator()<false>();
            }
            else
            {
                if (added_columns.need_filter)
                    loop.template operator()<true, false, true>(&results);
                else
                    loop.template operator()<false, false, true>(&results);
            }
            amac_ran = true;
        }
    }

    if (!amac_ran)
    {
        if constexpr (flat_lookup_supported)
        {
            auto flat_dispatch = [&]<bool need_filter, bool with_skip>()
            {
                if (selector.isContinuousRange())
                    flat_loop.template operator()<need_filter, with_skip, true>();
                else
                    flat_loop.template operator()<need_filter, with_skip, false>();
            };
            if (added_columns.need_filter)
            {
                if (fast_path)
                    flat_dispatch.template operator()<true, false>();
                else
                    flat_dispatch.template operator()<true, true>();
            }
            else
            {
                if (fast_path)
                    flat_dispatch.template operator()<false, false>();
                else
                    flat_dispatch.template operator()<false, true>();
            }
        }
        else
        {
            if (added_columns.need_filter)
            {
                if (fast_path)
                    loop.template operator()<true, false, false>(nullptr);
                else
                    loop.template operator()<true, true, false>(nullptr);
            }
            else
            {
                if (fast_path)
                    loop.template operator()<false, false, false>(nullptr);
                else
                    loop.template operator()<false, true, false>(nullptr);
            }
        }
    }

    added_columns.applyLazyDefaults();
    return 0;
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsShape>
JoinResultPtr PartitionedHashJoin::probeImpl(Block block, size_t lane)
{
    HashJoin & join = *leaf_join;

    for (const auto & onexpr : table_join->getClauses())
    {
        auto cond_column_name = onexpr.condColumnNames();
        JoinCommon::checkTypesOfKeys(
            block, onexpr.key_names_left, cond_column_name.first, join.right_sample_block, onexpr.key_names_right, cond_column_name.second);
    }

    join.materializeColumnsFromLeftBlock(block);
    ScatteredBlock scattered_block{std::move(block)};

    if (leaf_maps.empty() && scattered_block.rows() > 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PartitionedHashJoin: probe started before the build phase finished");

    constexpr JoinFeatures<KIND, STRICTNESS, MapsShape> join_features;

    const auto & clause = table_join->getOnlyClause();
    std::vector<JoinOnKeyColumns> join_on_keys;
    join_on_keys.emplace_back(
        scattered_block,
        clause.key_names_left,
        clause.condColumnNames().first,
        join.key_sizes[0],
        HashJoin::isLowCardinalityType(join.data->type));

    AddedColumns<!join_features.is_any_join> added_columns(
        scattered_block,
        join.sample_block_with_columns_to_add,
        join.savedBlockSample(),
        join,
        std::move(join_on_keys),
        join.table_join->getMixedJoinExpression(),
        join.additional_filter_required_rhs_pos,
        join_features.is_asof_join,
        /*is_join_get=*/false,
        /*record_refs_for_stats=*/false);

    /// Emits fixed-width right columns through the direct typed gather rather than the generic
    /// pair-expansion path; see `LazyOutput::buildOutputFromBlocks`. Only for the lazy shapes whose
    /// emit consumes ref words - ASOF's `AddedColumns` does not resolve the emit table.
    if constexpr (!join_features.is_any_join && !join_features.is_asof_join)
        added_columns.lazy_output.use_direct_typed_gather = true;

    const bool has_required_right_keys = join.required_right_keys.columns() != 0;
    added_columns.need_filter = join_features.need_filter || has_required_right_keys;
    added_columns.max_joined_block_rows = join.max_joined_block_rows;
    if (!added_columns.max_joined_block_rows)
        added_columns.max_joined_block_rows = std::numeric_limits<size_t>::max();
    else
        added_columns.reserve(join_features.need_replication);

    using OurMaps = PartitionedMapsFor<MapsShape>::Type;

    {
        /// Routing, lookups and match bookkeeping only. No column value is gathered yet - that is
        /// deferred to the lazy `HashJoinResult::next`, whose events are shared with the other
        /// hash-join algorithms.
        ProfileEventTimeIncrement<Microseconds> lookup_watch(ProfileEvents::PartitionedHashJoinProbeLookupMicroseconds);
        switch (join.data->type)
        {
#define M(TYPE) \
    case HashJoin::Type::TYPE: { \
        using Map = const decltype(OurMaps::TYPE)::element_type; \
        using KeyGetter = typename KeyGetterForType<HashJoin::Type::TYPE, Map>::Type; \
        routedJoinRightColumns<KIND, STRICTNESS, MapsShape, KeyGetter, Map>(added_columns, scattered_block, lane); \
        break; \
    }
            APPLY_FOR_PARTITIONED_JOIN_VARIANTS(M)
#undef M
            default:
                throw Exception(
                    ErrorCodes::UNSUPPORTED_JOIN_KEYS, "Unsupported JOIN keys for the partitioned join (type: {})", join.data->type);
        }
    }

    added_columns.join_on_keys.clear();

    return std::make_unique<HashJoinResult>(
        std::move(added_columns.lazy_output),
        std::move(added_columns.columns),
        std::move(added_columns.offsets_to_replicate),
        std::move(added_columns.filter),
        std::move(added_columns.matched_rows),
        std::move(scattered_block),
        HashJoinResult::Properties{
            *join.table_join,
            join.required_right_keys,
            join.required_right_keys_sources,
            join.max_joined_block_rows,
            join.max_joined_block_bytes,
            join.data->allocated_size / std::max<size_t>(1, join.data->rows_to_join),
            join_features.need_filter,
            /*is_join_get=*/false,
            join.joined_block_split_single_row,
            join.enable_lazy_columns_replication,
            join.enable_lazy_columns_indexing});
}

}
