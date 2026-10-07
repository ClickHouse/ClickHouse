#pragma once

#include <Interpreters/HashJoin/AddedColumns.h>
#include <Interpreters/HashJoin/HashJoinMethodsImpl.h>
#include <Interpreters/HashJoin/HashJoinResult.h>
#include <Interpreters/HashJoin/JoinUsedFlags.h>
#include <Interpreters/HashJoin/KeyGetter.h>
#include <Interpreters/HashJoin/MatchedRowsStats.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/PartitionedHashJoin/AmacRing.h>
#include <Interpreters/PartitionedHashJoin/PartitionedHashJoin.h>
#include <Interpreters/RowRefs.h>
#include <Interpreters/TableJoin.h>
#include <base/scope_guard.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/HashTable/HashTable.h>
#include <Common/ProfileEvents.h>

#include <array>

namespace ProfileEvents
{
extern const Event HashJoinPartitionedProbeLookupMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNSUPPORTED_JOIN_KEYS;
}

/// Mapped values the find pass records by value. A `RowRef` encodes to its ref word and a `RowRefList`
/// is one; both are 8-byte words. Neither is ever 0 for a built cell. A `RowRef` carries `INLINE_FLAG`
/// in bit 63. A `RowRefList` word is either an inline ref or a tagged non-null pointer. So 0
/// encodes a miss. The second pass reads only the word, and the probe table is immutable, so the copy
/// is as good as the cell.
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

/** The find pass of the two-pass probe: out-of-order lookups that emit nothing and only fill the
  * per-row result arrays - `found_word`, and for the joins that keep used flags the used-flags offset.
  * The word is recorded in the visit that reads the cell, so the second pass never touches the cell.
  * By the time the in-order pass reaches the row, up to a block of rows later, the line has usually
  * left the cache; re-reading it through a recorded pointer cost a second random miss per row. ASOF
  * does not fit a word and keeps the pointer.
  *
  * One table serves every row, so a slot carries only the resolved cell pointer and the key: a steady
  * visit dereferences nothing but the cell and the key, and wraps with the one mask the policy holds in
  * its frame. The selector variant is a template parameter, so `indexAt` has no per-visit branch.
  */
template <typename KeyGetter, typename Table, bool need_flags, bool selector_is_range>
struct SharedAmacFindPolicy
{
    using TableNonConst = std::remove_const_t<Table>;
    using Cell = typename TableNonConst::cell_type;
    static constexpr bool store_hash = cell_stores_hash<Cell>;
    static constexpr bool copy_into_frame = true; /// results live in the arrays; no state survives the run
    static constexpr bool mapped_by_value = amac_mapped_fits_word<typename TableNonConst::mapped_type>;

    static_assert(is_hash_join_table<TableNonConst>);
    static constexpr HashTableNoState no_state{};

    /// The key exactly as the table compares it: fixed keys by value, string keys as a view into the
    /// probe column. Trivially copyable across the whole admitted getter set - the serialized getter
    /// is gated out, and the arena-backed string holder persists nothing on the find path.
    using KeyHolder = std::remove_reference_t<decltype(std::declval<KeyGetter &>().getKeyHolder(0uz, std::declval<Arena &>()))>;
    using StoredKey = std::decay_t<decltype(keyHolderGetKey(std::declval<KeyHolder &>()))>;
    static_assert(std::is_trivially_copyable_v<StoredKey>);

    /** The find-ring state. The resolved cell pointer stands in for a position, and `cell == nullptr`
      * is the inactive sentinel - value-initialization means all-inactive - which frees `row` for the
      * full 16-bit range of the driver's chunks. The hit position is recovered as `cell - cells` once
      * per matched row. The key is packed once at admit and re-read from the slot per visit.
      * Re-fetching it through `getKeyHolder` would re-pack a wide fixed key from the column pointers on
      * every visit, which was the dominant per-visit cost of the wide-key ring.
      */
    template <size_t ring_size>
    struct RingBase
    {
        std::array<const Cell *, ring_size> cell{}; /// the cell the next visit reads; nullptr == inactive
        std::array<UInt16, ring_size> row{}; /// chunk-local probe row
        alignas(64) std::array<StoredKey, ring_size> key{};

        bool isActive(size_t s) const { return cell[s] != nullptr; }
        void deactivate(size_t s) { cell[s] = nullptr; }
    };
    template <size_t ring_size>
    struct RingWithHash : public RingBase<ring_size>
    {
        std::array<size_t, ring_size> hash{};
    };
    template <size_t ring_size>
    using Ring = std::conditional_t<store_hash, RingWithHash<ring_size>, RingBase<ring_size>>;

    /// Chunked so the ring's row index fits 16 bits; the default probe block is one chunk. Per chunk
    /// the selector view and the result arrays are re-based; `skip_data` is indexed by the selector's
    /// global row and is not.
    static constexpr size_t chunk_rows_max = 1uz << 13;

    /// By value where possible, so the key-column pointer is a field of the frame-local policy
    /// rather than two dependent loads behind a reference.
    std::conditional_t<std::is_trivially_copyable_v<KeyGetter>, KeyGetter, KeyGetter &> key_getter;
    const TableNonConst & table;
    const Cell * cells = nullptr;
    const Cell * cells_end = nullptr;
    size_t selector_base = 0; /// the first row of a continuous-range selector
    const UInt64 * selector_indexes = nullptr; /// the data of an explicit-indexes selector
    const UInt8 * skip_data = nullptr; /// null on the fast path
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

    /// `start`'s synchronous path for the zero key: the match, if any, is the zero-value cell, whose
    /// used-flags offset is 0.
    ALWAYS_INLINE void recordZeroKey(size_t row, const Cell * cell)
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
            found_offset[row] = table.offsetInternal(cell);
    }

    /// The cell is known non-zero, so its used-flags offset is its buffer position + 1 - what
    /// `offsetInternal` would return, without touching the table.
    ALWAYS_INLINE void recordHit(size_t row, const Cell * cell)
    {
        if constexpr (mapped_by_value)
            found_word[row] = mappedWordOf(cell->getMapped());
        else
            found_word[row] = reinterpret_cast<UInt64>(&cell->getMapped());
        if constexpr (need_flags)
            found_offset[row] = static_cast<size_t>(cell - cells) + 1;
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
        if (unlikely(TableNonConst::isZeroKey(key)))
        {
            /// The zero-value cell has no walk to overlap.
            recordZeroKey(i, table.find(key));
            return false;
        }
        const size_t hash = table.hash(key);
        ring.key[s] = key;
        const Cell * cell = cells + table.place(hash);
        ring.cell[s] = cell;
        ring.row[s] = static_cast<UInt16>(i);
        if constexpr (store_hash)
            ring.hash[s] = hash;
        prefetchCell(cell);
        return true;
    }

    /// Locality 3, not 1, and the whole cell rather than its first line. On AArch64 locality 1
    /// ("not revisited") compiles to `prfm pldl3keep`, which stages the line in L3 only. The visit's
    /// demand load then pays the full L1-miss latency. That was the ring's dominant stall on wide keys.
    /// A line that is not revisited pollutes L1 cheaply, but an L3-resident load is still slow. Cells
    /// wider than 24 bytes often straddle two lines (a 40-byte cell does at about 61% of positions).
    /// The second line is prefetched too. Otherwise its limb compares stall the same way.
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
        /// Only the saved-hash cells read `hash`; see `cell_stores_hash`.
        size_t hash = 0;
        if constexpr (store_hash)
            hash = ring.hash[s];
        if (cell->keyEquals(key, hash, no_state))
        {
            recordHit(ring.row[s], cell);
            return AmacStepResult::Done;
        }
        if (++cell == cells_end) [[unlikely]]
            cell = cells;
        ring.cell[s] = cell;
        prefetchCell(cell);
        return AmacStepResult::Advance;
    }
};

/** Probe of the shared table: the single-map `joinRightColumns` loop with this table's walk.
  * Probe blocks are never scattered. Unlike `switchJoinRightColumns` this never splits the block:
  * `HashJoinResult` caps the output.
  *
  * When `use_amac` holds (past the prefetch threshold and the row floor), a block is probed in two
  * passes. A find ring fills pooled scratch out of order. An in-order pass then consumes it
  * (`word_loop` when the mapped value fits a word and no used flags are kept; otherwise `loop`
  * with the lookup replaced by the recorded result). Either way the replication offsets,
  * used-flag semantics and per-kind logic are untouched.
  *
  * `MapsShape` drives `JoinFeatures` and `processMatch`. `Map` is the shared table (or the fixed
  * map) holding identical cells.
  */
template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsShape, typename KeyGetter, typename Map, typename AddedColumnsType> // NOLINT(readability-identifier-naming)
size_t PartitionedHashJoin::joinRightColumns(const Map & table, AddedColumnsType & added_columns, const ScatteredBlock & block, size_t lane)
{
    constexpr JoinFeatures<KIND, STRICTNESS, MapsShape> join_features;
    /// One clause addresses its flags per cell; the mixed ON condition of a RIGHT or FULL join, which
    /// marks per row, takes the filter path below.
    constexpr bool flag_per_row = false;

    const auto & join_keys = added_columns.join_on_keys.at(0);
    const auto & selector = block.getSelector();
    const size_t rows = selector.size();
    JoinStuff::JoinUsedFlags & used_flags = *hash_join->used_flags;

    /// Acquired only where it is needed - the find pass's result arrays - so the plain loop pays
    /// nothing for it.
    std::unique_ptr<ProbeScratch> scratch;
    SCOPE_EXIT({
        if (scratch)
            releaseProbeScratch(std::move(scratch), lane);
    });

    /// The ASOF getter excludes the inequality column; a range getter reads the key range the post-build
    /// conversion settled.
    auto key_getter
        = createKeyGetter<KeyGetter, join_features.is_asof_join>(join_keys.key_columns, join_keys.key_sizes, hash_join->data->key_range);

    /// A mixed ON condition is decided per candidate pair, over the right rows themselves, so the
    /// probe cannot record matches by cell word. The standard filter path runs over the shared
    /// table instead: it finds through the same key getter and marks the used flags through the same
    /// `FindResult` - per cell, or per right-table row on a RIGHT or FULL join, whose non-joined
    /// output needs to know which rows of a key the condition let through (`used_flags_per_row`). Only
    /// `RowRefList` maps hold every right row of a key, which is why `preferUseMapsAll` chose them for
    /// the build.
    if constexpr (join_features.is_maps_all)
    {
        if (added_columns.additional_filter_expression)
        {
            std::vector<KeyGetter> key_getters;
            key_getters.push_back(std::move(key_getter));
            return HashJoinMethods<KIND, STRICTNESS, MapsShape>::template joinRightColumnsWithAdditionalFilter<KeyGetter, Map>(
                std::move(key_getters),
                std::vector<const Map *>{&table},
                added_columns,
                used_flags,
                selector,
                added_columns.need_filter,
                /*flag_per_row=*/join_features.right || join_features.full);
        }
    }

    /// No null map and no ON mask: the loops run the instantiation without the per-row skip check.
    const bool fast_path = !join_keys.null_map && join_keys.join_mask_column.getKind() == JoinCommon::JoinMask::Kind::AllTrue;

    /// Deliberately the same condition as the `addFoundRowAll` branches of `processMatch`.
    if constexpr (!flag_per_row && join_features.emits_whole_key_per_word)
        added_columns.lazy_output.output_by_row_list = true;

    if constexpr (join_features.need_replication)
        added_columns.offsets_to_replicate = IColumn::Offsets(rows);

    Arena pool;

    /// One byte per row merging the null map and the ON mask, as `joinRightColumns` builds it.
    const UInt8 * skip_data = nullptr;
    IColumn::Filter skip_buffer;
    if (!fast_path)
    {
        if (selector.isContinuousRange())
            skip_data = join_keys.buildRowSkipData(skip_buffer, selector.getRange().first, rows);
        else
            skip_data = join_keys.buildRowSkipData(skip_buffer, selector.getIndexes());
    }

    /// Where the ring is supported it is the probe path above the threshold. On string-key tables it
    /// is also the only thing that overlaps cell misses. The look-ahead prefetcher is off there
    /// because `getKeyHolder` per look-ahead is too expensive for its heuristic. The conditions are
    /// the software-prefetch ones: the user toggle, and a table of at least L2 size, which the probe
    /// stream then keeps evicting. There is also a row floor. Below it the ring's prime and drain
    /// cost more than the overlap wins.
    using MapNonConst = std::remove_const_t<Map>;
    constexpr bool amac_supported = amac_join_supported<KeyGetter, MapNonConst>;
    constexpr bool can_prefetch = join_prefetch_supported<KeyGetter, Map>;
    /// Fixed-width keys on the shared table take the flat loop rather than the getter's `findKey`.
    constexpr bool flat_lookup_supported = can_prefetch && is_hash_join_table<MapNonConst>;
    bool use_amac = false;
    if constexpr (amac_supported)
        use_amac = clauses.front().amacEnabled() && added_columns.enable_prefetch && clauses.front().tableBytes() >= getMinBytesForPrefetchInJoin() && rows >= amac_min_rows;

    /// Mutually exclusive with the find pass, on the same threshold.
    bool use_prefetch = false;
    if constexpr (can_prefetch)
        use_prefetch = !use_amac && added_columns.enable_prefetch && clauses.front().tableBytes() >= getMinBytesForPrefetchInJoin();

    /// Used only by `loop`'s plain path; `flat_loop` builds its own over the flat lookup.
    auto prefetcher = makeJoinPrefetcher(
        use_prefetch,
        rows,
        [&](size_t k) __attribute__((always_inline))
        {
            if constexpr (can_prefetch)
                table.prefetch(key_getter.getKeyHolder(selector[k], pool));
        });

    /// Serves as both the in-order second pass and the plain loop. With `precomputed` the lookup is
    /// the find pass's result, and the skip check compiles out because skipped rows were recorded as
    /// misses there. Everything downstream is the standard machinery.
    auto loop = [&]<bool need_filter, bool with_skip, bool precomputed, bool selector_is_range>(const ProbeScratch * results)
    {
        if constexpr (need_filter)
        {
            added_columns.filter = IColumn::Filter(rows, 0);
            added_columns.matched_rows.reserve(rows);
        }

        using Mapped = typename MapNonConst::mapped_type;

        /// The loop invariants as locals. Captured by reference they would live in the closure, whose
        /// address the `appendFromBlock` call sees. The compiler then reloads the row count, the selector
        /// (with its variant check), the key getter, the table and the output after every call: a dozen
        /// loads per probe row that `HashJoinMethods::joinRightColumns` does not pay.
        const size_t num_rows = rows;
        AddedColumnsType & cols = added_columns;
        const Map & map = table;
        JoinStuff::JoinUsedFlags & flags = used_flags;
        [[maybe_unused]] const UInt8 * const skip_local = skip_data;
        /// A private copy keeps the key getter's column pointer in a register.
        std::conditional_t<std::is_trivially_copyable_v<KeyGetter>, KeyGetter, KeyGetter &> keys = key_getter;
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

        IColumn::Offset current_offset = 0;
        for (size_t i = 0; i < num_rows; ++i)
        {
            if constexpr (can_prefetch && !precomputed)
                prefetcher.prefetchAt(i);

            const size_t ind = index_at(i);

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
                    /// The find pass decided by-value recording from the table's mapped type and this
                    /// side decides from the `FindResult`'s. If they ever differ, a word would be
                    /// reinterpreted as a pointer.
                    static_assert(std::is_same_v<
                                  std::remove_const_t<std::remove_reference_t<decltype(std::declval<typename KeyGetter::FindResult &>().getMapped())>>,
                                  Mapped>);
                    if constexpr (amac_mapped_fits_word<Mapped>)
                    {
                        /// Rebuilt on the stack from the recorded word; the cell is not touched.
                        auto mapped_value = mappedFromWord<Mapped>(word);
                        typename KeyGetter::FindResult find_result(&mapped_value, true, offset);
                        processMatch<KIND, STRICTNESS, need_filter, flag_per_row, MapsShape, Map, KeyGetter>(
                            find_result, cols, flags, i, ind, current_offset, dummy_known_rows, /*is_last_disjunct=*/ true);
                    }
                    else
                    {
                        typename KeyGetter::FindResult find_result(
                            reinterpret_cast<Mapped *>(word), true, offset); /// NOLINT(performance-no-int-to-ptr)
                        processMatch<KIND, STRICTNESS, need_filter, flag_per_row, MapsShape, Map, KeyGetter>(
                            find_result, cols, flags, i, ind, current_offset, dummy_known_rows, /*is_last_disjunct=*/ true);
                    }
                }
            }
            else
            {
                bool skip_row = false;
                if constexpr (with_skip)
                    skip_row = skip_local && skip_local[ind];

                if (!skip_row)
                {
                    auto find_result = keys.findKey(map, ind, pool);
                    if (find_result.isFound())
                    {
                        right_row_found = true;
                        processMatch<KIND, STRICTNESS, need_filter, flag_per_row, MapsShape, Map, KeyGetter>(
                            find_result, cols, flags, i, ind, current_offset, dummy_known_rows, /*is_last_disjunct=*/ true);
                    }
                }
            }

            if (!right_row_found)
            {
                if constexpr (join_features.is_anti_join && join_features.left)
                    setUsed<need_filter>(cols.filter, i, cols.matched_rows);
                addNotFoundRow<join_features.add_missing, join_features.need_replication>(cols, current_offset);
            }

            if constexpr (join_features.need_replication)
                cols.offsets_to_replicate[i] = current_offset;
        }
    };

    /// `loop` over the block's selector kind, chosen once per block.
    auto run_loop = [&]<bool need_filter, bool with_skip, bool precomputed>(const ProbeScratch * results)
    {
        if (selector.isContinuousRange())
            loop.template operator()<need_filter, with_skip, precomputed, true>(results);
        else
            loop.template operator()<need_filter, with_skip, precomputed, false>(results);
    };

    /// Whether the second pass is `word_loop`. Three conditions hold. The recorded word is the mapped
    /// value itself. The output appends whole keys (one appended ref word per match). The join keeps no per-row
    /// state beyond the filter, the appended words and the replication offsets. Used-flag joins, ASOF
    /// and ANY keep the full loop.
    constexpr bool second_pass_is_word_loop = AddedColumnsType::appendsWholeKey() && amac_mapped_fits_word<typename MapNonConst::mapped_type>
        && !join_features.need_flags && !join_features.is_asof_join && !join_features.is_any_join;

    /// Under those conditions `processMatch` marks the row matched and appends one word. For ALL
    /// that word is the list word, and the replication offset advances by its row count. Otherwise
    /// it is the first ref. This pass reads `found_word` directly. Rebuilding a `FindResult` per row
    /// and calling the outlined `appendFromBlock` forced the loop-carried state to spill. At most
    /// one append per row, so the cursors write into pre-sized arrays without a capacity check. Row
    /// order, filter, offsets and `row_count` match the full loop.
    auto word_loop = [&]<bool need_filter, bool with_refs>(const ProbeScratch & results [[maybe_unused]])
    {
        if constexpr (second_pass_is_word_loop)
        {
            using Mapped = typename MapNonConst::mapped_type;

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
            /// Right rows matched, as `LazyOutput::addRef` counts them on the full loop; the planner's
            /// row store decision for the next run reads the published total.
            [[maybe_unused]] UInt64 matched_row_count = 0;
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
                            const size_t match_rows = refWordRows(word);
                            current_offset += match_rows;
                            if constexpr (with_refs)
                            {
                                *ref_cur++ = word;
                                appended_row_count += match_rows;
                                matched_row_count += match_rows;
                            }
                        }
                        else if constexpr (with_refs)
                        {
                            *ref_cur++ = firstRefWord(mappedFromWord<Mapped>(word));
                            ++appended_row_count;
                            ++matched_row_count;
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
                added_columns.lazy_output.hash_table_matches += matched_row_count;
            }
        }
    };

    /// The plain loop for fixed-width keys on the shared table - the common case. Every loop invariant
    /// is copied into a local, because the closure's fields sit behind a pointer that the compiler must
    /// reload after each opaque call. The locals are the row count, the table's placement shift and the
    /// selector view. Whether the lazy output records ref words at all is the `with_refs` template
    /// parameter. A miss on the lazy path then costs one offset increment and nothing else. The lookup
    /// is the table's `find` inlined, with the same offset semantics. Zero keys go through the table object.
    auto flat_loop = [&]<bool need_filter, bool with_skip, bool selector_is_range, bool with_refs>()
    {
        /// The call sites are gated on the same constant. Instantiating the enclosing function still
        /// substitutes into this body whether the lambda is called or not. The lookup below is only
        /// well-formed for the gated table types.
        if constexpr (flat_lookup_supported)
        {
            using Cell = typename MapNonConst::cell_type;

            const size_t num_rows = rows;

            if constexpr (need_filter)
            {
                added_columns.filter = IColumn::Filter(num_rows, 0);
                added_columns.matched_rows.reserve(num_rows);
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

            [[maybe_unused]] const UInt8 * const skip_local = skip_data;
            const Cell * const cells = table.cells();
            const size_t mask = table.cellMask();
            /// `table.place` with the shift held in a register.
            const size_t place_shift = 64 - table.sizeDegree();
            auto place = [place_shift](size_t hash_value) __attribute__((always_inline))
            { return static_cast<size_t>(hashJoinTablePlacement(hash_value) >> place_shift); };
            /// The gate guarantees the zero-check and key-compare read no table state.
            const HashTableNoState no_state{};
            /// A private copy keeps the key getter's column pointer in a register.
            std::conditional_t<std::is_trivially_copyable_v<KeyGetter>, KeyGetter, KeyGetter &> keys = key_getter;

            auto flat_prefetcher = makeJoinPrefetcher(
                use_prefetch,
                num_rows,
                [&](size_t k) __attribute__((always_inline))
                {
                    auto && key_holder = keys.getKeyHolder(index_at(k), pool);
                    __builtin_prefetch(cells + place(table.hash(keyHolderGetKey(key_holder))));
                });

            IColumn::Offset current_offset = 0;
            for (size_t i = 0; i < num_rows; ++i)
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
                    auto && key_holder = keys.getKeyHolder(ind, pool);
                    const auto & key = keyHolderGetKey(key_holder);
                    const Cell * cell = nullptr;
                    size_t offset = 0;
                    if (unlikely(Cell::isZero(key, no_state)))
                    {
                        /// The zero-value cell's `offsetInternal` is 0.
                        cell = table.find(key);
                    }
                    else
                    {
                        const size_t hash = table.hash(key);
                        size_t pos = place(hash);
                        while (!cells[pos].isZero(no_state) && !cells[pos].keyEquals(key, hash, no_state))
                            pos = (pos + 1) & mask;
                        if (!cells[pos].isZero(no_state))
                        {
                            cell = cells + pos;
                            offset = pos + 1;
                        }
                    }
                    if (cell)
                    {
                        right_row_found = true;
                        typename KeyGetter::FindResult find_result(&cell->getMapped(), true, offset);
                        processMatch<KIND, STRICTNESS, need_filter, flag_per_row, MapsShape, Map, KeyGetter>(
                            find_result, added_columns, used_flags, i, ind, current_offset, dummy_known_rows, /*is_last_disjunct=*/ true);
                    }
                }

                if (!right_row_found)
                {
                    if constexpr (join_features.is_anti_join && join_features.left)
                        setUsed<need_filter>(added_columns.filter, i, added_columns.matched_rows);
                    /// `addNotFoundRow` inlined, with the record flag resolved at compile time. The lazy
                    /// output takes a zero word only when it records refs. The eager output counts the
                    /// default row as before.
                    if constexpr (join_features.add_missing)
                    {
                        if constexpr (!AddedColumnsType::appendsWholeKey())
                            added_columns.appendDefaultRow();
                        else if constexpr (with_refs)
                            added_columns.lazy_output.addDefault();
                        if constexpr (join_features.need_replication)
                            ++current_offset;
                    }
                }

                if constexpr (join_features.need_replication)
                    added_columns.offsets_to_replicate[i] = current_offset;
            }
        }
    };

    if (use_amac)
    {
        if constexpr (amac_supported)
        {
            /// Every row gets a result. `start` records skipped and zero-key rows synchronously.
            /// `step` records hits and misses. The arrays therefore need no pre-fill, and the second
            /// pass needs no skip logic. Offsets are recorded, and sized, only for the joins that keep flags.
            scratch = acquireProbeScratch(lane);
            auto & results = *scratch;
            results.found_word.resize(rows);
            UInt64 * found_offset_data = nullptr;
            if constexpr (join_features.need_flags)
            {
                results.found_offset.resize(rows);
                found_offset_data = results.found_offset.data();
            }
            auto amac_find = [&]<bool selector_is_range>()
            {
                using Policy = SharedAmacFindPolicy<KeyGetter, Map, join_features.need_flags, selector_is_range>;
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
                        .table = table,
                        .cells = table.cells(),
                        .cells_end = table.cells() + table.cellCount(),
                        .selector_base = selector_base + chunk_begin,
                        .selector_indexes = selector_indexes ? selector_indexes + chunk_begin : nullptr,
                        .skip_data = skip_data,
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

            if constexpr (second_pass_is_word_loop)
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
                    run_loop.template operator()<true, false, true>(&results);
                else
                    run_loop.template operator()<false, false, true>(&results);
            }
        }
    }
    else
    {
        if constexpr (flat_lookup_supported)
        {
            auto flat_dispatch = [&]<bool need_filter, bool with_skip>()
            {
                auto by_selector = [&]<bool with_refs>()
                {
                    if (selector.isContinuousRange())
                        flat_loop.template operator()<need_filter, with_skip, true, with_refs>();
                    else
                        flat_loop.template operator()<need_filter, with_skip, false, with_refs>();
                };
                /// Chosen once per block; the eager output never records refs, so it gets one instantiation.
                if constexpr (AddedColumnsType::appendsWholeKey())
                {
                    if (added_columns.record_row_refs)
                        by_selector.template operator()<true>();
                    else
                        by_selector.template operator()<false>();
                }
                else
                    by_selector.template operator()<false>();
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
                    run_loop.template operator()<true, false, false>(nullptr);
                else
                    run_loop.template operator()<true, true, false>(nullptr);
            }
            else
            {
                if (fast_path)
                    run_loop.template operator()<false, false, false>(nullptr);
                else
                    run_loop.template operator()<false, true, false>(nullptr);
            }
        }
    }

    return rows;
}

/** The probe over the tables of several ON clauses (`ON a OR b`): the multi-map `joinRightColumns` of
  * `HashJoin`. Per probe row the clauses are walked in order; `KnownRowsHolder` keeps a right row an
  * earlier clause emitted from being emitted again, the last clause skips that bookkeeping, and ANY and
  * SEMI stop at the first clause that matches - except RIGHT and FULL ANY, which mark every clause's
  * rows. The used flags are kept per right-table row, since one row is reachable through several keys.
  * No find ring and no flat lookup: their out-of-order results could not be deduplicated across the
  * clauses, and `HashJoin` has neither on this path.
  */
template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsShape, typename KeyGetter, typename Map, typename AddedColumnsType> // NOLINT(readability-identifier-naming)
size_t PartitionedHashJoin::joinRightColumns(const std::vector<const Map *> & tables, AddedColumnsType & added_columns, const ScatteredBlock & block)
{
    constexpr JoinFeatures<KIND, STRICTNESS, MapsShape> join_features;
    if constexpr (join_features.is_asof_join)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PartitionedHashJoin: an ASOF join has exactly one ON clause");
    }
    else
    {
        constexpr bool flag_per_row = true;
        const auto & selector = block.getSelector();
        const size_t rows = selector.size();
        const size_t num_clauses = tables.size();
        chassert(added_columns.join_on_keys.size() == num_clauses);
        JoinStuff::JoinUsedFlags & used_flags = *hash_join->used_flags;

        std::vector<KeyGetter> key_getters;
        key_getters.reserve(num_clauses);
        for (const auto & join_keys : added_columns.join_on_keys)
            key_getters.push_back(
                createKeyGetter<KeyGetter, /*is_asof_join=*/false>(join_keys.key_columns, join_keys.key_sizes, hash_join->data->key_range));

        /// See the one-clause probe: the standard filter path, over every clause's table.
        if constexpr (join_features.is_maps_all)
        {
            if (added_columns.additional_filter_expression)
                return HashJoinMethods<KIND, STRICTNESS, MapsShape>::template joinRightColumnsWithAdditionalFilter<KeyGetter, Map>(
                    std::move(key_getters), tables, added_columns, used_flags, selector, added_columns.need_filter, flag_per_row);
        }

        /// One byte per row and clause merging the clause's null map and ON mask, as `joinRightColumns`
        /// builds it; null for a clause that skips no row.
        std::vector<const UInt8 *> skip_datas(num_clauses, nullptr);
        std::vector<IColumn::Filter> skip_buffers(num_clauses);
        for (size_t clause_idx = 0; clause_idx < num_clauses; ++clause_idx)
        {
            const auto & join_keys = added_columns.join_on_keys[clause_idx];
            if (!join_keys.null_map && join_keys.join_mask_column.getKind() == JoinCommon::JoinMask::Kind::AllTrue)
                continue;
            if (selector.isContinuousRange())
                skip_datas[clause_idx] = join_keys.buildRowSkipData(skip_buffers[clause_idx], selector.getRange().first, rows);
            else
                skip_datas[clause_idx] = join_keys.buildRowSkipData(skip_buffers[clause_idx], selector.getIndexes());
        }

        if constexpr (join_features.need_replication)
        {
            added_columns.offsets_to_replicate.clear();
            added_columns.offsets_to_replicate.reserve(rows);
        }

        Arena pool;

        /// The look-ahead prefetch of the first clause's table only, as `HashJoin` prefetches its first map.
        constexpr bool can_prefetch = join_prefetch_supported<KeyGetter, Map>;
        bool use_prefetch = false;
        if constexpr (can_prefetch)
            use_prefetch = added_columns.enable_prefetch && clauses.front().tableBytes() >= getMinBytesForPrefetchInJoin();
        auto prefetcher = makeJoinPrefetcher(
            use_prefetch,
            rows,
            [&](size_t k) __attribute__((always_inline))
            {
                if constexpr (can_prefetch)
                    tables[0]->prefetch(key_getters[0].getKeyHolder(selector[k], pool));
            });

        auto loop = [&]<bool need_filter>()
        {
            if constexpr (need_filter)
            {
                added_columns.filter = IColumn::Filter(rows, 0);
                added_columns.matched_rows.reserve(rows);
            }

            /// Stop once the result reaches `max_joined_block_rows`, as `HashJoin` does: one left row can match
            /// thousands of right rows through the clauses together. `probeImpl` hands the rest of the block back.
            IColumn::Offset current_offset = 0;
            size_t i = 0;
            for (; i < rows && current_offset < added_columns.max_joined_block_rows; ++i)
            {
                if constexpr (can_prefetch)
                    prefetcher.prefetchAt(i);

                const size_t ind = selector[i];

                bool right_row_found = false;
                KnownRowsHolder<flag_per_row> known_rows;
                for (size_t clause_idx = 0; clause_idx < num_clauses; ++clause_idx)
                {
                    if (skip_datas[clause_idx] && skip_datas[clause_idx][ind])
                        continue;

                    auto find_result = key_getters[clause_idx].findKey(*tables[clause_idx], ind, pool);
                    if (!find_result.isFound())
                        continue;

                    right_row_found = true;
                    const bool is_last_disjunct = clause_idx + 1 == num_clauses;
                    processMatch<KIND, STRICTNESS, need_filter, flag_per_row, MapsShape, Map, KeyGetter>(
                        find_result, added_columns, used_flags, i, ind, current_offset, known_rows, is_last_disjunct);

                    if constexpr (join_features.is_any_or_semi_join && !(join_features.is_any_join && (join_features.right || join_features.full)))
                        break;
                }

                if (!right_row_found)
                {
                    if constexpr (join_features.is_anti_join && join_features.left)
                        setUsed<need_filter>(added_columns.filter, i, added_columns.matched_rows);
                    addNotFoundRow<join_features.add_missing, join_features.need_replication>(added_columns, current_offset);
                }

                if constexpr (join_features.need_replication)
                    added_columns.offsets_to_replicate.push_back(current_offset);
            }
            return i;
        };

        if (added_columns.need_filter)
            return loop.template operator()<true>();
        return loop.template operator()<false>();
    }
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsShape>
JoinResultPtr PartitionedHashJoin::probeImpl(Block block, size_t lane, const Block * join_get_columns)
{
    HashJoin & join = *hash_join;
    const bool is_join_get = join_get_columns != nullptr;

    /// `joinGet` hands over the keys under the right-side names, checked by `joinGetCheckAndGetReturnType`.
    if (!is_join_get)
    {
        for (const auto & onexpr : table_join->getClauses())
        {
            auto cond_column_name = onexpr.condColumnNames();
            JoinCommon::checkTypesOfKeys(
                block, onexpr.key_names_left, cond_column_name.first, join.right_sample_block, onexpr.key_names_right, cond_column_name.second);
        }
        join.materializeColumnsFromLeftBlock(block);
    }
    ScatteredBlock scattered_block{std::move(block)};

    if (!clauses.front().hasTable() && scattered_block.rows() > 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PartitionedHashJoin: probe started before the build phase finished");

    constexpr JoinFeatures<KIND, STRICTNESS, MapsShape> join_features;

    std::vector<JoinOnKeyColumns> join_on_keys;
    join_on_keys.reserve(clauses.size());
    for (size_t clause_idx = 0; clause_idx < clauses.size(); ++clause_idx)
    {
        const auto & on_clause = table_join->getClauses()[clause_idx];
        join_on_keys.emplace_back(
            scattered_block,
            is_join_get ? on_clause.key_names_right : on_clause.key_names_left,
            on_clause.condColumnNames().first,
            join.key_sizes[clause_idx],
            HashJoin::isLowCardinalityType(join.data->type));
    }

    /// Only `MapsAll` keeps every right row of a key, so only there do the recorded words resolve to
    /// exact right rows. The residual-filter path marks its right matches itself, as in
    /// `HashJoinMethods::joinBlockImpl`.
    constexpr bool refs_can_carry_stats = join_features.is_maps_all && (join_features.inner || join_features.left || join_features.full);
    const bool record_refs_for_stats = refs_can_carry_stats && join.recordsRowRefsForStats();

    AddedColumns added_columns(
        scattered_block,
        is_join_get ? *join_get_columns : join.sample_block_with_columns_to_add,
        join.savedBlockSample(),
        join,
        std::move(join_on_keys),
        table_join->getMixedJoinExpression(),
        join.additional_filter_required_rhs_pos,
        join_features.is_asof_join,
        is_join_get,
        record_refs_for_stats);
    if (matched_rows_stats && matched_rows_stats->hasRightFlags())
        added_columns.match_stats = matched_rows_stats.get();

    const bool has_required_right_keys = join.required_right_keys.columns() != 0;
    added_columns.need_filter = join_features.need_filter || has_required_right_keys;
    added_columns.max_joined_block_rows = join.max_joined_block_rows;
    if (!added_columns.max_joined_block_rows)
        added_columns.max_joined_block_rows = std::numeric_limits<size_t>::max();

    using HashJoinTables = typename HashJoinTableMapsFor<MapsShape>::Type;

    size_t processed_rows = 0;
    if (scattered_block.rows() > 0)
    {
        /// Lookups and match bookkeeping only. No column value is gathered yet - that is deferred to
        /// the lazy `HashJoinResult::next`, whose events are shared with the other hash-join algorithms.
        ProfileEventTimeIncrement<Microseconds> lookup_watch(ProfileEvents::HashJoinPartitionedProbeLookupMicroseconds);
        /// Every clause's table has the one merged type (`HashJoin::mergeJoinMethods`), so one switch serves all.
        switch (join.data->type)
        {
#define M(TYPE) \
    case HashJoin::Type::TYPE: { \
        using Map = const typename decltype(HashJoinTables::TYPE)::element_type; \
        using KeyGetter = typename KeyGetterForType<HashJoin::Type::TYPE, Map, join_features.need_flags>::Type; \
        if (clauses.size() == 1) \
            processed_rows = joinRightColumns<KIND, STRICTNESS, MapsShape, KeyGetter, Map>( \
                *std::get<HashJoinTables>(clauses.front().tableMaps().maps).TYPE, added_columns, scattered_block, lane); \
        else \
        { \
            std::vector<const Map *> tables; \
            tables.reserve(clauses.size()); \
            for (const auto & clause : clauses) \
                tables.push_back(std::get<HashJoinTables>(clause.tableMaps().maps).TYPE.get()); \
            processed_rows = joinRightColumns<KIND, STRICTNESS, MapsShape, KeyGetter, Map>(tables, added_columns, scattered_block); \
        } \
        break; \
    }
            APPLY_FOR_PARTITIONED_JOIN_TABLES(M)
#undef M
            default:
                throw Exception(
                    ErrorCodes::UNSUPPORTED_JOIN_KEYS, "Unsupported JOIN keys for the partitioned join (type: {})", join.data->type);
        }
    }
    else
    {
        /// An empty probe block may legally arrive before any build data exists; nothing to look up.
        if constexpr (join_features.need_replication)
            added_columns.offsets_to_replicate = IColumn::Offsets(0);
    }

    added_columns.join_on_keys.clear();

    /// Per block, from what the kernel produced. The left matches come from the replication offsets,
    /// the default-row markers or the filter; the right matches from the recorded refs.
    if (auto * stats = matched_rows_stats.get())
    {
        const size_t probed_rows = processed_rows ? processed_rows : scattered_block.rows();
        stats->collectProbeBlock(probed_rows, countMatchedLeftRows<KIND, STRICTNESS>(added_columns, probed_rows));

        const bool right_matches_marked_inline = added_columns.additional_filter_expression != nullptr;
        if (stats->hasRightFlags() && !right_matches_marked_inline)
            markRightMatchedFromRowRefs(*stats, added_columns);
    }

    /// A mixed ON condition stops at `max_joined_block_rows`; the rows it did not reach go back to the
    /// transform as the next block, as in `HashJoinMethods::joinBlockImpl`.
    std::optional<ScatteredBlock> next_scattered_block;
    if (0 < processed_rows && processed_rows < scattered_block.rows())
    {
        auto [raw_block, raw_selector] = std::move(scattered_block).detachData();
        auto split_selector = raw_selector.split(processed_rows);
        scattered_block = ScatteredBlock(raw_block, std::move(split_selector.first));
        next_scattered_block = ScatteredBlock(std::move(raw_block), std::move(split_selector.second));
    }

    /// The count only advances while row refs are recorded; otherwise leave it empty rather than report
    /// a zero the planner would take as measured.
    const std::optional<size_t> matched_right_rows
        = added_columns.record_row_refs ? std::optional<size_t>(added_columns.lazy_output.hash_table_matches) : std::nullopt;

    auto join_result = std::make_unique<HashJoinResult>(
        std::move(added_columns.lazy_output),
        std::move(added_columns.columns),
        std::move(added_columns.offsets_to_replicate),
        std::move(added_columns.filter),
        std::move(added_columns.matched_rows),
        matched_right_rows,
        std::move(scattered_block),
        HashJoinResult::Properties{
            *join.table_join,
            join.required_right_keys,
            join.required_right_keys_sources,
            join.max_joined_block_rows,
            join.max_joined_block_bytes,
            join.data->allocated_size / std::max<size_t>(1, join.data->rows_to_join),
            join_features.need_filter,
            is_join_get,
            join.joined_block_split_single_row,
            join.enable_lazy_columns_replication,
            join.enable_lazy_columns_indexing});

    if (next_scattered_block)
        join_result->setNextBlock(std::move(*next_scattered_block));
    return join_result;
}

}
