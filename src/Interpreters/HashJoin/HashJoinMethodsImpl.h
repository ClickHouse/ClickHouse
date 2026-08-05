#pragma once

#include <Columns/ColumnsCommon.h>
#include <Columns/IColumn.h>
#include <Common/HashTable/Prefetching.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/HashJoin/AddedColumns.h>
#include <Interpreters/HashJoin/HashJoinMethods.h>
#include <Interpreters/HashJoin/HashJoinResult.h>
#include <Interpreters/HashJoin/MatchedRowsStats.h>
#include <Interpreters/HashJoin/ProbeLookup.h>
#include <Interpreters/JoinUtils.h>

#include <algorithm>
#include <type_traits>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

template <typename Map, typename KeyHolder>
concept HasPrefetchMemberFunc = requires
{
    {std::declval<Map>().prefetch(std::declval<KeyHolder>())};
};

/// Prefetching only pays if recomputing the key to prefetch it is cheap.
template <typename KeyGetter, typename Map>
constexpr bool join_prefetch_supported = KeyGetter::has_cheap_key_calculation
    && HasPrefetchMemberFunc<
        std::remove_const_t<Map>,
        decltype(std::declval<KeyGetter &>().getKeyHolder(std::declval<size_t>(), std::declval<Arena &>()))>;

/// Below the threshold the map fits in cache and the prefetch is pure overhead.
template <typename Map>
ALWAYS_INLINE bool shouldUseJoinPrefetch(bool enable_prefetch, const Map * map)
{
    return enable_prefetch && map != nullptr
        && map->getBufferSizeInBytes() > getMinBytesForPrefetchInJoin();
}

/** A named type rather than a lambda, so the lookup is not keyed on the join variant and
  * `need_filter` too. Construct one per probe call: calibration fires at a fixed absolute row, so
  * a fresh instance for a later batch never calibrates.
  */
template <typename Map, typename KeyGetter, typename Selector>
struct ProbePrefetch
{
    const Map * map = nullptr;
    KeyGetter * key_getter = nullptr;
    const Selector * selector = nullptr;
    Arena * pool = nullptr;
    bool use_prefetch = false;
    size_t total = 0;
    PrefetchingHelper prefetching{};
    size_t prefetch_look_ahead = PrefetchingHelper::getInitialLookAheadValue();

    ALWAYS_INLINE void operator()(size_t absolute_row)
    {
        if constexpr (join_prefetch_supported<KeyGetter, Map>)
        {
            if (!use_prefetch)
                return;

            if (absolute_row == PrefetchingHelper::iterationsToMeasure())
                prefetch_look_ahead = prefetching.calcPrefetchLookAhead();

            const size_t prefetch_idx = absolute_row + prefetch_look_ahead;
            if (prefetch_idx < total)
                map->prefetch(key_getter->getKeyHolder(selectorIndexAt(*selector, prefetch_idx), *pool));
        }
    }
};

template <typename PrefetchAction>
struct JoinPrefetcher
{
    bool use_prefetch = false;
    size_t total = 0;
    PrefetchAction prefetch_action;
    PrefetchingHelper prefetching{};
    size_t prefetch_look_ahead = PrefetchingHelper::getInitialLookAheadValue();

    ALWAYS_INLINE void prefetchAt(size_t i)
    {
        if (!use_prefetch)
            return;

        if (i == PrefetchingHelper::iterationsToMeasure())
            prefetch_look_ahead = prefetching.calcPrefetchLookAhead();

        const size_t prefetch_idx = i + prefetch_look_ahead;
        if (prefetch_idx < total)
            prefetch_action(prefetch_idx);
    }
};

template <typename PrefetchAction>
ALWAYS_INLINE auto makeJoinPrefetcher(bool use_prefetch, size_t total, PrefetchAction && prefetch_action)
{
    return JoinPrefetcher<std::decay_t<PrefetchAction>>{
        use_prefetch, total, std::forward<PrefetchAction>(prefetch_action)};
}

inline ALWAYS_INLINE bool clauseSkipsRows(const JoinOnKeyColumns & keys)
{
    return keys.null_map || keys.join_mask_column.getKind() != JoinCommon::JoinMask::Kind::AllTrue;
}

template <typename Sel>
ALWAYS_INLINE const UInt8 * buildClauseSkipData(const JoinOnKeyColumns & keys, IColumn::Filter & buffer, const Sel & sel, size_t rows)
{
    if (!clauseSkipsRows(keys))
        return nullptr;
    if constexpr (std::is_same_v<std::decay_t<Sel>, ScatteredBlock::Indexes>)
        return keys.buildRowSkipData(buffer, sel);
    else
        return keys.buildRowSkipData(buffer, sel.first, rows);
}

/// Empty rather than a vector of null pointers, so the caller can drop the check entirely.
template <typename Sel>
ALWAYS_INLINE void buildClauseSkipDatas(
    const std::vector<JoinOnKeyColumns> & join_on_keys,
    const Sel & sel,
    size_t rows,
    std::vector<const UInt8 *> & skip_datas,
    std::vector<IColumn::Filter> & skip_buffers)
{
    const size_t num_clauses = join_on_keys.size();
    if (std::ranges::none_of(join_on_keys, clauseSkipsRows))
    {
        skip_datas.clear();
        return;
    }

    skip_datas.resize(num_clauses);
    skip_buffers.resize(num_clauses);
    for (size_t d = 0; d < num_clauses; ++d)
        skip_datas[d] = buildClauseSkipData(join_on_keys[d], skip_buffers[d], sel, rows);
}

template <typename Map, typename KeyGetter, typename Sel>
ALWAYS_INLINE ProbePrefetch<Map, KeyGetter, Sel>
makeProbePrefetcher(const Map * map, KeyGetter & key_getter, const Sel & sel, Arena & pool, bool enable_prefetch, size_t rows)
{
    bool use_prefetch = false;
    if constexpr (join_prefetch_supported<KeyGetter, Map>)
        use_prefetch = shouldUseJoinPrefetch(enable_prefetch, map);
    return ProbePrefetch<Map, KeyGetter, Sel>{map, &key_getter, &sel, &pool, use_prefetch, rows};
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate> // NOLINT(readability-identifier-naming)
void HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::insertFromBlockImpl(
    HashJoin & join,
    HashJoin::Type type,
    MapsTemplate & maps,
    BlockKeyGetter & block_key_getter,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    UInt32 stored_block_no,
    const ScatteredBlock::Selector & selector,
    const Columns * dense_keys,
    ConstNullMapPtr null_map,
    const JoinCommon::JoinMask & join_mask,
    Arena & pool,
    BuildResult & result)
{
    switch (type)
    {
#define M(TYPE) \
    case HashJoin::Type::TYPE: { \
        using KeyGetterT = \
            typename KeyGetterForType<HashJoin::Type::TYPE, std::remove_reference_t<decltype(*maps.TYPE)>, needs_offset>::Type; \
        auto insert = [&](const auto & sel) __attribute__((always_inline)) \
        { \
            insertFromBlockImplTypeCase<KeyGetterT>( \
                join, \
                *maps.TYPE, \
                block_key_getter, \
                key_columns, \
                key_sizes, \
                stored_block_no, \
                sel, \
                dense_keys, \
                null_map, \
                join_mask, \
                pool, \
                result); \
        }; \
        if (selector.isContinuousRange()) \
            insert(selector.getRange()); \
        else \
            insert(selector.getIndexes()); \
        break; \
    }

        APPLY_FOR_JOIN_VARIANTS(M)
#undef M
    }
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate> // NOLINT(readability-identifier-naming)
JoinResultPtr HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::joinBlockImpl(
    const HashJoin & join, Block block, const Block & block_with_columns_to_add, const MapsTemplateVector & maps_, bool is_join_get)
{
    ScatteredBlock scattered_block{std::move(block)};
    return joinBlockImpl(join, std::move(scattered_block), block_with_columns_to_add, maps_, is_join_get);
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate> // NOLINT(readability-identifier-naming)
JoinResultPtr HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::joinBlockImpl(
    const HashJoin & join,
    ScatteredBlock block,
    const Block & block_with_columns_to_add,
    const MapsTemplateVector & maps_,
    bool is_join_get)
{
    constexpr JoinFeatures<KIND, STRICTNESS, MapsTemplate> join_features;

    std::vector<JoinOnKeyColumns> join_on_keys;
    const auto & onexprs = join.table_join->getClauses();
    for (size_t i = 0; i < onexprs.size(); ++i)
    {
        const auto & key_names = !is_join_get ? onexprs[i].key_names_left : onexprs[i].key_names_right;
        join_on_keys.emplace_back(
            block, key_names, onexprs[i].condColumnNames().first, join.key_sizes[i],
            HashJoin::isLowCardinalityType(join.data->type));
    }

    /// Only MapsAll keeps every right row of a key, so only there do the recorded words resolve to
    /// exact rows. The residual path is excluded: its words count output rows rather than left rows,
    /// and both metrics come from elsewhere there
    constexpr bool refs_can_carry_stats = join_features.is_maps_all
        && (join_features.inner || join_features.left || join_features.full);
    const bool record_refs_for_stats = refs_can_carry_stats && join.recordsRowRefsForStats();

    /** For LEFT/INNER JOIN, the saved blocks do not contain keys.
      * For FULL/RIGHT JOIN, the saved blocks contain keys;
      *  but they will not be used at this stage of joining (and will be in `CollectorNonJoined`), and they need to be skipped.
      * For ASOF, the last column is used as the ASOF column
      */
    AddedColumns<!join_features.is_any_join> added_columns(
        block,
        block_with_columns_to_add,
        join.savedBlockSample(),
        join,
        std::move(join_on_keys),
        join.table_join->getMixedJoinExpression(),
        join.additional_filter_required_rhs_pos,
        join_features.is_asof_join,
        is_join_get,
        record_refs_for_stats);

    if (join.matched_rows_stats && join.matched_rows_stats->hasRightFlags())
        added_columns.match_stats = join.matched_rows_stats.get();

    bool has_required_right_keys = (join.required_right_keys.columns() != 0);
    added_columns.need_filter = join_features.need_filter || has_required_right_keys;
    added_columns.max_joined_block_rows = join.max_joined_block_rows;

    if (!added_columns.max_joined_block_rows)
        added_columns.max_joined_block_rows = std::numeric_limits<size_t>::max();
    else
        added_columns.reserve(join_features.need_replication);

    size_t processed_rows
        = switchJoinRightColumns(maps_, added_columns, block.getSelector(), join.data->type, *join.used_flags, join.data->key_range);
    /// Do not hold memory for join_on_keys anymore
    added_columns.join_on_keys.clear();

    if (auto * stats = join.matched_rows_stats.get())
    {
        const size_t probed_rows = processed_rows ? processed_rows : block.rows();
        stats->collectProbeBlock(probed_rows, countMatchedLeftRows<KIND, STRICTNESS>(added_columns, probed_rows));

        const bool right_matches_marked_inline = added_columns.additional_filter_expression != nullptr;
        if (stats->hasRightFlags() && !right_matches_marked_inline)
            markRightMatchedFromRowRefs(*stats, added_columns);
    }

    std::optional<ScatteredBlock> next_scattered_block;
    if (0 < processed_rows && processed_rows < block.rows())
    {
        auto [raw_block, raw_selector] = std::move(block).detachData();
        auto split_selector = raw_selector.split(processed_rows);
        block = ScatteredBlock(raw_block, std::move(split_selector.first));
        next_scattered_block = ScatteredBlock(std::move(raw_block), std::move(split_selector.second));
    }

    auto join_result = std::make_unique<HashJoinResult>(
        std::move(added_columns.lazy_output),
        std::move(added_columns.columns),
        std::move(added_columns.offsets_to_replicate),
        std::move(added_columns.filter),
        std::move(added_columns.matched_rows),
        std::move(block),
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
            join.enable_lazy_columns_indexing
        });

    if (next_scattered_block)
        join_result->setNextBlock(std::move(next_scattered_block.value()));
    return join_result;
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate> // NOLINT(readability-identifier-naming)
template <typename KeyGetter, bool is_asof_join>
KeyGetter HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::createKeyGetter(const ColumnRawPtrs & key_columns, const Sizes & key_sizes, HashJoin::RightTableData::KeyRange key_range)
{
    KeyGetter getter = [&]()
    {
        if constexpr (is_asof_join)
        {
            auto key_column_copy = key_columns;
            auto key_size_copy = key_sizes;
            key_column_copy.pop_back();
            key_size_copy.pop_back();
            return KeyGetter(key_column_copy, key_size_copy, nullptr);
        }
        else
            return KeyGetter(key_columns, key_sizes, nullptr);
    }();

    if constexpr (ColumnsHashing::IsHashMethodInRange<KeyGetter>::value)
    {
        getter.min_key = static_cast<decltype(getter.min_key)>(key_range.min_key);
        getter.range_size = static_cast<decltype(getter.range_size)>(key_range.size);
    }

    return getter;
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate> // NOLINT(readability-identifier-naming)
template <typename KeyGetter, bool is_asof_join>
KeyGetter & HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::blockKeyGetter(
    BlockKeyGetter & block_key_getter, std::optional<KeyGetter> & own, const ColumnRawPtrs & key_columns, const Sizes & key_sizes)
{
    const auto create = [&] { return createKeyGetter<KeyGetter, is_asof_join>(key_columns, key_sizes); };

    if constexpr (shareKeyGetterAcrossBuckets<KeyGetter>())
        return block_key_getter.getOrBuild<KeyGetter>(create);
    else
        return own.emplace(create());
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate> // NOLINT(readability-identifier-naming)
template <typename KeyGetter, typename HashMap, typename Selector>
void HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::insertFromBlockImplTypeCase(
    HashJoin & join,
    HashMap & map,
    BlockKeyGetter & block_key_getter,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    UInt32 stored_block_no,
    const Selector & selector,
    const Columns * dense_keys,
    ConstNullMapPtr null_map,
    const JoinCommon::JoinMask & join_mask,
    Arena & pool,
    BuildResult & result)
{
    [[maybe_unused]] constexpr bool mapped_one = std::is_same_v<typename HashMap::mapped_type, RowRef>;
    constexpr bool is_asof_join = STRICTNESS == JoinStrictness::Asof;

    const IColumn * asof_column [[maybe_unused]] = nullptr;
    if constexpr (is_asof_join)
        asof_column = key_columns.back();

    const size_t rows = ScatteredBlock::Selector::size(selector);

    std::optional<KeyGetter> own_key_getter;
    ColumnRawPtrs dense_key_ptrs;
    KeyGetter * key_getter_ptr = nullptr;
    if (dense_keys)
    {
        chassert(!dense_keys->empty() && dense_keys->front()->size() == rows);
        dense_key_ptrs.reserve(dense_keys->size());
        for (const auto & column : *dense_keys)
            dense_key_ptrs.push_back(column.get());
        key_getter_ptr = &own_key_getter.emplace(createKeyGetter<KeyGetter, is_asof_join>(dense_key_ptrs, key_sizes));
    }
    else
    {
        key_getter_ptr = &blockKeyGetter<KeyGetter, is_asof_join>(block_key_getter, own_key_getter, key_columns, key_sizes);
    }
    auto & key_getter = *key_getter_ptr;

    /// For ALL and ASOF join always insert values
    result.is_inserted = !mapped_one || is_asof_join;

    constexpr bool can_prefetch = join_prefetch_supported<KeyGetter, HashMap>;

    bool use_prefetch = false;
    if constexpr (can_prefetch)
        use_prefetch = shouldUseJoinPrefetch(join.enable_prefetch, &map);

    const bool keys_are_dense = dense_keys != nullptr;

    auto prefetcher = makeJoinPrefetcher(use_prefetch, rows,
        [&](size_t k) __attribute__((always_inline))
        {
            if constexpr (can_prefetch)
                map.prefetch(key_getter.getKeyHolder(keys_are_dense ? k : selectorIndexAt(selector, k), pool));
        });

    for (size_t i = 0; i < rows; ++i)
    {
        if constexpr (can_prefetch)
            prefetcher.prefetchAt(i);

        const size_t ind = selectorIndexAt(selector, i);
        const size_t key_row = keys_are_dense ? i : ind;

        chassert(!null_map || ind < null_map->size());
        if (null_map && (*null_map)[ind])
        {
            /// nulls are not inserted into hash table,
            /// keep them for RIGHT and FULL joins
            result.is_inserted = true;
            continue;
        }

        /// Unlike the NULL rows above, these are not kept for RIGHT/FULL.
        if (join_mask.isRowFiltered(ind))
            continue;

        if constexpr (is_asof_join)
            Inserter<HashMap, KeyGetter>::insertAsof(
                join, map, key_getter, stored_block_no, key_row, ind, pool, result.new_keys, *asof_column);
        else if constexpr (mapped_one)
            result.is_inserted
                |= Inserter<HashMap, KeyGetter>::insertOne(join, map, key_getter, stored_block_no, key_row, ind, pool, result.new_keys);
        else
            result.all_values_unique
                &= Inserter<HashMap, KeyGetter>::insertAll(join, map, key_getter, stored_block_no, key_row, ind, pool, result.new_keys);
    }
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate> // NOLINT(readability-identifier-naming)
template <typename AddedColumns>
size_t HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::switchJoinRightColumns(
    const std::vector<const MapsTemplate *> & mapv,
    AddedColumns & added_columns,
    const ScatteredBlock::Selector & selector,
    HashJoin::Type type,
    JoinStuff::JoinUsedFlags & used_flags,
    HashJoin::RightTableData::KeyRange key_range)
{
    constexpr bool is_asof_join = STRICTNESS == JoinStrictness::Asof;
    switch (type)
    {
#define M(TYPE) \
    case HashJoin::Type::TYPE: { \
        using MapTypeVal = const typename std::remove_reference_t<decltype(MapsTemplate::TYPE)>::element_type; \
        using KeyGetter = typename KeyGetterForType<HashJoin::Type::TYPE, MapTypeVal, needs_offset>::Type; \
        std::vector<const MapTypeVal *> a_map_type_vector(mapv.size()); \
        std::vector<KeyGetter> key_getter_vector; \
        for (size_t d = 0; d < added_columns.join_on_keys.size(); ++d) \
        { \
            const auto & join_on_key = added_columns.join_on_keys[d]; \
            a_map_type_vector[d] = mapv[d]->TYPE.get(); \
            key_getter_vector.push_back( \
                std::move(createKeyGetter<KeyGetter, is_asof_join>(join_on_key.key_columns, join_on_key.key_sizes, key_range))); \
        } \
        return joinRightColumnsSwitchNullability<KeyGetter>( \
            std::move(key_getter_vector), a_map_type_vector, added_columns, selector, used_flags); \
    }
        APPLY_FOR_JOIN_VARIANTS(M)
#undef M

    }
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate> // NOLINT(readability-identifier-naming)
template <typename KeyGetter, typename Map, typename AddedColumns>
size_t HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::joinRightColumnsSwitchNullability(
    std::vector<KeyGetter> && key_getter_vector,
    const std::vector<const Map *> & mapv,
    AddedColumns & added_columns,
    const ScatteredBlock::Selector & selector,
    JoinStuff::JoinUsedFlags & used_flags)
{
    if (added_columns.need_filter)
    {
        return joinRightColumnsSwitchMultipleDisjuncts<KeyGetter, Map, true>(
            std::forward<std::vector<KeyGetter>>(key_getter_vector), mapv, added_columns, selector, used_flags);
    }
    else
    {
        return joinRightColumnsSwitchMultipleDisjuncts<KeyGetter, Map, false>(
            std::forward<std::vector<KeyGetter>>(key_getter_vector), mapv, added_columns, selector, used_flags);
    }
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate> // NOLINT(readability-identifier-naming)
template <typename KeyGetter, typename Map, bool need_filter, typename AddedColumns>
size_t HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::joinRightColumnsSwitchMultipleDisjuncts(
    std::vector<KeyGetter> && key_getter_vector,
    const std::vector<const Map *> & mapv,
    AddedColumns & added_columns,
    const ScatteredBlock::Selector & selector,
    JoinStuff::JoinUsedFlags & used_flags)
{
    constexpr JoinFeatures<KIND, STRICTNESS, MapsTemplate> join_features;
    if constexpr (join_features.is_maps_all)
    {
        if (added_columns.additional_filter_expression)
        {
            const bool mark_per_row_used = join_features.right || join_features.full || mapv.size() > 1;
            return joinRightColumnsWithAdditionalFilter<KeyGetter, Map>(
                std::forward<std::vector<KeyGetter>>(key_getter_vector),
                mapv,
                added_columns,
                used_flags,
                selector,
                need_filter,
                mark_per_row_used);
        }
    }

    if (added_columns.additional_filter_expression)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Additional filter expression is not supported for this JOIN");

    if (selector.isContinuousRange())
    {
        if (mapv.size() > 1 || added_columns.join_on_keys.empty())
            return joinRightColumns<KeyGetter, Map, need_filter>(
                std::move(key_getter_vector), mapv, added_columns, used_flags, selector.getRange());
        chassert(key_getter_vector.size() == 1);
        return joinRightColumns<KeyGetter, Map, need_filter>(
            key_getter_vector.at(0), mapv.at(0), added_columns, used_flags, selector.getRange());
    }
    if (mapv.size() > 1 || added_columns.join_on_keys.empty())
        return joinRightColumns<KeyGetter, Map, need_filter>(
            std::move(key_getter_vector), mapv, added_columns, used_flags, selector.getIndexes());
    chassert(key_getter_vector.size() == 1);
    return joinRightColumns<KeyGetter, Map, need_filter>(
        key_getter_vector.at(0), mapv.at(0), added_columns, used_flags, selector.getIndexes());
}

template <bool need_filter>
void setUsed(IColumn::Filter & filter [[maybe_unused]], size_t pos [[maybe_unused]], IColumn::Offsets & matched_rows [[maybe_unused]])
{
    if constexpr (need_filter)
    {
        filter[pos] = 1;
        matched_rows.push_back(pos);
    }
}

/// Lets `addFoundRowAll` collect refs for the additional filter instead of emitting them.
struct PreSelectedRows
{
    explicit PreSelectedRows(PODArray<UInt64> & container_)
        : container(container_)
    {
    }
    void appendFromBlock(UInt64 ref_word, bool /* has_default */) { container.push_back(ref_word); }
    static constexpr bool isLazy() { return false; }

    PODArray<UInt64> & container;
};

template <typename MappedValue>
ALWAYS_INLINE const MappedValue * mappedFromOutcomeWord(UInt64 word, MappedValue & storage)
{
    if constexpr (probe_mapped_fits_word<MappedValue>)
    {
        storage = mappedFromWord<MappedValue>(word);
        return &storage;
    }
    else
    {
        return reinterpret_cast<const MappedValue *>(word); /// NOLINT(performance-no-int-to-ptr)
    }
}

/// Leaves `offsets_to_replicate` to the caller: the two emit paths write it differently.
template <
    JoinKind KIND, // NOLINT(readability-identifier-naming)
    JoinStrictness STRICTNESS, // NOLINT(readability-identifier-naming)
    bool need_filter,
    typename MapsTemplate,
    typename AddedColumns>
ALWAYS_INLINE void addMissRow(AddedColumns & added_columns, size_t row, IColumn::Offset & current_offset)
{
    constexpr JoinFeatures<KIND, STRICTNESS, MapsTemplate> join_features;
    if constexpr (join_features.is_anti_join && join_features.left)
        setUsed<need_filter>(added_columns.filter, row, added_columns.matched_rows);
    addNotFoundRow<join_features.add_missing, join_features.need_replication>(added_columns, current_offset);
}

/** What to emit for one matched right key. Shared by the single-clause and the clause-major emit
  * paths so the two cannot drift apart. `ind` is read by ASOF only, for the left ASOF key.
  */
template <
    JoinKind KIND, // NOLINT(readability-identifier-naming)
    JoinStrictness STRICTNESS, // NOLINT(readability-identifier-naming)
    bool need_filter,
    bool flag_per_row,
    typename MapsTemplate,
    typename FindResult,
    typename AddedColumns>
ALWAYS_INLINE void matchFoundRow(
    FindResult & find_result,
    AddedColumns & added_columns,
    JoinStuff::JoinUsedFlags & used_flags,
    size_t row,
    size_t ind,
    IColumn::Offset & current_offset,
    KnownRowsHolder<flag_per_row> & known_rows,
    bool is_last_disjunct)
{
    constexpr JoinFeatures<KIND, STRICTNESS, MapsTemplate> join_features;
    using MappedValue = MapsTemplate::MappedType;
    auto & mapped = find_result.getMapped();
    if constexpr (join_features.is_asof_join)
    {
        const IColumn & left_asof_key = added_columns.leftAsofKey();

        const auto * row_ref = mapped->findAsof(left_asof_key, ind);
        if (row_ref)
        {
            setUsed<need_filter>(added_columns.filter, row, added_columns.matched_rows);
            added_columns.appendFromBlock(row_ref->encode(), join_features.add_missing);
        }
        else
            addNotFoundRow<join_features.add_missing, join_features.need_replication>(added_columns, current_offset);
    }
    else if constexpr (join_features.is_all_join)
    {
        setUsed<need_filter>(added_columns.filter, row, added_columns.matched_rows);
        used_flags.template setUsed<join_features.need_flags, flag_per_row>(find_result);
        /// `setUsed` already marked the list; `addFoundRowAll` must not `setUsedOnce` again.
        addFoundRowAll<MappedValue, join_features.add_missing>(
            mapped, added_columns, current_offset, known_rows, nullptr, is_last_disjunct);
    }
    else if constexpr ((join_features.is_any_join || join_features.is_semi_join) && join_features.right)
    {
        /// Use first appeared left key + it needs left columns replication
        bool used_once = used_flags.template setUsedOnce<join_features.need_flags, flag_per_row>(find_result);
        if (used_once)
        {
            auto used_flags_opt = join_features.need_flags ? &used_flags : nullptr;
            setUsed<need_filter>(added_columns.filter, row, added_columns.matched_rows);
            addFoundRowAll<MappedValue, join_features.add_missing>(
                mapped, added_columns, current_offset, known_rows, used_flags_opt, is_last_disjunct);
        }
    }
    else if constexpr (join_features.is_any_join && join_features.inner)
    {
        /// Use first appeared left key only
        bool used_once = used_flags.template setUsedOnce<join_features.need_flags, flag_per_row>(find_result);
        if (used_once)
        {
            setUsed<need_filter>(added_columns.filter, row, added_columns.matched_rows);
            added_columns.appendFromBlock(firstRefWord(mapped), join_features.add_missing);
        }
    }
    else if constexpr (join_features.is_any_join && join_features.full)
    {
        /// Unreachable: `TreeRewriter` rejects ANY FULL JOIN.
    }
    else if constexpr (join_features.is_anti_join)
    {
        if constexpr (join_features.right && join_features.need_flags)
            used_flags.template setUsed<join_features.need_flags, flag_per_row>(find_result);
    }
    else /// ANY LEFT, SEMI LEFT, old ANY (RightAny)
    {
        setUsed<need_filter>(added_columns.filter, row, added_columns.matched_rows);
        used_flags.template setUsed<join_features.need_flags, flag_per_row>(find_result);
        added_columns.appendFromBlock(firstRefWord(mapped), join_features.add_missing);
    }
}

/** Emit from the recorded outcomes: no hash table, and skips were already recorded as misses.
  * Keyed on the mapped type, not on Map/KeyGetter, so it is not emitted once per key type.
  */
template <
    JoinKind KIND, // NOLINT(readability-identifier-naming)
    JoinStrictness STRICTNESS, // NOLINT(readability-identifier-naming)
    bool need_filter,
    typename MapsTemplate,
    typename AddedColumns,
    typename Selector>
ALWAYS_INLINE void consumeProbeBatchImpl(
    const ProbeOutcomes & outcomes,
    AddedColumns & added_columns,
    JoinStuff::JoinUsedFlags & used_flags,
    const Selector * selector,
    size_t begin,
    size_t count,
    IColumn::Offset & current_offset)
{
    constexpr JoinFeatures<KIND, STRICTNESS, MapsTemplate> join_features;
    static_assert(
        join_features.is_asof_join != std::is_same_v<Selector, std::nullptr_t>,
        "the selector is passed for ASOF only, which needs the probe row for its asof key");
    static constexpr bool flag_per_row = false; /// this path runs for a single map only

    using Mapped = const MapsTemplate::MappedType;
    using MappedValue = MapsTemplate::MappedType;
    using FindResult = ColumnsHashing::columns_hashing_impl::FindResultImpl<Mapped, join_features.need_flags>;

    const UInt64 * const found = outcomes.found;
    const UInt64 * const offsets [[maybe_unused]] = join_features.need_flags ? outcomes.offset.data() : nullptr;

    for (size_t j = 0; j < count; ++j)
    {
        const size_t i = begin + j;
        size_t ind = 0;
        if constexpr (join_features.is_asof_join)
            ind = selectorIndexAt(*selector, i);

        bool right_row_found = false;
        KnownRowsHolder<flag_per_row> dummy_known_rows;

        if (const UInt64 word = found[j])
        {
            right_row_found = true;

            size_t offset = 0;
            if constexpr (join_features.need_flags)
                offset = offsets[j];

            MappedValue mapped_value_storage{};
            Mapped * mapped_ptr = mappedFromOutcomeWord<MappedValue>(word, mapped_value_storage);
            FindResult find_result(mapped_ptr, true, offset);

            matchFoundRow<KIND, STRICTNESS, need_filter, flag_per_row, MapsTemplate>(
                find_result, added_columns, used_flags, /*row=*/i, ind, current_offset, dummy_known_rows, /*is_last_disjunct=*/true);
        }

        if (!right_row_found)
            addMissRow<KIND, STRICTNESS, need_filter, MapsTemplate>(added_columns, i, current_offset);

        if constexpr (join_features.need_replication)
            added_columns.offsets_to_replicate[i] = current_offset;
    }
}

/** Two overloads rather than one taking `const Selector *`: only ASOF needs it, and making every
  * other caller pass a dead null measured +3430 `.text` instructions over the join TUs.
  */
template <
    JoinKind KIND, // NOLINT(readability-identifier-naming)
    JoinStrictness STRICTNESS, // NOLINT(readability-identifier-naming)
    bool need_filter,
    typename MapsTemplate,
    typename AddedColumns>
NO_INLINE void consumeProbeBatch(
    const ProbeOutcomes & outcomes,
    AddedColumns & added_columns,
    JoinStuff::JoinUsedFlags & used_flags,
    size_t begin,
    size_t count,
    IColumn::Offset & current_offset)
{
    consumeProbeBatchImpl<KIND, STRICTNESS, need_filter, MapsTemplate, AddedColumns, std::nullptr_t>(
        outcomes, added_columns, used_flags, nullptr, begin, count, current_offset);
}

template <
    JoinKind KIND, // NOLINT(readability-identifier-naming)
    JoinStrictness STRICTNESS, // NOLINT(readability-identifier-naming)
    bool need_filter,
    typename MapsTemplate,
    typename AddedColumns,
    typename Selector>
NO_INLINE void consumeProbeBatch(
    const ProbeOutcomes & outcomes,
    AddedColumns & added_columns,
    JoinStuff::JoinUsedFlags & used_flags,
    const Selector & selector,
    size_t begin,
    size_t count,
    IColumn::Offset & current_offset)
{
    consumeProbeBatchImpl<KIND, STRICTNESS, need_filter, MapsTemplate, AddedColumns>(
        outcomes, added_columns, used_flags, &selector, begin, count, current_offset);
}

/** Whether the lookup can record straight into the output array instead of scratch. Only when the
  * output is one word per probe row and a zero word already means what `addDefault` would write,
  * which is the lazy ALL case.
  */
template <typename AddedColumns, typename JoinFeaturesT>
constexpr bool outputIsProbeOutcomes(const JoinFeaturesT & join_features)
{
    return AddedColumns::isLazy() && join_features.is_all_join && join_features.add_missing && join_features.need_replication
        && !join_features.is_asof_join;
}

/// Goes with the fused layout above: the words are in place, only the bookkeeping is left.
template <
    JoinKind KIND, // NOLINT(readability-identifier-naming)
    JoinStrictness STRICTNESS, // NOLINT(readability-identifier-naming)
    bool need_filter,
    typename MapsTemplate,
    typename AddedColumns>
NO_INLINE void consumeFusedBatch(
    const ProbeOutcomes & outcomes,
    AddedColumns & added_columns,
    JoinStuff::JoinUsedFlags & used_flags,
    size_t begin,
    size_t count,
    IColumn::Offset & current_offset)
{
    constexpr JoinFeatures<KIND, STRICTNESS, MapsTemplate> join_features;
    static_assert(outputIsProbeOutcomes<AddedColumns>(join_features));

    const UInt64 * const found = outcomes.found;
    const UInt64 * const offsets [[maybe_unused]] = join_features.need_flags ? outcomes.offset.data() : nullptr;

    size_t rows_added = 0;
    for (size_t j = 0; j < count; ++j)
    {
        const size_t i = begin + j;
        const UInt64 word = found[j];

        /// A zero word is the default row `addDefault` would have written: one output row.
        UInt32 rows_of_key = 1;
        if (word)
        {
            setUsed<need_filter>(added_columns.filter, i, added_columns.matched_rows);
            if constexpr (join_features.need_flags)
                used_flags.template setUsed<true, false>(0, 0, offsets[j]);
            rows_of_key = refWordRows(word);
        }

        rows_added += rows_of_key;
        current_offset += rows_of_key;
        added_columns.offsets_to_replicate[i] = current_offset;
    }

    added_columns.lazy_output.row_count += rows_added;
}

/// Keyed on (Map, KeyGetter, Selector) and `need_flags` only; adding `need_filter` would double
/// the emitted bodies, which is why `ProbePrefetch` does not depend on it.
template <bool need_flags, typename Map, typename KeyGetter, typename Selector, typename PrefetchAt>
NO_INLINE void lookupBatch(
    KeyGetter & key_getter,
    const Map & map,
    const Selector & selector,
    const UInt8 * skip_data,
    Arena & pool,
    size_t begin,
    size_t count,
    PrefetchAt && prefetch_at,
    ProbeOutcomes & outcomes)
{
    SequentialLookup::run<need_flags>(
        key_getter, map, selector, skip_data, pool, begin, count, std::forward<PrefetchAt>(prefetch_at), outcomes);
}


template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate> // NOLINT(readability-identifier-naming)
template <typename KeyGetter, typename Map, bool need_filter, typename AddedColumns, typename Selector>
size_t HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::joinRightColumns(
    KeyGetter & key_getter, const Map * map, AddedColumns & added_columns, JoinStuff::JoinUsedFlags & used_flags, const Selector & selector)
{
    static constexpr bool flag_per_row = false; // Always false in single map case
    const auto & join_keys = added_columns.join_on_keys.at(0);

    constexpr JoinFeatures<KIND, STRICTNESS, MapsTemplate> join_features;

    size_t rows = ScatteredBlock::Selector::size(selector);

    IColumn::Filter skip_buffer;
    const UInt8 * skip_data = buildClauseSkipData(join_keys, skip_buffer, selector, rows);
    if constexpr (need_filter)
    {
        added_columns.filter = IColumn::Filter(rows, 0);
        added_columns.matched_rows.reserve(rows);
    }
    if constexpr (!flag_per_row && (STRICTNESS == JoinStrictness::All || (STRICTNESS == JoinStrictness::Semi && KIND == JoinKind::Right)))
        added_columns.lazy_output.output_by_row_list = true;

    Arena pool;

    if constexpr (join_features.need_replication)
        added_columns.offsets_to_replicate = IColumn::Offsets(rows);

    /// Outside the batch loop on purpose - see `ProbePrefetch`.
    auto prefetch_at = makeProbePrefetcher(map, key_getter, selector, pool, added_columns.enable_prefetch, rows);

    IColumn::Offset current_offset = 0;
    ProbeOutcomes outcomes;
    const size_t scratch_rows = std::min(rows, PROBE_BATCH_ROWS);

    if constexpr (outputIsProbeOutcomes<AddedColumns>(join_features))
    {
        /// Without recorded row refs there is no output array to fuse into.
        if (added_columns.record_row_refs)
        {
            auto & row_refs = added_columns.lazy_output.row_refs;
            const size_t base = row_refs.size();
            /// Grown once for the block: a per-batch grow could move it under the lookup's
            /// pointer. Every slot is written, so the uninitialized POD resize is fine.
            row_refs.resize(base + rows);
            outcomes.useExternal(row_refs.data() + base, scratch_rows, join_features.need_flags);

            for (size_t begin = 0; begin < rows; begin += PROBE_BATCH_ROWS)
            {
                const size_t count = std::min(PROBE_BATCH_ROWS, rows - begin);
                outcomes.found = row_refs.data() + base + begin;
                lookupBatch<join_features.need_flags>(key_getter, *map, selector, skip_data, pool, begin, count, prefetch_at, outcomes);
                consumeFusedBatch<KIND, STRICTNESS, need_filter, MapsTemplate>(
                    outcomes, added_columns, used_flags, begin, count, current_offset);
            }
            added_columns.applyLazyDefaults();
            return 0;
        }
    }

    outcomes.useScratch(scratch_rows, join_features.need_flags);

    for (size_t begin = 0; begin < rows; begin += PROBE_BATCH_ROWS)
    {
        const size_t count = std::min(PROBE_BATCH_ROWS, rows - begin);
        lookupBatch<join_features.need_flags>(key_getter, *map, selector, skip_data, pool, begin, count, prefetch_at, outcomes);
        if constexpr (join_features.is_asof_join)
        {
            consumeProbeBatch<KIND, STRICTNESS, need_filter, MapsTemplate>(
                outcomes, added_columns, used_flags, selector, begin, count, current_offset);
        }
        else
        {
            consumeProbeBatch<KIND, STRICTNESS, need_filter, MapsTemplate>(
                outcomes, added_columns, used_flags, begin, count, current_offset);
        }
    }

    added_columns.applyLazyDefaults();
    return 0;
}

/** Emit a batch across all clauses; the miss row is added only when every clause is zero.
  *
  * `is_last_disjunct` is positional, not "the last one that matched". The offset is always zero
  * because `flag_per_row` flags are keyed by row, not by cell. The row limit is checked before a
  * row, so the row that crosses it is emitted in full.
  */
template <
    JoinKind KIND, // NOLINT(readability-identifier-naming)
    JoinStrictness STRICTNESS, // NOLINT(readability-identifier-naming)
    bool need_filter,
    typename MapsTemplate,
    typename AddedColumns,
    typename ProbeOutcomesAllocator>
NO_INLINE size_t emitBatch(
    const std::vector<ProbeOutcomes, ProbeOutcomesAllocator> & outcomes,
    size_t num_clauses,
    AddedColumns & added_columns,
    JoinStuff::JoinUsedFlags & used_flags,
    size_t begin,
    size_t count,
    size_t max_joined_rows,
    IColumn::Offset & current_offset)
{
    constexpr JoinFeatures<KIND, STRICTNESS, MapsTemplate> join_features;
    static constexpr bool flag_per_row = true; // Always true in multiple maps case

    using Mapped = const MapsTemplate::MappedType;
    using MappedValue = MapsTemplate::MappedType;
    using FindResult = ColumnsHashing::columns_hashing_impl::FindResultImpl<Mapped, join_features.need_flags>;

    size_t j = 0;
    for (; j < count && current_offset < max_joined_rows; ++j)
    {
        const size_t row = begin + j;
        bool right_row_found = false;
        KnownRowsHolder<flag_per_row> known_rows;
        for (size_t k = 0; k < num_clauses; ++k)
        {
            const UInt64 word = outcomes[k].found[j];
            if (!word)
                continue;

            right_row_found = true;
            const bool is_last_disjunct = (k + 1 == num_clauses);

            MappedValue mapped_value_storage{};
            Mapped * mapped_ptr = mappedFromOutcomeWord<MappedValue>(word, mapped_value_storage);
            FindResult find_result(mapped_ptr, true, /*off=*/0);

            matchFoundRow<KIND, STRICTNESS, need_filter, flag_per_row, MapsTemplate>(
                find_result, added_columns, used_flags, row, /*ind=*/0, current_offset, known_rows, is_last_disjunct);
        }

        if (!right_row_found)
            addMissRow<KIND, STRICTNESS, need_filter, MapsTemplate>(added_columns, row, current_offset);

        if constexpr (join_features.need_replication)
            added_columns.offsets_to_replicate.push_back(current_offset);
    }
    return j;
}

/// Clause-major rather than row-major, so the clauses reuse the single-clause `lookupBatch` bodies.
template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate> // NOLINT(readability-identifier-naming)
template <typename KeyGetter, typename Map, bool need_filter, typename AddedColumns, typename Selector>
size_t HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::joinRightColumns(
    std::vector<KeyGetter> && key_getter_vector,
    const std::vector<const Map *> & mapv,
    AddedColumns & added_columns,
    JoinStuff::JoinUsedFlags & used_flags,
    const Selector & selector)
{
    constexpr JoinFeatures<KIND, STRICTNESS, MapsTemplate> join_features;

    size_t rows = ScatteredBlock::Selector::size(selector);

    /// Counted from the key getters, not `mapv`: a join may have no ON keys at all.
    chassert(key_getter_vector.size() == added_columns.join_on_keys.size());
    chassert(key_getter_vector.size() == mapv.size() || key_getter_vector.empty());
    std::vector<const UInt8 *> skip_datas;
    std::vector<IColumn::Filter> skip_buffers;
    const size_t num_clauses = key_getter_vector.size();
    buildClauseSkipDatas(added_columns.join_on_keys, selector, rows, skip_datas, skip_buffers);
    if constexpr (need_filter)
    {
        added_columns.filter = IColumn::Filter(rows, 0);
        added_columns.matched_rows.reserve(rows);
    }

    Arena pool;

    if constexpr (join_features.need_replication)
    {
        added_columns.offsets_to_replicate.clear();
        added_columns.offsets_to_replicate.reserve(rows);
    }

    std::vector<ProbePrefetch<Map, KeyGetter, Selector>> prefetchers;
    prefetchers.reserve(num_clauses);
    for (size_t k = 0; k < num_clauses; ++k)
        prefetchers.push_back(makeProbePrefetcher(mapv[k], key_getter_vector[k], selector, pool, added_columns.enable_prefetch, rows));

    size_t max_joined_rows
        = added_columns.max_joined_block_rows > 0 ? added_columns.max_joined_block_rows : std::numeric_limits<size_t>::max();

    IColumn::Offset current_offset = 0;
    constexpr bool stop_after_first_match
        = join_features.is_any_or_semi_join && !(join_features.is_any_join && (join_features.right || join_features.full));

    const size_t scratch_rows = std::min(rows, PROBE_BATCH_ROWS);
    VectorWithMemoryTracking<ProbeOutcomes> outcomes(num_clauses);
    for (auto & outcome : outcomes)
        outcome.useScratch(scratch_rows, /*need_flags=*/false);

    /// Stands in for the `break` a row-major loop would do on the first match: what has matched so
    /// far is folded into the next clause's skip bytes. `combined_skip` is indexed by source row,
    /// like `buildRowSkipData` output, and `matched_in_batch` by batch position.
    PODArray<UInt8> matched_in_batch;
    IColumn::Filter combined_skip;
    if constexpr (stop_after_first_match)
    {
        matched_in_batch.resize(scratch_rows);
        if (num_clauses > 0)
            combined_skip.resize(added_columns.join_on_keys[0].join_mask_column.getSize());
    }

    size_t i = 0;
    for (size_t begin = 0; begin < rows; begin += PROBE_BATCH_ROWS)
    {
        const size_t count = std::min(PROBE_BATCH_ROWS, rows - begin);
        bool any_matched = false;
        if constexpr (stop_after_first_match)
            std::fill_n(matched_in_batch.begin(), count, 0);

        for (size_t k = 0; k < num_clauses; ++k)
        {
            const UInt8 * skip = skip_datas.empty() ? nullptr : skip_datas[k];
            if constexpr (stop_after_first_match)
            {
                if (k > 0 && any_matched)
                {
                    for (size_t j = 0; j < count; ++j)
                    {
                        const size_t ind = selectorIndexAt(selector, begin + j);
                        chassert(ind < combined_skip.size());
                        combined_skip[ind] = (skip ? skip[ind] : 0) | matched_in_batch[j];
                    }
                    skip = combined_skip.data();
                }
            }

            lookupBatch</*need_flags=*/false>(
                key_getter_vector[k], *mapv[k], selector, skip, pool, begin, count, prefetchers[k], outcomes[k]);

            if constexpr (stop_after_first_match)
            {
                if (k + 1 < num_clauses)
                {
                    for (size_t j = 0; j < count; ++j)
                    {
                        const UInt8 m = (outcomes[k].found[j] != 0);
                        matched_in_batch[j] |= m;
                        any_matched |= m;
                    }
                }
            }
        }

        const size_t consumed = emitBatch<KIND, STRICTNESS, need_filter, MapsTemplate>(
            outcomes, num_clauses, added_columns, used_flags, begin, count, max_joined_rows, current_offset);
        i = begin + consumed;
        if (consumed < count)
            break;
    }

    added_columns.applyLazyDefaults();
    return i;
}

template <typename AddedColumns, typename Selector>
static ColumnPtr buildAdditionalFilter(
    const Selector & selector,
    const PODArray<UInt64> & selected_rows,
    const IColumn::Offsets & row_replicate_offset,
    const AddedColumns & added_columns)
{
    ColumnPtr result_column;
    do
    {
        if (selected_rows.empty())
        {
            result_column = ColumnUInt8::create();
            break;
        }

        if (!added_columns.additional_filter_expression)
        {
            auto filter = ColumnUInt8::create();
            filter->insertMany(1, selected_rows.size());
            result_column = std::move(filter);
            break;
        }

        auto required_cols = added_columns.additional_filter_expression->getRequiredColumnsWithTypes();
        if (required_cols.empty())
        {
            Block block;
            added_columns.additional_filter_expression->execute(block);
            result_column = block.getByPosition(0).column->cloneResized(selected_rows.size());
            break;
        }

        ColumnsWithTypeAndName required_columns;
        required_columns.reserve(required_cols.size());
        auto rhs_pos_it = added_columns.additional_filter_required_rhs_pos.begin();
        auto req_cols_it = required_cols.begin();
        for (size_t pos = 0; pos < required_cols.size(); ++pos, ++req_cols_it)
        {
            if (rhs_pos_it != added_columns.additional_filter_required_rhs_pos.end() && pos == rhs_pos_it->first)
            {
                const auto & req_col = *req_cols_it;
                required_columns.emplace_back(nullptr, req_col.type, req_col.name);

                auto col = req_col.type->createColumn();
                for (const UInt64 selected_row : selected_rows)
                {
                    const auto * block = added_columns.lazy_output.stored_columns[refWordBlockNo(selected_row)];
                    const auto [src_col, row_pos] = getBlockColumnAndRow(block, refWordRowNo(selected_row), rhs_pos_it->second);
                    col->insertFrom(*src_col, row_pos);
                }
                required_columns[pos].column = std::move(col);
                ++rhs_pos_it;
            }
            else
            {
                const auto & col_name = req_cols_it->name;
                const auto * src_col = added_columns.left_block.findByName(col_name);
                if (!src_col)
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "required columns: [{}], but not found any in left table. left table: {}, required column: {}",
                        required_cols.toString(),
                        added_columns.left_block.dumpNames(),
                        col_name);

                auto new_col = src_col->column->cloneEmpty();
                for (size_t i = 0; i < row_replicate_offset.size(); ++i)
                {
                    size_t rows = row_replicate_offset[i] - row_replicate_offset[i - 1];
                    if (rows)
                    {
                        new_col->insertManyFrom(*src_col->column, selectorIndexAt(selector, i), rows);
                    }
                }
                required_columns.push_back({std::move(new_col), src_col->type, col_name});
            }
        }

        Block executed_block(std::move(required_columns));

        for (const auto & col : executed_block.getColumnsWithTypeAndName())
            if (!col.column || !col.type)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal nullptr column in input block: {}", executed_block.dumpStructure());

        added_columns.additional_filter_expression->execute(executed_block);
        result_column = executed_block.getByPosition(0).column->convertToFullColumnIfConst();
        executed_block.clear();
    } while (false);

    result_column = result_column->convertToFullIfWrapped()->convertToFullColumnIfLowCardinality();
    if (result_column->isNullable())
    {
        /// Convert Nullable(UInt8) to UInt8 ensuring that nulls are zeros
        /// Trying to avoid copying data, since we are the only owner of the column.
        ColumnPtr mask_column = assert_cast<const ColumnNullable &>(*result_column).getNullMapColumnPtr();

        MutableColumnPtr mutable_column;
        {
            ColumnPtr nested_column = assert_cast<const ColumnNullable &>(*result_column).getNestedColumnPtr();
            result_column.reset();
            mutable_column = IColumn::mutate(std::move(nested_column));
        }

        auto & column_data = assert_cast<ColumnUInt8 &>(*mutable_column).getData();
        const auto & mask_column_data = assert_cast<const ColumnUInt8 &>(*mask_column).getData();
        for (size_t i = 0; i < column_data.size(); ++i)
        {
            if (mask_column_data[i])
                column_data[i] = 0;
        }
        return mutable_column;
    }
    return result_column;
}

/** Collect a batch's candidate right rows for the additional filter to run over. No first-match
  * short-circuit: the filter needs every ref, and SEMI/ANY apply only after it.
  *
  * `selected_offsets` holds plain offsets, not `FindResult`s, whose mapped pointer is rebuilt from
  * the outcome word and would not outlive this call.
  */
template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate, typename KnownRows> // NOLINT(readability-identifier-naming)
NO_INLINE size_t collectAdditionalFilterBatch(
    const std::vector<ProbeOutcomes> & outcomes,
    size_t num_clauses,
    PODArray<UInt64> & selected_rows,
    std::vector<size_t> & selected_offsets,
    size_t count,
    size_t max_joined_rows,
    IColumn::Offset & current_added_rows,
    IColumn::Offsets & row_replicate_offset)
{
    constexpr JoinFeatures<KIND, STRICTNESS, MapsTemplate> join_features;
    constexpr bool flag_per_row = std::is_same_v<KnownRows, KnownRowsHolder<true>>;
    using MappedValue = MapsTemplate::MappedType;

    PreSelectedRows view{selected_rows};

    size_t j = 0;
    for (; j < count && current_added_rows < max_joined_rows; ++j)
    {
        KnownRows known_rows;
        for (size_t k = 0; k < num_clauses; ++k)
        {
            const UInt64 word = outcomes[k].found[j];
            if (!word)
                continue;

            const bool is_last_disjunct = (k + 1 == num_clauses);
            if constexpr (!flag_per_row)
            {
                size_t offset = 0;
                if constexpr (join_features.need_flags)
                    offset = outcomes[k].offset[j];
                selected_offsets.push_back(offset);
            }

            /// Missing rows are added after the additional filter, not here.
            auto mapped_value = mappedFromWord<MappedValue>(word);
            addFoundRowAll<MappedValue, /*add_missing=*/false, flag_per_row>(
                mapped_value, view, current_added_rows, known_rows, nullptr, is_last_disjunct);
        }
        row_replicate_offset.push_back(current_added_rows);
    }
    return j;
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate> // NOLINT(readability-identifier-naming)
template <typename KeyGetter, typename Map, typename AddedColumns>
size_t HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::joinRightColumnsWithAdditionalFilter(
    std::vector<KeyGetter> && key_getter_vector,
    const std::vector<const Map *> & mapv,
    AddedColumns & added_columns,
    JoinStuff::JoinUsedFlags & used_flags [[maybe_unused]],
    const ScatteredBlock::Selector & selector,
    bool need_filter [[maybe_unused]],
    bool flag_per_row [[maybe_unused]])
{
    constexpr JoinFeatures<KIND, STRICTNESS, MapsTemplate> join_features;
    size_t left_block_rows = selector.size();
    if (need_filter)
    {
        added_columns.filter = IColumn::Filter(left_block_rows, 0);
        added_columns.matched_rows.reserve(left_block_rows);
    }

    /// Sized for the whole selector and trimmed after the filter, which may consume fewer rows.
    if constexpr (join_features.need_replication)
        added_columns.offsets_to_replicate = IColumn::Offsets(left_block_rows);

    PODArray<UInt64> selected_rows;
    selected_rows.reserve(left_block_rows);
    std::vector<size_t> selected_offsets;
    IColumn::Offset total_added_rows = 0;

    IColumn::Offsets row_replicate_offset;
    row_replicate_offset.reserve(left_block_rows);

    size_t max_joined_rows = added_columns.max_joined_block_rows;
    if (max_joined_rows == 0)
        max_joined_rows = std::numeric_limits<size_t>::max();

    Arena pool;
    IColumn::Offset current_added_rows = 0;

    chassert(key_getter_vector.size() == added_columns.join_on_keys.size());
    chassert(key_getter_vector.size() == mapv.size() || key_getter_vector.empty());
    chassert(!mapv.empty());
    std::vector<const UInt8 *> skip_datas;
    std::vector<IColumn::Filter> skip_buffers;
    const size_t num_clauses = key_getter_vector.size();

    /// Resolves the selector variant once, so the batch loop can index it directly.
    auto run_preselect = [&]<typename Sel>(const Sel & sel) -> size_t
    {
        const size_t rows = ScatteredBlock::Selector::size(sel);
        buildClauseSkipDatas(added_columns.join_on_keys, sel, rows, skip_datas, skip_buffers);

        std::vector<ProbePrefetch<Map, KeyGetter, Sel>> prefetchers;
        prefetchers.reserve(num_clauses);
        for (size_t k = 0; k < num_clauses; ++k)
            prefetchers.push_back(makeProbePrefetcher(mapv[k], key_getter_vector[k], sel, pool, added_columns.enable_prefetch, rows));

        const size_t scratch_rows = std::min(rows, PROBE_BATCH_ROWS);
        std::vector<ProbeOutcomes> outcomes(num_clauses);
        for (auto & outcome : outcomes)
            outcome.useScratch(scratch_rows, join_features.need_flags);

        auto collect = [&]<typename KnownRows>() -> size_t
        {
            size_t i = 0;
            for (size_t begin = 0; begin < rows; begin += PROBE_BATCH_ROWS)
            {
                const size_t count = std::min(PROBE_BATCH_ROWS, rows - begin);
                for (size_t k = 0; k < num_clauses; ++k)
                {
                    const UInt8 * skip = skip_datas.empty() ? nullptr : skip_datas[k];
                    lookupBatch<join_features.need_flags>(
                        key_getter_vector[k], *mapv[k], sel, skip, pool, begin, count, prefetchers[k], outcomes[k]);
                }
                const size_t consumed = collectAdditionalFilterBatch<KIND, STRICTNESS, MapsTemplate, KnownRows>(
                    outcomes,
                    num_clauses,
                    selected_rows,
                    selected_offsets,
                    count,
                    max_joined_rows,
                    current_added_rows,
                    row_replicate_offset);
                i = begin + consumed;
                if (consumed < count)
                    break;
            }
            return i;
        };

        /// RIGHT/FULL always flag per row, so `KnownRowsHolder<false>` is dead code for them.
        if constexpr (join_features.right || join_features.full)
        {
            chassert(flag_per_row);
            return collect.template operator()<KnownRowsHolder<true>>();
        }
        else if (flag_per_row)
        {
            return collect.template operator()<KnownRowsHolder<true>>();
        }
        else
        {
            return collect.template operator()<KnownRowsHolder<false>>();
        }
    };

    const size_t processed_rows = selector.isContinuousRange() ? run_preselect(selector.getRange()) : run_preselect(selector.getIndexes());

    if (selected_rows.size() != current_added_rows)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Sizes are mismatched. selected_rows.size:{}, current_added_rows:{}, row_replicate_offset.size:{}",
            selected_rows.size(),
            current_added_rows,
            row_replicate_offset.size());

    left_block_rows = processed_rows;
    chassert(left_block_rows == row_replicate_offset.size());

    {
        auto filter_col = buildAdditionalFilter(selector, selected_rows, row_replicate_offset, added_columns);

        const PaddedPODArray<UInt8> & filter_flags = assert_cast<const ColumnUInt8 &>(*filter_col).getData();

        if (added_columns.match_stats) [[unlikely]]
            for (size_t row = 0; row < selected_rows.size(); ++row)
                if (filter_flags[row])
                    added_columns.match_stats->markRightMatched(selected_rows[row]);

        [[maybe_unused]] UInt64 matched_left = 0;
        size_t prev_replicated_row = 0;
        auto * selected_right_row_it = selected_rows.begin();
        size_t find_result_index = 0;
        for (size_t i = 0, n = row_replicate_offset.size(); i < n; ++i)
        {
            bool any_matched = false;
            /// right/full join or multiple disjuncts, we need to mark used flags for each row.
            if (flag_per_row)
            {
                for (size_t replicated_row = prev_replicated_row; replicated_row < row_replicate_offset[i]; ++replicated_row)
                {
                    if (filter_flags[replicated_row])
                    {
                        const UInt64 selected_ref = *selected_right_row_it;
                        if constexpr (join_features.is_semi_join || join_features.is_any_join)
                        {
                            /// For LEFT/INNER SEMI/ANY JOIN, we need to add only first appeared row from left,
                            if constexpr (join_features.left || join_features.inner)
                            {
                                if (!any_matched)
                                {
                                    // For inner join, we need mark each right row'flag, because we only use each right row once.
                                    auto used_once = used_flags.template setUsedOnce<join_features.need_flags, true>(
                                        refWordBlockNo(selected_ref), refWordRowNo(selected_ref), 0);
                                    if (used_once)
                                    {
                                        any_matched = true;
                                        total_added_rows += 1;
                                        added_columns.appendFromBlock(selected_ref, join_features.add_missing);
                                    }
                                }
                            }
                            else
                            {
                                auto used_once = used_flags.template setUsedOnce<join_features.need_flags, true>(
                                    refWordBlockNo(selected_ref), refWordRowNo(selected_ref), 0);
                                if (used_once)
                                {
                                    any_matched = true;
                                    total_added_rows += 1;
                                    added_columns.appendFromBlock(selected_ref, join_features.add_missing);
                                }
                            }
                        }
                        else if constexpr (join_features.is_anti_join)
                        {
                            any_matched = true;
                            if constexpr (join_features.right && join_features.need_flags)
                                used_flags.template setUsed<true, true>(refWordBlockNo(selected_ref), refWordRowNo(selected_ref), 0);
                        }
                        else
                        {
                            any_matched = true;
                            total_added_rows += 1;
                            added_columns.appendFromBlock(selected_ref, join_features.add_missing);
                            used_flags.template setUsed<join_features.need_flags, true>(refWordBlockNo(selected_ref), refWordRowNo(selected_ref), 0);
                        }
                    }

                    ++selected_right_row_it;
                }
            }
            else
            {
                for (size_t replicated_row = prev_replicated_row; replicated_row < row_replicate_offset[i]; ++replicated_row)
                {
                    if constexpr (join_features.is_anti_join)
                    {
                        any_matched |= filter_flags[replicated_row];
                    }
                    else if constexpr (join_features.need_replication)
                    {
                        if (filter_flags[replicated_row])
                        {
                            any_matched = true;
                            added_columns.appendFromBlock(*selected_right_row_it, join_features.add_missing);
                            total_added_rows += 1;
                        }
                        ++selected_right_row_it;
                    }
                    else
                    {
                        if (filter_flags[replicated_row])
                        {
                            any_matched = true;
                            added_columns.appendFromBlock(*selected_right_row_it, join_features.add_missing);
                            total_added_rows += 1;
                            selected_right_row_it = selected_right_row_it + row_replicate_offset[i] - replicated_row;
                            break;
                        }
                        ++selected_right_row_it;
                    }
                }
            }


            if constexpr (join_features.is_anti_join)
            {
                if (!any_matched)
                {
                    if constexpr (join_features.left)
                        if (need_filter)
                            setUsed<true>(added_columns.filter, i, added_columns.matched_rows);
                    addNotFoundRow<join_features.add_missing, join_features.need_replication>(added_columns, total_added_rows);
                }
            }
            else
            {
                if (!any_matched)
                {
                    addNotFoundRow<join_features.add_missing, join_features.need_replication>(added_columns, total_added_rows);
                }
                else
                {
                    if (!flag_per_row)
                    {
                        /// The offset is all that survived pre-select, and all `setUsed` reads here.
                        using Mapped = const MapsTemplate::MappedType;
                        using FindResult = ColumnsHashing::columns_hashing_impl::FindResultImpl<Mapped, join_features.need_flags>;
                        FindResult find_result(nullptr, true, selected_offsets[find_result_index]);
                        used_flags.template setUsed<join_features.need_flags, false>(find_result);
                    }
                    if (need_filter)
                        setUsed<true>(added_columns.filter, i, added_columns.matched_rows);
                    if constexpr (join_features.add_missing)
                        added_columns.applyLazyDefaults();
                }
            }
            if constexpr (leftMatchedSource(KIND, STRICTNESS) == LeftMatchedSource::DefaultRowMarkers)
                matched_left += any_matched;

            find_result_index += (prev_replicated_row != row_replicate_offset[i]);

            if constexpr (join_features.need_replication)
            {
                added_columns.offsets_to_replicate[i] = total_added_rows;
            }
            prev_replicated_row = row_replicate_offset[i];
        }

        if constexpr (leftMatchedSource(KIND, STRICTNESS) == LeftMatchedSource::DefaultRowMarkers)
            added_columns.matched_left_rows = matched_left;
    }

    if constexpr (join_features.need_replication)
    {
        added_columns.offsets_to_replicate.resize(left_block_rows);
        added_columns.filter.resize(left_block_rows);
    }
    else if (need_filter)
    {
        /// An early stop at `max_joined_block_rows` leaves a shorter left block than filter size.
        added_columns.filter.resize(left_block_rows);
    }
    added_columns.applyLazyDefaults();
    return left_block_rows;
}

}
