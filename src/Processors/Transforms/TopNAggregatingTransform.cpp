#include <Processors/Transforms/TopNAggregatingTransform.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Processors/Port.h>

#include <typeinfo>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Compute aligned state layout for a set of aggregate functions.
/// Returns {offsets[], total_size, max_alignment}.
struct StateLayout
{
    std::vector<size_t> offsets;
    size_t total_size = 0;
    size_t alignment = 1;
};

StateLayout computeStateLayout(const AggregateDescriptions & aggregates)
{
    StateLayout layout;
    layout.offsets.resize(aggregates.size());
    for (size_t i = 0; i < aggregates.size(); ++i)
    {
        size_t align = aggregates[i].function->alignOfData();
        layout.total_size = (layout.total_size + align - 1) / align * align;
        layout.offsets[i] = layout.total_size;
        layout.total_size += aggregates[i].function->sizeOfData();
        layout.alignment = std::max(layout.alignment, align);
    }
    return layout;
}

/// Find the aggregate index whose column_name matches the ORDER BY column.
size_t findOrderByAggIndex(const AggregateDescriptions & aggregates, const SortDescription & sort_desc)
{
    const auto & sort_col_name = sort_desc.front().column_name;
    for (size_t i = 0; i < aggregates.size(); ++i)
        if (aggregates[i].column_name == sort_col_name)
            return i;
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "TopNAggregatingTransform: ORDER BY column '{}' not found among aggregate outputs",
        sort_col_name);
}

/// Partial-sort and permute output columns, returning the first output_limit rows.
Chunk sortAndLimitColumns(
    MutableColumns & output,
    size_t order_col_idx,
    size_t output_limit,
    int sort_direction,
    const SortColumnDescription & sort_col_desc)
{
    auto direction = (sort_direction < 0)
        ? IColumn::PermutationSortDirection::Descending
        : IColumn::PermutationSortDirection::Ascending;

    IColumn::Permutation permutation;
    if (sort_col_desc.collator)
    {
        output[order_col_idx]->getPermutationWithCollation(
            *sort_col_desc.collator, direction, IColumn::PermutationSortStability::Unstable,
            output_limit, sort_col_desc.nulls_direction, permutation);
    }
    else
    {
        output[order_col_idx]->getPermutation(
            direction, IColumn::PermutationSortStability::Unstable,
            output_limit, sort_col_desc.nulls_direction, permutation);
    }

    Columns final_columns;
    final_columns.reserve(output.size());
    for (auto & col : output)
        final_columns.push_back(col->permute(permutation, output_limit));

    return Chunk(std::move(final_columns), output_limit);
}

}


/// ---- buildIntermediateHeader ----

Block buildIntermediateHeader(const Block & input_header, const Names & key_names, const AggregateDescriptions & aggregates)
{
    Block res;
    for (const auto & key : key_names)
        res.insert(input_header.getByName(key).cloneEmpty());

    for (const auto & aggregate : aggregates)
    {
        DataTypes arg_types;
        arg_types.reserve(aggregate.argument_names.size());
        for (const auto & arg_name : aggregate.argument_names)
            arg_types.push_back(input_header.getByName(arg_name).type);

        auto agg_type = std::make_shared<DataTypeAggregateFunction>(aggregate.function, arg_types, aggregate.parameters);
        res.insert({agg_type, aggregate.column_name});
    }
    return res;
}


/// ---- TopNAggregatingTransformBase ----

TopNAggregatingTransformBase::TopNAggregatingTransformBase(
    const Block & input_header_,
    const Block & output_header_,
    const Names & key_names_,
    const AggregateDescriptions & aggregates_,
    const SortDescription & sort_description_,
    size_t limit_)
    : IAccumulatingTransform(
        std::make_shared<const Block>(input_header_),
        std::make_shared<const Block>(output_header_))
    , key_names(key_names_)
    , aggregates(aggregates_)
    , sort_description(sort_description_)
    , limit(limit_)
    , stored_input_header(input_header_)
    , arena(std::make_shared<Arena>())
{
    initColumnIndices(input_header_);
    sort_direction = sort_description.front().direction;

    auto layout = computeStateLayout(aggregates);
    agg_state_offsets = std::move(layout.offsets);
    total_state_size = layout.total_size;
    state_align = layout.alignment;
}

void TopNAggregatingTransformBase::initColumnIndices(const Block & input_header_)
{
    key_column_indices.resize(key_names.size());
    for (size_t i = 0; i < key_names.size(); ++i)
        key_column_indices[i] = input_header_.getPositionByName(key_names[i]);

    order_by_agg_index = findOrderByAggIndex(aggregates, sort_description);

    agg_arg_columns.resize(aggregates.size());
    for (size_t i = 0; i < aggregates.size(); ++i)
    {
        const auto & agg = aggregates[i];
        agg_arg_columns[i].resize(agg.argument_names.size());
        for (size_t j = 0; j < agg.argument_names.size(); ++j)
            agg_arg_columns[i][j] = input_header_.getPositionByName(agg.argument_names[j]);
    }
}

void TopNAggregatingTransformBase::prepareKeyColumnPtrs(const Columns & columns)
{
    key_column_holders.clear();
    key_column_ptrs.resize(key_column_indices.size());
    for (size_t k = 0; k < key_column_indices.size(); ++k)
    {
        /// Strip Const/Sparse wrappers but preserve LowCardinality: the output
        /// header is built from the input header (see `buildOutputHeader` in
        /// `TopNAggregatingStep.cpp`) which keeps the LowCardinality type, so
        /// `accumulated_keys` -- cloned from these runtime columns -- must
        /// also stay LowCardinality, otherwise downstream serialization
        /// (e.g. `SerializationLowCardinality::serializeBinaryBulkWithMultipleStreams`)
        /// sees a concrete `ColumnString`/`ColumnUInt*` where a
        /// `ColumnLowCardinality` is expected.
        auto converted = columns[key_column_indices[k]]
            ->convertToFullColumnIfConst()
            ->convertToFullColumnIfSparse();
        key_column_ptrs[k] = converted.get();
        if (converted != columns[key_column_indices[k]])
            key_column_holders.push_back(std::move(converted));
    }
}

void TopNAggregatingTransformBase::prepareArgColumnPtrs(const Columns & columns)
{
    agg_arg_column_holders.clear();
    agg_arg_column_ptrs.resize(aggregates.size());
    for (size_t i = 0; i < aggregates.size(); ++i)
    {
        const auto & arg_indices = agg_arg_columns[i];
        agg_arg_column_ptrs[i].resize(arg_indices.size());
        for (size_t j = 0; j < arg_indices.size(); ++j)
        {
            auto converted = columns[arg_indices[j]]->convertToFullIfNeeded();
            agg_arg_column_ptrs[i][j] = converted.get();
            if (converted != columns[arg_indices[j]])
                agg_arg_column_holders.push_back(std::move(converted));
        }
    }
}

SerializedKeyHolder TopNAggregatingTransformBase::serializeGroupKey(const Columns & columns, size_t row) const
{
    const char * begin = nullptr;
    std::string_view key;
    for (size_t idx : key_column_indices)
        key = columns[idx]->serializeValueIntoArena(row, *arena, begin, nullptr);
    return SerializedKeyHolder{std::string_view(begin, key.data() + key.size() - begin), *arena};
}

void TopNAggregatingTransformBase::createAggregateStates(AggregateDataPtr place) const
{
    for (size_t i = 0; i < aggregates.size(); ++i)
    {
        try
        {
            aggregates[i].function->create(place + agg_state_offsets[i]);
        }
        catch (...)
        {
            for (size_t rollback = 0; rollback < i; ++rollback)
                aggregates[rollback].function->destroy(place + agg_state_offsets[rollback]);
            throw;
        }
    }
}

void TopNAggregatingTransformBase::destroyAggregateStates(AggregateDataPtr place) const
{
    for (size_t i = 0; i < aggregates.size(); ++i)
        aggregates[i].function->destroy(place + agg_state_offsets[i]);
}

void TopNAggregatingTransformBase::addRowToAggregateStates(AggregateDataPtr place, size_t row)
{
    for (size_t i = 0; i < aggregates.size(); ++i)
        aggregates[i].function->add(
            place + agg_state_offsets[i], agg_arg_column_ptrs[i].data(), row, arena.get());
}

void TopNAggregatingTransformBase::insertResultsFromStates(
    AggregateDataPtr place, MutableColumns & output_columns)
{
    size_t num_keys = key_names.size();
    for (size_t i = 0; i < aggregates.size(); ++i)
        aggregates[i].function->insertResultInto(
            place + agg_state_offsets[i], *output_columns[num_keys + i], arena.get());
}


/// ---- TopNSortedAggregatingTransform (Mode 1) ----

TopNSortedAggregatingTransform::TopNSortedAggregatingTransform(
    const Block & input_header_,
    const Block & output_header_,
    const Names & key_names_,
    const AggregateDescriptions & aggregates_,
    const SortDescription & sort_description_,
    size_t limit_)
    : TopNAggregatingTransformBase(
        input_header_, output_header_, key_names_, aggregates_, sort_description_, limit_)
{
}

void TopNSortedAggregatingTransform::consume(Chunk chunk)
{
    if (num_groups >= limit)
    {
        finishConsume();
        return;
    }

    const auto & columns = chunk.getColumns();
    size_t num_rows = chunk.getNumRows();

    prepareKeyColumnPtrs(columns);
    prepareArgColumnPtrs(columns);

    if (result_columns.empty())
    {
        result_columns = getOutputPort().getHeader().cloneEmptyColumns();
        /// Replace key columns with `cloneEmpty` of the unwrapped runtime
        /// columns so `insertFrom` sees matching concrete column types and
        /// the output column type still matches the header (LowCardinality
        /// is preserved); see comment in `TopNDirectAggregatingTransform::consume`.
        for (size_t k = 0; k < key_names.size(); ++k)
            result_columns[k] = key_column_ptrs[k]->cloneEmpty();
    }

    for (size_t row = 0; row < num_rows; ++row)
    {
        auto key_holder = serializeGroupKey(columns, row);

        decltype(group_indices)::LookupResult it;
        bool inserted = false;
        group_indices.emplace(key_holder, it, inserted);

        if (!inserted)
            continue;

        it->getMapped() = num_groups;

        for (size_t k = 0; k < key_names.size(); ++k)
            result_columns[k]->insertFrom(*key_column_ptrs[k], row);

        auto * buf = arena->alignedAlloc(total_state_size, state_align);
        AggregateDataPtr state = reinterpret_cast<AggregateDataPtr>(buf);
        createAggregateStates(state);
        try
        {
            addRowToAggregateStates(state, row);
            insertResultsFromStates(state, result_columns);
        }
        catch (...)
        {
            destroyAggregateStates(state);
            throw;
        }
        destroyAggregateStates(state);

        ++num_groups;

        if (num_groups >= limit)
        {
            finishConsume();
            return;
        }
    }
}

Chunk TopNSortedAggregatingTransform::generate()
{
    if (generated)
        return {};
    generated = true;

    if (num_groups == 0)
        return {};

    Columns final_columns;
    final_columns.reserve(result_columns.size());
    for (auto & col : result_columns)
        final_columns.push_back(std::move(col));

    return Chunk(std::move(final_columns), num_groups);
}


/// ---- TopNDirectAggregatingTransform (Mode 2) ----

TopNDirectAggregatingTransform::TopNDirectAggregatingTransform(
    const Block & input_header_,
    const Block & output_header_,
    const Names & key_names_,
    const AggregateDescriptions & aggregates_,
    const SortDescription & sort_description_,
    size_t limit_,
    bool partial_,
    bool enable_threshold_pruning_,
    TopKThresholdTrackerPtr threshold_tracker_)
    : TopNAggregatingTransformBase(
        input_header_, output_header_, key_names_, aggregates_, sort_description_, limit_)
    , partial(partial_)
    , enable_threshold_pruning(enable_threshold_pruning_)
    , threshold_tracker(std::move(threshold_tracker_))
{
    if (enable_threshold_pruning)
    {
        const auto & order_arg_name = aggregates[order_by_agg_index].argument_names.back();
        order_agg_arg_col_idx = input_header_.getPositionByName(order_arg_name);
        boundary_column = aggregates[order_by_agg_index].function->getResultType()->createColumn();
    }
}

TopNDirectAggregatingTransform::~TopNDirectAggregatingTransform()
{
    for (auto & gs : group_states)
        if (gs.state)
            destroyAggregateStates(gs.state);
}

void TopNDirectAggregatingTransform::consume(Chunk chunk)
{
    const auto & columns = chunk.getColumns();
    size_t num_rows = chunk.getNumRows();
    if (num_rows == 0)
        return;

    ++chunks_seen;
    ++chunks_since_last_threshold_refresh;

    prepareKeyColumnPtrs(columns);
    prepareArgColumnPtrs(columns);

    /// Clone `accumulated_keys` from the unwrapped runtime columns rather
    /// than the input/output header so the column type matches whatever
    /// `prepareKeyColumnPtrs` produces: it strips `ColumnConst`/`ColumnSparse`
    /// (which appear in the header for constant GROUP BY expressions) but
    /// keeps `ColumnLowCardinality` so the result still matches the
    /// LowCardinality output header type expected by downstream operators.
    if (accumulated_keys.empty())
    {
        for (size_t k = 0; k < key_names.size(); ++k)
            accumulated_keys.push_back(key_column_ptrs[k]->cloneEmpty());
    }

    ColumnPtr order_arg_col_holder;
    const IColumn * order_arg_col = nullptr;
    if (enable_threshold_pruning)
    {
        order_arg_col_holder = columns[order_agg_arg_col_idx]->convertToFullIfNeeded();
        order_arg_col = order_arg_col_holder.get();
    }
    ColumnPtr threshold_keep_mask;
    const PaddedPODArray<UInt8> * threshold_keep_data = nullptr;

    if (order_arg_col)
    {
        threshold_keep_mask = buildThresholdKeepMask(order_arg_col_holder, num_rows);
        if (threshold_keep_mask)
        {
            auto full_mask = threshold_keep_mask->convertToFullColumnIfConst();
            if (const auto * mask_col = checkAndGetColumn<ColumnUInt8>(full_mask.get()))
            {
                threshold_keep_mask = std::move(full_mask);
                threshold_keep_data = &mask_col->getData();
            }
        }
    }

    for (size_t row = 0; row < num_rows; ++row)
    {
        if (threshold_keep_data && !(*threshold_keep_data)[row])
            continue;
        if (!threshold_keep_data && order_arg_col && isBelowThreshold(*order_arg_col, row))
            continue;

        auto key_holder = serializeGroupKey(columns, row);

        decltype(group_indices)::LookupResult it;
        bool inserted = false;
        group_indices.emplace(key_holder, it, inserted);

        if (inserted)
        {
            it->getMapped() = num_groups;

            for (size_t k = 0; k < key_names.size(); ++k)
                accumulated_keys[k]->insertFrom(*key_column_ptrs[k], row);

            auto * buf = arena->alignedAlloc(total_state_size, state_align);
            AggregateDataPtr state = reinterpret_cast<AggregateDataPtr>(buf);
            createAggregateStates(state);
            group_states.push_back({state});
            ++num_groups;

            addRowToAggregateStates(state, row);
        }
        else
        {
            addRowToAggregateStates(group_states[it->getMapped()].state, row);
        }
    }

    maybeRefreshThreshold();
}

Chunk TopNDirectAggregatingTransform::generate()
{
    if (generated)
        return {};
    generated = true;

    if (partial)
        return generatePartial();
    else
        return generateFull();
}

Chunk TopNDirectAggregatingTransform::generateFull()
{
    if (num_groups == 0)
        return {};

    const auto & out_header = getOutputPort().getHeader();
    size_t num_keys = key_names.size();
    MutableColumns output = out_header.cloneEmptyColumns();

    for (size_t k = 0; k < num_keys; ++k)
        output[k] = std::move(accumulated_keys[k]);

    for (size_t g = 0; g < num_groups; ++g)
        insertResultsFromStates(group_states[g].state, output);

    size_t output_limit = std::min(limit, num_groups);
    return sortAndLimitColumns(output, num_keys + order_by_agg_index, output_limit,
                               sort_direction, sort_description.front());
}

Chunk TopNDirectAggregatingTransform::generatePartial()
{
    if (num_groups == 0)
        return {};

    size_t output_limit = std::min(limit, num_groups);

    /// Materialize ORDER BY aggregate values to determine local top K.
    auto order_result_type = aggregates[order_by_agg_index].function->getResultType();
    auto order_col = order_result_type->createColumn();
    order_col->reserve(num_groups);
    for (size_t g = 0; g < num_groups; ++g)
        aggregates[order_by_agg_index].function->insertResultInto(
            group_states[g].state + agg_state_offsets[order_by_agg_index],
            *order_col, arena.get());

    auto perm = getSortPermutation(*order_col, output_limit);

    /// Feed back the local K-th aggregate to the shared threshold tracker.
    /// This is safe: a partial's K-th group aggregate is always <= the global K-th
    /// (because each group's local max <= global max, so the partial's top-K ranking
    /// cannot exceed the global ranking).
    if (enable_threshold_pruning && threshold_tracker && output_limit == limit)
    {
        Field val;
        order_col->get(perm[output_limit - 1], val);
        threshold_tracker->testAndSet(val);
    }

    const auto & out_header = getOutputPort().getHeader();
    size_t num_keys = key_names.size();
    MutableColumns output = out_header.cloneEmptyColumns();

    for (size_t k = 0; k < num_keys; ++k)
    {
        output[k]->reserve(output_limit);
        for (size_t j = 0; j < output_limit; ++j)
            output[k]->insertFrom(*accumulated_keys[k], perm[j]);
    }

    for (size_t i = 0; i < aggregates.size(); ++i)
    {
        auto & col = assert_cast<ColumnAggregateFunction &>(*output[num_keys + i]);
        col.reserve(output_limit);
        for (size_t j = 0; j < output_limit; ++j)
            col.insertFrom(group_states[perm[j]].state + agg_state_offsets[i]);
    }

    /// ColumnAggregateFunction::insertFrom merges (copies) each state into a
    /// new column-owned state. Destroy ALL original states -- including the
    /// top-K ones whose data has already been merged into the output columns.
    for (size_t g = 0; g < num_groups; ++g)
        destroyAggregateStates(group_states[g].state);
    group_states.clear();

    return Chunk(out_header.cloneWithColumns(std::move(output)).getColumns(), output_limit);
}


/// ---- TopNDirectAggregatingTransform: sort helper ----

IColumn::Permutation TopNDirectAggregatingTransform::getSortPermutation(const IColumn & order_col, size_t output_limit) const
{
    auto direction = (sort_direction < 0)
        ? IColumn::PermutationSortDirection::Descending
        : IColumn::PermutationSortDirection::Ascending;

    IColumn::Permutation perm;
    order_col.getPermutation(
        direction, IColumn::PermutationSortStability::Unstable,
        output_limit, sort_description.front().nulls_direction, perm);
    return perm;
}


/// ---- TopNDirectAggregatingTransform: threshold pruning ----

void TopNDirectAggregatingTransform::refreshThresholdFromStates()
{
    size_t n = group_states.size();
    if (limit == 0 || n < limit)
        return;

    auto result_type = aggregates[order_by_agg_index].function->getResultType();
    auto agg_col = result_type->createColumn();
    size_t off = agg_state_offsets[order_by_agg_index];
    for (size_t g = 0; g < n; ++g)
        aggregates[order_by_agg_index].function->insertResultInto(
            group_states[g].state + off, *agg_col, arena.get());

    auto perm = getSortPermutation(*agg_col, limit);
    size_t boundary_pos = std::min(limit, n) - 1;

    boundary_column->popBack(boundary_column->size());
    boundary_column->insertFrom(*agg_col, perm[boundary_pos]);
    threshold_active = true;

    if (threshold_tracker)
    {
        Field val;
        boundary_column->get(0, val);
        threshold_tracker->testAndSet(val);
    }
}

void TopNDirectAggregatingTransform::maybeRefreshThreshold()
{
    if (!enable_threshold_pruning)
        return;

    const size_t groups = group_states.size();
    if (groups < limit)
        return;

    /// First usable threshold should be published immediately once we have >= K groups,
    /// even if the group count already exceeds the refresh cap.
    if (!threshold_active)
    {
        refreshThresholdFromStates();
        chunks_since_last_threshold_refresh = 0;
        return;
    }

    if (groups / 10000 > limit)
        return;

    /// Start frequent, then gradually reduce refresh frequency as more chunks arrive.
    size_t refresh_period_chunks = 1;
    if (chunks_seen >= 32)
        refresh_period_chunks = 2;
    if (chunks_seen >= 128)
        refresh_period_chunks = 4;
    if (chunks_seen >= 512)
        refresh_period_chunks = 8;
    if (chunks_seen >= 2048)
        refresh_period_chunks = 16;

    /// When groups >> K, boundary updates are typically less sensitive per chunk.
    const size_t group_to_limit_ratio = groups / std::max<size_t>(1, limit);
    if (group_to_limit_ratio >= 100)
        refresh_period_chunks = std::max<size_t>(refresh_period_chunks, 16);
    if (group_to_limit_ratio >= 1000)
        refresh_period_chunks = std::max<size_t>(refresh_period_chunks, 32);

    if (chunks_since_last_threshold_refresh < refresh_period_chunks)
        return;

    refreshThresholdFromStates();
    chunks_since_last_threshold_refresh = 0;
}

bool TopNDirectAggregatingTransform::isBelowThreshold(const IColumn & col, size_t row) const
{
    if (!threshold_active)
        return false;

    const auto & boundary_ref = *boundary_column;
    chassert(typeid(col) == typeid(boundary_ref));
    int cmp = col.compareAt(row, 0, boundary_ref, sort_description.front().nulls_direction);
    return (sort_direction < 0) ? (cmp < 0) : (cmp > 0);
}

ColumnPtr TopNDirectAggregatingTransform::buildThresholdKeepMask(const ColumnPtr & column, size_t rows)
{
    if (!threshold_active)
        return {};
    const auto & col_ref = *column;
    const auto & boundary_ref = *boundary_column;
    chassert(typeid(col_ref) == typeid(boundary_ref));
    PaddedPODArray<Int8> compare_results;
    column->compareColumn(*boundary_column, 0, nullptr, compare_results, sort_direction, sort_description.front().nulls_direction);
    if (compare_results.size() != rows)
        return {};

    auto mask = ColumnUInt8::create(rows);
    auto & mask_data = mask->getData();
    for (size_t i = 0; i < rows; ++i)
        mask_data[i] = (compare_results[i] <= 0);
    return mask;
}


/// ---- TopNAggregatingMergeTransform ----

TopNAggregatingMergeTransform::TopNAggregatingMergeTransform(
    const Block & intermediate_header_,
    const Block & output_header_,
    const Names & key_names_,
    const AggregateDescriptions & aggregates_,
    const SortDescription & sort_description_,
    size_t limit_)
    : IAccumulatingTransform(
        std::make_shared<const Block>(intermediate_header_),
        std::make_shared<const Block>(output_header_))
    , key_names(key_names_)
    , aggregates(aggregates_)
    , sort_description(sort_description_)
    , limit(limit_)
    , stored_header(intermediate_header_)
    , arena(std::make_shared<Arena>())
{
    order_by_agg_index = findOrderByAggIndex(aggregates, sort_description);
    sort_direction = sort_description.front().direction;

    key_column_indices.resize(key_names.size());
    for (size_t i = 0; i < key_names.size(); ++i)
        key_column_indices[i] = intermediate_header_.getPositionByName(key_names[i]);

    agg_column_indices.resize(aggregates.size());
    for (size_t i = 0; i < aggregates.size(); ++i)
        agg_column_indices[i] = intermediate_header_.getPositionByName(aggregates[i].column_name);

    auto layout = computeStateLayout(aggregates);
    agg_state_offsets = std::move(layout.offsets);
    total_state_size = layout.total_size;
    state_align = layout.alignment;
}

TopNAggregatingMergeTransform::~TopNAggregatingMergeTransform()
{
    for (auto & gs : group_states)
        if (gs.state)
            for (size_t i = 0; i < aggregates.size(); ++i)
                aggregates[i].function->destroy(gs.state + agg_state_offsets[i]);
}

void TopNAggregatingMergeTransform::consume(Chunk chunk)
{
    if (chunk.getNumRows() == 0)
        return;

    const auto & columns = chunk.getColumns();
    size_t num_rows = chunk.getNumRows();

    Columns key_cols_converted;
    ColumnRawPtrs key_col_ptrs(key_column_indices.size());
    for (size_t k = 0; k < key_column_indices.size(); ++k)
    {
        /// Strip Const/Sparse but keep LowCardinality so the output column
        /// type matches the intermediate header; see comment in
        /// `TopNAggregatingTransformBase::prepareKeyColumnPtrs`.
        auto converted = columns[key_column_indices[k]]
            ->convertToFullColumnIfConst()
            ->convertToFullColumnIfSparse();
        key_col_ptrs[k] = converted.get();
        if (converted != columns[key_column_indices[k]])
            key_cols_converted.push_back(std::move(converted));
    }

    if (accumulated_keys.empty())
    {
        for (size_t k = 0; k < key_names.size(); ++k)
            accumulated_keys.push_back(key_col_ptrs[k]->cloneEmpty());
    }

    for (size_t row = 0; row < num_rows; ++row)
    {
        const char * begin = nullptr;
        std::string_view key;
        for (size_t k = 0; k < key_column_indices.size(); ++k)
            key = key_col_ptrs[k]->serializeValueIntoArena(row, *arena, begin, nullptr);
        SerializedKeyHolder key_holder{std::string_view(begin, key.data() + key.size() - begin), *arena};

        decltype(group_indices)::LookupResult it;
        bool inserted = false;
        group_indices.emplace(key_holder, it, inserted);

        size_t group_idx;
        if (inserted)
        {
            group_idx = num_groups++;
            it->getMapped() = group_idx;

            for (size_t k = 0; k < key_names.size(); ++k)
                accumulated_keys[k]->insertFrom(*key_col_ptrs[k], row);

            auto * buf = arena->alignedAlloc(total_state_size, state_align);
            AggregateDataPtr state = reinterpret_cast<AggregateDataPtr>(buf);
            for (size_t i = 0; i < aggregates.size(); ++i)
                aggregates[i].function->create(state + agg_state_offsets[i]);
            group_states.push_back({state});
        }
        else
        {
            group_idx = it->getMapped();
        }

        AggregateDataPtr state = group_states[group_idx].state;
        for (size_t i = 0; i < aggregates.size(); ++i)
        {
            const auto & agg_col = assert_cast<const ColumnAggregateFunction &>(*columns[agg_column_indices[i]]);
            aggregates[i].function->merge(
                state + agg_state_offsets[i],
                agg_col.getData()[row],
                arena.get());
        }
    }
}

Chunk TopNAggregatingMergeTransform::generate()
{
    if (generated)
        return {};
    generated = true;

    if (num_groups == 0)
        return {};

    const auto & out_header = getOutputPort().getHeader();
    size_t num_keys = key_names.size();
    MutableColumns output = out_header.cloneEmptyColumns();

    for (size_t k = 0; k < num_keys; ++k)
        output[k] = std::move(accumulated_keys[k]);

    for (size_t g = 0; g < num_groups; ++g)
        for (size_t i = 0; i < aggregates.size(); ++i)
            aggregates[i].function->insertResultInto(
                group_states[g].state + agg_state_offsets[i],
                *output[num_keys + i],
                arena.get());

    size_t output_limit = std::min(limit, num_groups);
    return sortAndLimitColumns(output, num_keys + order_by_agg_index, output_limit,
                               sort_direction, sort_description.front());
}

}
