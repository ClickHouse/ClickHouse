#include <Processors/Transforms/TopNAggregatingTransform.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Processors/Port.h>

namespace DB
{

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

TopNAggregatingTransform::TopNAggregatingTransform(
    const Block & input_header_,
    const Block & output_header_,
    const Names & key_names_,
    const AggregateDescriptions & aggregates_,
    const SortDescription & sort_description_,
    size_t limit_,
    bool sorted_input_,
    bool partial_,
    bool enable_threshold_pruning_,
    TopKThresholdTrackerPtr threshold_tracker_)
    : IAccumulatingTransform(
        std::make_shared<const Block>(input_header_),
        std::make_shared<const Block>(output_header_))
    , key_names(key_names_)
    , aggregates(aggregates_)
    , sort_description(sort_description_)
    , limit(limit_)
    , sorted_input(sorted_input_)
    , partial(partial_)
    , enable_threshold_pruning(enable_threshold_pruning_)
    , threshold_tracker(std::move(threshold_tracker_))
    , stored_input_header(input_header_)
    , arena(std::make_shared<Arena>())
{
    initColumnIndices(input_header_);

    sort_direction = sort_description.front().direction;

    total_state_size = 0;
    state_align = 1;
    agg_state_offsets.resize(aggregates.size());
    for (size_t i = 0; i < aggregates.size(); ++i)
    {
        total_state_size = (total_state_size + aggregates[i].function->alignOfData() - 1)
            / aggregates[i].function->alignOfData() * aggregates[i].function->alignOfData();
        agg_state_offsets[i] = total_state_size;
        total_state_size += aggregates[i].function->sizeOfData();
        state_align = std::max(state_align, aggregates[i].function->alignOfData());
    }

    if (enable_threshold_pruning && !sorted_input)
    {
        const auto & order_arg_name = aggregates[order_by_agg_index].argument_names.back();
        order_agg_arg_col_idx = input_header_.getPositionByName(order_arg_name);
        auto result_type = aggregates[order_by_agg_index].function->getResultType();
        boundary_column = result_type->createColumn();
    }
}

TopNAggregatingTransform::~TopNAggregatingTransform()
{
    for (auto & gs : group_states)
    {
        if (gs.state)
            destroyAggregateStates(gs.state);
    }
}

void TopNAggregatingTransform::initColumnIndices(const Block & input_header_)
{
    key_column_indices.resize(key_names.size());
    for (size_t i = 0; i < key_names.size(); ++i)
        key_column_indices[i] = input_header_.getPositionByName(key_names[i]);

    const auto & sort_col_name = sort_description.front().column_name;
    for (size_t i = 0; i < aggregates.size(); ++i)
    {
        if (aggregates[i].column_name == sort_col_name)
        {
            order_by_agg_index = i;
            break;
        }
    }

    agg_arg_columns.resize(aggregates.size());
    for (size_t i = 0; i < aggregates.size(); ++i)
    {
        const auto & agg = aggregates[i];
        agg_arg_columns[i].resize(agg.argument_names.size());
        for (size_t j = 0; j < agg.argument_names.size(); ++j)
            agg_arg_columns[i][j] = input_header_.getPositionByName(agg.argument_names[j]);
    }
}

SerializedKeyHolder TopNAggregatingTransform::serializeGroupKey(const Columns & columns, size_t row) const
{
    const char * begin = nullptr;
    std::string_view key;
    for (size_t idx : key_column_indices)
        key = columns[idx]->serializeValueIntoArena(row, *arena, begin, nullptr);
    return SerializedKeyHolder{std::string_view(begin, key.data() + key.size() - begin), *arena};
}

void TopNAggregatingTransform::createAggregateStates(AggregateDataPtr place) const
{
    for (size_t i = 0; i < aggregates.size(); ++i)
        aggregates[i].function->create(place + agg_state_offsets[i]);
}

void TopNAggregatingTransform::destroyAggregateStates(AggregateDataPtr place) const
{
    for (size_t i = 0; i < aggregates.size(); ++i)
        aggregates[i].function->destroy(place + agg_state_offsets[i]);
}

void TopNAggregatingTransform::addRowToAggregateStates(AggregateDataPtr place, size_t row)
{
    for (size_t i = 0; i < aggregates.size(); ++i)
        aggregates[i].function->add(
            place + agg_state_offsets[i], agg_arg_column_ptrs[i].data(), row, arena.get());
}

void TopNAggregatingTransform::insertResultsFromStates(
    AggregateDataPtr place, MutableColumns & output_columns)
{
    size_t num_keys = key_names.size();
    for (size_t i = 0; i < aggregates.size(); ++i)
        aggregates[i].function->insertResultInto(
            place + agg_state_offsets[i], *output_columns[num_keys + i], arena.get());
}

void TopNAggregatingTransform::refreshThresholdFromStates()
{
    size_t n = group_states.size();
    if (n < limit)
        return;

    auto result_type = aggregates[order_by_agg_index].function->getResultType();
    auto agg_col = result_type->createColumn();
    size_t off = agg_state_offsets[order_by_agg_index];
    for (size_t g = 0; g < n; ++g)
        aggregates[order_by_agg_index].function->insertResultInto(
            group_states[g].state + off, *agg_col, arena.get());

    auto direction = (sort_direction < 0)
        ? IColumn::PermutationSortDirection::Descending
        : IColumn::PermutationSortDirection::Ascending;

    IColumn::Permutation perm;
    agg_col->getPermutation(direction, IColumn::PermutationSortStability::Unstable,
                            limit, /*nan_direction_hint=*/1, perm);

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

bool TopNAggregatingTransform::isBelowThreshold(const IColumn & col, size_t row) const
{
    if (!threshold_active)
        return false;

    int cmp = col.compareAt(row, 0, *boundary_column, sort_direction);
    if (sort_direction < 0)
        return cmp < 0;
    else
        return cmp > 0;
}

void TopNAggregatingTransform::consume(Chunk chunk)
{
    if (sorted_input)
        consumeMode1(chunk);
    else
        consumeMode2(chunk);
}

void TopNAggregatingTransform::consumeMode1(Chunk & chunk)
{
    if (num_groups >= limit)
    {
        finishConsume();
        return;
    }

    const auto & columns = chunk.getColumns();
    size_t num_rows = chunk.getNumRows();

    if (result_columns.empty())
    {
        const auto & out_header = getOutputPort().getHeader();
        result_columns = out_header.cloneEmptyColumns();
    }

    agg_arg_column_ptrs.resize(aggregates.size());
    for (size_t i = 0; i < aggregates.size(); ++i)
    {
        const auto & arg_indices = agg_arg_columns[i];
        agg_arg_column_ptrs[i].resize(arg_indices.size());
        for (size_t j = 0; j < arg_indices.size(); ++j)
            agg_arg_column_ptrs[i][j] = columns[arg_indices[j]].get();
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
            result_columns[k]->insertFrom(*columns[key_column_indices[k]], row);

        for (size_t i = 0; i < aggregates.size(); ++i)
        {
            const auto & agg = aggregates[i];
            size_t agg_state_size = agg.function->sizeOfData();
            size_t agg_align = agg.function->alignOfData();
            auto * buf = arena->alignedAlloc(agg_state_size, agg_align);
            AggregateDataPtr state = reinterpret_cast<AggregateDataPtr>(buf);
            agg.function->create(state);

            agg.function->add(state, agg_arg_column_ptrs[i].data(), row, arena.get());
            agg.function->insertResultInto(state, *result_columns[key_names.size() + i], arena.get());
            agg.function->destroy(state);
        }

        ++num_groups;

        if (num_groups >= limit)
        {
            finishConsume();
            return;
        }
    }
}

void TopNAggregatingTransform::consumeMode2(Chunk & chunk)
{
    const auto & columns = chunk.getColumns();
    size_t num_rows = chunk.getNumRows();
    if (num_rows == 0)
        return;

    if (mode2_accumulated_keys.empty())
    {
        for (size_t k = 0; k < key_names.size(); ++k)
            mode2_accumulated_keys.push_back(
                stored_input_header.getByPosition(key_column_indices[k]).column->cloneEmpty());
    }

    agg_arg_column_ptrs.resize(aggregates.size());
    for (size_t i = 0; i < aggregates.size(); ++i)
    {
        const auto & arg_indices = agg_arg_columns[i];
        agg_arg_column_ptrs[i].resize(arg_indices.size());
        for (size_t j = 0; j < arg_indices.size(); ++j)
            agg_arg_column_ptrs[i][j] = columns[arg_indices[j]].get();
    }

    const IColumn * order_arg_col = enable_threshold_pruning
        ? columns[order_agg_arg_col_idx].get() : nullptr;

    for (size_t row = 0; row < num_rows; ++row)
    {
        if (enable_threshold_pruning && isBelowThreshold(*order_arg_col, row))
            continue;

        auto key_holder = serializeGroupKey(columns, row);

        decltype(group_indices)::LookupResult it;
        bool inserted = false;
        group_indices.emplace(key_holder, it, inserted);

        if (inserted)
        {
            it->getMapped() = num_groups;

            for (size_t k = 0; k < key_names.size(); ++k)
                mode2_accumulated_keys[k]->insertFrom(*columns[key_column_indices[k]], row);

            auto * buf = arena->alignedAlloc(total_state_size, state_align);
            AggregateDataPtr state = reinterpret_cast<AggregateDataPtr>(buf);
            createAggregateStates(state);
            group_states.push_back({state});

            addRowToAggregateStates(state, row);
            ++num_groups;
        }
        else
        {
            addRowToAggregateStates(group_states[it->getMapped()].state, row);
        }
    }

    /// Refresh threshold from actual group-level aggregates. The threshold is the
    /// K-th best aggregate value seen so far -- a safe lower bound that cannot
    /// over-prune. Capped at limit*10000 groups to keep per-chunk cost bounded.
    if (enable_threshold_pruning && group_states.size() >= limit
        && group_states.size() <= limit * 10000)
        refreshThresholdFromStates();
}

Chunk TopNAggregatingTransform::generate()
{
    if (generated)
        return {};
    generated = true;

    if (sorted_input)
        return generateMode1();
    else if (partial)
        return generateMode2Partial();
    else
        return generateMode2();
}

Chunk TopNAggregatingTransform::generateMode1()
{
    if (num_groups == 0)
        return {};

    Columns final_columns;
    final_columns.reserve(result_columns.size());
    for (auto & col : result_columns)
        final_columns.push_back(std::move(col));

    return Chunk(std::move(final_columns), num_groups);
}

Chunk TopNAggregatingTransform::generateMode2()
{
    if (num_groups == 0)
        return {};

    const auto & out_header = getOutputPort().getHeader();
    size_t num_keys = key_names.size();
    MutableColumns output = out_header.cloneEmptyColumns();

    for (size_t k = 0; k < num_keys; ++k)
        output[k] = std::move(mode2_accumulated_keys[k]);

    for (size_t g = 0; g < num_groups; ++g)
        insertResultsFromStates(group_states[g].state, output);

    size_t order_col_idx = num_keys + order_by_agg_index;
    size_t output_limit = std::min(limit, num_groups);

    const auto & sort_col_desc = sort_description.front();
    int nulls_dir = sort_col_desc.nulls_direction;
    const auto & collator = sort_col_desc.collator;

    auto direction = (sort_direction < 0)
        ? IColumn::PermutationSortDirection::Descending
        : IColumn::PermutationSortDirection::Ascending;

    IColumn::Permutation permutation;
    if (collator)
    {
        output[order_col_idx]->getPermutationWithCollation(
            *collator, direction, IColumn::PermutationSortStability::Unstable,
            output_limit, nulls_dir, permutation);
    }
    else
    {
        output[order_col_idx]->getPermutation(
            direction, IColumn::PermutationSortStability::Unstable,
            output_limit, nulls_dir, permutation);
    }

    Columns final_columns;
    final_columns.reserve(output.size());
    for (auto & col : output)
        final_columns.push_back(col->permute(permutation, output_limit));

    return Chunk(std::move(final_columns), output_limit);
}

Chunk TopNAggregatingTransform::generateMode2Partial()
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

    auto direction = (sort_direction < 0)
        ? IColumn::PermutationSortDirection::Descending
        : IColumn::PermutationSortDirection::Ascending;

    const auto & sort_col_desc = sort_description.front();
    int nulls_dir = sort_col_desc.nulls_direction;

    IColumn::Permutation perm;
    order_col->getPermutation(
        direction, IColumn::PermutationSortStability::Unstable,
        output_limit, nulls_dir, perm);

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
            output[k]->insertFrom(*mode2_accumulated_keys[k], perm[j]);
    }

    for (size_t i = 0; i < aggregates.size(); ++i)
    {
        auto & col = assert_cast<ColumnAggregateFunction &>(*output[num_keys + i]);
        col.reserve(output_limit);
        for (size_t j = 0; j < output_limit; ++j)
            col.insertFrom(group_states[perm[j]].state + agg_state_offsets[i]);
    }

    /// States in top K are now owned by ColumnAggregateFunction columns.
    /// Destroy remaining states that were not selected.
    std::vector<bool> in_top_k(num_groups, false);
    for (size_t j = 0; j < output_limit; ++j)
        in_top_k[perm[j]] = true;

    for (size_t g = 0; g < num_groups; ++g)
    {
        if (!in_top_k[g])
            destroyAggregateStates(group_states[g].state);
    }
    group_states.clear();

    return Chunk(out_header.cloneWithColumns(std::move(output)).getColumns(), output_limit);
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
    const auto & sort_col_name = sort_description.front().column_name;
    for (size_t i = 0; i < aggregates.size(); ++i)
    {
        if (aggregates[i].column_name == sort_col_name)
        {
            order_by_agg_index = i;
            break;
        }
    }

    sort_direction = sort_description.front().direction;

    key_column_indices.resize(key_names.size());
    for (size_t i = 0; i < key_names.size(); ++i)
        key_column_indices[i] = intermediate_header_.getPositionByName(key_names[i]);

    agg_column_indices.resize(aggregates.size());
    for (size_t i = 0; i < aggregates.size(); ++i)
        agg_column_indices[i] = intermediate_header_.getPositionByName(aggregates[i].column_name);

    total_state_size = 0;
    state_align = 1;
    agg_state_offsets.resize(aggregates.size());
    for (size_t i = 0; i < aggregates.size(); ++i)
    {
        total_state_size = (total_state_size + aggregates[i].function->alignOfData() - 1)
            / aggregates[i].function->alignOfData() * aggregates[i].function->alignOfData();
        agg_state_offsets[i] = total_state_size;
        total_state_size += aggregates[i].function->sizeOfData();
        state_align = std::max(state_align, aggregates[i].function->alignOfData());
    }
}

TopNAggregatingMergeTransform::~TopNAggregatingMergeTransform()
{
    for (auto & gs : group_states)
    {
        if (gs.state)
        {
            for (size_t i = 0; i < aggregates.size(); ++i)
                aggregates[i].function->destroy(gs.state + agg_state_offsets[i]);
        }
    }
}

void TopNAggregatingMergeTransform::consume(Chunk chunk)
{
    if (chunk.getNumRows() == 0)
        return;

    const auto & columns = chunk.getColumns();
    size_t num_rows = chunk.getNumRows();

    if (accumulated_keys.empty())
    {
        for (size_t k = 0; k < key_names.size(); ++k)
            accumulated_keys.push_back(columns[key_column_indices[k]]->cloneEmpty());
    }

    for (size_t row = 0; row < num_rows; ++row)
    {
        const char * begin = nullptr;
        std::string_view key;
        for (size_t idx : key_column_indices)
            key = columns[idx]->serializeValueIntoArena(row, *arena, begin, nullptr);
        SerializedKeyHolder key_holder{std::string_view(begin, key.data() + key.size() - begin), *arena};

        decltype(group_indices)::LookupResult it;
        bool inserted = false;
        group_indices.emplace(key_holder, it, inserted);

        if (inserted)
        {
            it->getMapped() = num_groups;

            for (size_t k = 0; k < key_names.size(); ++k)
                accumulated_keys[k]->insertFrom(*columns[key_column_indices[k]], row);

            auto * buf = arena->alignedAlloc(total_state_size, state_align);
            AggregateDataPtr state = reinterpret_cast<AggregateDataPtr>(buf);
            for (size_t i = 0; i < aggregates.size(); ++i)
                aggregates[i].function->create(state + agg_state_offsets[i]);
            group_states.push_back({state});

            for (size_t i = 0; i < aggregates.size(); ++i)
            {
                const auto & agg_col = assert_cast<const ColumnAggregateFunction &>(*columns[agg_column_indices[i]]);
                aggregates[i].function->merge(
                    state + agg_state_offsets[i],
                    agg_col.getData()[row],
                    arena.get());
            }

            ++num_groups;
        }
        else
        {
            AggregateDataPtr state = group_states[it->getMapped()].state;
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
    {
        for (size_t i = 0; i < aggregates.size(); ++i)
            aggregates[i].function->insertResultInto(
                group_states[g].state + agg_state_offsets[i],
                *output[num_keys + i],
                arena.get());
    }

    size_t order_col_idx = num_keys + order_by_agg_index;
    size_t output_limit = std::min(limit, num_groups);

    const auto & sort_col_desc = sort_description.front();
    int nulls_dir = sort_col_desc.nulls_direction;
    const auto & collator = sort_col_desc.collator;

    auto direction = (sort_direction < 0)
        ? IColumn::PermutationSortDirection::Descending
        : IColumn::PermutationSortDirection::Ascending;

    IColumn::Permutation permutation;
    if (collator)
    {
        output[order_col_idx]->getPermutationWithCollation(
            *collator, direction, IColumn::PermutationSortStability::Unstable,
            output_limit, nulls_dir, permutation);
    }
    else
    {
        output[order_col_idx]->getPermutation(
            direction, IColumn::PermutationSortStability::Unstable,
            output_limit, nulls_dir, permutation);
    }

    Columns final_columns;
    final_columns.reserve(output.size());
    for (auto & col : output)
        final_columns.push_back(col->permute(permutation, output_limit));

    return Chunk(std::move(final_columns), output_limit);
}

}
