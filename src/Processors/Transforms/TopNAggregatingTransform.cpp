#include <Processors/Transforms/TopNAggregatingTransform.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Processors/Port.h>

#include <algorithm>
#include <limits>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

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

    if (!sorted_input)
    {
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
    if (!sorted_input && !partial)
    {
        for (auto & gs : group_states)
        {
            if (gs.state)
                destroyAggregateStates(gs.state);
        }
    }
    if (!sorted_input && partial && !generated)
    {
        for (auto & gs : group_states)
        {
            if (gs.state)
                destroyAggregateStates(gs.state);
        }
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

UInt128 TopNAggregatingTransform::hashGroupKey(const Columns & columns, size_t row) const
{
    SipHash hash;
    for (size_t idx : key_column_indices)
        columns[idx]->updateHashWithValue(row, hash);
    return hash.get128();
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

void TopNAggregatingTransform::addRowToAggregateStates(AggregateDataPtr place, const Columns & /*columns*/, size_t row)
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

    /// Materialize current ORDER BY aggregate values from all group states.
    auto result_type = aggregates[order_by_agg_index].function->getResultType();
    auto agg_col = result_type->createColumn();
    size_t off = agg_state_offsets[order_by_agg_index];
    for (size_t g = 0; g < n; ++g)
        aggregates[order_by_agg_index].function->insertResultInto(
            group_states[g].state + off, *agg_col, arena.get());

    /// Partial sort to find the K-th best value.
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

    /// Populate per-chunk cached column pointers.
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
        UInt128 hash = hashGroupKey(columns, row);

        auto it = group_indices.find(hash);
        if (it != group_indices.end())
            continue;

        group_indices[hash] = num_groups;

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

    if (result_columns.empty())
    {
        result_columns.resize(key_names.size());
        for (size_t k = 0; k < key_names.size(); ++k)
        {
            const auto & out_header = getOutputPort().getHeader();
            result_columns[k] = out_header.getByPosition(k).type->createColumn();
        }
    }

    /// Populate per-chunk cached raw column pointers for aggregate arguments.
    agg_arg_column_ptrs.resize(aggregates.size());
    for (size_t i = 0; i < aggregates.size(); ++i)
    {
        const auto & arg_indices = agg_arg_columns[i];
        agg_arg_column_ptrs[i].resize(arg_indices.size());
        for (size_t j = 0; j < arg_indices.size(); ++j)
            agg_arg_column_ptrs[i][j] = columns[arg_indices[j]].get();
    }

    const IColumn * order_arg_col = enable_threshold_pruning ? columns[order_agg_arg_col_idx].get() : nullptr;

    for (size_t row = 0; row < num_rows; ++row)
    {
        if (enable_threshold_pruning && isBelowThreshold(*order_arg_col, row))
            continue;

        UInt128 hash = hashGroupKey(columns, row);

        auto it = group_indices.find(hash);
        if (it != group_indices.end())
        {
            addRowToAggregateStates(group_states[it->getMapped()].state, columns, row);
        }
        else
        {
            size_t group_idx = group_states.size();
            group_indices[hash] = group_idx;

            for (size_t k = 0; k < key_names.size(); ++k)
                result_columns[k]->insertFrom(*columns[key_column_indices[k]], row);

            auto * place = arena->alignedAlloc(total_state_size, state_align);
            createAggregateStates(reinterpret_cast<AggregateDataPtr>(place));

            GroupState gs{.state = reinterpret_cast<AggregateDataPtr>(place)};
            group_states.push_back(gs);

            addRowToAggregateStates(gs.state, columns, row);

            ++num_groups;
        }
    }

    /// Refresh threshold from actual aggregate states. Cap at limit*10000 groups
    /// to keep per-chunk cost bounded (O(100K) for K=10) even with high cardinality.
    if (enable_threshold_pruning && group_states.size() >= limit && group_states.size() <= limit * 10000)
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
    if (group_states.empty())
        return {};

    size_t num_groups_total = group_states.size();
    size_t num_keys = key_names.size();

    const auto & out_header = getOutputPort().getHeader();
    MutableColumns all_columns = out_header.cloneEmptyColumns();

    for (size_t g = 0; g < num_groups_total; ++g)
    {
        for (size_t k = 0; k < num_keys; ++k)
            all_columns[k]->insertFrom(*result_columns[k], g);

        insertResultsFromStates(group_states[g].state, all_columns);
    }

    /// Use IColumn::getPermutation for vectorized partial sort instead of
    /// std::partial_sort with per-element virtual compareAt calls.
    size_t order_col_idx = num_keys + order_by_agg_index;
    const auto & order_col = all_columns[order_col_idx];
    size_t output_limit = std::min(limit, num_groups_total);

    const auto & sort_col_desc = sort_description.front();
    int nulls_dir = sort_col_desc.nulls_direction;
    const auto & collator = sort_col_desc.collator;

    auto direction = (sort_direction < 0)
        ? IColumn::PermutationSortDirection::Descending
        : IColumn::PermutationSortDirection::Ascending;

    IColumn::Permutation permutation;
    if (collator)
    {
        order_col->getPermutationWithCollation(
            *collator, direction, IColumn::PermutationSortStability::Unstable,
            output_limit, nulls_dir, permutation);
    }
    else
    {
        order_col->getPermutation(
            direction, IColumn::PermutationSortStability::Unstable,
            output_limit, nulls_dir, permutation);
    }

    Columns final_columns;
    final_columns.reserve(all_columns.size());
    for (auto & col : all_columns)
        final_columns.push_back(col->permute(permutation, output_limit));

    return Chunk(std::move(final_columns), output_limit);
}

Chunk TopNAggregatingTransform::generateMode2Partial()
{
    if (group_states.empty())
        return {};

    size_t num_keys = key_names.size();
    size_t num_groups_total = group_states.size();

    Columns output;
    output.reserve(num_keys + aggregates.size());

    for (size_t k = 0; k < num_keys; ++k)
        output.push_back(std::move(result_columns[k]));

    for (size_t i = 0; i < aggregates.size(); ++i)
    {
        auto col = ColumnAggregateFunction::create(aggregates[i].function);
        col->addArena(arena);

        auto & data = col->getData();
        data.reserve(num_groups_total);

        size_t off = agg_state_offsets[i];
        for (size_t g = 0; g < num_groups_total; ++g)
            data.push_back(group_states[g].state + off);

        output.push_back(std::move(col));
    }

    for (auto & gs : group_states)
        gs.state = nullptr;

    return Chunk(std::move(output), num_groups_total);
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
    , final_header(output_header_)
{
    key_column_indices.resize(key_names.size());
    for (size_t i = 0; i < key_names.size(); ++i)
        key_column_indices[i] = intermediate_header_.getPositionByName(key_names[i]);

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

    total_state_size = 0;
    state_align = 1;
    for (const auto & aggregate : aggregates)
    {
        total_state_size = (total_state_size + aggregate.function->alignOfData() - 1)
            / aggregate.function->alignOfData() * aggregate.function->alignOfData();
        total_state_size += aggregate.function->sizeOfData();
        state_align = std::max(state_align, aggregate.function->alignOfData());
    }
}

TopNAggregatingMergeTransform::~TopNAggregatingMergeTransform()
{
    for (auto & gs : group_states)
    {
        if (gs.state)
            destroyAggregateStates(gs.state);
    }
}

UInt128 TopNAggregatingMergeTransform::hashGroupKey(const Columns & columns, size_t row) const
{
    SipHash hash;
    for (size_t idx : key_column_indices)
        columns[idx]->updateHashWithValue(row, hash);
    return hash.get128();
}

size_t TopNAggregatingMergeTransform::findGroupIndex(UInt128 hash, const Columns & /*columns*/, size_t /*row*/) const
{
    auto it = group_indices.find(hash);
    if (it != group_indices.end())
        return it->getMapped();
    return std::numeric_limits<size_t>::max();
}

void TopNAggregatingMergeTransform::createAggregateStates(AggregateDataPtr place) const
{
    size_t offset = 0;
    for (const auto & aggregate : aggregates)
    {
        offset = (offset + aggregate.function->alignOfData() - 1)
            / aggregate.function->alignOfData() * aggregate.function->alignOfData();
        aggregate.function->create(place + offset);
        offset += aggregate.function->sizeOfData();
    }
}

void TopNAggregatingMergeTransform::destroyAggregateStates(AggregateDataPtr place) const
{
    size_t offset = 0;
    for (const auto & aggregate : aggregates)
    {
        offset = (offset + aggregate.function->alignOfData() - 1)
            / aggregate.function->alignOfData() * aggregate.function->alignOfData();
        aggregate.function->destroy(place + offset);
        offset += aggregate.function->sizeOfData();
    }
}

void TopNAggregatingMergeTransform::consume(Chunk chunk)
{
    if (chunk.getNumRows() == 0)
        return;

    const auto & columns = chunk.getColumns();
    size_t num_rows = chunk.getNumRows();
    size_t num_keys = key_names.size();

    if (key_columns.empty())
    {
        key_columns.resize(num_keys);
        for (size_t k = 0; k < num_keys; ++k)
            key_columns[k] = columns[key_column_indices[k]]->cloneEmpty();
    }

    for (size_t row = 0; row < num_rows; ++row)
    {
        UInt128 hash = hashGroupKey(columns, row);
        auto it = group_indices.find(hash);

        if (it != group_indices.end())
        {
            size_t found = it->getMapped();
            size_t offset = 0;
            for (size_t i = 0; i < aggregates.size(); ++i)
            {
                offset = (offset + aggregates[i].function->alignOfData() - 1)
                    / aggregates[i].function->alignOfData() * aggregates[i].function->alignOfData();

                const auto & src_col = assert_cast<const ColumnAggregateFunction &>(*columns[num_keys + i]);
                ConstAggregateDataPtr src_state = src_col.getData()[row];
                aggregates[i].function->merge(
                    group_states[found].state + offset, src_state, &arena);
                offset += aggregates[i].function->sizeOfData();
            }
        }
        else
        {
            size_t group_idx = num_groups;
            group_indices[hash] = group_idx;

            for (size_t k = 0; k < num_keys; ++k)
                key_columns[k]->insertFrom(*columns[key_column_indices[k]], row);

            auto * place = arena.alignedAlloc(total_state_size, state_align);
            auto state_ptr = reinterpret_cast<AggregateDataPtr>(place);
            createAggregateStates(state_ptr);
            group_states.push_back({.state = state_ptr});

            size_t offset = 0;
            for (size_t i = 0; i < aggregates.size(); ++i)
            {
                offset = (offset + aggregates[i].function->alignOfData() - 1)
                    / aggregates[i].function->alignOfData() * aggregates[i].function->alignOfData();

                const auto & src_col = assert_cast<const ColumnAggregateFunction &>(*columns[num_keys + i]);
                ConstAggregateDataPtr src_state = src_col.getData()[row];
                aggregates[i].function->merge(state_ptr + offset, src_state, &arena);
                offset += aggregates[i].function->sizeOfData();
            }

            ++num_groups;
        }
    }
}

Chunk TopNAggregatingMergeTransform::generate()
{
    if (generated)
        return {};
    generated = true;

    if (group_states.empty())
        return {};

    size_t num_keys = key_names.size();

    MutableColumns all_columns = final_header.cloneEmptyColumns();

    for (size_t g = 0; g < num_groups; ++g)
    {
        for (size_t k = 0; k < num_keys; ++k)
            all_columns[k]->insertFrom(*key_columns[k], g);

        size_t offset = 0;
        for (size_t i = 0; i < aggregates.size(); ++i)
        {
            offset = (offset + aggregates[i].function->alignOfData() - 1)
                / aggregates[i].function->alignOfData() * aggregates[i].function->alignOfData();
            aggregates[i].function->insertResultInto(
                group_states[g].state + offset, *all_columns[num_keys + i], &arena);
            offset += aggregates[i].function->sizeOfData();
        }
    }

    /// Vectorized partial sort via getPermutation.
    size_t order_col_idx = num_keys + order_by_agg_index;
    const auto & order_col = all_columns[order_col_idx];
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
        order_col->getPermutationWithCollation(
            *collator, direction, IColumn::PermutationSortStability::Unstable,
            output_limit, nulls_dir, permutation);
    }
    else
    {
        order_col->getPermutation(
            direction, IColumn::PermutationSortStability::Unstable,
            output_limit, nulls_dir, permutation);
    }

    Columns final_columns;
    final_columns.reserve(all_columns.size());
    for (auto & col : all_columns)
        final_columns.push_back(col->permute(permutation, output_limit));

    return Chunk(std::move(final_columns), output_limit);
}

}
