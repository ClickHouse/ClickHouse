#include <Processors/Transforms/TopNAggregatingTransform.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/IColumn.h>
#include <Processors/Port.h>

#include <algorithm>
#include <limits>
#include <numeric>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

TopNAggregatingTransform::TopNAggregatingTransform(
    const Block & input_header_,
    const Block & output_header_,
    const Names & key_names_,
    const AggregateDescriptions & aggregates_,
    const SortDescription & sort_description_,
    size_t limit_,
    bool sorted_input_)
    : IAccumulatingTransform(
        std::make_shared<const Block>(input_header_),
        std::make_shared<const Block>(output_header_))
    , key_names(key_names_)
    , aggregates(aggregates_)
    , sort_description(sort_description_)
    , limit(limit_)
    , sorted_input(sorted_input_)
    , stored_input_header(input_header_)
{
    initColumnIndices(input_header_);

    sort_direction = sort_description.front().direction;

    if (!sorted_input)
    {
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
}

TopNAggregatingTransform::~TopNAggregatingTransform()
{
    if (!sorted_input)
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
}

UInt128 TopNAggregatingTransform::hashGroupKey(const Columns & columns, size_t row) const
{
    SipHash hash;
    for (size_t idx : key_column_indices)
        columns[idx]->updateHashWithValue(row, hash);
    return hash.get128();
}

size_t TopNAggregatingTransform::findGroupIndex(UInt128 hash, const Columns & columns, size_t row) const
{
    auto [it, end] = group_indices.equal_range(hash);
    for (; it != end; ++it)
    {
        size_t group_idx = it->second;
        bool match = true;
        for (size_t k = 0; k < key_column_indices.size(); ++k)
        {
            if (result_columns[k]->compareAt(group_idx, row, *columns[key_column_indices[k]], 0) != 0)
            {
                match = false;
                break;
            }
        }
        if (match)
            return group_idx;
    }
    return num_groups;
}

void TopNAggregatingTransform::createAggregateStates(AggregateDataPtr place) const
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

void TopNAggregatingTransform::destroyAggregateStates(AggregateDataPtr place) const
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

void TopNAggregatingTransform::addRowToAggregateStates(AggregateDataPtr place, const Columns & columns, size_t row)
{
    size_t offset = 0;
    for (const auto & aggregate : aggregates)
    {
        offset = (offset + aggregate.function->alignOfData() - 1)
            / aggregate.function->alignOfData() * aggregate.function->alignOfData();

        ColumnRawPtrs arg_columns(aggregate.argument_names.size());
        for (size_t j = 0; j < aggregate.argument_names.size(); ++j)
        {
            size_t col_idx = stored_input_header.getPositionByName(aggregate.argument_names[j]);
            arg_columns[j] = columns[col_idx].get();
        }

        aggregate.function->add(place + offset, arg_columns.data(), row, &arena);
        offset += aggregate.function->sizeOfData();
    }
}

void TopNAggregatingTransform::insertResultsFromStates(
    AggregateDataPtr place, MutableColumns & output_columns)
{
    size_t num_keys = key_names.size();
    size_t offset = 0;
    for (size_t i = 0; i < aggregates.size(); ++i)
    {
        offset = (offset + aggregates[i].function->alignOfData() - 1)
            / aggregates[i].function->alignOfData() * aggregates[i].function->alignOfData();
        aggregates[i].function->insertResultInto(place + offset, *output_columns[num_keys + i], &arena);
        offset += aggregates[i].function->sizeOfData();
    }
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

    for (size_t row = 0; row < num_rows; ++row)
    {
        UInt128 hash = hashGroupKey(columns, row);
        size_t existing = findGroupIndex(hash, columns, row);

        if (existing < num_groups)
            continue;

        group_indices.emplace(hash, num_groups);

        for (size_t k = 0; k < key_names.size(); ++k)
            result_columns[k]->insertFrom(*columns[key_column_indices[k]], row);

        for (size_t i = 0; i < aggregates.size(); ++i)
        {
            const auto & agg = aggregates[i];
            size_t agg_state_size = agg.function->sizeOfData();
            size_t agg_align = agg.function->alignOfData();
            auto * buf = arena.alignedAlloc(agg_state_size, agg_align);
            AggregateDataPtr state = reinterpret_cast<AggregateDataPtr>(buf);
            agg.function->create(state);

            ColumnRawPtrs arg_columns(agg.argument_names.size());
            for (size_t j = 0; j < agg.argument_names.size(); ++j)
            {
                size_t col_idx = stored_input_header.getPositionByName(agg.argument_names[j]);
                arg_columns[j] = columns[col_idx].get();
            }

            agg.function->add(state, arg_columns.data(), row, &arena);
            agg.function->insertResultInto(state, *result_columns[key_names.size() + i], &arena);
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
        const auto & out_header = getOutputPort().getHeader();
        result_columns = out_header.cloneEmptyColumns();
    }

    for (size_t row = 0; row < num_rows; ++row)
    {
        UInt128 hash = hashGroupKey(columns, row);

        size_t found = std::numeric_limits<size_t>::max();
        {
            auto [it, end] = group_indices.equal_range(hash);
            for (; it != end; ++it)
            {
                size_t group_idx = it->second;
                bool match = true;
                for (size_t k = 0; k < key_column_indices.size(); ++k)
                {
                    if (result_columns[k]->compareAt(group_idx, row, *columns[key_column_indices[k]], 0) != 0)
                    {
                        match = false;
                        break;
                    }
                }
                if (match)
                {
                    found = group_idx;
                    break;
                }
            }
        }

        if (found != std::numeric_limits<size_t>::max())
        {
            addRowToAggregateStates(group_states[found].state, columns, row);
        }
        else
        {
            size_t group_idx = group_states.size();
            group_indices.emplace(hash, group_idx);

            for (size_t k = 0; k < key_names.size(); ++k)
                result_columns[k]->insertFrom(*columns[key_column_indices[k]], row);

            auto * place = arena.alignedAlloc(total_state_size, state_align);
            createAggregateStates(reinterpret_cast<AggregateDataPtr>(place));

            GroupState gs{.state = reinterpret_cast<AggregateDataPtr>(place)};
            group_states.push_back(gs);

            addRowToAggregateStates(gs.state, columns, row);
        }
    }
}

Chunk TopNAggregatingTransform::generate()
{
    if (generated)
        return {};
    generated = true;

    if (sorted_input)
        return generateMode1();
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

    size_t order_col_idx = num_keys + order_by_agg_index;
    const auto & order_col = all_columns[order_col_idx];

    std::vector<size_t> indices(num_groups_total);
    std::iota(indices.begin(), indices.end(), 0);

    const auto & sort_col_desc = sort_description.front();
    int nulls_dir = sort_col_desc.nulls_direction;
    const auto & collator = sort_col_desc.collator;

    if (collator)
    {
        std::partial_sort(
            indices.begin(),
            indices.begin() + static_cast<std::ptrdiff_t>(std::min(limit, num_groups_total)),
            indices.end(),
            [&](size_t a, size_t b)
            {
                int cmp = order_col->compareAtWithCollation(a, b, *order_col, nulls_dir, *collator);
                return (sort_direction < 0) ? (cmp > 0) : (cmp < 0);
            });
    }
    else
    {
        std::partial_sort(
            indices.begin(),
            indices.begin() + static_cast<std::ptrdiff_t>(std::min(limit, num_groups_total)),
            indices.end(),
            [&](size_t a, size_t b)
            {
                int cmp = order_col->compareAt(a, b, *order_col, nulls_dir);
                return (sort_direction < 0) ? (cmp > 0) : (cmp < 0);
            });
    }

    size_t output_size = std::min(limit, num_groups_total);

    MutableColumns output_columns = out_header.cloneEmptyColumns();
    for (size_t i = 0; i < output_size; ++i)
    {
        size_t idx = indices[i];
        for (size_t c = 0; c < all_columns.size(); ++c)
            output_columns[c]->insertFrom(*all_columns[c], idx);
    }

    Columns final_columns;
    final_columns.reserve(output_columns.size());
    for (auto & col : output_columns)
        final_columns.push_back(std::move(col));

    return Chunk(std::move(final_columns), output_size);
}

}
