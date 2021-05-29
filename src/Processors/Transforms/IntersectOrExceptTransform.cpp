#include <Processors/Transforms/IntersectOrExceptTransform.h>

namespace DB
{
IntersectOrExceptTransform::IntersectOrExceptTransform(bool is_except_, const Block & header_)
    : IProcessor(InputPorts(2, header_), {header_}), is_except(is_except_), output(outputs.front())
{
    const Names & columns = header_.getNames();
    size_t num_columns = columns.empty() ? header_.columns() : columns.size();

    key_columns_pos.reserve(columns.size());
    for (size_t i = 0; i < num_columns; ++i)
    {
        auto pos = columns.empty() ? i : header_.getPositionByName(columns[i]);

        const auto & col = header_.getByPosition(pos).column;

        if (!(col && isColumnConst(*col)))
            key_columns_pos.emplace_back(pos);
    }
}

IntersectOrExceptTransform::Status IntersectOrExceptTransform::prepare()
{
    /// Check can output.
    if (output.isFinished())
    {
        for (auto & in : inputs)
            in.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        if (inputs.front().isFinished())
        {
            inputs.back().setNotNeeded();
        }
        else
        {
            inputs.front().setNotNeeded();
        }
        return Status::PortFull;
    }

    /// Output if has data.
    if (current_output_chunk)
    {
        output.push(std::move(current_output_chunk));
    }

    if (push_empty_chunk)
    {
        output.push(std::move(empty_chunk));
        push_empty_chunk = false;
    }

    if (finished_second_input)
    {
        if (inputs.front().isFinished())
        {
            output.finish();
            return Status::Finished;
        }
    }
    else if (inputs.back().isFinished())
    {
        finished_second_input = true;
    }

    InputPort & input = finished_second_input ? inputs.front() : inputs.back();

    /// Check can input.
    if (!has_input)
    {
        input.setNeeded();
        if (!input.hasData())
        {
            return Status::NeedData;
        }

        current_input_chunk = input.pull();
        has_input = true;
    }

    return Status::Ready;
}

void IntersectOrExceptTransform::work()
{
    if (!finished_second_input)
    {
        accumulate(std::move(current_input_chunk));
    }
    else
    {
        filter(current_input_chunk);
        current_output_chunk = std::move(current_input_chunk);
    }

    has_input = false;
}

template <typename Method>
void IntersectOrExceptTransform::addToSet(Method & method, const ColumnRawPtrs & columns, size_t rows, SetVariants & variants) const
{
    typename Method::State state(columns, key_sizes, nullptr);

    for (size_t i = 0; i < rows; ++i)
    {
        state.emplaceKey(method.data, i, variants.string_pool);
    }
}

template <typename Method>
size_t IntersectOrExceptTransform::buildFilter(
    Method & method, const ColumnRawPtrs & columns, IColumn::Filter & filter, size_t rows, SetVariants & variants) const
{
    typename Method::State state(columns, key_sizes, nullptr);
    size_t new_rows_num = 0;

    for (size_t i = 0; i < rows; ++i)
    {
        auto find_result = state.findKey(method.data, i, variants.string_pool);
        filter[i] = is_except ? !find_result.isFound() : find_result.isFound();
        if (filter[i])
            ++new_rows_num;
    }
    return new_rows_num;
}

void IntersectOrExceptTransform::accumulate(Chunk chunk)
{
    auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    ColumnRawPtrs column_ptrs;
    column_ptrs.reserve(key_columns_pos.size());
    for (auto pos : key_columns_pos)
        column_ptrs.emplace_back(columns[pos].get());

    if (data.empty())
        data.init(SetVariants::chooseMethod(column_ptrs, key_sizes));

    switch (data.type)
    {
        case SetVariants::Type::EMPTY:
            break;
#define M(NAME) \
    case SetVariants::Type::NAME: \
        addToSet(*data.NAME, column_ptrs, num_rows, data); \
        break;
            APPLY_FOR_SET_VARIANTS(M)
#undef M
    }
}

void IntersectOrExceptTransform::filter(Chunk & chunk)
{
    auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    ColumnRawPtrs column_ptrs;
    column_ptrs.reserve(key_columns_pos.size());
    for (auto pos : key_columns_pos)
        column_ptrs.emplace_back(columns[pos].get());

    if (data.empty())
        data.init(SetVariants::chooseMethod(column_ptrs, key_sizes));

    IColumn::Filter filter(num_rows);

    size_t new_rows_num = 0;
    switch (data.type)
    {
        case SetVariants::Type::EMPTY:
            break;
#define M(NAME) \
    case SetVariants::Type::NAME: \
        new_rows_num = buildFilter(*data.NAME, column_ptrs, filter, num_rows, data); \
        break;
            APPLY_FOR_SET_VARIANTS(M)
#undef M
    }

    for (auto & column : columns)
        column = column->filter(filter, -1);

    chunk.setColumns(std::move(columns), new_rows_num);
}

}
