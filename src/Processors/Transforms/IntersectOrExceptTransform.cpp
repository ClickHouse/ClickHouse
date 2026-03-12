#include <Processors/Port.h>
#include <Processors/Transforms/IntersectOrExceptTransform.h>

namespace DB
{

/// After visitor is applied, ASTSelectIntersectExcept always has two child nodes.
IntersectOrExceptTransform::IntersectOrExceptTransform(SharedHeader header_, Operator operator_)
    : IProcessor(InputPorts(2, header_), {header_})
    , current_operator(operator_)
{
    const Names & columns = header_->getNames();
    size_t num_columns = columns.empty() ? header_->columns() : columns.size();

    key_columns_pos.reserve(columns.size());
    for (size_t i = 0; i < num_columns; ++i)
    {
        auto pos = columns.empty() ? i : header_->getPositionByName(columns[i]);
        key_columns_pos.emplace_back(pos);
    }
}


IntersectOrExceptTransform::Status IntersectOrExceptTransform::prepare()
{
    auto & output = outputs.front();

    if (output.isFinished())
    {
        for (auto & in : inputs)
            in.close();

        return Status::Finished;
    }

    if (!output.canPush())
    {
        for (auto & input : inputs)
            input.setNotNeeded();

        return Status::PortFull;
    }

    if (current_output_chunk)
    {
        output.push(std::move(current_output_chunk));
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

    if (!has_input)
    {
        InputPort & input = finished_second_input ? inputs.front() : inputs.back();

        input.setNeeded();
        if (!input.hasData())
            return Status::NeedData;

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


UInt128 IntersectOrExceptTransform::hashRow(const ColumnRawPtrs & columns, size_t row)
{
    SipHash hash;
    for (const auto * column : columns)
        column->updateHashWithValue(row, hash);
    return hash.get128();
}


template <typename Method>
void IntersectOrExceptTransform::addToSet(Method & method, const ColumnRawPtrs & columns, size_t rows, SetVariants & variants) const
{
    typename Method::State state(columns, key_sizes, nullptr);

    for (size_t i = 0; i < rows; ++i)
        state.emplaceKey(method.data, i, variants.string_pool);
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
        filter[i] = (current_operator == ASTSelectIntersectExceptQuery::Operator::EXCEPT_ALL
                     || current_operator == ASTSelectIntersectExceptQuery::Operator::EXCEPT_DISTINCT)
            ? !find_result.isFound()
            : find_result.isFound();
        if (filter[i])
            ++new_rows_num;
    }
    return new_rows_num;
}


void IntersectOrExceptTransform::accumulate(Chunk chunk)
{
    removeSpecialColumnRepresentations(chunk);

    auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    ColumnRawPtrs column_ptrs;
    column_ptrs.reserve(key_columns_pos.size());

    for (auto pos : key_columns_pos)
    {
        /// Hash methods expect non-const column
        columns[pos] = columns[pos]->convertToFullColumnIfConst();
        column_ptrs.emplace_back(columns[pos].get());
    }

    if (isAllOperator())
    {
        /// For ALL variants, track occurrence counts using a HashMap.
        for (size_t i = 0; i < num_rows; ++i)
        {
            auto key = hashRow(column_ptrs, i);
            ++counts[key];
        }
        return;
    }

    if (!data)
        data.emplace();

    if (data->empty())
        data->init(SetVariants::chooseMethod(column_ptrs, key_sizes));

    auto & data_set = *data;
    switch (data->type)
    {
        case SetVariants::Type::EMPTY:
            break;
#define M(NAME) \
    case SetVariants::Type::NAME: \
        addToSet(*data_set.NAME, column_ptrs, num_rows, data_set); \
        break;
            APPLY_FOR_SET_VARIANTS(M)
#undef M
    }
}


void IntersectOrExceptTransform::filter(Chunk & chunk)
{
    removeSpecialColumnRepresentations(chunk);

    auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    ColumnRawPtrs column_ptrs;
    column_ptrs.reserve(key_columns_pos.size());

    for (auto pos : key_columns_pos)
    {
        /// Hash methods expect non-const column
        columns[pos] = columns[pos]->convertToFullColumnIfConst();
        column_ptrs.emplace_back(columns[pos].get());
    }

    size_t new_rows_num = 0;
    IColumn::Filter row_filter(num_rows);

    if (isAllOperator())
    {
        /// For ALL variants, decrement counts to respect row multiplicities.
        bool is_except = (current_operator == Operator::EXCEPT_ALL);

        for (size_t i = 0; i < num_rows; ++i)
        {
            auto key = hashRow(column_ptrs, i);
            auto * it = counts.find(key);

            if (it != nullptr && it->getMapped() > 0)
            {
                --it->getMapped();
                /// EXCEPT ALL: found in right side, exclude this row.
                /// INTERSECT ALL: found in right side, include this row.
                row_filter[i] = is_except ? 0 : 1;
            }
            else
            {
                /// EXCEPT ALL: not in right side, include this row.
                /// INTERSECT ALL: not in right side, exclude this row.
                row_filter[i] = is_except ? 1 : 0;
            }

            if (row_filter[i])
                ++new_rows_num;
        }
    }
    else
    {
        if (!data)
            data.emplace();

        if (data->empty())
            data->init(SetVariants::chooseMethod(column_ptrs, key_sizes));

        auto & data_set = *data;

        switch (data->type)
        {
            case SetVariants::Type::EMPTY:
                break;
#define M(NAME) \
    case SetVariants::Type::NAME: \
        new_rows_num = buildFilter(*data_set.NAME, column_ptrs, row_filter, num_rows, data_set); \
        break;
                APPLY_FOR_SET_VARIANTS(M)
#undef M
        }
    }

    if (!new_rows_num)
        return;

    for (auto & column : columns)
        column = column->filter(row_filter, -1);

    chunk.setColumns(std::move(columns), new_rows_num);
}

}
