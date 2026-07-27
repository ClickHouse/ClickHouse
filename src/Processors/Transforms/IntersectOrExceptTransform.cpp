#include <Processors/Port.h>
#include <Processors/Transforms/IntersectOrExceptTransform.h>

namespace DB
{

/// After visitor is applied, ASTSelectIntersectExcept always has two child nodes.
IntersectOrExceptTransform::IntersectOrExceptTransform(SharedHeader header_, Operator operator_)
    : IProcessor(InputPorts(2, header_), {header_})
    , current_operator(operator_)
{
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

    if (!has_input)
    {
        while (true)
        {
            if (stage == Stage::ReadLeftInput)
            {
                auto & input = inputs.front();

                if (input.isFinished())
                {
                    inputs.back().close();
                    output.finish();
                    return Status::Finished;
                }

                input.setNeeded();
                if (!input.hasData())
                    return Status::NeedData;

                current_input_chunk = input.pull();
                has_input = true;
                break;
            }

            if (stage == Stage::ReadRightInput)
            {
                auto & input = inputs.back();

                if (input.isFinished())
                {
                    if (isIntersectOperator() && !has_right_input_rows)
                    {
                        inputs.front().close();
                        output.finish();
                        return Status::Finished;
                    }

                    stage = Stage::ReadRemainingLeftInput;
                    continue;
                }

                input.setNeeded();
                if (!input.hasData())
                    return Status::NeedData;

                current_input_chunk = input.pull();
                has_input = true;
                break;
            }

            if (has_left_input_chunk)
            {
                current_input_chunk = std::move(left_input_chunk);
                has_left_input_chunk = false;
                has_input = true;
                break;
            }

            auto & input = inputs.front();

            if (input.isFinished())
            {
                output.finish();
                return Status::Finished;
            }

            input.setNeeded();
            if (!input.hasData())
                return Status::NeedData;

            current_input_chunk = input.pull();
            has_input = true;
            break;
        }
    }

    return Status::Ready;
}


void IntersectOrExceptTransform::work()
{
    if (stage == Stage::ReadLeftInput)
    {
        if (current_input_chunk.hasRows())
        {
            left_input_chunk = std::move(current_input_chunk);
            has_left_input_chunk = true;
            stage = Stage::ReadRightInput;
        }
    }
    else if (stage == Stage::ReadRightInput)
    {
        has_right_input_rows |= current_input_chunk.hasRows();
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
void IntersectOrExceptTransform::addToCounts(
    Method & method, const ColumnRawPtrs & columns, size_t rows, CountingSetVariants & variants) const
{
    typename Method::State state(columns, key_sizes, nullptr);

    for (size_t i = 0; i < rows; ++i)
        ++state.emplaceKey(method.data, i, variants.string_pool).getMapped();
}


template <typename Method>
size_t IntersectOrExceptTransform::filterWithCounts(
    Method & method, const ColumnRawPtrs & columns, IColumn::Filter & filter, size_t rows, CountingSetVariants & variants) const
{
    typename Method::State state(columns, key_sizes, nullptr);
    const bool is_except = (current_operator == Operator::EXCEPT_ALL);
    size_t new_rows_num = 0;

    for (size_t i = 0; i < rows; ++i)
    {
        auto find_result = state.findKey(method.data, i, variants.string_pool);

        /// A remaining right-side occurrence of this row.
        bool matched = find_result.isFound() && find_result.getMapped() > 0;
        if (matched)
            --find_result.getMapped();

        /// EXCEPT ALL keeps unmatched rows; INTERSECT ALL keeps matched rows.
        filter[i] = matched != is_except;

        if (filter[i])
            ++new_rows_num;
    }
    return new_rows_num;
}


template <typename Method>
void IntersectOrExceptTransform::addToSet(Method & method, const ColumnRawPtrs & columns, size_t rows, SetVariants & variants) const
{
    chassert(!isAllOperator(), "addToSet must only be used for DISTINCT operators");

    typename Method::State state(columns, key_sizes, nullptr);

    for (size_t i = 0; i < rows; ++i)
        state.emplaceKey(method.data, i, variants.string_pool);
}


template <typename Method>
size_t IntersectOrExceptTransform::buildFilter(
    Method & method, const ColumnRawPtrs & columns, IColumn::Filter & filter, size_t rows, SetVariants & variants) const
{
    /// ALL operators use the counting path, so only DISTINCT operators reach here; the branch
    /// below therefore only needs to distinguish EXCEPT_DISTINCT from INTERSECT_DISTINCT.
    chassert(!isAllOperator(), "buildFilter must only be used for DISTINCT operators");

    typename Method::State state(columns, key_sizes, nullptr);
    size_t new_rows_num = 0;

    for (size_t i = 0; i < rows; ++i)
    {
        auto find_result = state.findKey(method.data, i, variants.string_pool);
        filter[i] = (current_operator == ASTSelectIntersectExceptQuery::Operator::EXCEPT_DISTINCT)
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
    column_ptrs.reserve(columns.size());

    for (auto & column : columns)
    {
        /// Hash methods expect non-const columns.
        column = column->convertToFullColumnIfConst();
        column_ptrs.emplace_back(column.get());
    }

    if (isAllOperator())
    {
        /// For ALL variants, track right-side occurrence counts keyed on the real row value.
        if (!counts_data)
            counts_data.emplace();
        if (counts_data->empty())
            counts_data->init(CountingSetVariants::chooseMethod(column_ptrs, key_sizes));

        auto & variants = *counts_data;
        switch (variants.type)
        {
            case CountingSetVariants::Type::EMPTY:
                break;
#define M(NAME) \
    case CountingSetVariants::Type::NAME: \
        addToCounts(*variants.NAME, column_ptrs, num_rows, variants); \
        break;
            APPLY_FOR_SET_VARIANTS(M)
#undef M
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
    column_ptrs.reserve(columns.size());

    for (auto & column : columns)
    {
        /// Hash methods expect non-const columns.
        column = column->convertToFullColumnIfConst();
        column_ptrs.emplace_back(column.get());
    }

    size_t new_rows_num = 0;
    IColumn::Filter row_filter(num_rows);

    if (isAllOperator())
    {
        if (!counts_data)
            counts_data.emplace();
        if (counts_data->empty())
            counts_data->init(CountingSetVariants::chooseMethod(column_ptrs, key_sizes));

        auto & variants = *counts_data;
        switch (variants.type)
        {
            case CountingSetVariants::Type::EMPTY:
                break;
#define M(NAME) \
    case CountingSetVariants::Type::NAME: \
        new_rows_num = filterWithCounts(*variants.NAME, column_ptrs, row_filter, num_rows, variants); \
        break;
            APPLY_FOR_SET_VARIANTS(M)
#undef M
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
