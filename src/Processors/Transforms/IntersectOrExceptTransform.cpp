#include <Processors/Port.h>
#include <Processors/Transforms/IntersectOrExceptTransform.h>
#include <Common/MemoryTrackerUtils.h>

namespace DB
{

/// After visitor is applied, ASTSelectIntersectExcept always has two child nodes.
IntersectOrExceptTransform::IntersectOrExceptTransform(SharedHeader header_, Operator operator_, size_t right_rows_estimate_)
    : IProcessor(InputPorts(2, header_), {header_})
    , current_operator(operator_)
    , right_rows_estimate(right_rows_estimate_)
{
    /// Arm the first reservation review to fire just before the counting table's first resize (its
    /// load threshold is half the initial buffer). Subsequent reviews re-arm to each next resize.
    counts_inserts_until_review = 1UL << (COUNTING_SET_INITIAL_SIZE_DEGREE - 1);
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
    Method & method, const ColumnRawPtrs & columns, size_t rows, CountingSetVariants & variants)
{
    typename Method::State state(columns, key_sizes, nullptr);

    for (size_t i = 0; i < rows; ++i)
    {
        auto emplaced = state.emplaceKey(method.data, i, variants.string_pool);
        ++emplaced.getMapped();

        /// Growth tracking only matters for resizable tables; for fixed ones (key8/key16) the whole
        /// block compiles away. Per-row cost is then just the (already-computed) inserted flag and a
        /// decrement; the review, which reads the table size and may resize, runs amortized once
        /// enough new keys accumulate.
        if constexpr (requires { method.data.reserve(size_t{}); })
        {
            if (emplaced.isInserted() && --counts_inserts_until_review == 0)
                reviewCountsReservation(method);
        }
    }
}


template <typename Method>
void IntersectOrExceptTransform::reserveCounts(Method & method, size_t target)
{
    if constexpr (requires { method.data.reserve(target); })
        method.data.reserve(target);
}


namespace
{
    constexpr size_t NEVER_AGAIN = std::numeric_limits<size_t>::max();

    /// Jump straight to the target once the live key count is within this factor of it (bounds a
    /// wrong target's over-reserve to this factor).
    constexpr size_t COUNTS_RESERVE_GATE_FACTOR = 4;

    /// With no row estimate, still pre-grow tables up to this many keys. A table that keeps getting
    /// new keys is worth pre-sizing; the cap keeps the speculation small and leaves large
    /// no-estimate tables to the hash table's natural growth.
    constexpr size_t COUNTS_NO_ESTIMATE_LIMIT = 1 << 18;
}

template <typename Method>
size_t IntersectOrExceptTransform::cappedReserveTarget(Method & method, size_t distinct, size_t target) const
{
    if (auto hard_limit = getCurrentQueryHardLimit())
    {
        Int64 available = static_cast<Int64>(*hard_limit) - getCurrentQueryMemoryUsage();
        if (available <= 0)
            return distinct;

        size_t bytes_per_key = std::max<size_t>(1, method.data.getBufferSizeInBytes() / std::max<size_t>(distinct, 1));
        size_t budget_keys = (static_cast<size_t>(available) / 4) / bytes_per_key;
        target = std::min(target, budget_keys);
    }
    return target;
}

template <typename Method>
void IntersectOrExceptTransform::reviewCountsReservation(Method & method)
{
    /// Called right before the table would resize itself (size has reached its load threshold).
    size_t distinct = method.data.size();

    /// Grow toward the estimate; without one, toward a small bound so small-but-growing tables still
    /// skip their early resizes (see COUNTS_NO_ESTIMATE_LIMIT).
    size_t target = right_rows_estimate ? right_rows_estimate : COUNTS_NO_ESTIMATE_LIMIT;

    /// Not within reach of the target yet: let the table resize naturally and re-arm to review just
    /// before its next resize (threshold is half the buffer; the grower keeps a 0.5 load factor).
    if (distinct * COUNTS_RESERVE_GATE_FACTOR < target)
    {
        size_t max_fill = method.data.getBufferSizeInCells() / 2;
        counts_inserts_until_review = max_fill > distinct ? max_fill - distinct : 1;
        return;
    }

    /// Within reach of the target: reserve straight to it (capped), skipping this resize and the
    /// rest of the cascade, then stop (no overshoot; beyond a no-estimate bound natural doubling
    /// takes over).
    counts_inserts_until_review = NEVER_AGAIN;
    target = cappedReserveTarget(method, distinct, target);
    if (target > distinct)
        reserveCounts(method, target);
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
