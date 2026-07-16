#include <Processors/Transforms/ExtremesTransform.h>
#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <Common/NaNUtils.h>

namespace DB
{

ExtremesTransform::ExtremesTransform(SharedHeader header)
    : ISimpleTransform(header, header, true)
{
    /// Port for Extremes.
    outputs.emplace_back(outputs.front().getHeader(), this);
}

IProcessor::Status ExtremesTransform::prepare()
{
    if (!finished_transform)
    {
        auto status = ISimpleTransform::prepare();

        if (status != Status::Finished)
            return status;

        finished_transform = true;
    }

    auto & totals_output = getExtremesPort();

    /// Check can output.
    if (totals_output.isFinished())
        return Status::Finished;

    if (!totals_output.canPush())
        return Status::PortFull;

    if (!extremes && !extremes_columns.empty())
        return Status::Ready;

    if (extremes)
        totals_output.push(std::move(extremes));

    totals_output.finish();
    return Status::Finished;
}

void ExtremesTransform::work()
{
    if (finished_transform)
    {
        if (!extremes && !extremes_columns.empty())
            extremes.setColumns(std::move(extremes_columns), 2);
    }
    else
        ISimpleTransform::work();
}

void ExtremesTransform::transform(DB::Chunk & chunk)
{

    if (chunk.getNumRows() == 0)
        return;

    size_t num_columns = chunk.getNumColumns();
    const auto & columns = chunk.getColumns();

    if (extremes_columns.empty())
    {
        extremes_columns.resize(num_columns);

        for (size_t i = 0; i < num_columns; ++i)
        {
            const ColumnPtr & src = columns[i];

            if (isColumnConst(*src))
            {
                /// Equal min and max.
                extremes_columns[i] = src->cloneResized(2);
            }
            else
            {
                Field min_value;
                Field max_value;

                src->getExtremes(min_value, max_value, 0, src->size());

                extremes_columns[i] = src->cloneEmpty();

                extremes_columns[i]->insert(min_value);
                extremes_columns[i]->insert(max_value);
            }
        }
    }
    else
    {
        for (size_t i = 0; i < num_columns; ++i)
        {
            if (isColumnConst(*extremes_columns[i]))
                continue;

            Field min_value = (*extremes_columns[i])[0];
            Field max_value = (*extremes_columns[i])[1];

            Field cur_min_value;
            Field cur_max_value;

            columns[i]->getExtremes(cur_min_value, cur_max_value, 0, columns[i]->size());

            // getExtremes implementations for Nullable and floating point are ignoring Nulls, so do the same here
            auto isNullORNaN = [] (const Field & value)
            {
                if (value.isNull())
                    return true;
                Float64 rawVal;
                return value.tryGet<Float64>(rawVal) && isNaN(rawVal);
            };
            if (isNullORNaN(min_value) || (!isNullORNaN(cur_min_value) && cur_min_value < min_value))
                min_value = cur_min_value;
            if (isNullORNaN(max_value) || (!isNullORNaN(cur_max_value) && cur_max_value > max_value))
                max_value = cur_max_value;

            MutableColumnPtr new_extremes = extremes_columns[i]->cloneEmpty();

            new_extremes->insert(min_value);
            new_extremes->insert(max_value);

            extremes_columns[i] = std::move(new_extremes);
        }
    }
}

}
