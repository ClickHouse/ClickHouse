#include <WindowFunctions/Aggregate/AggregateWindowFunctionRecalculate.h>

#include <Columns/IColumn.h>
#include <WindowFunctions/Common/WindowBounds.h>

namespace DB
{

namespace
{

Int64 equalFrameRun(const WindowBounds & bounds, Int64 from)
{
    for (Int64 to = from + 1; to < bounds.rows; ++to)
        if (bounds.frame_start[to] != bounds.frame_start[from] || bounds.frame_end[to] != bounds.frame_end[from])
            return to;

    return bounds.rows;
}

}

AggregateWindowFunctionRecalculate::AggregateWindowFunctionRecalculate(AggregateFunctionPtr function)
    : state(std::move(function))
{
}

void AggregateWindowFunctionRecalculate::resetPartition()
{
    state.recreate();
    covered_start = 0;
    covered_end = 0;
}

Int64 AggregateWindowFunctionRecalculate::evaluate(const WindowBounds & bounds, const WindowArguments & arguments, IColumn & result)
{
    for (Int64 from = 0; from < bounds.rows;)
    {
        const Int64 to = equalFrameRun(bounds, from);
        const Int64 frame_start = bounds.frame_start[from];
        const Int64 frame_end = bounds.frame_end[from];
        if (frame_start != covered_start)
        {
            state.recreate();
            covered_start = frame_start;
            covered_end = frame_start;
        }

        if (covered_end < frame_end)
        {
            state.add(arguments, covered_end, frame_end);
            covered_end = frame_end;
        }

        state.insertResultInto(result, frame_end - frame_start);
        for (Int64 row = from + 1; row < to; ++row)
            result.insertRangeFrom(result, result.size() - 1, 1);
        from = to;
    }

    return covered_start;
}

}
