#include <WindowFunctions/Aggregate/AggregateWindowFunctionPrefix.h>

#include <Columns/IColumn.h>
#include <WindowFunctions/Common/WindowBounds.h>

namespace DB
{

namespace
{

Int64 equalFrameEndRun(const WindowBounds & bounds, Int64 from)
{
    for (Int64 to = from + 1; to < bounds.rows; ++to)
        if (bounds.frame_end[to] != bounds.frame_end[from])
            return to;

    return bounds.rows;
}

}

AggregateWindowFunctionPrefix::AggregateWindowFunctionPrefix(AggregateFunctionPtr function)
    : state(std::move(function))
{
}

void AggregateWindowFunctionPrefix::resetPartition()
{
    state.recreate();
    covered_end = 0;
}

Int64 AggregateWindowFunctionPrefix::evaluate(const WindowBounds & bounds, const WindowArguments & arguments, IColumn & result)
{
    for (Int64 from = 0; from < bounds.rows;)
    {
        const Int64 to = equalFrameEndRun(bounds, from);
        const Int64 frame_end = bounds.frame_end[from];
        if (covered_end < frame_end)
        {
            state.add(arguments, covered_end, frame_end);
            covered_end = frame_end;
        }

        state.insertResultInto(result, frame_end);
        for (Int64 row = from + 1; row < to; ++row)
            result.insertRangeFrom(result, result.size() - 1, 1);
        from = to;
    }

    return covered_end;
}

}
