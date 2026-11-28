#include <WindowFunctions/Aggregate/AggregateWindowFunctionRecalculate.h>
#include <WindowFunctions/Aggregate/AggregateState.h>

#include <Columns/IColumn.h>
#include <WindowFunctions/Common/WindowBounds.h>

namespace DB
{

namespace
{

class AggregateWindowFunctionRecalculate final : public IAggregateWindowFunction
{
public:
    explicit AggregateWindowFunctionRecalculate(AggregateFunctionPtr function)
        : state(std::move(function))
    {
    }

    void resetPartition() override
    {
        state.recreate();
        covered_start = 0;
        covered_end = 0;
    }

    Int64 evaluate(const WindowBounds & bounds, const WindowArguments & arguments, IColumn & result) override
    {
        for (Int64 from = 0; from < bounds.rows;)
        {
            const Int64 to = findNextFrameChange(bounds, from, /*account_start=*/ true, /*account_end=*/ true);
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

            state.insertResultInto(result);
            for (Int64 row = from + 1; row < to; ++row)
                result.insertRangeFrom(result, result.size() - 1, 1);
            from = to;
        }

        return covered_start;
    }

private:
    AggregateState state;
    Int64 covered_start = 0;
    Int64 covered_end = 0;
};

}

AggregateWindowFunctionPtr tryCreateAggregateWindowFunctionRecalculate(const AggregateFunctionPtr & function, const WindowDescription &)
{
    return std::make_shared<AggregateWindowFunctionRecalculate>(function);
}

}
