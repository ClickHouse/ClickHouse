#include <WindowFunctions/Aggregate/AggregateWindowFunctionPrefix.h>
#include <WindowFunctions/Aggregate/AggregateState.h>

#include <Columns/IColumn.h>
#include <Interpreters/WindowDescription.h>
#include <WindowFunctions/Common/WindowBounds.h>

namespace DB
{

namespace
{

class AggregateWindowFunctionPrefix final : public IAggregateWindowFunction
{
public:
    explicit AggregateWindowFunctionPrefix(AggregateFunctionPtr function)
        : state(std::move(function))
    {
    }

    void resetPartition() override
    {
        state.recreate();
        covered_end = 0;
    }

    Int64 evaluate(const WindowBounds & bounds, const WindowArguments & arguments, IColumn & result) override
    {
        for (Int64 from = 0; from < bounds.rows;)
        {
            const Int64 to = findNextFrameChange(bounds, from, /*account_start=*/ false, /*account_end=*/ true);
            const Int64 frame_end = bounds.frame_end[from];
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

        return covered_end;
    }

private:
    AggregateState state;
    Int64 covered_end = 0;
};

}

AggregateWindowFunctionPtr tryCreateAggregateWindowFunctionPrefix(const AggregateFunctionPtr & function, const WindowDescription & window)
{
    if (window.frame.begin_type == WindowFrame::BoundaryType::Unbounded)
        return std::make_shared<AggregateWindowFunctionPrefix>(function);

    return nullptr;
}

}
