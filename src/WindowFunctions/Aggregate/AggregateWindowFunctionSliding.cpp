#include <WindowFunctions/Aggregate/AggregateWindowFunctionSliding.h>
#include <WindowFunctions/Aggregate/AggregateState.h>
#include <WindowFunctions/Common/WindowArguments.h>
#include <WindowFunctions/Common/WindowBounds.h>

#include <AggregateFunctions/IAggregateFunction.h>

#include <Interpreters/WindowDescription.h>

#include <Columns/IColumn.h>

#include <Common/Arena.h>
#include <Common/VectorWithMemoryTracking.h>

#include <base/types.h>

#include <algorithm>
#include <limits>
#include <memory>

namespace DB
{

namespace
{

constexpr size_t MAX_STATE_BYTES = 256;
constexpr size_t ROWS_IN_CHUNK = 8;

class AggregationStack
{
public:
    explicit AggregationStack(AggregateFunctionPtr function_)
        : function(std::move(function_))
    {
    }

    [[nodiscard]] ConstAggregateDataPtr getTotalState() const
    {
        return states.back();
    }

    void pushChunk(const WindowArguments & arguments, Int64 rows_begin, Int64 rows_end)
    {
        if (!arena)
            arena = std::make_unique<Arena>();

        AggregateDataPtr new_state = arena->alignedAlloc(std::max<size_t>(function->sizeOfData(), 1), function->alignOfData());
        function->create(new_state);
        while (rows_begin < rows_end)
        {
            const ArgumentsSegment segment = arguments.segment(rows_begin, rows_end);
            function->addBatchSinglePlace(segment.row_begin, segment.row_end, new_state, const_cast<const IColumn **>(segment.columns.data()), nullptr);
            rows_begin += segment.row_end - segment.row_begin;
        }

        if (!states.empty())
            function->merge(new_state, states.back(), nullptr);

        states.push_back(new_state);
    }

    void popChunk()
    {
        states.pop_back();
    }

    void clear()
    {
        arena.reset();
        states.clear();
    }

private:
    const AggregateFunctionPtr function;
    std::unique_ptr<Arena> arena;
    VectorWithMemoryTracking<AggregateDataPtr> states;
};

/// The aggregate over a window of rows that only moves forward. A move costs the same for any window size.
/// It is the two-stack sliding window aggregation: https://orlp.net/blog/two-stack-sliding-window-aggregation/
///
/// It holds the rows [`begin`, `end`), cut in two at `midpoint`:
///
///     begin                   midpoint               end
///       <-----------------------> <-------------------->
///       | chunk | chunk | chunk  |      right part     |
///       \_____ left_stack ______/ \____ right_state ___/
///
class SlidingAggregation
{
public:
    SlidingAggregation(AggregateFunctionPtr function, Int64 rows_in_chunk_)
        : rows_in_chunk(rows_in_chunk_)
        , left_stack(function)
        , right_state(function)
    {
    }

    [[nodiscard]] Int64 getCoveredBegin() const
    {
        return begin;
    }

    [[nodiscard]] Int64 getCoveredEnd() const
    {
        return end;
    }

    void reset()
    {
        left_stack.clear();
        right_state.recreate();
        begin = 0;
        midpoint = 0;
        end = 0;
    }

    void pushRowsUntil(const WindowArguments & arguments, Int64 row)
    {
        right_state.add(arguments, end, row);
        end = row;
    }

    void popRowsUntil(const WindowArguments & arguments, Int64 row)
    {
        end = std::max(end, row);
        if (midpoint < row)
        {
            left_stack.clear();
            begin = end;
            while (begin - rows_in_chunk >= row)
            {
                left_stack.pushChunk(arguments, begin - rows_in_chunk, begin);
                begin -= rows_in_chunk;
            }

            right_state.recreate();
            midpoint = end;
        }

        while (begin < row)
        {
            left_stack.popChunk();
            begin += rows_in_chunk;
        }
    }

    void mergeTotalStateInto(AggregateState & state) const
    {
        if (begin < midpoint)
            state.merge(left_stack.getTotalState());

        state.merge(right_state.getData());
    }

private:
    const Int64 rows_in_chunk;
    AggregationStack left_stack;
    AggregateState right_state;
    Int64 begin = 0;
    Int64 midpoint = 0;
    Int64 end = 0;
};

class AggregateWindowFunctionSliding final : public IAggregateWindowFunction
{
public:
    AggregateWindowFunctionSliding(AggregateFunctionPtr function, Int64 min_frame_rows_)
        : min_frame_rows(min_frame_rows_)
        , state(function)
        , aggregation(function, ROWS_IN_CHUNK)
    {
    }

    void resetPartition() override
    {
        state.recreate();
        aggregation.reset();
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
                if (frame_end - frame_start >= min_frame_rows)
                {
                    aggregation.popRowsUntil(arguments, frame_start);
                    aggregation.pushRowsUntil(arguments, frame_end);
                    state.add(arguments, frame_start, aggregation.getCoveredBegin());
                    aggregation.mergeTotalStateInto(state);
                    covered_end = aggregation.getCoveredEnd();
                }
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
    const Int64 min_frame_rows;
    AggregateState state;
    SlidingAggregation aggregation;
    Int64 covered_start = 0;
    Int64 covered_end = 0;
};

}

AggregateWindowFunctionPtr tryCreateAggregateWindowFunctionSliding(const AggregateFunctionPtr & function, const WindowDescription & window, UInt64 min_frame_rows)
{
    if (min_frame_rows == 0)
        return nullptr;

    if (window.frame.begin_type == WindowFrame::BoundaryType::Unbounded)
        return nullptr;

    if (function->getArgumentTypes().empty())
        return nullptr;

    if (!function->hasTrivialDestructor() || function->allocatesMemoryInArena())
        return nullptr;

    if (function->sizeOfData() > MAX_STATE_BYTES)
        return nullptr;

    return std::make_shared<AggregateWindowFunctionSliding>(function, static_cast<Int64>(std::min<UInt64>(min_frame_rows, std::numeric_limits<Int64>::max())));
}

}
