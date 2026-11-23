#include <WindowFunctions/Aggregate/AggregateWindowFunctionCrossTab.h>
#include <WindowFunctions/Common/WindowArguments.h>
#include <WindowFunctions/Common/WindowBounds.h>

#include <AggregateFunctions/AggregateFunctionTheilsU.h>
#include <AggregateFunctions/CrossTab.h>
#include <AggregateFunctions/UniqVariadicHash.h>
#include <Interpreters/CancellationBudget.h>
#include <Interpreters/WindowDescription.h>

#include <Columns/ColumnsNumber.h>

#include <Common/assert_cast.h>

#include <base/defines.h>

#include <cmath>
#include <functional>
#include <limits>
#include <optional>

namespace DB
{

namespace
{

enum class PhiSquaredStatistic : uint8_t
{
    CramersV,
    CramersVBiasCorrected,
    Contingency,
};

Float64 cramersV(const CrossTabPhiSquaredWindowData & data)
{
    if (data.count < 2)
        return std::numeric_limits<Float64>::quiet_NaN();

    const UInt64 q = std::min(data.a_marginal_count.size(), data.b_marginal_count.size());
    if (q <= 1)
        return std::numeric_limits<Float64>::quiet_NaN();

    return std::sqrt(data.getPhiSquared() / static_cast<Float64>(q - 1));
}

Float64 cramersVBiasCorrected(const CrossTabPhiSquaredWindowData & data)
{
    if (data.count < 2)
        return std::numeric_limits<Float64>::quiet_NaN();

    const Float64 phi = data.getPhiSquared();

    const Float64 a_size = static_cast<Float64>(data.a_marginal_count.size());
    const Float64 b_size = static_cast<Float64>(data.b_marginal_count.size());

    const Float64 a_size_adjusted = a_size - 1;
    const Float64 b_size_adjusted = b_size - 1;
    const Float64 count_adjusted = static_cast<Float64>(data.count - 1);

    const Float64 res = std::max(0.0, phi - a_size_adjusted * b_size_adjusted / count_adjusted);
    const Float64 correction_a = a_size - a_size_adjusted * a_size_adjusted / count_adjusted;
    const Float64 correction_b = b_size - b_size_adjusted * b_size_adjusted / count_adjusted;

    const Float64 denom = std::min(correction_a, correction_b) - 1.0;
    if (denom <= 0.0)
        return std::numeric_limits<Float64>::quiet_NaN();

    return std::sqrt(res / denom);
}

Float64 contingency(const CrossTabPhiSquaredWindowData & data)
{
    if (data.count < 2)
        return std::numeric_limits<Float64>::quiet_NaN();

    const Float64 phi_sq = data.getPhiSquared();
    return std::sqrt(phi_sq / (phi_sq + 1.0));
}

Float64 calculateStatistic(PhiSquaredStatistic statistic, const CrossTabPhiSquaredWindowData & data)
{
    switch (statistic)
    {
        case PhiSquaredStatistic::CramersV:
            return cramersV(data);
        case PhiSquaredStatistic::CramersVBiasCorrected:
            return cramersVBiasCorrected(data);
        case PhiSquaredStatistic::Contingency:
            return contingency(data);
    }

    UNREACHABLE();
}

const char * statisticName(PhiSquaredStatistic statistic)
{
    switch (statistic)
    {
        case PhiSquaredStatistic::CramersV:
            return "cramersV";
        case PhiSquaredStatistic::CramersVBiasCorrected:
            return "cramersVBiasCorrected";
        case PhiSquaredStatistic::Contingency:
            return "contingency";
    }

    UNREACHABLE();
}

UInt64 hashArgument(const ArgumentsSegment & segment, size_t argument, size_t row)
{
    return UniqVariadicHash<false, false>::apply(1, const_cast<const IColumn **>(segment.columns.data()) + argument, row);
}

Int64 equalFrameEndRun(const WindowBounds & bounds, Int64 from)
{
    for (Int64 to = from + 1; to < bounds.rows; ++to)
        if (bounds.frame_end[to] != bounds.frame_end[from])
            return to;

    return bounds.rows;
}

class AggregateWindowFunctionPhiSquared final : public IAggregateWindowFunction
{
public:
    explicit AggregateWindowFunctionPhiSquared(PhiSquaredStatistic statistic_)
        : statistic(statistic_)
        , check_cancellation(makeCancellationCheck(statisticName(statistic_)))
        , budget(check_cancellation)
    {
        data.emplace();
    }

    void resetPartition() override
    {
        data.emplace();
        covered_end = 0;
    }

    Int64 evaluate(const WindowBounds & bounds, const WindowArguments & arguments, IColumn & result) override
    {
        auto & values = assert_cast<ColumnFloat64 &>(result).getData();

        for (Int64 from = 0; from < bounds.rows;)
        {
            const Int64 to = equalFrameEndRun(bounds, from);
            const Int64 frame_end = bounds.frame_end[from];
            while (covered_end < frame_end)
            {
                const ArgumentsSegment segment = arguments.segment(covered_end, frame_end);
                budget.chargeUnits(segment.row_end - segment.row_begin);
                for (size_t row = segment.row_begin; row < segment.row_end; ++row)
                    data->add(hashArgument(segment, 0, row), hashArgument(segment, 1, row));
                covered_end += segment.row_end - segment.row_begin;
            }

            values.resize_fill(values.size() + static_cast<size_t>(to - from), calculateStatistic(statistic, *data));
            from = to;
        }

        return covered_end;
    }

private:
    const PhiSquaredStatistic statistic;
    std::optional<CrossTabPhiSquaredWindowData> data;
    std::function<void()> check_cancellation;
    CancellationBudget budget;
    Int64 covered_end = 0;
};

class AggregateWindowFunctionTheilsU final : public IAggregateWindowFunction
{
public:
    AggregateWindowFunctionTheilsU()
        : check_cancellation(makeCancellationCheck("theilsU"))
        , budget(check_cancellation)
    {
        data.emplace();
    }

    void resetPartition() override
    {
        data.emplace();
        covered_end = 0;
    }

    Int64 evaluate(const WindowBounds & bounds, const WindowArguments & arguments, IColumn & result) override
    {
        auto & values = assert_cast<ColumnFloat64 &>(result).getData();

        for (Int64 from = 0; from < bounds.rows;)
        {
            const Int64 to = equalFrameEndRun(bounds, from);
            const Int64 frame_end = bounds.frame_end[from];
            while (covered_end < frame_end)
            {
                const ArgumentsSegment segment = arguments.segment(covered_end, frame_end);
                budget.chargeUnits(segment.row_end - segment.row_begin);
                for (size_t row = segment.row_begin; row < segment.row_end; ++row)
                    data->add(hashArgument(segment, 0, row), hashArgument(segment, 1, row));
                covered_end += segment.row_end - segment.row_begin;
            }

            values.resize_fill(values.size() + static_cast<size_t>(to - from), data->getResult());
            from = to;
        }

        return covered_end;
    }

private:
    std::optional<TheilsUWindowData> data;
    std::function<void()> check_cancellation;
    CancellationBudget budget;
    Int64 covered_end = 0;
};

}

AggregateWindowFunctionPtr tryCreateAggregateWindowFunctionCrossTab(const AggregateFunctionPtr & function, const WindowDescription & window)
{
    if (window.frame.begin_type == WindowFrame::BoundaryType::Unbounded && !function->getNestedFunction())
    {
        const String name = function->getName();
        if (name == "cramersV")
            return std::make_shared<AggregateWindowFunctionPhiSquared>(PhiSquaredStatistic::CramersV);
        if (name == "cramersVBiasCorrected")
            return std::make_shared<AggregateWindowFunctionPhiSquared>(PhiSquaredStatistic::CramersVBiasCorrected);
        if (name == "contingency")
            return std::make_shared<AggregateWindowFunctionPhiSquared>(PhiSquaredStatistic::Contingency);
        if (name == "theilsU")
            return std::make_shared<AggregateWindowFunctionTheilsU>();
    }

    return nullptr;
}

}
