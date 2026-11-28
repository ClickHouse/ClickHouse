#include <WindowFunctions/Aggregate/AggregateWindowFunctionCrossTab.h>
#include <WindowFunctions/Common/WindowArguments.h>
#include <WindowFunctions/Common/WindowBounds.h>

#include <AggregateFunctions/AggregateFunctionTheilsU.h>
#include <AggregateFunctions/CrossTab.h>
#include <AggregateFunctions/UniqVariadicHash.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/CancellationBudget.h>
#include <Interpreters/WindowDescription.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>

#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <base/defines.h>

#include <algorithm>
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
}

UInt64 hashArgument(const ArgumentsSegment & segment, size_t argument, size_t row)
{
    return UniqVariadicHash<false, false>::apply(1, const_cast<const IColumn **>(segment.columns.data()) + argument, row);
}

bool passesFilter(const ArgumentsSegment & segment, size_t row)
{
    const bool first_is_null = segment.columns[0]->isNullAt(row);
    const bool second_is_null = segment.columns[1]->isNullAt(row);
    const bool condition_is_true = segment.columns.size() == 2 || segment.columns[2]->getBool(row);
    return !first_is_null && !second_is_null && condition_is_true;
}

class AggregateWindowFunctionPhiSquared final : public IAggregateWindowFunction
{
public:
    AggregateWindowFunctionPhiSquared(PhiSquaredStatistic statistic_, bool filters_rows_)
        : statistic(statistic_)
        , filters_rows(filters_rows_)
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
        auto * nullable = typeid_cast<ColumnNullable *>(&result);
        auto & values = assert_cast<ColumnFloat64 &>(nullable ? nullable->getNestedColumn() : result).getData();

        for (Int64 from = 0; from < bounds.rows;)
        {
            const Int64 to = findNextFrameChange(bounds, from, /*account_start=*/ false, /*account_end=*/ true);
            const Int64 frame_end = bounds.frame_end[from];
            while (covered_end < frame_end)
            {
                const ArgumentsSegment segment = arguments.segment(covered_end, frame_end);
                budget.chargeUnits(segment.row_end - segment.row_begin);
                for (size_t row = segment.row_begin; row < segment.row_end; ++row)
                    if (!filters_rows || passesFilter(segment, row))
                        data->add(hashArgument(segment, 0, row), hashArgument(segment, 1, row));
                covered_end += segment.row_end - segment.row_begin;
            }

            values.resize_fill(values.size() + static_cast<size_t>(to - from), calculateStatistic(statistic, *data));
            if (nullable)
                nullable->getNullMapData().resize_fill(values.size(), data->count == 0);
            from = to;
        }

        return covered_end;
    }

private:
    const PhiSquaredStatistic statistic;
    const bool filters_rows;

    std::optional<CrossTabPhiSquaredWindowData> data;
    Int64 covered_end = 0;

    std::function<void()> check_cancellation;
    CancellationBudget budget;
};

class AggregateWindowFunctionTheilsU final : public IAggregateWindowFunction
{
public:
    explicit AggregateWindowFunctionTheilsU(bool filters_rows_)
        : filters_rows(filters_rows_)
        , check_cancellation(makeCancellationCheck("theilsU"))
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
        auto * nullable = typeid_cast<ColumnNullable *>(&result);
        auto & values = assert_cast<ColumnFloat64 &>(nullable ? nullable->getNestedColumn() : result).getData();

        for (Int64 from = 0; from < bounds.rows;)
        {
            const Int64 to = findNextFrameChange(bounds, from, /*account_start=*/ false, /*account_end=*/ true);
            const Int64 frame_end = bounds.frame_end[from];
            while (covered_end < frame_end)
            {
                const ArgumentsSegment segment = arguments.segment(covered_end, frame_end);
                budget.chargeUnits(segment.row_end - segment.row_begin);
                for (size_t row = segment.row_begin; row < segment.row_end; ++row)
                    if (!filters_rows || passesFilter(segment, row))
                        data->add(hashArgument(segment, 0, row), hashArgument(segment, 1, row));
                covered_end += segment.row_end - segment.row_begin;
            }

            values.resize_fill(values.size() + static_cast<size_t>(to - from), data->getResult());
            if (nullable)
                nullable->getNullMapData().resize_fill(values.size(), data->count == 0);
            from = to;
        }

        return covered_end;
    }

private:
    const bool filters_rows;

    std::optional<TheilsUWindowData> data;
    Int64 covered_end = 0;

    std::function<void()> check_cancellation;
    CancellationBudget budget;
};

String getNameWithoutIf(const IAggregateFunction & function)
{
    String name = function.getName();
    if (name.ends_with("If"))
        name.resize(name.size() - 2);

    return name;
}

bool hasNullableArgument(const IAggregateFunction & function)
{
    return std::ranges::any_of(function.getArgumentTypes(), [](const DataTypePtr & type) { return type->isNullable(); });
}

bool hasArgumentWithInternalNulls(const IAggregateFunction & function)
{
    return std::ranges::any_of(function.getArgumentTypes(), [](const DataTypePtr & type) { return canContainNull(*type) && !type->isNullable(); });
}

}

AggregateWindowFunctionPtr tryCreateAggregateWindowFunctionCrossTab(const AggregateFunctionPtr & function, const WindowDescription & window)
{
    const String name = getNameWithoutIf(*function);
    if (window.frame.begin_type == WindowFrame::BoundaryType::Unbounded && !hasArgumentWithInternalNulls(*function))
    {
        const bool filters_rows = name != function->getName() || hasNullableArgument(*function);
        if (name == "cramersV")
            return std::make_shared<AggregateWindowFunctionPhiSquared>(PhiSquaredStatistic::CramersV, filters_rows);
        if (name == "cramersVBiasCorrected")
            return std::make_shared<AggregateWindowFunctionPhiSquared>(PhiSquaredStatistic::CramersVBiasCorrected, filters_rows);
        if (name == "contingency")
            return std::make_shared<AggregateWindowFunctionPhiSquared>(PhiSquaredStatistic::Contingency, filters_rows);
        if (name == "theilsU")
            return std::make_shared<AggregateWindowFunctionTheilsU>(filters_rows);
    }

    return nullptr;
}

}
