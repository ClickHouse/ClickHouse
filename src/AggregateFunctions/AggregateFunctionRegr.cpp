#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/Moments.h>
#include <Columns/ColumnVector.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypesNumber.h>

#include <algorithm>
#include <limits>
#include <string_view>

namespace DB
{

struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/// The SQL standard regression aggregates. Every one of them takes the dependent variable first:
/// regr_slope(y, x). They all read the same six sums, so they share one state.
enum class RegrKind : uint8_t
{
    regr_count,
    regr_avgx,
    regr_avgy,
    regr_sxx,
    regr_syy,
    regr_sxy,
    regr_slope,
    regr_intercept,
    regr_r2,
};

using RegrData = CorrMoments<Float64>;

template <RegrKind kind>
constexpr std::string_view regrName()
{
    if constexpr (kind == RegrKind::regr_count) return "regr_count";
    else if constexpr (kind == RegrKind::regr_avgx) return "regr_avgx";
    else if constexpr (kind == RegrKind::regr_avgy) return "regr_avgy";
    else if constexpr (kind == RegrKind::regr_sxx) return "regr_sxx";
    else if constexpr (kind == RegrKind::regr_syy) return "regr_syy";
    else if constexpr (kind == RegrKind::regr_sxy) return "regr_sxy";
    else if constexpr (kind == RegrKind::regr_slope) return "regr_slope";
    else if constexpr (kind == RegrKind::regr_intercept) return "regr_intercept";
    else return "regr_r2";
}

/// The sums of squares are non-negative by construction, but computing them as `Sxx - N * avgx^2`
/// can land just below zero for a constant input, so they are clamped.
struct RegrValues
{
    Float64 n;
    Float64 avg_x;
    Float64 avg_y;
    Float64 sxx;
    Float64 syy;
    Float64 sxy;

    explicit RegrValues(const RegrData & data)
        : n(data.m0)
        , avg_x(data.x1 / n)
        , avg_y(data.y1 / n)
        , sxx(std::max(0.0, data.x2 - data.x1 * avg_x))
        , syy(std::max(0.0, data.y2 - data.y1 * avg_y))
        , sxy(data.xy - data.x1 * avg_y)
    {
    }
};

template <RegrKind kind>
Float64 getRegrResult(const RegrData & data)
{
    static constexpr auto nan = std::numeric_limits<Float64>::quiet_NaN();

    if (data.m0 == 0)
        return nan;

    const RegrValues v(data);

    if constexpr (kind == RegrKind::regr_avgx)
        return v.avg_x;
    else if constexpr (kind == RegrKind::regr_avgy)
        return v.avg_y;
    else if constexpr (kind == RegrKind::regr_sxx)
        return v.sxx;
    else if constexpr (kind == RegrKind::regr_syy)
        return v.syy;
    else if constexpr (kind == RegrKind::regr_sxy)
        return v.sxy;
    else if constexpr (kind == RegrKind::regr_slope)
        return v.sxx == 0 ? nan : v.sxy / v.sxx;
    else if constexpr (kind == RegrKind::regr_intercept)
        return v.sxx == 0 ? nan : v.avg_y - (v.sxy / v.sxx) * v.avg_x;
    else if constexpr (kind == RegrKind::regr_r2)
    {
        /// A vertical line explains none of the variance, a horizontal one explains all of it.
        if (v.sxx == 0)
            return nan;
        if (v.syy == 0)
            return 1.0;
        return (v.sxy * v.sxy) / (v.sxx * v.syy);
    }
    else
    {
        static_assert(kind != RegrKind::regr_count, "regr_count does not go through getRegrResult");
        return nan;
    }
}

template <RegrKind kind>
class AggregateFunctionRegr final : public IAggregateFunctionDataHelper<RegrData, AggregateFunctionRegr<kind>>
{
public:
    static constexpr bool returns_count = (kind == RegrKind::regr_count);
    using ResultType = std::conditional_t<returns_count, UInt64, Float64>;
    using Base = IAggregateFunctionDataHelper<RegrData, AggregateFunctionRegr<kind>>;

    explicit AggregateFunctionRegr(const DataTypes & arguments, const Array & params)
        : Base(arguments, params, std::make_shared<DataTypeNumber<ResultType>>())
    {
    }

    String getName() const override { return String(regrName<kind>()); }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        /// The state stores the independent variable as x, so the arguments are swapped here.
        this->data(place).add(columns[1]->getFloat64(row_num), columns[0]->getFloat64(row_num));
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t>) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t>, Arena *) const override
    {
        this->data(place).read(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        const auto & data = this->data(place);
        auto & column = assert_cast<ColumnVector<ResultType> &>(to);

        if constexpr (returns_count)
            column.getData().push_back(static_cast<UInt64>(data.m0));
        else
            column.getData().push_back(getRegrResult<kind>(data));
    }
};

template <RegrKind kind>
AggregateFunctionPtr createAggregateFunctionRegr(
    const String & name, const DataTypes & arguments, const Array & params, const Settings *)
{
    assertNoParameters(name, params);
    assertBinary(name, arguments);

    if (!isNumber(arguments[0]) || !isNumber(arguments[1]))
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal types ({}, {}) of arguments of aggregate function {}, must be Native Ints, Native UInts or Floats",
            arguments[0]->getName(), arguments[1]->getName(), name);

    return std::make_shared<AggregateFunctionRegr<kind>>(arguments, params);
}

}

void registerAggregateFunctionsRegr(AggregateFunctionFactory & factory);
void registerAggregateFunctionsRegr(AggregateFunctionFactory & factory)
{
    const FunctionDocumentation::Arguments arguments = {
        {"y", "Dependent variable.", {"(U)Int*", "Float*"}},
        {"x", "Independent variable.", {"(U)Int*", "Float*"}}};

    auto make_documentation = [&](const String & name, const String & description, const String & returned, const String & result)
    {
        FunctionDocumentation::Examples examples = {
            {"Basic usage",
             R"(
SELECT )" + name + R"((y, x)
FROM VALUES('x Float64, y Float64', (1, 2), (2, 4), (3, 6), (4, 8))
             )",
             R"(
┌─)" + name + R"((y, x)─┐
│ )" + result + R"( │
└──────────────┘
             )"}};

        return FunctionDocumentation{
            description,
            name + "(y, x)",
            arguments,
            {},
            {returned, {name == "regr_count" ? "UInt64" : "Float64"}},
            examples,
            {26, 9},
            FunctionDocumentation::Category::AggregateFunction};
    };

    factory.registerFunction(
        "regr_count",
        {createAggregateFunctionRegr<RegrKind::regr_count>,
         make_documentation(
             "regr_count",
             "Counts the rows where both `y` and `x` are not `NULL`.",
             "Returns the number of non-null pairs.",
             "            4")},
        AggregateFunctionFactory::Case::Insensitive);

    factory.registerFunction(
        "regr_avgx",
        {createAggregateFunctionRegr<RegrKind::regr_avgx>,
         make_documentation("regr_avgx", "Returns the average of the independent variable.", "Returns `avg(x)`.", "          2.5")},
        AggregateFunctionFactory::Case::Insensitive);

    factory.registerFunction(
        "regr_avgy",
        {createAggregateFunctionRegr<RegrKind::regr_avgy>,
         make_documentation("regr_avgy", "Returns the average of the dependent variable.", "Returns `avg(y)`.", "            5")},
        AggregateFunctionFactory::Case::Insensitive);

    factory.registerFunction(
        "regr_sxx",
        {createAggregateFunctionRegr<RegrKind::regr_sxx>,
         make_documentation(
             "regr_sxx",
             R"(
Returns the sum of squares of the independent variable:

$$
\Sigma{(x - \bar{x})^2}
$$
             )",
             "Returns the sum of squares of `x` around its mean.",
             "            5")},
        AggregateFunctionFactory::Case::Insensitive);

    factory.registerFunction(
        "regr_syy",
        {createAggregateFunctionRegr<RegrKind::regr_syy>,
         make_documentation(
             "regr_syy",
             R"(
Returns the sum of squares of the dependent variable:

$$
\Sigma{(y - \bar{y})^2}
$$
             )",
             "Returns the sum of squares of `y` around its mean.",
             "           20")},
        AggregateFunctionFactory::Case::Insensitive);

    factory.registerFunction(
        "regr_sxy",
        {createAggregateFunctionRegr<RegrKind::regr_sxy>,
         make_documentation(
             "regr_sxy",
             R"(
Returns the sum of products of the two variables:

$$
\Sigma{(x - \bar{x})(y - \bar{y})}
$$
             )",
             "Returns the sum of products of `x` and `y` around their means.",
             "           10")},
        AggregateFunctionFactory::Case::Insensitive);

    factory.registerFunction(
        "regr_slope",
        {createAggregateFunctionRegr<RegrKind::regr_slope>,
         make_documentation(
             "regr_slope",
             R"(
Returns the slope of the least-squares fit of `y` on `x`:

$$
\frac{\Sigma{(x - \bar{x})(y - \bar{y})}}{\Sigma{(x - \bar{x})^2}}
$$

Returns `nan` when `x` is constant, because the fitted line is vertical.
             )",
             "Returns the slope of the regression line.",
             "            2")},
        AggregateFunctionFactory::Case::Insensitive);

    factory.registerFunction(
        "regr_intercept",
        {createAggregateFunctionRegr<RegrKind::regr_intercept>,
         make_documentation(
             "regr_intercept",
             R"(
Returns the intercept of the least-squares fit of `y` on `x`:

$$
\bar{y} - slope \cdot \bar{x}
$$

Returns `nan` when `x` is constant, because the fitted line is vertical.
             )",
             "Returns the intercept of the regression line.",
             "            0")},
        AggregateFunctionFactory::Case::Insensitive);

    factory.registerFunction(
        "regr_r2",
        {createAggregateFunctionRegr<RegrKind::regr_r2>,
         make_documentation(
             "regr_r2",
             R"(
Returns the coefficient of determination of the least-squares fit, that is, the square of the
correlation coefficient. Returns `nan` when `x` is constant, and `1` when `y` is constant.
             )",
             "Returns a value between 0 and 1.",
             "            1")},
        AggregateFunctionFactory::Case::Insensitive);
}

}
