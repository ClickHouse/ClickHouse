#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/Moments.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <algorithm>
#include <limits>
#include <memory>

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
/// regr_slope(y, x). They are all read off the same six sums, so they share one state.
///
/// regr_count is not here: it is an exact row count rather than a value derived from the sums, so
/// it keeps its own UInt64 counter below instead of rounding the Float64 one of the moments.
enum class RegrKind : uint8_t
{
    regr_avgx,
    regr_avgy,
    regr_sxx,
    regr_syy,
    regr_sxy,
    regr_slope,
    regr_intercept,
    regr_r2,
};

constexpr std::string_view regrKindName(RegrKind kind)
{
    switch (kind)
    {
        case RegrKind::regr_avgx: return "regr_avgx";
        case RegrKind::regr_avgy: return "regr_avgy";
        case RegrKind::regr_sxx: return "regr_sxx";
        case RegrKind::regr_syy: return "regr_syy";
        case RegrKind::regr_sxy: return "regr_sxy";
        case RegrKind::regr_slope: return "regr_slope";
        case RegrKind::regr_intercept: return "regr_intercept";
        case RegrKind::regr_r2: return "regr_r2";
    }
}

/// Computes the regression aggregates of a finished state.
///
/// The sums of squares are non-negative by construction, but computing them as `Sxx - n * avgx^2`
/// can land just below zero for a constant input, so they are clamped.
template <typename T>
struct RegrResult
{
    T n;
    T avg_x;
    T avg_y;
    T sxx;
    T syy;
    T sxy;

    template <typename Data>
    explicit RegrResult(const Data & data)
        : n(data.m0)
        , avg_x(data.x1 / n)
        , avg_y(data.y1 / n)
        , sxx(std::max(T{0}, data.x2 - data.x1 * avg_x))
        , syy(std::max(T{0}, data.y2 - data.y1 * avg_y))
        , sxy(data.xy - data.x1 * avg_y)
    {
    }

    T get(RegrKind kind) const
    {
        static constexpr auto nan = std::numeric_limits<T>::quiet_NaN();

        switch (kind)
        {
            case RegrKind::regr_avgx:
                return avg_x;
            case RegrKind::regr_avgy:
                return avg_y;
            case RegrKind::regr_sxx:
                return sxx;
            case RegrKind::regr_syy:
                return syy;
            case RegrKind::regr_sxy:
                return sxy;
            case RegrKind::regr_slope:
                /// A constant x makes the fitted line vertical, so it has no slope.
                return sxx == 0 ? nan : sxy / sxx;
            case RegrKind::regr_intercept:
                return sxx == 0 ? nan : avg_y - (sxy / sxx) * avg_x;
            case RegrKind::regr_r2:
                /// A vertical line explains none of the variance, a horizontal one explains all of it.
                if (sxx == 0)
                    return nan;
                if (syy == 0)
                    return T{1};
                return (sxy * sxy) / (sxx * syy);
        }
    }
};

/// TY is the type of the dependent variable (the first argument), TX of the independent one.
template <typename TY, typename TX>
class AggregateFunctionRegr final
    : public IAggregateFunctionDataHelper<
          CorrMoments<std::conditional_t<std::is_same_v<TY, TX> && std::is_same_v<TY, Float32>, Float32, Float64>>,
          AggregateFunctionRegr<TY, TX>>
{
public:
    using ResultType = std::conditional_t<std::is_same_v<TY, TX> && std::is_same_v<TY, Float32>, Float32, Float64>;
    using Data = CorrMoments<ResultType>;
    using ColVecTY = ColumnVector<TY>;
    using ColVecTX = ColumnVector<TX>;
    using ColVecResult = ColumnVector<ResultType>;
    using Base = IAggregateFunctionDataHelper<Data, AggregateFunctionRegr<TY, TX>>;

    explicit AggregateFunctionRegr(const DataTypes & argument_types_, RegrKind kind_)
        : Base(argument_types_, {}, std::make_shared<DataTypeNumber<ResultType>>())
        , kind(kind_)
    {
    }

    String getName() const override { return String(regrKindName(kind)); }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        /// The state holds the independent variable as x, so the arguments are swapped here.
        this->data(place).add(
            static_cast<ResultType>(static_cast<const ColVecTX &>(*columns[1]).getData()[row_num]),
            static_cast<ResultType>(static_cast<const ColVecTY &>(*columns[0]).getData()[row_num]));
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena *,
        ssize_t if_argument_pos) const override
    {
        const auto * x_ptr = static_cast<const ColVecTX &>(*columns[1]).getData().data();
        const auto * y_ptr = static_cast<const ColVecTY &>(*columns[0]).getData().data();
        auto & data = this->data(place);

        if (if_argument_pos >= 0)
        {
            const auto * flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data();
            data.template addManyConditional<TX, TY, false>(x_ptr, y_ptr, flags, row_begin, row_end);
        }
        else
        {
            data.addMany(x_ptr, y_ptr, row_begin, row_end);
        }
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena *,
        ssize_t if_argument_pos) const override
    {
        const auto * x_ptr = static_cast<const ColVecTX &>(*columns[1]).getData().data();
        const auto * y_ptr = static_cast<const ColVecTY &>(*columns[0]).getData().data();
        auto & data = this->data(place);

        if (if_argument_pos >= 0)
        {
            /// Merging the two sets of flags into a temporary buffer vectorizes better
            /// than fusing both flags into the accumulation loop.
            const auto * if_flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data();
            /// Default-init: the loop below fills [row_begin, row_end) and nothing reads the rest.
            std::unique_ptr<UInt8[]> final_flags(new UInt8[row_end]);
            for (size_t i = row_begin; i < row_end; ++i)
                final_flags[i] = (!null_map[i]) & !!if_flags[i];

            data.template addManyConditional<TX, TY, false>(x_ptr, y_ptr, final_flags.get(), row_begin, row_end);
        }
        else
        {
            data.template addManyConditional<TX, TY, true>(x_ptr, y_ptr, null_map, row_begin, row_end);
        }
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
        auto & dst = static_cast<ColVecResult &>(to).getData();

        if (data.m0 == 0)
            dst.push_back(std::numeric_limits<ResultType>::quiet_NaN());
        else
            dst.push_back(RegrResult<ResultType>(data).get(kind));
    }

private:
    RegrKind kind;
};

/// regr_count is the number of rows where neither argument is NULL. It reads no values, and it is
/// counted in UInt64 rather than taken from the Float64 counter of the moments, which stops
/// counting in whole numbers past 2^53.
struct AggregateFunctionRegrCountData
{
    /// The state is created with `new (place) Data`, which leaves a bare UInt64 uninitialized.
    UInt64 count = 0;
};

class AggregateFunctionRegrCount final
    : public IAggregateFunctionDataHelper<AggregateFunctionRegrCountData, AggregateFunctionRegrCount>
{
public:
    explicit AggregateFunctionRegrCount(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<AggregateFunctionRegrCountData, AggregateFunctionRegrCount>(
              argument_types_, {}, std::make_shared<DataTypeUInt64>())
    {
    }

    String getName() const override { return "regr_count"; }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn **, size_t, Arena *) const override { ++data(place).count; }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena *,
        ssize_t if_argument_pos) const override
    {
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            data(place).count += countBytesInFilterWithNull(flags.data(), nullptr, row_begin, row_end);
        }
        else
        {
            data(place).count += row_end - row_begin;
        }
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena *,
        ssize_t if_argument_pos) const override
    {
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            data(place).count += countBytesInFilterWithNull(flags.data(), null_map, row_begin, row_end);
        }
        else
        {
            size_t count = 0;
            for (size_t i = row_begin; i < row_end; ++i)
                count += !null_map[i];
            data(place).count += count;
        }
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        data(place).count += data(rhs).count;
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t>) const override
    {
        writeBinaryLittleEndian(data(place).count, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t>, Arena *) const override
    {
        readBinaryLittleEndian(data(place).count, buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnUInt64 &>(to).getData().push_back(data(place).count);
    }

private:
    static size_t countBytesInFilterWithNull(const UInt8 * flags, const UInt8 * null_map, size_t row_begin, size_t row_end)
    {
        size_t count = 0;
        if (null_map)
            for (size_t i = row_begin; i < row_end; ++i)
                count += (!null_map[i]) & !!flags[i];
        else
            for (size_t i = row_begin; i < row_end; ++i)
                count += !!flags[i];
        return count;
    }
};

template <RegrKind kind>
AggregateFunctionPtr createAggregateFunctionRegr(
    const String & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertBinary(name, argument_types);

    AggregateFunctionPtr res(
        createWithTwoBasicNumericTypes<AggregateFunctionRegr>(*argument_types[0], *argument_types[1], argument_types, kind));
    if (!res)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal types {} and {} of arguments for aggregate function {}",
            argument_types[0]->getName(), argument_types[1]->getName(), name);

    return res;
}

AggregateFunctionPtr createAggregateFunctionRegrCount(
    const String & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertBinary(name, argument_types);

    for (const auto & argument_type : argument_types)
        if (!isNumber(argument_type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal types {} and {} of arguments for aggregate function {}",
                argument_types[0]->getName(), argument_types[1]->getName(), name);

    return std::make_shared<AggregateFunctionRegrCount>(argument_types);
}

}

void registerAggregateFunctionsRegr(AggregateFunctionFactory & factory);
void registerAggregateFunctionsRegr(AggregateFunctionFactory & factory)
{
    const FunctionDocumentation::Arguments arguments = {
        {"y", "Dependent variable.", {"(U)Int*", "Float*"}},
        {"x", "Independent variable.", {"(U)Int*", "Float*"}}};

    auto make_documentation
        = [&](const String & name, const String & description, const String & returned, const String & returned_type, const String & response)
    {
        FunctionDocumentation::Examples examples = {
            {"Basic usage",
             "SELECT " + name + "(y, x)\nFROM VALUES('x Float64, y Float64', (1, 2), (2, 4), (3, 6), (4, 8))",
             response}};

        return FunctionDocumentation{
            description,
            name + "(y, x)",
            arguments,
            {},
            {returned, {returned_type}},
            examples,
            {26, 9},
            FunctionDocumentation::Category::AggregateFunction};
    };

    factory.registerFunction(
        "regr_count",
        {createAggregateFunctionRegrCount,
         make_documentation(
             "regr_count",
             "Counts the rows where neither `y` nor `x` is `NULL`.",
             "Returns the number of non-null pairs.",
             "UInt64",
             R"(┌─regr_count(y, x)─┐
│                4 │
└──────────────────┘)")},
        AggregateFunctionFactory::Case::Insensitive);

    factory.registerFunction(
        "regr_avgx",
        {createAggregateFunctionRegr<RegrKind::regr_avgx>,
         make_documentation(
             "regr_avgx",
             "Returns the average of the independent variable.",
             "Returns `avg(x)`.",
             "Float64",
             R"(┌─regr_avgx(y, x)─┐
│             2.5 │
└─────────────────┘)")},
        AggregateFunctionFactory::Case::Insensitive);

    factory.registerFunction(
        "regr_avgy",
        {createAggregateFunctionRegr<RegrKind::regr_avgy>,
         make_documentation(
             "regr_avgy",
             "Returns the average of the dependent variable.",
             "Returns `avg(y)`.",
             "Float64",
             R"(┌─regr_avgy(y, x)─┐
│               5 │
└─────────────────┘)")},
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
             "Float64",
             R"(┌─regr_sxx(y, x)─┐
│              5 │
└────────────────┘)")},
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
             "Float64",
             R"(┌─regr_syy(y, x)─┐
│             20 │
└────────────────┘)")},
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
             "Float64",
             R"(┌─regr_sxy(y, x)─┐
│             10 │
└────────────────┘)")},
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
             "Float64",
             R"(┌─regr_slope(y, x)─┐
│                2 │
└──────────────────┘)")},
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
             "Float64",
             R"(┌─regr_intercept(y, x)─┐
│                    0 │
└──────────────────────┘)")},
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
             "Float64",
             R"(┌─regr_r2(y, x)─┐
│             1 │
└───────────────┘)")},
        AggregateFunctionFactory::Case::Insensitive);
}

}
