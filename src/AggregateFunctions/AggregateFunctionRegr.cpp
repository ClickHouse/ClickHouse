#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/Moments.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <algorithm>
#include <type_traits>
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

/// The state of the eight aggregates that are read off the sums.
///
/// The sums are taken around the first pair that is added rather than around zero. Summing raw
/// squares and reconstructing `Sxx = sum(x^2) - n * avg(x)^2` cancels catastrophically as soon as
/// the values carry an offset that dwarfs their spread, which is the ordinary shape of a timestamp:
/// on `x = 1e12 + number` over a thousand rows the reconstruction returns 0 for every sum, and the
/// fit of an exact line comes back as nan. Shifting by a value from the data itself keeps every
/// term at the scale of the spread, and leaves the loop a plain sequence of multiply-adds that
/// still vectorizes, which a Welford-style incremental mean does not.
///
/// Every quantity the aggregates need is invariant under the shift, so it cancels in the results.
/// The difference of two values of the argument type, exactly.
///
/// An integer column is not narrowed to Float64 first: a UInt64 timestamp in nanoseconds is past
/// 2^53, where Float64 no longer has consecutive integers, and the low bits that carry the spread
/// would be rounded away before the subtraction ever happens - a thousand rows spread over 406
/// nanoseconds collapse onto four distinct Float64 values.
///
/// The 64-bit types are subtracted in a wider one. Their difference does not fit in Int64 across
/// the whole of their range - UInt64 spans up to 2^64 - and wrapping it would not merely lose
/// precision, it would flip the sign of the slope.
template <typename T>
Float64 exactDelta(T value, T shift)
{
    if constexpr (is_integer<T>)
    {
        /// The magnitude is taken in the unsigned domain, where it is exact for the whole range of
        /// the type, and the sign comes from the comparison of the values themselves.
        using Unsigned = std::make_unsigned_t<T>;
        const auto a = static_cast<Unsigned>(value);
        const auto b = static_cast<Unsigned>(shift);
        return value >= shift
            ? static_cast<Float64>(static_cast<Unsigned>(a - b))
            : -static_cast<Float64>(static_cast<Unsigned>(b - a));
    }
    else
    {
        return static_cast<Float64>(value) - static_cast<Float64>(shift);
    }
}

template <typename TX, typename TY>
struct RegrMoments
{
    static constexpr size_t unroll_count = 128 / sizeof(Float64);

    UInt64 count = 0;
    /// The shift, set from the first pair added to an empty state, and kept in the type of the
    /// argument so that the centering below loses nothing.
    TX x0 = 0;
    TY y0 = 0;
    Float64 sx = 0;
    Float64 sy = 0;
    Float64 sxx = 0;
    Float64 syy = 0;
    Float64 sxy = 0;

    void setShift(TX x, TY y)
    {
        x0 = x;
        y0 = y;
    }

    void add(TX x, TY y)
    {
        if (count == 0)
            setShift(x, y);
        addShifted(exactDelta(x, x0), exactDelta(y, y0));
    }

    void addShifted(Float64 dx, Float64 dy)
    {
        ++count;
        sx += dx;
        sy += dy;
        sxx += dx * dx;
        syy += dy * dy;
        sxy += dx * dy;
    }

    void addMany(const TX * __restrict x_ptr, const TY * __restrict y_ptr, size_t row_begin, size_t row_end)
    {
        if (row_begin >= row_end)
            return;
        if (count == 0)
            setShift(x_ptr[row_begin], y_ptr[row_begin]);

        /// Summing into a single accumulator would let the rounding error grow with the row count:
        /// over a million rows it cost the slope five digits. Partial sums keep it near the error
        /// of a single addition, and let the loop vectorize.
        Float64 acc_sx[unroll_count]{};
        Float64 acc_sy[unroll_count]{};
        Float64 acc_sxx[unroll_count]{};
        Float64 acc_syy[unroll_count]{};
        Float64 acc_sxy[unroll_count]{};

        size_t i = row_begin;
        for (; i + unroll_count <= row_end; i += unroll_count)
        {
            for (size_t j = 0; j < unroll_count; ++j)
            {
                const Float64 dx = exactDelta(x_ptr[i + j], x0);
                const Float64 dy = exactDelta(y_ptr[i + j], y0);
                acc_sx[j] += dx;
                acc_sy[j] += dy;
                acc_sxx[j] += dx * dx;
                acc_syy[j] += dy * dy;
                acc_sxy[j] += dx * dy;
            }
        }
        for (size_t j = 0; j < unroll_count; ++j)
        {
            sx += acc_sx[j];
            sy += acc_sy[j];
            sxx += acc_sxx[j];
            syy += acc_syy[j];
            sxy += acc_sxy[j];
        }
        count += i - row_begin;

        for (; i < row_end; ++i)
            addShifted(exactDelta(x_ptr[i], x0), exactDelta(y_ptr[i], y0));
    }

    template <bool add_if_zero>
    void addManyConditional(
        const TX * __restrict x_ptr,
        const TY * __restrict y_ptr,
        const UInt8 * __restrict condition_map,
        size_t row_begin,
        size_t row_end)
    {
        if (count == 0)
        {
            /// The shift has to come from a row that is actually added.
            for (size_t i = row_begin; i < row_end; ++i)
            {
                if (!!condition_map[i] ^ add_if_zero)
                {
                    setShift(x_ptr[i], y_ptr[i]);
                    break;
                }
            }
        }

        UInt64 acc_count = 0;
        Float64 acc_sx[unroll_count]{};
        Float64 acc_sy[unroll_count]{};
        Float64 acc_sxx[unroll_count]{};
        Float64 acc_syy[unroll_count]{};
        Float64 acc_sxy[unroll_count]{};

        size_t i = row_begin;
        for (; i + unroll_count <= row_end; i += unroll_count)
        {
            for (size_t j = 0; j < unroll_count; ++j)
            {
                const bool add = !!condition_map[i + j] ^ add_if_zero;
                /// Zeroing the bit pattern rather than multiplying by the flag: a discarded row may
                /// hold a NaN or an Inf, and 0 * NaN is NaN, which would poison every sum.
                const Float64 dx = maskFloatingPoint(exactDelta(x_ptr[i + j], x0), add);
                const Float64 dy = maskFloatingPoint(exactDelta(y_ptr[i + j], y0), add);
                acc_count += add;
                acc_sx[j] += dx;
                acc_sy[j] += dy;
                acc_sxx[j] += dx * dx;
                acc_syy[j] += dy * dy;
                acc_sxy[j] += dx * dy;
            }
        }
        for (size_t j = 0; j < unroll_count; ++j)
        {
            sx += acc_sx[j];
            sy += acc_sy[j];
            sxx += acc_sxx[j];
            syy += acc_syy[j];
            sxy += acc_sxy[j];
        }
        count += acc_count;

        for (; i < row_end; ++i)
        {
            if (!!condition_map[i] ^ add_if_zero)
                addShifted(exactDelta(x_ptr[i], x0), exactDelta(y_ptr[i], y0));
        }
    }

    /// Rebases the sums of `rhs` onto this shift before adding them. The two states were filled by
    /// different threads and were shifted by whatever each of them saw first.
    void merge(const RegrMoments & rhs)
    {
        if (rhs.count == 0)
            return;
        if (count == 0)
        {
            *this = rhs;
            return;
        }

        const Float64 dx = exactDelta(rhs.x0, x0);
        const Float64 dy = exactDelta(rhs.y0, y0);
        const auto rhs_count = static_cast<Float64>(rhs.count);

        sxx += rhs.sxx + 2 * dx * rhs.sx + rhs_count * dx * dx;
        syy += rhs.syy + 2 * dy * rhs.sy + rhs_count * dy * dy;
        sxy += rhs.sxy + dx * rhs.sy + dy * rhs.sx + rhs_count * dx * dy;
        sx += rhs.sx + rhs_count * dx;
        sy += rhs.sy + rhs_count * dy;
        count += rhs.count;
    }

    void write(WriteBuffer & buf) const { writePODBinary(*this, buf); }
    void read(ReadBuffer & buf) { readPODBinary(*this, buf); }
};

/// Computes the regression aggregates of a finished state.
///
/// The shift cancels in every one of them: the sums of squares and the cross-product are already
/// taken around the shift, and the averages add it back. The sums of squares are non-negative by
/// construction, but subtracting the square of the sum can land just below zero for a constant
/// input, so they are clamped.
struct RegrResult
{
    Float64 n;
    Float64 x0;
    Float64 y0;
    Float64 mean_dx;
    Float64 mean_dy;
    Float64 avg_x;
    Float64 avg_y;
    Float64 sxx;
    Float64 syy;
    Float64 sxy;

    template <typename Data>
    explicit RegrResult(const Data & data)
        : n(static_cast<Float64>(data.count))
        , x0(static_cast<Float64>(data.x0))
        , y0(static_cast<Float64>(data.y0))
        , mean_dx(data.sx / n)
        , mean_dy(data.sy / n)
        , avg_x(x0 + mean_dx)
        , avg_y(y0 + mean_dy)
        , sxx(std::max(0.0, data.sxx - data.sx * data.sx / n))
        , syy(std::max(0.0, data.syy - data.sy * data.sy / n))
        , sxy(data.sxy - data.sx * data.sy / n)
    {
    }

    Float64 get(RegrKind kind) const
    {
        static constexpr auto nan = std::numeric_limits<Float64>::quiet_NaN();

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
                /// Not avg_y - slope * avg_x: at a timestamp scale those are two huge and nearly
                /// equal numbers, and their difference keeps only the noise. Around the shift the
                /// large part is y0 - slope * x0, where the two cancel exactly for a line through
                /// the data, and what is left is at the scale of the spread.
                return sxx == 0 ? nan : (y0 - (sxy / sxx) * x0) + (mean_dy - (sxy / sxx) * mean_dx);
            case RegrKind::regr_r2:
                /// A vertical line explains none of the variance, a horizontal one explains all of it.
                if (sxx == 0)
                    return nan;
                if (syy == 0)
                    return 1.0;
                return (sxy * sxy) / (sxx * syy);
        }
    }
};

/// TY is the type of the dependent variable (the first argument), TX of the independent one.
///
/// The sums are always accumulated in Float64, whatever the arguments are. corr and covar* narrow
/// their state to Float32 for a pair of Float32 columns, but the regression aggregates add up
/// squares and cross-products, and Float32 loses them: over two million rows of an exact
/// y = 3x + 7, a Float32 state returned an intercept of -23.25 instead of 7. Float64 also keeps
/// regr_avgx and regr_avgy equal to avg(x) and avg(y), which finalize to Float64 as well.
template <typename TY, typename TX>
class AggregateFunctionRegr final
    : public IAggregateFunctionDataHelper<RegrMoments<TX, TY>, AggregateFunctionRegr<TY, TX>>
{
public:
    using ResultType = Float64;
    using Data = RegrMoments<TX, TY>;
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
            static_cast<const ColVecTX &>(*columns[1]).getData()[row_num],
            static_cast<const ColVecTY &>(*columns[0]).getData()[row_num]);
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
            data.template addManyConditional<false>(x_ptr, y_ptr, flags, row_begin, row_end);
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

            data.template addManyConditional<false>(x_ptr, y_ptr, final_flags.get(), row_begin, row_end);
        }
        else
        {
            data.template addManyConditional<true>(x_ptr, y_ptr, null_map, row_begin, row_end);
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

        if (data.count == 0)
            dst.push_back(std::numeric_limits<ResultType>::quiet_NaN());
        else
            dst.push_back(RegrResult(data).get(kind));
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
    {
        /// Nothing reaches this creator for `regr_count(NULL, NULL)`: unlike the other eight, this
        /// one answers with a number even when every row was NULL, so the Null combinator does not
        /// wrap it and take the literal NULLs off its hands. There is nothing to count in that
        /// column, which is an answer of zero rather than an error.
        const auto & nested_type = removeNullable(argument_type);
        if (!isNumber(nested_type) && !isNothing(nested_type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal types {} and {} of arguments for aggregate function {}",
                argument_types[0]->getName(), argument_types[1]->getName(), name);
    }

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

    /// Like count, regr_count answers with a number even when every row was NULL, rather than
    /// being wrapped by the generic Null combinator and answering NULL.
    const AggregateFunctionProperties count_properties = {.returns_default_when_only_null = true};

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
└──────────────────┘)"),
         count_properties},
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
