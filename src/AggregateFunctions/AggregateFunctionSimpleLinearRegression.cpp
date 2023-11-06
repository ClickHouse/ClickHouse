#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnTuple.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <limits>


namespace DB
{

struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

template <typename T>
struct AggregateFunctionSimpleLinearRegressionData final
{
    size_t count = 0;
    T sum_x = 0;
    T sum_y = 0;
    T sum_xx = 0;
    T sum_xy = 0;

    void add(T x, T y)
    {
        count += 1;
        sum_x += x;
        sum_y += y;
        sum_xx += x * x;
        sum_xy += x * y;
    }

    void merge(const AggregateFunctionSimpleLinearRegressionData & other)
    {
        count += other.count;
        sum_x += other.sum_x;
        sum_y += other.sum_y;
        sum_xx += other.sum_xx;
        sum_xy += other.sum_xy;
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinary(count, buf);
        writeBinary(sum_x, buf);
        writeBinary(sum_y, buf);
        writeBinary(sum_xx, buf);
        writeBinary(sum_xy, buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        readBinary(count, buf);
        readBinary(sum_x, buf);
        readBinary(sum_y, buf);
        readBinary(sum_xx, buf);
        readBinary(sum_xy, buf);
    }

    T getK() const
    {
        T divisor = sum_xx * count - sum_x * sum_x;

        if (divisor == 0)
            return std::numeric_limits<T>::quiet_NaN();

        return (sum_xy * count - sum_x * sum_y) / divisor;
    }

    T getB(T k) const
    {
        if (count == 0)
            return std::numeric_limits<T>::quiet_NaN();

        return (sum_y - k * sum_x) / count;
    }
};

/// Calculates simple linear regression parameters.
/// Result is a tuple (k, b) for y = k * x + b equation, solved by least squares approximation.
template <typename X, typename Y, typename Ret = Float64>
class AggregateFunctionSimpleLinearRegression final : public IAggregateFunctionDataHelper<
    AggregateFunctionSimpleLinearRegressionData<Ret>,
    AggregateFunctionSimpleLinearRegression<X, Y, Ret>
>
{
public:
    AggregateFunctionSimpleLinearRegression(
        const DataTypes & arguments,
        const Array & params
    ):
        IAggregateFunctionDataHelper<
            AggregateFunctionSimpleLinearRegressionData<Ret>,
            AggregateFunctionSimpleLinearRegression<X, Y, Ret>
        > {arguments, params, createResultType()}
    {
        // notice: arguments has been checked before
    }

    String getName() const override
    {
        return "simpleLinearRegression";
    }

    void add(
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        size_t row_num,
        Arena *
    ) const override
    {
        auto col_x = assert_cast<const ColumnVector<X> *>(columns[0]);
        auto col_y = assert_cast<const ColumnVector<Y> *>(columns[1]);

        X x = col_x->getData()[row_num];
        Y y = col_y->getData()[row_num];

        this->data(place).add(x, y);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    static DataTypePtr createResultType()
    {
        DataTypes types
        {
            std::make_shared<DataTypeNumber<Ret>>(),
            std::make_shared<DataTypeNumber<Ret>>(),
        };

        Strings names
        {
            "k",
            "b",
        };

        return std::make_shared<DataTypeTuple>(
            std::move(types),
            std::move(names)
        );
    }

    bool allocatesMemoryInArena() const override { return false; }

    void insertResultInto(
        AggregateDataPtr __restrict place,
        IColumn & to,
        Arena *) const override
    {
        Ret k = this->data(place).getK();
        Ret b = this->data(place).getB(k);

        auto & col_tuple = assert_cast<ColumnTuple &>(to);
        auto & col_k = assert_cast<ColumnVector<Ret> &>(col_tuple.getColumn(0));
        auto & col_b = assert_cast<ColumnVector<Ret> &>(col_tuple.getColumn(1));

        col_k.getData().push_back(k);
        col_b.getData().push_back(b);
    }
};


AggregateFunctionPtr createAggregateFunctionSimpleLinearRegression(
    const String & name,
    const DataTypes & arguments,
    const Array & params,
    const Settings *)
{
    assertNoParameters(name, params);
    assertBinary(name, arguments);

    const IDataType * x_arg = arguments.front().get();
    WhichDataType which_x = x_arg;

    const IDataType * y_arg = arguments.back().get();
    WhichDataType which_y = y_arg;


    #define FOR_LEASTSQR_TYPES_2(M, T) \
        M(T, UInt8) \
        M(T, UInt16) \
        M(T, UInt32) \
        M(T, UInt64) \
        M(T, Int8) \
        M(T, Int16) \
        M(T, Int32) \
        M(T, Int64) \
        M(T, Float32) \
        M(T, Float64)
    #define FOR_LEASTSQR_TYPES(M) \
        FOR_LEASTSQR_TYPES_2(M, UInt8) \
        FOR_LEASTSQR_TYPES_2(M, UInt16) \
        FOR_LEASTSQR_TYPES_2(M, UInt32) \
        FOR_LEASTSQR_TYPES_2(M, UInt64) \
        FOR_LEASTSQR_TYPES_2(M, Int8) \
        FOR_LEASTSQR_TYPES_2(M, Int16) \
        FOR_LEASTSQR_TYPES_2(M, Int32) \
        FOR_LEASTSQR_TYPES_2(M, Int64) \
        FOR_LEASTSQR_TYPES_2(M, Float32) \
        FOR_LEASTSQR_TYPES_2(M, Float64)
    #define DISPATCH(T1, T2) \
        if (which_x.idx == TypeIndex::T1 && which_y.idx == TypeIndex::T2) \
            return std::make_shared<AggregateFunctionSimpleLinearRegression<T1, T2>>(/* NOLINT */ \
                arguments, \
                params \
            );

    FOR_LEASTSQR_TYPES(DISPATCH)

    #undef FOR_LEASTSQR_TYPES_2
    #undef FOR_LEASTSQR_TYPES
    #undef DISPATCH

    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal types ({}, {}) of arguments of aggregate function {}, must "
                    "be Native Ints, Native UInts or Floats", x_arg->getName(), y_arg->getName(), name);
}

}

void registerAggregateFunctionSimpleLinearRegression(AggregateFunctionFactory & factory)
{
    factory.registerFunction("simpleLinearRegression", createAggregateFunctionSimpleLinearRegression);
}

}
