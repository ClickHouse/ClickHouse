#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <DataTypes/DataTypeTuple.h>
#include <AggregateFunctions/AggregateFunctionAvg.h>


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
class AggregateFunctionSumCount final : public AggregateFunctionAvg<T>
{
public:
    using Base = AggregateFunctionAvg<T>;

    explicit AggregateFunctionSumCount(const DataTypes & argument_types_, UInt32 num_scale_ = 0)
        : Base(argument_types_, createResultType(num_scale_), num_scale_)
    {}

    static DataTypePtr createResultType(UInt32 num_scale_)
    {
        auto second_elem = std::make_shared<DataTypeUInt64>();
        return std::make_shared<DataTypeTuple>(DataTypes{getReturnTypeFirstElement(num_scale_), std::move(second_elem)});
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const final
    {
        assert_cast<ColumnVectorOrDecimal<AvgFieldType<T>> &>((assert_cast<ColumnTuple &>(to)).getColumn(0)).getData().push_back(
            this->data(place).numerator);

        assert_cast<ColumnUInt64 &>((assert_cast<ColumnTuple &>(to)).getColumn(1)).getData().push_back(
            this->data(place).denominator);
    }

    String getName() const final { return "sumCount"; }

#if USE_EMBEDDED_COMPILER

    bool isCompilable() const override
    {
        return false;
    }

#endif

private:
    static auto getReturnTypeFirstElement(UInt32 num_scale_)
    {
        using FieldType = AvgFieldType<T>;

        if constexpr (!is_decimal<T>)
            return std::make_shared<DataTypeNumber<FieldType>>();
        else
        {
            using DataType = DataTypeDecimal<FieldType>;
            return std::make_shared<DataType>(DataType::maxPrecision(), num_scale_);
        }
    }
};


bool allowType(const DataTypePtr& type) noexcept
{
    const WhichDataType t(type);
    return t.isInt() || t.isUInt() || t.isFloat() || t.isDecimal();
}

AggregateFunctionPtr
createAggregateFunctionSumCount(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    AggregateFunctionPtr res;
    const DataTypePtr & data_type = argument_types[0];
    if (!allowType(data_type))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}",
            data_type->getName(), name);

    if (isDecimal(data_type))
        res.reset(createWithDecimalType<AggregateFunctionSumCount>(
            *data_type, argument_types, getDecimalScale(*data_type)));
    else
        res.reset(createWithNumericType<AggregateFunctionSumCount>(*data_type, argument_types));

    return res;
}

}

void registerAggregateFunctionSumCount(AggregateFunctionFactory & factory)
{
    factory.registerFunction("sumCount", createAggregateFunctionSumCount);
}

}
