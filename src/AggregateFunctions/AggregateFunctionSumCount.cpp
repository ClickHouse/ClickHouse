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
    FunctionDocumentation::Description description_sumCount = R"(
Calculates the sum of the numbers and counts the number of rows at the same time. The function is used by ClickHouse query optimizer: if there are multiple `sum`, `count` or `avg` functions in a query, they can be replaced to single `sumCount` function to reuse the calculations. The function is rarely needed to use explicitly.

**See also**

- [`optimize_syntax_fuse_functions`](../../../operations/settings/settings.md#optimize_syntax_fuse_functions) setting.
    )";
    FunctionDocumentation::Syntax syntax_sumCount = R"(
sumCount(x)
    )";
    FunctionDocumentation::Parameters parameters_sumCount = {};
    FunctionDocumentation::Arguments arguments_sumCount = {
        {"x", "Input value.", {"(U)Int*", "Float", "Decimal"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_sumCount = {"Returns a tuple `(sum, count)`, where `sum` is the sum of numbers and `count` is the number of rows with not-NULL values.", {"Tuple"}};
    FunctionDocumentation::Examples examples_sumCount = {
    {
        "Basic usage",
        R"(
CREATE TABLE s_table (x Int8) ENGINE = Log;
INSERT INTO s_table SELECT number FROM numbers(0, 20);
INSERT INTO s_table VALUES (NULL);
SELECT sumCount(x) FROM s_table;
        )",
        R"(
┌─sumCount(x)─┐
│ (190,20)    │
└─────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_sumCount = {21, 6};
    FunctionDocumentation::Category category_sumCount = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_sumCount = {description_sumCount, syntax_sumCount, arguments_sumCount, parameters_sumCount, returned_value_sumCount, examples_sumCount, introduced_in_sumCount, category_sumCount};

    factory.registerFunction("sumCount", {createAggregateFunctionSumCount, documentation_sumCount});
}

}
