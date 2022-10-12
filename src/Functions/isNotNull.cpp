#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Core/ColumnNumbers.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnLowCardinality.h>
#include <Common/assert_cast.h>


namespace DB
{
namespace
{

/// Implements the function isNotNull which returns true if a value
/// is not null, false otherwise.
class FunctionIsNotNull : public IFunction
{
public:
    static constexpr auto name = "isNotNull";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionIsNotNull>();
    }

    std::string getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const override { return {0}; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnWithTypeAndName & elem = arguments[0];
        if (elem.type->isLowCardinalityNullable())
        {
            const auto * low_cardinality_column = checkAndGetColumn<ColumnLowCardinality>(*elem.column);
            const size_t null_index = low_cardinality_column->getDictionary().getNullValueIndex();
            auto res = DataTypeUInt8().createColumn();
            auto & data = typeid_cast<ColumnUInt8 &>(*res).getData();
            data.reserve(low_cardinality_column->size());
            for (size_t i = 0; i != low_cardinality_column->size(); ++i)
                data.push_back(low_cardinality_column->getIndexAt(i) != null_index);
            return res;
        }

        if (const auto * nullable = checkAndGetColumn<ColumnNullable>(*elem.column))
        {
            /// Return the negated null map.
            auto res_column = ColumnUInt8::create(input_rows_count);
            const auto & src_data = nullable->getNullMapData();
            auto & res_data = assert_cast<ColumnUInt8 &>(*res_column).getData();

            for (size_t i = 0; i < input_rows_count; ++i)
                res_data[i] = !src_data[i];

            return res_column;
        }
        else
        {
            /// Since no element is nullable, return a constant one.
            return DataTypeUInt8().createColumnConst(elem.column->size(), 1u);
        }
    }
};

}

void registerFunctionIsNotNull(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIsNotNull>();
}

}
