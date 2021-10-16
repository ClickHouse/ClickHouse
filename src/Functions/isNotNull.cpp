#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Core/ColumnNumbers.h>
#include <Columns/ColumnNullable.h>
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
    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const override { return {0}; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnWithTypeAndName & elem = arguments[0];
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
