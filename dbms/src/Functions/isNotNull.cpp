#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Core/ColumnNumbers.h>
#include <Columns/ColumnNullable.h>


namespace DB
{

/// Implements the function isNotNull which returns true if a value
/// is not null, false otherwise.
class FunctionIsNotNull : public IFunction
{
public:
    static constexpr auto name = "isNotNull";

    static FunctionPtr create(const Context &)
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

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        const ColumnWithTypeAndName & elem = block.getByPosition(arguments[0]);
        if (elem.column->isColumnNullable())
        {
            /// Return the negated null map.
            auto res_column = ColumnUInt8::create(input_rows_count);
            const auto & src_data = static_cast<const ColumnNullable &>(*elem.column).getNullMapData();
            auto & res_data = static_cast<ColumnUInt8 &>(*res_column).getData();

            for (size_t i = 0; i < input_rows_count; ++i)
                res_data[i] = !src_data[i];

            block.getByPosition(result).column = std::move(res_column);
        }
        else
        {
            /// Since no element is nullable, return a constant one.
            block.getByPosition(result).column = DataTypeUInt8().createColumnConst(elem.column->size(), UInt64(1));
        }
    }
};

void registerFunctionIsNotNull(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIsNotNull>();
}

}
