#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Core/ColumnNumbers.h>
#include <Columns/ColumnNullable.h>


namespace DB
{

/// Implements the function isNull which returns true if a value
/// is null, false otherwise.
class FunctionIsNull : public IFunction
{
public:
    static constexpr auto name = "isNull";

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionIsNull>();
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

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t) override
    {
        const ColumnWithTypeAndName & elem = block.getByPosition(arguments[0]);
        if (elem.column->isColumnNullable())
        {
            /// Merely return the embedded null map.
            block.getByPosition(result).column = static_cast<const ColumnNullable &>(*elem.column).getNullMapColumnPtr();
        }
        else
        {
            /// Since no element is nullable, return a zero-constant column representing
            /// a zero-filled null map.
            block.getByPosition(result).column = DataTypeUInt8().createColumnConst(elem.column->size(), UInt64(0));
        }
    }
};


void registerFunctionIsNull(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIsNull>();
}

}
