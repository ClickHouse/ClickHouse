#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>


namespace DB
{

/** Creates an array, multiplying the column (the first argument) by the number of elements in the array (the second argument).
  */
class FunctionReplicate : public IFunction
{
public:
    static constexpr auto name = "replicate";

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionReplicate>();
    }

    String getName() const override
    {
        return name;
    }

    String getSignature() const override { return "f(T, Array) -> Array(T)"; }

    bool useDefaultImplementationForNulls() const override { return false; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t) override
    {
        ColumnPtr first_column = block.getByPosition(arguments[0]).column;
        const ColumnArray * array_column = checkAndGetColumn<ColumnArray>(block.getByPosition(arguments[1]).column.get());
        ColumnPtr temp_column;
        if (!array_column)
        {
            auto const_array_column = checkAndGetColumnConst<ColumnArray>(block.getByPosition(arguments[1]).column.get());
            if (!const_array_column)
                throw Exception("Unexpected column for replicate", ErrorCodes::ILLEGAL_COLUMN);
            temp_column = const_array_column->convertToFullColumn();
            array_column = checkAndGetColumn<ColumnArray>(temp_column.get());
        }
        block.getByPosition(result).column
            = ColumnArray::create(first_column->replicate(array_column->getOffsets()), array_column->getOffsetsPtr());
    }
};


void registerFunctionReplicate(FunctionFactory & factory)
{
    factory.registerFunction<FunctionReplicate>();
}

}
