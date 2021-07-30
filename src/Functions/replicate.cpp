#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

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

    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[1].get());
        if (!array_type)
            throw Exception("Second argument for function " + getName() + " must be array.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return std::make_shared<DataTypeArray>(arguments[0]);
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t) const override
    {
        ColumnPtr first_column = block.getByPosition(arguments[0]).column;
        const ColumnArray * array_column = checkAndGetColumn<ColumnArray>(block.getByPosition(arguments[1]).column.get());
        ColumnPtr temp_column;
        if (!array_column)
        {
            const auto * const_array_column = checkAndGetColumnConst<ColumnArray>(block.getByPosition(arguments[1]).column.get());
            if (!const_array_column)
                throw Exception("Unexpected column for replicate", ErrorCodes::ILLEGAL_COLUMN);
            temp_column = const_array_column->convertToFullColumn();
            array_column = checkAndGetColumn<ColumnArray>(temp_column.get());
        }
        block.getByPosition(result).column
            = ColumnArray::create(first_column->replicate(array_column->getOffsets())->convertToFullColumnIfConst(), array_column->getOffsetsPtr());
    }
};


void registerFunctionReplicate(FunctionFactory & factory)
{
    factory.registerFunction<FunctionReplicate>();
}

}
