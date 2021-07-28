#include <Functions/replicate.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>
#include <Interpreters/Context.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
}

DataTypePtr FunctionReplicate::getReturnTypeImpl(const DataTypes & arguments) const
{
    if (arguments.size() < 2)
        throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION,
                        "Function {} expect at least two arguments, got {}", getName(), arguments.size());

    for (size_t i = 1; i < arguments.size(); ++i)
    {
        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[i].get());
        if (!array_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Argument {} for function {} must be array.",
                            i + 1, getName());
    }
    return std::make_shared<DataTypeArray>(arguments[0]);
}

ColumnPtr FunctionReplicate::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const
{
    ColumnPtr first_column = arguments[0].column;
    ColumnPtr offsets;

    for (size_t i = 1; i < arguments.size(); ++i)
    {
        const ColumnArray * array_column = checkAndGetColumn<ColumnArray>(arguments[i].column.get());
        ColumnPtr temp_column;
        if (!array_column)
        {
            const auto * const_array_column = checkAndGetColumnConst<ColumnArray>(arguments[i].column.get());
            if (!const_array_column)
                throw Exception("Unexpected column for replicate", ErrorCodes::ILLEGAL_COLUMN);
            temp_column = const_array_column->convertToFullColumn();
            array_column = checkAndGetColumn<ColumnArray>(temp_column.get());
        }

        if (!offsets || offsets->empty())
            offsets = array_column->getOffsetsPtr();
    }

    const auto & offsets_data = assert_cast<const ColumnArray::ColumnOffsets &>(*offsets).getData(); /// NOLINT
    return ColumnArray::create(first_column->replicate(offsets_data)->convertToFullColumnIfConst(), offsets);
}

void registerFunctionReplicate(FunctionFactory & factory)
{
    factory.registerFunction<FunctionReplicate>();
}

}
