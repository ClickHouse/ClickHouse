#pragma once

#include <common/Backtrace.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class FunctionSymbolizeTrace : public IFunction
{
public:
    static constexpr auto name = "symbolizeTrace";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionSymbolizeTrace>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception("Function " + getName() + " needs exactly one argument; passed "
                            + toString(arguments.size()) + ".",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const auto array_type = checkAndGetDataType<DataTypeArray>(arguments[0].type.get());

        if (!array_type)
            throw Exception("The only argument for function " + getName() + " must be array. Found "
                            + arguments[0].type->getName() + " instead.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        DataTypePtr nested_type = array_type->getNestedType();

        if (!WhichDataType(nested_type).isUInt64())
            throw Exception("The only argument for function " + getName() + " must be array of UInt64. Found "
                            + arguments[0].type->getName() + " instead.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const ColumnPtr column = block.getByPosition(arguments[0]).column;
        const ColumnArray * column_array = checkAndGetColumn<ColumnArray>(column.get());

        if (!column_array)
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of argument of function " + getName(),
                            ErrorCodes::ILLEGAL_COLUMN);

        const ColumnPtr data_ptr = column_array->getDataPtr();
        const ColumnVector<UInt64> * data_vector = checkAndGetColumn<ColumnVector<UInt64>>(&*data_ptr);

        const typename ColumnVector<UInt64>::Container & data = data_vector->getData();
        const ColumnArray::Offsets & offsets = column_array->getOffsets();

        auto result_column = ColumnString::create();

        size_t pos = 0;
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            std::vector<void *> frames;
            for (; pos < offsets[i]; ++pos)
            {
                frames.push_back(reinterpret_cast<void *>(data[pos]));
            }
            std::string backtrace = Backtrace(frames).toString("\n");

            result_column->insertDataWithTerminatingZero(backtrace.c_str(), backtrace.length() + 1);
        }

        block.getByPosition(result).column = std::move(result_column);
    }
};

}
