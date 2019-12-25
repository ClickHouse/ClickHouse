#include <Columns/ColumnTuple.h>
#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>
#include "registerFunctionsArray.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/// arrayZip(['a', 'b', 'c'], ['d', 'e', 'f']) = [('a', 'd'), ('b', 'e'), ('c', 'f')]
class FunctionArrayZip : public IFunction
{
public:
    static constexpr auto name = "arrayZip";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionArrayZip>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() < 1)
            throw Exception("Function " + getName() + " needs at least one argument; passed " + toString(arguments.size()) + "."
                , ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        DataTypes arguments_types;
        for (size_t index = 0; index < arguments.size(); ++index)
        {
            const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[index].type.get());

            if (!array_type)
                throw Exception(
                    "Argument " + toString(index + 1) + " of function must be array. Found " + arguments[0].type->getName() + " instead.",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            arguments_types.emplace_back(array_type->getNestedType());
        }

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(arguments_types));
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        auto first_argument = block.getByPosition(arguments[0]);
        const auto & first_array_column = checkAndGetColumn<ColumnArray>(first_argument.column.get());

        Columns res_tuple_columns(arguments.size());
        res_tuple_columns[0] = first_array_column->getDataPtr();

        for (size_t index = 1; index < arguments.size(); ++index)
        {
            const auto & argument_type_and_column = block.getByPosition(arguments[index]);
            const auto & argument_array_column = checkAndGetColumn<ColumnArray>(argument_type_and_column.column.get());

            if (!first_array_column->hasEqualOffsets(*argument_array_column))
                throw Exception("The argument 1 and argument " + toString(index + 1) + " of function have different array sizes",
                                ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);

            res_tuple_columns[index] = argument_array_column->getDataPtr();
        }

        block.getByPosition(result).column = ColumnArray::create(
            ColumnTuple::create(res_tuple_columns), first_array_column->getOffsetsPtr());
    }
};

void registerFunctionArrayZip(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayZip>();
}

}

