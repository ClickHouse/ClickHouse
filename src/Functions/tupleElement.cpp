#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <memory>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_INDEX;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
}

namespace
{

/** Extract element of tuple by constant index or name. The operation is essentially free.
  * Also the function looks through Arrays: you can get Array of tuple elements from Array of Tuples.
  */
class FunctionTupleElement : public IFunction
{
public:
    static constexpr auto name = "tupleElement";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionTupleElement>();
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override
    {
        return {1};
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const size_t number_of_arguments = arguments.size();

        if (number_of_arguments < 2 || number_of_arguments > 3)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                            + toString(number_of_arguments) + ", should be 2 or 3",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        size_t count_arrays = 0;

        const IDataType * tuple_col = arguments[0].type.get();
        while (const DataTypeArray * array = checkAndGetDataType<DataTypeArray>(tuple_col))
        {
            tuple_col = array->getNestedType().get();
            ++count_arrays;
        }

        const DataTypeTuple * tuple = checkAndGetDataType<DataTypeTuple>(tuple_col);
        if (!tuple)
            throw Exception("First argument for function " + getName() + " must be tuple or array of tuple.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        size_t index = 0;
        if (!getElementNum(arguments[1].column, *tuple, index, number_of_arguments))
        {
            return arguments[2].type;
        }

        DataTypePtr out_return_type = tuple->getElements()[index];

        for (; count_arrays; --count_arrays)
            out_return_type = std::make_shared<DataTypeArray>(out_return_type);

        return out_return_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        Columns array_offsets;

        const auto & first_arg = arguments[0];

        const IDataType * tuple_type = first_arg.type.get();
        const IColumn * tuple_col = first_arg.column.get();
        while (const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(tuple_type))
        {
            const ColumnArray * array_col = assert_cast<const ColumnArray *>(tuple_col);

            tuple_type = array_type->getNestedType().get();
            tuple_col = &array_col->getData();
            array_offsets.push_back(array_col->getOffsetsPtr());
        }

        const DataTypeTuple * tuple_type_concrete = checkAndGetDataType<DataTypeTuple>(tuple_type);
        const ColumnTuple * tuple_col_concrete = checkAndGetColumn<ColumnTuple>(tuple_col);
        if (!tuple_type_concrete || !tuple_col_concrete)
            throw Exception("First argument for function " + getName() + " must be tuple or array of tuple.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        size_t index = 0;
        if (!getElementNum(arguments[1].column, *tuple_type_concrete, index, arguments.size()))
        {
            return ColumnConst::create(arguments[2].column, input_rows_count);
        }
        ColumnPtr res = tuple_col_concrete->getColumns()[index];

        /// Wrap into Arrays
        for (auto it = array_offsets.rbegin(); it != array_offsets.rend(); ++it)
            res = ColumnArray::create(res, *it);

        return res;
    }

private:
    bool getElementNum(const ColumnPtr & index_column, const DataTypeTuple & tuple, size_t & index, const size_t argument_size) const
    {
        if (
            checkAndGetColumnConst<ColumnUInt8>(index_column.get())
                || checkAndGetColumnConst<ColumnUInt16>(index_column.get())
                || checkAndGetColumnConst<ColumnUInt32>(index_column.get())
                || checkAndGetColumnConst<ColumnUInt64>(index_column.get())
        )
        {
            index = index_column->getUInt(0);

            if (index == 0)
                throw Exception("Indices in tuples are 1-based.", ErrorCodes::ILLEGAL_INDEX);

            if (index > tuple.getElements().size())
                throw Exception("Index for tuple element is out of range.", ErrorCodes::ILLEGAL_INDEX);
            index--;
            return true;
        }
        else if (const auto * name_col = checkAndGetColumnConst<ColumnString>(index_column.get()))
        {
            if (tuple.getPositionByName(name_col->getValue<String>(), index))
            {
                return true;
            }

            if (argument_size == 2)
            {
                throw Exception("Tuple doesn't have element with name '" + name_col->getValue<String>() + "'", ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);
            }

            return false;
        }
        else
            throw Exception("Second argument to " + getName() + " must be a constant UInt or String", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
};

}

void registerFunctionTupleElement(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTupleElement>();
}

}
