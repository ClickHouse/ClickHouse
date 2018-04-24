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
#include <Interpreters/ExpressionActions.h>
#include <memory>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_INDEX;
}


/** tuple(x, y, ...) is a function that allows you to group several columns
  * tupleElement(tuple, n) is a function that allows you to retrieve a column from tuple.
  */

class FunctionTuple : public IFunction
{
public:
    static constexpr auto name = "tuple";

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionTuple>();
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override
    {
        return true;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool isInjective(const Block &) override
    {
        return true;
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 1)
            throw Exception("Function " + getName() + " requires at least one argument.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return std::make_shared<DataTypeTuple>(arguments);
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        size_t tuple_size = arguments.size();
        Columns tuple_columns(tuple_size);
        for (size_t i = 0; i < tuple_size; ++i)
        {
            tuple_columns[i] = block.getByPosition(arguments[i]).column;

            /** If tuple is mixed of constant and not constant columns,
            *  convert all to non-constant columns,
            *  because many places in code expect all non-constant columns in non-constant tuple.
            */
            if (ColumnPtr converted = tuple_columns[i]->convertToFullColumnIfConst())
                tuple_columns[i] = converted;
        }
        block.getByPosition(result).column = ColumnTuple::create(tuple_columns);
    }
};


/** Extract element of tuple by constant index or name. The operation is essentially free.
  * Also the function looks through Arrays: you can get Array of tuple elements from Array of Tuples.
  */
class FunctionTupleElement : public IFunction
{
public:
    static constexpr auto name = "tupleElement";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionTupleElement>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override
    {
        return {1};
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
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

        size_t index = getElementNum(arguments[1].column, *tuple);
        DataTypePtr out_return_type = tuple->getElements()[index];

        for (; count_arrays; --count_arrays)
            out_return_type = std::make_shared<DataTypeArray>(out_return_type);

        return out_return_type;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        Columns array_offsets;

        const auto & first_arg = block.getByPosition(arguments[0]);

        const IDataType * tuple_type = first_arg.type.get();
        const IColumn * tuple_col = first_arg.column.get();
        while (const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(tuple_type))
        {
            const ColumnArray * array_col = static_cast<const ColumnArray *>(tuple_col);

            tuple_type = array_type->getNestedType().get();
            tuple_col = &array_col->getData();
            array_offsets.push_back(array_col->getOffsetsPtr());
        }

        const DataTypeTuple * tuple_type_concrete = checkAndGetDataType<DataTypeTuple>(tuple_type);
        const ColumnTuple * tuple_col_concrete = checkAndGetColumn<ColumnTuple>(tuple_col);
        if (!tuple_type_concrete || !tuple_col_concrete)
            throw Exception("First argument for function " + getName() + " must be tuple or array of tuple.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        size_t index = getElementNum(block.getByPosition(arguments[1]).column, *tuple_type_concrete);
        ColumnPtr res = tuple_col_concrete->getColumns()[index];

        /// Wrap into Arrays
        for (auto it = array_offsets.rbegin(); it != array_offsets.rend(); ++it)
            res = ColumnArray::create(res, *it);

        block.getByPosition(result).column = res;
    }

private:
    size_t getElementNum(const ColumnPtr & index_column, const DataTypeTuple & tuple) const
    {
        if (auto index_col = checkAndGetColumnConst<ColumnUInt8>(index_column.get()))
        {
            size_t index = index_col->getValue<UInt8>();

            if (index == 0)
                throw Exception("Indices in tuples are 1-based.", ErrorCodes::ILLEGAL_INDEX);

            if (index > tuple.getElements().size())
                throw Exception("Index for tuple element is out of range.", ErrorCodes::ILLEGAL_INDEX);

            return index - 1;
        }
        else if (auto name_col = checkAndGetColumnConst<ColumnString>(index_column.get()))
        {
            return tuple.getPositionByName(name_col->getValue<String>());
        }
        else
            throw Exception("Second argument to " + getName() + " must be a constant UInt8 or String", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
};


void registerFunctionsTuple(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTuple>();
    factory.registerFunction<FunctionTupleElement>();
}

}
