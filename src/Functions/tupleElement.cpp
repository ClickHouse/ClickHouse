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

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTupleElement>(); }
    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const size_t number_of_arguments = arguments.size();

        if (number_of_arguments < 2 || number_of_arguments > 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Number of arguments for function {} doesn't match: passed {}, should be 2 or 3",
                            getName(), number_of_arguments);

        size_t count_arrays = 0;
        const IDataType * input_type = arguments[0].type.get();
        while (const DataTypeArray * array = checkAndGetDataType<DataTypeArray>(input_type))
        {
            input_type = array->getNestedType().get();
            ++count_arrays;
        }

        const DataTypeTuple * tuple = checkAndGetDataType<DataTypeTuple>(input_type);
        if (!tuple)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} must be tuple or array of tuple. Actual {}",
                getName(),
                arguments[0].type->getName());

        std::optional<size_t> index = getElementIndex(arguments[1].column, *tuple, number_of_arguments);
        if (index.has_value())
        {
            DataTypePtr return_type = tuple->getElements()[index.value()];

            for (; count_arrays; --count_arrays)
                return_type = std::make_shared<DataTypeArray>(return_type);

            return return_type;
        }
        return arguments[2].type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & input_arg = arguments[0];
        const IDataType * input_type = input_arg.type.get();
        const IColumn * input_col = input_arg.column.get();

        bool input_arg_is_const = false;
        if (typeid_cast<const ColumnConst *>(input_col))
        {
            input_col = assert_cast<const ColumnConst *>(input_col)->getDataColumnPtr().get();
            input_arg_is_const = true;
        }

        Columns array_offsets;
        while (const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(input_type))
        {
            const ColumnArray * array_col = assert_cast<const ColumnArray *>(input_col);

            input_type = array_type->getNestedType().get();
            input_col = &array_col->getData();
            array_offsets.push_back(array_col->getOffsetsPtr());
        }

        const DataTypeTuple * input_type_as_tuple = checkAndGetDataType<DataTypeTuple>(input_type);
        const ColumnTuple * input_col_as_tuple = checkAndGetColumn<ColumnTuple>(input_col);
        if (!input_type_as_tuple || !input_col_as_tuple)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} must be tuple or array of tuple. Actual {}", getName(), input_arg.type->getName());

        std::optional<size_t> index = getElementIndex(arguments[1].column, *input_type_as_tuple, arguments.size());

        if (!index.has_value())
            return arguments[2].column;

        ColumnPtr res = input_col_as_tuple->getColumns()[index.value()];

        /// Wrap into Arrays
        for (auto it = array_offsets.rbegin(); it != array_offsets.rend(); ++it)
            res = ColumnArray::create(res, *it);

        if (input_arg_is_const)
            res = ColumnConst::create(res, input_rows_count);
        return res;
    }

private:
    std::optional<size_t> getElementIndex(const ColumnPtr & index_column, const DataTypeTuple & tuple, size_t argument_size) const
    {
        if (checkAndGetColumnConst<ColumnUInt8>(index_column.get())
            || checkAndGetColumnConst<ColumnUInt16>(index_column.get())
            || checkAndGetColumnConst<ColumnUInt32>(index_column.get())
            || checkAndGetColumnConst<ColumnUInt64>(index_column.get()))
        {
            const size_t index = index_column->getUInt(0);

            if (index > 0 && index <= tuple.getElements().size())
                return {index - 1};

            if (argument_size == 2)
                throw Exception(ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK, "Tuple doesn't have element with index '{}'", index);
            return std::nullopt;
        }
        if (const auto * name_col = checkAndGetColumnConst<ColumnString>(index_column.get()))
        {
            std::optional<size_t> index = tuple.tryGetPositionByName(name_col->getValue<String>());

            if (index.has_value())
                return index;

            if (argument_size == 2)
                throw Exception(
                    ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK, "Tuple doesn't have element with name '{}'", name_col->getValue<String>());
            return std::nullopt;
        }
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument to {} must be a constant UInt or String", getName());
    }
};

}

REGISTER_FUNCTION(TupleElement)
{
    factory.registerFunction<FunctionTupleElement>();
}

}
