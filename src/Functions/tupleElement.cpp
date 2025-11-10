#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeQBit.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/NullableUtils.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnQBit.h>
#include <Columns/ColumnNullable.h>
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
  * The logic of qbitElement is integrated into this function because AST makes any dot syntax (vec.i) a tupleElement(vec, i) call.
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

        /// Need later to decide whether to wrap result_type with Nullable
        bool is_input_type_nullable = false;
        if (const DataTypeNullable * nullable_type = checkAndGetDataType<DataTypeNullable>(input_type))
        {
            is_input_type_nullable = true;
            input_type = nullable_type->getNestedType().get();
        }

        if (const DataTypeTuple * tuple = checkAndGetDataType<DataTypeTuple>(input_type))
        {
            std::optional<size_t> index = getTupleElementIndex(arguments[1].column, *tuple, number_of_arguments);
            if (index.has_value())
            {
                DataTypePtr return_type = tuple->getElements()[index.value()];

                if (is_input_type_nullable && return_type->canBeInsideNullable())
                    return_type = std::make_shared<DataTypeNullable>(return_type);

                for (; count_arrays; --count_arrays)
                    return_type = std::make_shared<DataTypeArray>(return_type);

                return return_type;
            }
            return arguments[2].type;
        }
        else if (const DataTypeQBit * qbit = checkAndGetDataType<DataTypeQBit>(input_type))
        {
            if (is_input_type_nullable)
            {
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "First argument for function {} cannot be Nullable(QBit). Actual {}",
                    getName(),
                    arguments[0].type->getName());
            }

            std::optional<size_t> index = getQBitElementIndex(arguments[1].column, *qbit, number_of_arguments);
            if (index.has_value())
            {
                DataTypePtr return_type = qbit->getNestedTupleElementType();

                for (; count_arrays; --count_arrays)
                    return_type = std::make_shared<DataTypeArray>(return_type);

                return return_type;
            }
            return arguments[2].type;
        }

        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "First argument for function {} must be Tuple, Nullable(Tuple), array of Tuple, array of Nullable(Tuple), QBit or array of QBit. Actual {}",
            getName(),
            arguments[0].type->getName());
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

        /// Handle Nullable(Tuple)
        ColumnPtr null_map_column = nullptr;
        if (const ColumnNullable * nullable_col = checkAndGetColumn<ColumnNullable>(input_col))
        {
            null_map_column = nullable_col->getNullMapColumnPtr();
            input_col = &nullable_col->getNestedColumn();

            if (const DataTypeNullable * nullable_type = checkAndGetDataType<DataTypeNullable>(input_type))
            {
                input_type = nullable_type->getNestedType().get();
            }
        }


        const DataTypeTuple * input_type_as_tuple = checkAndGetDataType<DataTypeTuple>(input_type);
        const ColumnTuple * input_col_as_tuple = checkAndGetColumn<ColumnTuple>(input_col);

        if (input_type_as_tuple && input_col_as_tuple)
        {
            std::optional<size_t> index = getTupleElementIndex(arguments[1].column, *input_type_as_tuple, arguments.size());

            if (!index.has_value())
                return arguments[2].column;

            ColumnPtr res = input_col_as_tuple->getColumnPtr(index.value());

            DataTypePtr element_type = input_type_as_tuple->getElements()[index.value()];

            /// If both Nullable(Tuple) and Nullable(Tuple[index]) possible
            bool should_wrap_nullable = null_map_column && element_type->canBeInsideNullable();

            if (should_wrap_nullable)
            {
                res = ColumnNullable::create(res, null_map_column);
            }
            else if (null_map_column)
            {
                const auto & null_map = assert_cast<const ColumnUInt8 &>(*null_map_column).getData();

                auto result_column = element_type->createColumn();
                result_column->reserve(res->size());

                Field default_field = element_type->getDefault();

                for (size_t i = 0; i < res->size(); ++i)
                {
                    if (null_map[i])
                    {
                        result_column->insert(default_field);
                    }
                    else
                    {
                        result_column->insertFrom(*res, i);
                    }
                }

                res = std::move(result_column);
            }

            /// Wrap into Arrays
            for (auto it = array_offsets.rbegin(); it != array_offsets.rend(); ++it)
                res = ColumnArray::create(res, *it);

            if (input_arg_is_const)
                res = ColumnConst::create(res, input_rows_count);

            return res;
        }


        const DataTypeQBit * input_type_as_qbit = checkAndGetDataType<DataTypeQBit>(input_type);
        const ColumnQBit * input_col_as_qbit = checkAndGetColumn<ColumnQBit>(input_col);

        if (input_type_as_qbit && input_col_as_qbit)
        {
            std::optional<size_t> index = getQBitElementIndex(arguments[1].column, *input_type_as_qbit, arguments.size());

            if (!index.has_value())
                return arguments[2].column;

            ColumnPtr res = assert_cast<const ColumnTuple &>(input_col_as_qbit->getTupleColumn()).getColumnPtr(index.value());

            /// Wrap into Arrays
            for (auto it = array_offsets.rbegin(); it != array_offsets.rend(); ++it)
                res = ColumnArray::create(res, *it);

            if (input_arg_is_const)
                res = ColumnConst::create(res, input_rows_count);

            return res;
        }


        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "First argument for function {} must be Tuple, array of Tuple, QBit or array of QBit. Actual {}",
            getName(),
            input_arg.type->getName());
    }

private:
    std::optional<size_t> getTupleElementIndex(const ColumnPtr & index_column, const DataTypeTuple & tuple, size_t argument_size) const
    {
        if (checkAndGetColumnConst<ColumnUInt8>(index_column.get()) || checkAndGetColumnConst<ColumnUInt16>(index_column.get())
            || checkAndGetColumnConst<ColumnUInt32>(index_column.get()) || checkAndGetColumnConst<ColumnUInt64>(index_column.get()))
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

    std::optional<size_t> getQBitElementIndex(const ColumnPtr & index_column, const DataTypeQBit & qbit, size_t argument_size) const
    {
        if (checkAndGetColumnConst<ColumnUInt8>(index_column.get()) || checkAndGetColumnConst<ColumnUInt16>(index_column.get())
            || checkAndGetColumnConst<ColumnUInt32>(index_column.get()) || checkAndGetColumnConst<ColumnUInt64>(index_column.get()))
        {
            const size_t index = index_column->getUInt(0);

            if (index > 0 && index <= qbit.getElementSize())
                return {index - 1};

            if (argument_size == 2)
                throw Exception(ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK, "QBit doesn't have an element with index '{}'", index);

            return std::nullopt;
        }
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument to {} must be a constant UInt", getName());
    }
};

}

REGISTER_FUNCTION(TupleElement)
{
    FunctionDocumentation::Description description = R"(
Extracts an element from a tuple by index or name.

For access by index, an 1-based numeric index is expected.
For access by name, the element name can be provided as a string (works only for named tuples).

An optional third argument specifies a default value which is returned instead of throwing an exception when the accessed element does not exist.
All arguments must be constants.

This function has zero runtime cost and implements the operators `x.index` and `x.name`.
)";
    FunctionDocumentation::Syntax syntax = "tupleElement(tuple, index|name[, default_value])";
    FunctionDocumentation::Arguments arguments = {
        {"tuple", "A tuple or array of tuples.", {"Tuple(T)", "Array(Tuple(T))"}},
        {"index", "Column index, starting from 1.", {"const UInt8/16/32/64"}},
        {"name", "Name of the element.", {"const String"}},
        {"default_value", "Default value returned when index is out of bounds or element doesn't exist.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the element at the specified index or name.", {"Any"}};
    FunctionDocumentation::Examples examples = {
    {
         "Index access",
         "SELECT tupleElement((1, 'hello'), 2)",
         "hello"
    },
    {
        "Named tuple with table",
         R"(
CREATE TABLE example (values Tuple(name String, age UInt32)) ENGINE = Memory;
INSERT INTO example VALUES (('Alice', 30));
SELECT tupleElement(values, 'name') FROM example;
         )",
         "Alice"
    },
    {
        "With default value",
        "SELECT tupleElement((1, 2), 5, 'not_found')",
        "not_found"
    },
    {
        "Operator syntax",
        "SELECT (1, 'hello').2",
        "hello"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Tuple;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTupleElement>(documentation);
}

}
