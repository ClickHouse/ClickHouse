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
    extern const int NUMBER_OF_DIMENSIONS_MISMATCHED;
    extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
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

        auto index = getElementNum(arguments[1].column, *tuple, number_of_arguments);
        if (index.has_value())
        {
            DataTypePtr out_return_type = tuple->getElements()[index.value()];

            for (; count_arrays; --count_arrays)
                out_return_type = std::make_shared<DataTypeArray>(out_return_type);

            return out_return_type;
        } else
        {
            const IDataType * default_col = arguments[2].type.get();
            size_t default_argument_count_arrays = 0;
            if (const DataTypeArray * array = checkAndGetDataType<DataTypeArray>(default_col))
            {
                default_argument_count_arrays = array->getNumberOfDimensions();
            }

            if (count_arrays != default_argument_count_arrays)
            {
                throw Exception(ErrorCodes::NUMBER_OF_DIMENSIONS_MISMATCHED, "Dimension of types mismatched between first argument and third argument. Dimension of 1st argument: {}. Dimension of 3rd argument: {}.",count_arrays, default_argument_count_arrays);
            }
            return arguments[2].type;
        }
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        Columns array_offsets;

        const auto & first_arg = arguments[0];

        const IDataType * tuple_type = first_arg.type.get();
        const IColumn * tuple_col = first_arg.column.get();
        bool first_arg_is_const = false;
        if (typeid_cast<const ColumnConst *>(tuple_col))
        {
            tuple_col = assert_cast<const ColumnConst *>(tuple_col)->getDataColumnPtr().get();
            first_arg_is_const = true;
        }
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

        auto index = getElementNum(arguments[1].column, *tuple_type_concrete, arguments.size());

        if (!index.has_value())
        {
            if (!array_offsets.empty())
            {
                checkArrayOffsets(arguments[0].column, arguments[2].column);
            }
            return arguments[2].column;
        }

        ColumnPtr res = tuple_col_concrete->getColumns()[index.value()];

        /// Wrap into Arrays
        for (auto it = array_offsets.rbegin(); it != array_offsets.rend(); ++it)
            res = ColumnArray::create(res, *it);

        if (first_arg_is_const)
        {
            res = ColumnConst::create(res, input_rows_count);
        }
        return res;
    }

private:

    void checkArrayOffsets(ColumnPtr col_x, ColumnPtr col_y) const
    {
        if (isColumnConst(*col_x))
        {
            checkArrayOffsetsWithFirstArgConst(col_x, col_y);
        } else if (isColumnConst(*col_y))
        {
            checkArrayOffsetsWithFirstArgConst(col_y, col_x);
        } else
        {
            const auto & array_x = *assert_cast<const ColumnArray *>(col_x.get());
            const auto & array_y = *assert_cast<const ColumnArray *>(col_y.get());
            if (!array_x.hasEqualOffsets(array_y))
            {
                throw Exception("The argument 1 and argument 3 of function " + getName() + " have different array sizes", ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);
            }
        }
    }

    void checkArrayOffsetsWithFirstArgConst(ColumnPtr col_x, ColumnPtr col_y) const
    {
        col_x = assert_cast<const ColumnConst *>(col_x.get())->getDataColumnPtr();
        col_y = col_y->convertToFullColumnIfConst();
        const auto & array_x = *assert_cast<const ColumnArray *>(col_x.get());
        const auto & array_y = *assert_cast<const ColumnArray *>(col_y.get());

        const auto & offsets_x = array_x.getOffsets();
        const auto & offsets_y = array_y.getOffsets();

        ColumnArray::Offset prev_offset = 0;
        size_t row_size = offsets_y.size();
        for (size_t row = 0; row < row_size; ++row)
        {
            if (unlikely(offsets_x[0] != offsets_y[row] - prev_offset))
            {
                throw Exception("The argument 1 and argument 3 of function " + getName() + " have different array sizes", ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);
            }
            prev_offset = offsets_y[row];
        }
    }

    std::optional<size_t> getElementNum(const ColumnPtr & index_column, const DataTypeTuple & tuple, const size_t argument_size) const
    {
        if (
            checkAndGetColumnConst<ColumnUInt8>(index_column.get())
                || checkAndGetColumnConst<ColumnUInt16>(index_column.get())
                || checkAndGetColumnConst<ColumnUInt32>(index_column.get())
                || checkAndGetColumnConst<ColumnUInt64>(index_column.get())
        )
        {
            size_t index = index_column->getUInt(0);

            if (index == 0)
                throw Exception("Indices in tuples are 1-based.", ErrorCodes::ILLEGAL_INDEX);

            if (index > tuple.getElements().size())
                throw Exception("Index for tuple element is out of range.", ErrorCodes::ILLEGAL_INDEX);

            return std::optional<size_t>(index - 1);
        }
        else if (const auto * name_col = checkAndGetColumnConst<ColumnString>(index_column.get()))
        {
            auto index = tuple.tryGetPositionByName(name_col->getValue<String>());
            if (index.has_value())
            {
                return index;
            }

            if (argument_size == 2)
            {
                throw Exception("Tuple doesn't have element with name '" + name_col->getValue<String>() + "'", ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);
            }
            return std::nullopt;
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
