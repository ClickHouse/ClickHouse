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
#include "Columns/ColumnNullable.h"
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

        std::vector<bool> arrays_is_nullable;
        DataTypePtr input_type = arguments[0].type;
        while (const DataTypeArray * array = checkAndGetDataType<DataTypeArray>(removeNullable(input_type).get()))
        {
            arrays_is_nullable.push_back(input_type->isNullable());
            input_type = array->getNestedType();
        }

        const DataTypeTuple * tuple = checkAndGetDataType<DataTypeTuple>(removeNullable(input_type).get());
        if (!tuple)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} must be tuple or array of tuple. Actual {}",
                getName(),
                arguments[0].type->getName());

        std::optional<size_t> index = getElementIndex(arguments[1].column, *tuple, number_of_arguments);
        if (index.has_value())
        {
            DataTypePtr return_type = tuple->getElements()[index.value()];

            /// Tuple may be wrapped in Nullable
            if (input_type->isNullable())
                return_type = makeNullable(return_type);

            /// Array may be wrapped in Nullable
            for (auto it = arrays_is_nullable.rbegin(); it != arrays_is_nullable.rend(); ++it)
            {
                return_type = std::make_shared<DataTypeArray>(return_type);
                if (*it)
                    return_type = makeNullable(return_type);
            }

            // std::cout << "return_type:" << return_type->getName() << std::endl;

            return return_type;
        }
        else
            return arguments[2].type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & input_arg = arguments[0];
        DataTypePtr input_type = input_arg.type;
        const IColumn * input_col = input_arg.column.get();

        bool input_arg_is_const = false;
        if (typeid_cast<const ColumnConst *>(input_col))
        {
            input_col = assert_cast<const ColumnConst *>(input_col)->getDataColumnPtr().get();
            input_arg_is_const = true;
        }

        Columns array_offsets;
        Columns null_maps;
        while (const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(removeNullable(input_type).get()))
        {
            const ColumnNullable * nullable_array_col = input_type->isNullable() ? checkAndGetColumn<ColumnNullable>(input_col) : nullptr;
            const ColumnArray * array_col = nullable_array_col ? checkAndGetColumn<ColumnArray>(&nullable_array_col->getNestedColumn())
                                                               : checkAndGetColumn<ColumnArray>(input_col);

            array_offsets.push_back(array_col->getOffsetsPtr());
            null_maps.push_back(nullable_array_col ? nullable_array_col->getNullMapColumnPtr() : nullptr);
            input_type = array_type->getNestedType();
            input_col = &array_col->getData();
        }

        const DataTypeTuple * input_type_as_tuple = checkAndGetDataType<DataTypeTuple>(removeNullable(input_type).get());
        if (!input_type_as_tuple)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} must be tuple or array of tuple. Actual {}", getName(), input_arg.type->getName());

        const ColumnNullable * input_col_as_nullable_tuple
            = input_type->isNullable() ? checkAndGetColumn<ColumnNullable>(input_col) : nullptr;
        const ColumnTuple * input_col_as_tuple = input_col_as_nullable_tuple
            ? checkAndGetColumn<ColumnTuple>(&input_col_as_nullable_tuple->getNestedColumn())
            : checkAndGetColumn<ColumnTuple>(input_col);

        std::optional<size_t> index = getElementIndex(arguments[1].column, *input_type_as_tuple, arguments.size());
        if (!index.has_value())
            return arguments[2].column;

        ColumnPtr res = input_col_as_tuple->getColumns()[index.value()];

        /// Wrap into Nullable if needed
        if (input_col_as_nullable_tuple)
        {
            auto res_type = input_type_as_tuple->getElements()[index.value()];
            ColumnPtr res_null_map = input_col_as_nullable_tuple->getNullMapColumnPtr();
            if (res_type->isNullable())
            {
                MutableColumnPtr mutable_res_null_map = IColumn::mutate(std::move(res_null_map));

                NullMap & res_null_map_data = assert_cast<ColumnUInt8 &>(*mutable_res_null_map).getData();
                const NullMap & src_null_map = assert_cast<const ColumnNullable &>(*res).getNullMapData();

                for (size_t i = 0, size = res_null_map_data.size(); i < size; ++i)
                    res_null_map_data[i] |= src_null_map[i];

                res_null_map = std::move(mutable_res_null_map);
                res = ColumnNullable::create(assert_cast<const ColumnNullable &>(*res).getNestedColumnPtr(), res_null_map);
            }
            else
                res = ColumnNullable::create(res, res_null_map);
        }

        /// Wrap into Arrays
        for (ssize_t i = array_offsets.size() - 1; i >= 0; --i)
        {
            res = ColumnArray::create(res, array_offsets[i]);

            /// Wrap into Nullable if needed
            if (null_maps[i])
                res = ColumnNullable::create(res, null_maps[i]);
        }

        if (input_arg_is_const)
            res = ColumnConst::create(res, input_rows_count);

        // std::cout << "res column:" << res->getName() << std::endl;

        return res;
    }

private:
    std::optional<size_t> getElementIndex(const ColumnPtr & index_column, const DataTypeTuple & tuple, size_t argument_size) const
    {
        if (checkAndGetColumnConst<ColumnUInt8>(index_column.get()) || checkAndGetColumnConst<ColumnUInt16>(index_column.get())
            || checkAndGetColumnConst<ColumnUInt32>(index_column.get()) || checkAndGetColumnConst<ColumnUInt64>(index_column.get())
            || checkAndGetColumnConst<ColumnInt8>(index_column.get()) || checkAndGetColumnConst<ColumnInt16>(index_column.get())
            || checkAndGetColumnConst<ColumnInt32>(index_column.get()) || checkAndGetColumnConst<ColumnInt64>(index_column.get()))
        {
            const ssize_t index = index_column->getInt(0);
            if (index > 0 && index <= static_cast<ssize_t>(tuple.getElements().size()))
                return {index - 1};
            else
            {
                if (argument_size == 2)
                    throw Exception(ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK, "Tuple doesn't have element with index '{}'", index);
                return std::nullopt;
            }

        }
        else if (const auto * name_col = checkAndGetColumnConst<ColumnString>(index_column.get()))
        {
            std::optional<size_t> index = tuple.tryGetPositionByName(name_col->getValue<String>());

            if (index.has_value())
                return index;
            else
            {
                if (argument_size == 2)
                    throw Exception(ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK, "Tuple doesn't have element with name '{}'", name_col->getValue<String>());
                return std::nullopt;
            }
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument to {} must be a constant UInt or String",
                getName());
    }
};

}

REGISTER_FUNCTION(TupleElement)
{
    factory.registerFunction<FunctionTupleElement>();
}

}
