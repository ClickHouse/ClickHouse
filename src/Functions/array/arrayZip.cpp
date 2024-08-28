#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int SIZES_OF_ARRAYS_DONT_MATCH;
extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
extern const int ILLEGAL_COLUMN;
}

/// arrayZip(['a', 'b', 'c'], ['d', 'e', 'f']) = [('a', 'd'), ('b', 'e'), ('c', 'f')]
/// arrayZipUnaligned(['a', 'b', 'c'], ['d', 'e']) = [('a', 'd'), ('b', 'e'), ('c', null)]
template <bool allow_unaligned>
class FunctionArrayZip : public IFunction
{
public:
    static constexpr auto name = allow_unaligned ? "arrayZipUnaligned" : "arrayZip";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayZip>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.empty())
            throw Exception(
                ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION,
                "Function {} needs at least one argument; passed {}.",
                getName(),
                arguments.size());

        DataTypes arguments_types;
        for (size_t index = 0; index < arguments.size(); ++index)
        {
            const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[index].type.get());

            if (!array_type)
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Argument {} of function {} must be array. Found {} instead.",
                    toString(index + 1),
                    getName(),
                    arguments[0].type->getName());

            auto nested_type = array_type->getNestedType();
            if constexpr (allow_unaligned)
                nested_type = makeNullable(nested_type);
            arguments_types.emplace_back(nested_type);
        }

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(arguments_types));
    }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        size_t num_arguments = arguments.size();

        ColumnPtr first_array_column;
        Columns tuple_columns(num_arguments);

        for (size_t i = 0; i < num_arguments; ++i)
        {
            /// Constant columns cannot be inside tuple. It's only possible to have constant tuple as a whole.
            ColumnPtr holder = arguments[i].column->convertToFullColumnIfConst();
            const ColumnArray * column_array = checkAndGetColumn<ColumnArray>(holder.get());
            if (!column_array)
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Argument {} of function {} must be array. Found column {} instead.",
                    i + 1,
                    getName(),
                    holder->getName());

            tuple_columns[i] = column_array->getDataPtr();

            if constexpr (allow_unaligned)
                tuple_columns[i] = makeNullable(tuple_columns[i]);

            if (i == 0)
            {
                first_array_column = holder;
            }
            else if (!column_array->hasEqualOffsets(static_cast<const ColumnArray &>(*first_array_column)))
            {
                if constexpr (allow_unaligned)
                    return executeUnaligned(static_cast<const ColumnArray &>(*first_array_column), *column_array, input_rows_count);
                else
                    throw Exception(
                        ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                        "The argument 1 and argument {} of function {} have different array sizes",
                        i + 1,
                        getName());
            }
        }

        return ColumnArray::create(
            ColumnTuple::create(std::move(tuple_columns)), static_cast<const ColumnArray &>(*first_array_column).getOffsetsPtr());
    }

private:
    ColumnPtr
    executeUnaligned(const ColumnArray & first_array_colmn, const ColumnArray & second_array_column, size_t input_rows_count) const
    {
        const auto & first_data = first_array_colmn.getDataPtr();
        const auto & second_data = second_array_column.getDataPtr();
        const auto & nullable_first_data = makeNullable(first_data);
        const auto & nullable_second_data = makeNullable(second_data);
        auto res_first_data = nullable_first_data->cloneEmpty();
        auto res_second_data = nullable_second_data->cloneEmpty();
        auto res_offsets_column = ColumnArray::ColumnOffsets::create(input_rows_count);
        auto & res_offsets = assert_cast<ColumnArray::ColumnOffsets &>(*res_offsets_column).getData();

        const auto & first_offsets = first_array_colmn.getOffsets();
        const auto & second_offsets = second_array_column.getOffsets();
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            size_t first_size = first_offsets[i] - first_offsets[i - 1];
            size_t second_size = second_offsets[i] - second_offsets[i - 1];

            res_first_data->insertRangeFrom(*nullable_first_data, first_offsets[i - 1], first_size);
            res_second_data->insertRangeFrom(*nullable_second_data, second_offsets[i - 1], second_size);

            if (first_size < second_size)
                res_first_data->insertManyDefaults(second_size - first_size);
            else if (first_size > second_size)
                res_second_data->insertManyDefaults(first_size - second_size);

            res_offsets[i] = std::max(first_size, second_size);
        }

        Columns tuple_columns{std::move(res_first_data), std::move(res_second_data)};
        return ColumnArray::create(ColumnTuple::create(std::move(tuple_columns)), std::move(res_offsets_column));
    }
};

REGISTER_FUNCTION(ArrayZip)
{
    factory.registerFunction<FunctionArrayZip<false>>();
    factory.registerFunction<FunctionArrayZip<true>>();
}

}
