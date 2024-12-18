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
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        size_t num_arguments = arguments.size();
        if (num_arguments == 0)
        {
            auto res_col = result_type->createColumn();
            res_col->insertDefault();
            return ColumnConst::create(std::move(res_col), input_rows_count);
        }

        Columns holders(num_arguments);
        Columns tuple_columns(num_arguments);

        bool has_unaligned = false;
        size_t unaligned_index = 0;
        for (size_t i = 0; i < num_arguments; ++i)
        {
            /// Constant columns cannot be inside tuple. It's only possible to have constant tuple as a whole.
            ColumnPtr holder = arguments[i].column->convertToFullColumnIfConst();
            holders[i] = holder;

            const ColumnArray * column_array = checkAndGetColumn<ColumnArray>(holder.get());
            if (!column_array)
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Argument {} of function {} must be array. Found column {} instead.",
                    i + 1,
                    getName(),
                    holder->getName());
            tuple_columns[i] = column_array->getDataPtr();

            if (i && !column_array->hasEqualOffsets(static_cast<const ColumnArray &>(*holders[0])))
            {
                has_unaligned = true;
                unaligned_index = i;
            }
        }

        if constexpr (!allow_unaligned)
        {
            if (has_unaligned)
                throw Exception(
                    ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                    "The argument 1 and argument {} of function {} have different array sizes",
                    unaligned_index + 1,
                    getName());
            return ColumnArray::create(
                ColumnTuple::create(std::move(tuple_columns)), static_cast<const ColumnArray &>(*holders[0]).getOffsetsPtr());
        }
        else
            return executeUnaligned(holders, tuple_columns, input_rows_count, has_unaligned);
    }

private:
    ColumnPtr executeUnaligned(const Columns & holders, Columns & tuple_columns, size_t input_rows_count, bool has_unaligned) const
    {
        std::vector<const ColumnArray *> array_columns(holders.size());
        for (size_t i = 0; i < holders.size(); ++i)
            array_columns[i] = checkAndGetColumn<ColumnArray>(holders[i].get());

        for (auto & tuple_column : tuple_columns)
            tuple_column = makeNullable(tuple_column);

        if (!has_unaligned)
            return ColumnArray::create(ColumnTuple::create(std::move(tuple_columns)), array_columns[0]->getOffsetsPtr());

        MutableColumns res_tuple_columns(tuple_columns.size());
        for (size_t i = 0; i < tuple_columns.size(); ++i)
        {
            res_tuple_columns[i] = tuple_columns[i]->cloneEmpty();
            res_tuple_columns[i]->reserve(tuple_columns[i]->size());
        }

        auto res_offsets_column = ColumnArray::ColumnOffsets::create(input_rows_count);
        auto & res_offsets = assert_cast<ColumnArray::ColumnOffsets &>(*res_offsets_column).getData();
        size_t curr_offset = 0;
        for (size_t row_i = 0; row_i < input_rows_count; ++row_i)
        {
            size_t max_size = 0;
            for (size_t arg_i = 0; arg_i < holders.size(); ++arg_i)
            {
                const auto * array_column = array_columns[arg_i];
                const auto & offsets = array_column->getOffsets();
                size_t array_offset = offsets[row_i - 1];
                size_t array_size = offsets[row_i] - array_offset;

                res_tuple_columns[arg_i]->insertRangeFrom(*tuple_columns[arg_i], array_offset, array_size);
                max_size = std::max(max_size, array_size);
            }

            for (size_t arg_i = 0; arg_i < holders.size(); ++arg_i)
            {
                const auto * array_column = array_columns[arg_i];
                const auto & offsets = array_column->getOffsets();
                size_t array_offset = offsets[row_i - 1];
                size_t array_size = offsets[row_i] - array_offset;

                res_tuple_columns[arg_i]->insertManyDefaults(max_size - array_size);
            }

            curr_offset += max_size;
            res_offsets[row_i] = curr_offset;
        }

        return ColumnArray::create(ColumnTuple::create(std::move(res_tuple_columns)), std::move(res_offsets_column));
    }
};

REGISTER_FUNCTION(ArrayZip)
{
    factory.registerFunction<FunctionArrayZip<false>>(
        {.description = R"(
Combines multiple arrays into a single array. The resulting array contains the corresponding elements of the source arrays grouped into tuples in the listed order of arguments.
)",
         .categories{"String"}});

    factory.registerFunction<FunctionArrayZip<true>>(
        {.description = R"(
Combines multiple arrays into a single array, allowing for unaligned arrays. The resulting array contains the corresponding elements of the source arrays grouped into tuples in the listed order of arguments.

If the arrays have different sizes, the shorter arrays will be padded with `null` values.
)",
         .categories{"String"}}

    );
}

}
