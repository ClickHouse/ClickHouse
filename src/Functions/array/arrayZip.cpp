#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
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
                    arguments[index].type->getName());

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
    FunctionDocumentation::Description description = "Combines multiple arrays into a single array. The resulting array contains the corresponding elements of the source arrays grouped into tuples in the listed order of arguments.";
    FunctionDocumentation::Syntax syntax = "arrayZip(arr1, arr2, ... , arrN)";
    FunctionDocumentation::Arguments argument = {{"arr1, arr2, ... , arrN", "N arrays to combine into a single array.", {"Array(T)"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an array with elements from the source arrays grouped in tuples. Data types in the tuple are the same as types of the input arrays and in the same order as arrays are passed", {"Array(T)"}};
    FunctionDocumentation::Examples example = {{"Usage example", "SELECT arrayZip(['a', 'b', 'c'], [5, 2, 1]);", "[('a', 5), ('b', 2), ('c', 1)]"}};
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, argument, returned_value, example, introduced_in, category};

    factory.registerFunction<FunctionArrayZip<false>>(documentation);

    FunctionDocumentation::Description description_unaligned = "Combines multiple arrays into a single array, allowing for unaligned arrays (arrays of differing lengths). The resulting array contains the corresponding elements of the source arrays grouped into tuples in the listed order of arguments.";
    FunctionDocumentation::Syntax syntax_unaligned = "arrayZipUnaligned(arr1, arr2, ..., arrN)";
    FunctionDocumentation::Arguments argument_unaligned = {{"arr1, arr2, ..., arrN", "N arrays to combine into a single array.", {"Array(T)"}}};
    FunctionDocumentation::ReturnedValue returned_value_unaligned = {"Returns an array with elements from the source arrays grouped in tuples. Data types in the tuple are the same as types of the input arrays and in the same order as arrays are passed.", {"Array(T)", "Tuple(T1, T2, ...)"}};
    FunctionDocumentation::Examples example_unaligned = {{"Usage example", "SELECT arrayZipUnaligned(['a'], [1, 2, 3]);", "[('a', 1),(NULL, 2),(NULL, 3)]"}};
    FunctionDocumentation::IntroducedIn introduced_in_unaligned = {20, 1};
    FunctionDocumentation::Category category_unaligned = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation_unaligned = {description_unaligned, syntax_unaligned, argument_unaligned, returned_value_unaligned, example_unaligned, introduced_in_unaligned, category_unaligned};

    factory.registerFunction<FunctionArrayZip<true>>(documentation_unaligned);
}

}
