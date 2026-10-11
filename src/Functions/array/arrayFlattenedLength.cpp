#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

class FunctionArrayFlattenedLength final : public IFunction
{
public:
    static constexpr auto name = "arrayFlattenedLength";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayFlattenedLength>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    /// Only the offsets of the argument are read, so lazy evaluation would cost more than the function itself, same as for `length`.
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isArray(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}, expected Array",
                            arguments[0]->getName(), getName());

        return std::make_shared<DataTypeUInt64>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        /** Only the offsets of the argument column are needed: descend to the innermost level the same way as
          * `arrayFlatten` does, by selecting elements of the deeper offsets by values of the ancestor offsets,
          * and take the differences of the result.
          * See the comment in arrayFlatten.cpp for a detailed description of the offsets cascade.
          * The elements are still read from storage: unlike `length`, this function is not rewritten to the
          * `sizeN` subcolumns in `FunctionToSubcolumnsPass`.
          */

        const ColumnArray * src_col = checkAndGetColumn<ColumnArray>(arguments[0].column.get());

        if (!src_col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} in argument of function {}",
                arguments[0].column->getName(), getName());

        const IColumn::Offsets * offsets = &src_col->getOffsets();
        const IColumn * data = &src_col->getData();

        ColumnArray::ColumnOffsets::MutablePtr flat_offsets_column;
        while (const ColumnArray * nested_col = checkAndGetColumn<ColumnArray>(data))
        {
            if (!flat_offsets_column)
                flat_offsets_column = ColumnArray::ColumnOffsets::create(input_rows_count);

            IColumn::Offsets & flat_offsets = flat_offsets_column->getData();
            const IColumn::Offsets & nested_offsets = nested_col->getOffsets();

            for (size_t i = 0; i < input_rows_count; ++i)
                flat_offsets[i] = nested_offsets[(*offsets)[i] - 1];    /// -1 array subscript is Ok, see PaddedPODArray

            offsets = &flat_offsets;
            data = &nested_col->getData();
        }

        auto result_column = ColumnUInt64::create(input_rows_count);
        ColumnUInt64::Container & result_data = result_column->getData();

        for (size_t i = 0; i < input_rows_count; ++i)
            result_data[i] = (*offsets)[i] - (*offsets)[i - 1];    /// -1 array subscript is Ok, see PaddedPODArray

        return result_column;
    }
};


REGISTER_FUNCTION(ArrayFlattenedLength)
{
    FunctionDocumentation::Description description = R"(
Returns the total number of elements of a multidimensional array, as if it was flattened with the [`arrayFlatten`](/sql-reference/functions/array-functions#arrayFlatten) function.

Function:

- Applies to any depth of nested arrays.
- Is equivalent to [`length`](/sql-reference/functions/array-functions#length) for arrays that are already flat.
- Follows only `Array` nesting. The elements are counted at the first level that is not an array, so elements of a nested [`Map`](/sql-reference/data-types/map), [`Tuple`](/sql-reference/data-types/tuple), [`Dynamic`](/sql-reference/data-types/dynamic) or [`Variant`](/sql-reference/data-types/variant) are not counted individually, even when they hold arrays themselves.

This is the equivalent of `cardinality` in PostgreSQL, whereas in ClickHouse `cardinality` is an alias of `length` and counts only the elements of the outermost array.
)";
    FunctionDocumentation::Syntax syntax = "arrayFlattenedLength(arr)";
    FunctionDocumentation::Arguments arguments = {
        {"arr", "A possibly multidimensional array.", {"Array(T)"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the number of elements of the array after all nested arrays are flattened.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {
        {"Nested array", "SELECT arrayFlattenedLength([[1, 2], [3, 4]]);", "4"},
        {"Deeper nesting", "SELECT arrayFlattenedLength([[[1]], [[2], [3]]]);", "3"},
        {"Flat array", "SELECT arrayFlattenedLength([1, 2, 3]);", "3"},
        {"Non-array elements are not traversed", "SELECT arrayFlattenedLength([map('a', [1, 2, 3])]);", "1"},
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 9};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayFlattenedLength>(documentation);
}

}
