#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

/// arrayFlatten([[1, 2, 3], [4, 5]]) = [1, 2, 3, 4, 5] - flatten array.
class ArrayFlatten : public IFunction
{
public:
    static constexpr auto name = "arrayFlatten";

    static FunctionPtr create(ContextPtr) { return std::make_shared<ArrayFlatten>(); }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isArray(arguments[0]))
            throw Exception("Illegal type " + arguments[0]->getName() +
                            " of argument of function " + getName() +
                            ", expected Array", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        DataTypePtr nested_type = arguments[0];
        while (isArray(nested_type))
            nested_type = checkAndGetDataType<DataTypeArray>(nested_type.get())->getNestedType();

        return std::make_shared<DataTypeArray>(nested_type);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        /** We create an array column with array elements as the most deep elements of nested arrays,
          * and construct offsets by selecting elements of most deep offsets by values of ancestor offsets.
          *
Example 1:

Source column: Array(Array(UInt8)):
Row 1: [[1, 2, 3], [4, 5]], Row 2: [[6], [7, 8]]
data: [1, 2, 3], [4, 5], [6], [7, 8]
offsets: 2, 4
data.data: 1 2 3 4 5 6 7 8
data.offsets: 3 5 6 8

Result column: Array(UInt8):
Row 1: [1, 2, 3, 4, 5], Row 2: [6, 7, 8]
data: 1 2 3 4 5 6 7 8
offsets: 5 8

Result offsets are selected from the most deep (data.offsets) by previous deep (offsets) (and values are decremented by one):
3 5 6 8
  ^   ^

Example 2:

Source column: Array(Array(Array(UInt8))):
Row 1: [[], [[1], [], [2, 3]]], Row 2: [[[4]]]

most deep data: 1 2 3 4

offsets1: 2 3
offsets2: 0 3 4
-           ^ ^ - select by prev offsets
offsets3: 1 1 3 4
-             ^ ^ - select by prev offsets

result offsets: 3, 4
result: Row 1: [1, 2, 3], Row2: [4]
          */

        const ColumnArray * src_col = checkAndGetColumn<ColumnArray>(arguments[0].column.get());

        if (!src_col)
            throw Exception("Illegal column " + arguments[0].column->getName() + " in argument of function 'arrayFlatten'",
                ErrorCodes::ILLEGAL_COLUMN);

        const IColumn::Offsets & src_offsets = src_col->getOffsets();

        ColumnArray::ColumnOffsets::MutablePtr result_offsets_column;
        const IColumn::Offsets * prev_offsets = &src_offsets;
        const IColumn * prev_data = &src_col->getData();

        while (const ColumnArray * next_col = checkAndGetColumn<ColumnArray>(prev_data))
        {
            if (!result_offsets_column)
                result_offsets_column = ColumnArray::ColumnOffsets::create(input_rows_count);

            IColumn::Offsets & result_offsets = result_offsets_column->getData();

            const IColumn::Offsets * next_offsets = &next_col->getOffsets();

            for (size_t i = 0; i < input_rows_count; ++i)
                result_offsets[i] = (*next_offsets)[(*prev_offsets)[i] - 1];    /// -1 array subscript is Ok, see PaddedPODArray

            prev_offsets = &result_offsets;
            prev_data = &next_col->getData();
        }

        return ColumnArray::create(
            prev_data->getPtr(),
            result_offsets_column ? std::move(result_offsets_column) : src_col->getOffsetsPtr());
    }

private:
    String getName() const override
    {
        return name;
    }
};


void registerFunctionArrayFlatten(FunctionFactory & factory)
{
    factory.registerFunction<ArrayFlatten>();
    factory.registerAlias("flatten", "arrayFlatten", FunctionFactory::CaseInsensitive);
}

}
