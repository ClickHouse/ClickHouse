#include <limits>
#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/castColumn.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
}

enum class ShiftRotateStrategy : uint8_t
{
    Shift,
    Rotate
};

enum class ShiftRotateDirection : uint8_t
{
    Left,
    Right
};

template <typename Impl, typename Name>
class FunctionArrayShiftRotate : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static constexpr ShiftRotateStrategy strategy = Impl::strategy;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayShiftRotate>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return strategy == ShiftRotateStrategy::Shift; }
    size_t getNumberOfArguments() const override { return strategy == ShiftRotateStrategy::Rotate ? 2 : 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if constexpr (strategy == ShiftRotateStrategy::Shift)
        {
            if (arguments.size() < 2)
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires at least two arguments.", getName());

            if (arguments.size() > 3)
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires at most three arguments.", getName());
        }

        const DataTypePtr & first_arg = arguments[0];
        if (!isArray(first_arg))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}, expected Array",
                arguments[0]->getName(),
                getName());

        if (!isNativeInteger(arguments[1]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}, expected Native Integer",
                arguments[1]->getName(),
                getName());

        const DataTypePtr & elem_type = static_cast<const DataTypeArray &>(*first_arg).getNestedType();
        if (arguments.size() == 3)
        {
            auto ret = tryGetLeastSupertype(DataTypes{elem_type, arguments[2]});
            // Note that this will fail if the default value does not fit into the array element type (e.g. UInt64 and Array(UInt8)).
            // In this case array should be converted to Array(UInt64) explicitly.
            if (!ret || !ret->equals(*elem_type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument of function {}, expected {}",
                    arguments[2]->getName(),
                    getName(),
                    elem_type->getName());
        }

        return std::make_shared<DataTypeArray>(elem_type);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        ColumnPtr column_array_ptr = arguments[0].column;
        const auto * column_array = checkAndGetColumn<ColumnArray>(column_array_ptr.get());

        if (!column_array)
        {
            const auto * column_const_array = checkAndGetColumnConst<ColumnArray>(column_array_ptr.get());
            if (!column_const_array)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Expected Array column, found {}", column_array_ptr->getName());

            column_array_ptr = column_const_array->convertToFullColumn();
            column_array = assert_cast<const ColumnArray *>(column_array_ptr.get());
        }

        ColumnPtr shift_num_column = arguments[1].column;

        if constexpr (strategy == ShiftRotateStrategy::Shift)
        {
            ColumnPtr default_column;
            const auto elem_type = static_cast<const DataTypeArray &>(*result_type).getNestedType();

            if (arguments.size() == 3)
                default_column = castColumn(arguments[2], elem_type);
            else
                default_column = elem_type->createColumnConstWithDefaultValue(input_rows_count);

            default_column = default_column->convertToFullColumnIfConst();

            return Impl::execute(*column_array, shift_num_column, default_column, input_rows_count);
        }
        else
        {
            return Impl::execute(*column_array, shift_num_column, input_rows_count);
        }
    }
};

template <ShiftRotateDirection direction>
struct ArrayRotateImpl
{
    static constexpr ShiftRotateStrategy strategy = ShiftRotateStrategy::Rotate;
    static ColumnPtr execute(const ColumnArray & array, ColumnPtr shift_num_column, size_t input_rows_count)
    {
        size_t batch_size = array.getData().size();

        IColumn::Permutation permutation(batch_size);
        const IColumn::Offsets & offsets = array.getOffsets();

        IColumn::Offset current_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const size_t offset = offsets[i];
            const size_t nested_size = offset - current_offset;
            Int64 shift_num_value = shift_num_column->getInt(i);

            // Rotating left to -N is the same as rotating right to N.
            ShiftRotateDirection actual_direction = direction;
            if (shift_num_value < 0)
            {
                if (shift_num_value == std::numeric_limits<Int64>::min())
                    throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Shift number {} is out of range", shift_num_value);
                actual_direction = (direction == ShiftRotateDirection::Left) ? ShiftRotateDirection::Right : ShiftRotateDirection::Left;
                shift_num_value = -shift_num_value;
            }

            size_t shift_num = static_cast<size_t>(shift_num_value);
            if (nested_size > 0 && shift_num >= nested_size)
                shift_num %= nested_size;

            // Rotating left to N is the same as shifting right to (size - N).
            if (actual_direction == ShiftRotateDirection::Right)
                shift_num = nested_size - shift_num;

            for (size_t j = 0; j < nested_size; ++j)
                permutation[current_offset + j] = current_offset + (j + shift_num) % nested_size;

            current_offset = offset;
        }

        return ColumnArray::create(array.getData().permute(permutation, 0), array.getOffsetsPtr());
    }
};

template <ShiftRotateDirection direction>
struct ArrayShiftImpl
{
    static constexpr ShiftRotateStrategy strategy = ShiftRotateStrategy::Shift;

    static ColumnPtr
    execute(const ColumnArray & array, ColumnPtr shift_column, ColumnPtr default_column, size_t input_column_rows)
    {
        const IColumn::Offsets & offsets = array.getOffsets();
        const IColumn & array_data = array.getData();
        const size_t data_size = array_data.size();

        auto result_column = array.getData().cloneEmpty();
        result_column->reserve(data_size);

        IColumn::Offset current_offset = 0;
        for (size_t i = 0; i < input_column_rows; ++i)
        {
            const size_t offset = offsets[i];
            const size_t nested_size = offset - current_offset;
            Int64 shift_num_value = shift_column->getInt(i);

            // Shifting left to -N is the same as shifting right to N.
            ShiftRotateDirection actual_direction = direction;
            if (shift_num_value < 0)
            {
                if (shift_num_value == std::numeric_limits<Int64>::min())
                    throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Shift number {} is out of range", shift_num_value);
                actual_direction = (direction == ShiftRotateDirection::Left) ? ShiftRotateDirection::Right : ShiftRotateDirection::Left;
                shift_num_value = -shift_num_value;
            }

            const size_t number_of_default_values = std::min(static_cast<size_t>(shift_num_value), nested_size);
            const size_t num_of_original_values = nested_size - number_of_default_values;

            if (actual_direction == ShiftRotateDirection::Right)
            {
                result_column->insertManyFrom(*default_column, i, number_of_default_values);
                result_column->insertRangeFrom(array_data, current_offset, num_of_original_values);
            }
            else
            {
                result_column->insertRangeFrom(array_data, current_offset + number_of_default_values, num_of_original_values);
                result_column->insertManyFrom(*default_column, i, number_of_default_values);
            }

            current_offset = offset;
        }

        return ColumnArray::create(std::move(result_column), array.getOffsetsPtr());
    }
};

struct NameArrayShiftLeft
{
    static constexpr auto name = "arrayShiftLeft";
};

struct NameArrayShiftRight
{
    static constexpr auto name = "arrayShiftRight";
};

struct NameArrayRotateLeft
{
    static constexpr auto name = "arrayRotateLeft";
};

struct NameArrayRotateRight
{
    static constexpr auto name = "arrayRotateRight";
};

using ArrayShiftLeftImpl = ArrayShiftImpl<ShiftRotateDirection::Left>;
using FunctionArrayShiftLeft = FunctionArrayShiftRotate<ArrayShiftLeftImpl, NameArrayShiftLeft>;

using ArrayShiftRightImpl = ArrayShiftImpl<ShiftRotateDirection::Right>;
using FunctionArrayShiftRight = FunctionArrayShiftRotate<ArrayShiftRightImpl, NameArrayShiftRight>;

using ArrayRotateLeftImpl = ArrayRotateImpl<ShiftRotateDirection::Left>;
using FunctionArrayRotateLeft = FunctionArrayShiftRotate<ArrayRotateLeftImpl, NameArrayRotateLeft>;

using ArrayRotateRightImpl = ArrayRotateImpl<ShiftRotateDirection::Right>;
using FunctionArrayRotateRight = FunctionArrayShiftRotate<ArrayRotateRightImpl, NameArrayRotateRight>;


REGISTER_FUNCTION(ArrayShiftOrRotate)
{
    FunctionDocumentation::Description description_rotateleft = "Rotates an array to the left by the specified number of elements. Negative values of `n` are treated as rotating to the right by the absolute value of the rotation.";
    FunctionDocumentation::Syntax syntax_rotateleft = "arrayRotateLeft(arr, n)";
    FunctionDocumentation::Arguments arguments_rotateleft =
    {
        {"arr", "The array for which to rotate the elements.[`Array(T)`](/sql-reference/data-types/array)."},
        {"n", "Number of elements to rotate. [`(U)Int8/16/32/64`](/sql-reference/data-types/int-uint)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_rotateleft = {"An array rotated to the left by the specified number of elements", {"Array(T)"}};
    FunctionDocumentation::Examples examples_rotateleft = {
        {"Usage example", "SELECT arrayRotateLeft([1,2,3,4,5,6], 2) as res;", "[3,4,5,6,1,2]"},
        {"Negative value of n", "SELECT arrayRotateLeft([1,2,3,4,5,6], -2) as res;", "[5,6,1,2,3,4]"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_rotateleft = {23, 8};
    FunctionDocumentation::Category category_rotateleft = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation_rotateleft = {
        description_rotateleft,
        syntax_rotateleft,
        arguments_rotateleft,
        returned_value_rotateleft,
        examples_rotateleft,
        introduced_in_rotateleft,
        category_rotateleft
    };

    factory.registerFunction<FunctionArrayRotateLeft>(documentation_rotateleft);

    FunctionDocumentation::Description description_rotateright = "Rotates an array to the right by the specified number of elements. Negative values of `n` are treated as rotating to the left by the absolute value of the rotation.";
    FunctionDocumentation::Syntax syntax_rotateright = "arrayRotateRight(arr, n)";
    FunctionDocumentation::Arguments arguments_rotateright =
    {
        {"arr", "The array for which to rotate the elements.[`Array(T)`](/sql-reference/data-types/array)."},
        {"n", "Number of elements to rotate. [`(U)Int8/16/32/64`](/sql-reference/data-types/int-uint)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_rotateright = {"An array rotated to the right by the specified number of elements", {"Array(T)"}};
    FunctionDocumentation::Examples examples_rotateright =
    {
        {"Usage example", "SELECT arrayRotateRight([1,2,3,4,5,6], 2) as res;", "[5,6,1,2,3,4]"},
        {"Negative value of n", "SELECT arrayRotateRight([1,2,3,4,5,6], -2) as res;", "[3,4,5,6,1,2]"},
    };
    FunctionDocumentation::IntroducedIn introduced_in_rotateright = {23, 8};
    FunctionDocumentation::Category category_rotateright = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation_rotateright = {
        description_rotateright,
        syntax_rotateright,
        arguments_rotateright,
        returned_value_rotateright,
        examples_rotateright,
        introduced_in_rotateright,
        category_rotateright
    };

    factory.registerFunction<FunctionArrayRotateRight>(documentation_rotateright);

    FunctionDocumentation::Description description_shiftleft = R"(
Shifts an array to the left by the specified number of elements.
New elements are filled with the provided argument or the default value of the array element type.
If the number of elements is negative, the array is shifted to the right.
    )";
    FunctionDocumentation::Syntax syntax_shiftleft = "arrayShiftLeft(arr, n[, default])";
    FunctionDocumentation::Arguments arguments_shiftleft =
    {
        {"arr", "The array for which to shift the elements.[`Array(T)`](/sql-reference/data-types/array)."},
        {"n", "Number of elements to shift.[`(U)Int8/16/32/64`](/sql-reference/data-types/int-uint)."},
        {"default", "Optional. Default value for new elements."}
    };
    FunctionDocumentation::ReturnedValue returned_value_shiftleft = {"An array shifted to the left by the specified number of elements", {"Array(T)"}};
    FunctionDocumentation::Examples examples_shiftleft =
    {
        {"Usage example", "SELECT arrayShiftLeft([1,2,3,4,5,6], 2) as res;", "[3,4,5,6,0,0]"},
        {"Negative value of n", "SELECT arrayShiftLeft([1,2,3,4,5,6], -2) as res;", "[0,0,1,2,3,4]"},
        {"Using a default value", "SELECT arrayShiftLeft([1,2,3,4,5,6], 2, 42) as res;", "[3,4,5,6,42,42]"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_shiftleft = {23, 8};
    FunctionDocumentation::Category category_shiftleft = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation_shiftleft =
    {
        description_shiftleft,
        syntax_shiftleft,
        arguments_shiftleft,
        returned_value_shiftleft,
        examples_shiftleft,
        introduced_in_shiftleft,
        category_shiftleft
    };

    factory.registerFunction<FunctionArrayShiftLeft>(documentation_shiftleft);

    FunctionDocumentation::Description description_shiftright = R"(
Shifts an array to the right by the specified number of elements.
New elements are filled with the provided argument or the default value of the array element type.
If the number of elements is negative, the array is shifted to the left.
    )";
    FunctionDocumentation::Syntax syntax_shiftright = "arrayShiftRight(arr, n[, default])";
    FunctionDocumentation::Arguments arguments_shiftright =
    {
        {"arr", "The array for which to shift the elements.", {"Array(T)"}},
        {"n", "Number of elements to shift.", {"(U)Int8/16/32/64"}},
        {"default", "Optional. Default value for new elements."},
    };
    FunctionDocumentation::ReturnedValue returned_value_shiftright =
    {
        "An array shifted to the right by the specified number of elements", {"Array(T)"}
    };
    FunctionDocumentation::Examples examples_shiftright =
    {
        {"Usage example", "SELECT arrayShiftRight([1, 2, 3, 4, 5, 6], 2) as res;", "[0, 0, 1, 2, 3, 4]"},
        {"Negative value of n", "SELECT arrayShiftRight([1, 2, 3, 4, 5, 6], -2) as res;", "[3, 4, 5, 6, 0, 0]"},
        {"Using a default value", "SELECT arrayShiftRight([1, 2, 3, 4, 5, 6], 2, 42) as res;", "[42, 42, 1, 2, 3, 4]"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_shiftright = {23, 8};
    FunctionDocumentation::Category category_shiftright = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation_shiftright = {description_shiftright, syntax_shiftright, arguments_shiftright, returned_value_shiftright, examples_shiftright, introduced_in_shiftright, category_shiftright};

    factory.registerFunction<FunctionArrayShiftRight>(documentation_shiftright);
}

}
