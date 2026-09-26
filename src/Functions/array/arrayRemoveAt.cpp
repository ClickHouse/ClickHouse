#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeArray.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

#include <algorithm>
#include <limits>
#include <optional>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ZERO_ARRAY_OR_TUPLE_INDEX;
}

namespace
{

std::optional<size_t> getRemovePosition(Int64 index, size_t array_size)
{
    if (index == 0)
        throw Exception(ErrorCodes::ZERO_ARRAY_OR_TUPLE_INDEX, "Array indices are 1-based");

    if (index > 0)
    {
        const UInt64 positive_index = static_cast<UInt64>(index);
        if (positive_index > array_size)
            return std::nullopt;

        return static_cast<size_t>(positive_index - 1);
    }

    /// Compute |index| in the unsigned domain so INT64_MIN is handled without overflow.
    const UInt64 distance_from_end = UInt64(0) - static_cast<UInt64>(index);
    if (distance_from_end > array_size)
        return std::nullopt;

    return array_size - static_cast<size_t>(distance_from_end);
}

std::optional<size_t> getRemovePosition(UInt64 index, size_t array_size)
{
    if (index == 0)
        throw Exception(ErrorCodes::ZERO_ARRAY_OR_TUPLE_INDEX, "Array indices are 1-based");

    if (index > array_size)
        return std::nullopt;

    return static_cast<size_t>(index - 1);
}

class FunctionArrayRemoveAt final : public IFunction
{
public:
    static constexpr auto name = "arrayRemoveAt";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayRemoveAt>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments[0]->onlyNull())
            return arguments[0];

        const auto * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
        if (!array_type)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} must be an array but it has type {}",
                getName(),
                arguments[0]->getName());

        if (!isNativeInteger(arguments[1]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument for function {} must be a non-Nullable native integer but it has type {}",
                getName(),
                arguments[1]->getName());

        return arguments[0];
    }

    ColumnPtr executeImpl(
        const ColumnsWithTypeAndName & arguments,
        const DataTypePtr & result_type,
        size_t input_rows_count) const override
    {
        if (result_type->onlyNull())
            return result_type->createColumnConstWithDefaultValue(input_rows_count);

        ColumnPtr array_column = arguments[0].column;
        bool array_is_const = false;
        if (const auto * const_array = checkAndGetColumnConst<ColumnArray>(array_column.get()))
        {
            array_is_const = true;
            array_column = const_array->getDataColumnPtr();
        }

        const auto * array = checkAndGetColumn<ColumnArray>(array_column.get());
        if (!array)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "First argument for function {} must be Array, got {}",
                getName(),
                arguments[0].column->getName());

        const auto & source_data = array->getData();
        const auto & source_offsets = array->getOffsets();
        const auto & index_column = *arguments[1].column;
        const bool index_is_unsigned = isUInt(arguments[1].type);
        const bool index_is_const = isColumnConst(index_column);

        UInt64 constant_index_distance = 0;
        bool constant_index_from_end = false;
        if (index_is_const)
        {
            if (index_is_unsigned)
            {
                constant_index_distance = index_column.getUInt(0);
                if (constant_index_distance == 0)
                    throw Exception(ErrorCodes::ZERO_ARRAY_OR_TUPLE_INDEX, "Array indices are 1-based");
            }
            else
            {
                const Int64 constant_index = index_column.getInt(0);
                if (constant_index == 0)
                    throw Exception(ErrorCodes::ZERO_ARRAY_OR_TUPLE_INDEX, "Array indices are 1-based");

                constant_index_from_end = constant_index < 0;
                constant_index_distance = constant_index_from_end
                    ? UInt64(0) - static_cast<UInt64>(constant_index)
                    : static_cast<UInt64>(constant_index);
            }

            /// No individual array can be longer than the whole nested column.
            /// If even that is shorter than the requested position, every row is unchanged.
            if (constant_index_distance > static_cast<UInt64>(source_data.size()))
                return arguments[0].column;
        }

        size_t result_size = 0;

        auto get_remove_position_for_row = [&](size_t row, size_t array_size) -> std::optional<size_t>
        {
            if (index_is_const)
            {
                if (constant_index_distance <= array_size)
                {
                    return constant_index_from_end
                        ? array_size - static_cast<size_t>(constant_index_distance)
                        : static_cast<size_t>(constant_index_distance - 1);
                }

                return std::nullopt;
            }

            return index_is_unsigned
                ? getRemovePosition(index_column.getUInt(row), array_size)
                : getRemovePosition(index_column.getInt(row), array_size);
        };

        if (array_is_const)
        {
            const size_t constant_array_size = source_offsets.empty() ? 0 : source_offsets[0];

            size_t removal_count = 0;
            if (index_is_const)
            {
                removal_count = input_rows_count;
            }
            else
            {
                /// Do not materialize a constant array when every dynamic index is out of bounds.
                for (size_t row = 0; row < input_rows_count; ++row)
                {
                    if (get_remove_position_for_row(row, constant_array_size))
                        ++removal_count;
                }

                if (removal_count == 0)
                    return arguments[0].column;
            }

            auto result_data = source_data.cloneEmpty();
            size_t reserve_size = 0;
            if (constant_array_size != 0 && input_rows_count != 0)
            {
                if (constant_array_size <= std::numeric_limits<size_t>::max() / input_rows_count)
                    reserve_size = constant_array_size * input_rows_count - removal_count;
                else if (constant_array_size - 1 <= std::numeric_limits<size_t>::max() / input_rows_count)
                    reserve_size = (constant_array_size - 1) * input_rows_count;
            }
            result_data->reserve(reserve_size);

            auto result_offsets_column = ColumnArray::ColumnOffsets::create(input_rows_count);
            auto & result_offsets = result_offsets_column->getData();
            for (size_t row = 0; row < input_rows_count; ++row)
            {
                const auto remove_position = get_remove_position_for_row(row, constant_array_size);
                if (!remove_position)
                {
                    if (constant_array_size != 0)
                        result_data->insertRangeFrom(source_data, 0, constant_array_size);
                    result_size += constant_array_size;
                }
                else
                {
                    const size_t prefix_size = *remove_position;
                    const size_t suffix_size = constant_array_size - prefix_size - 1;

                    if (prefix_size != 0)
                        result_data->insertRangeFrom(source_data, 0, prefix_size);
                    if (suffix_size != 0)
                        result_data->insertRangeFrom(source_data, prefix_size + 1, suffix_size);

                    result_size += constant_array_size - 1;
                }

                result_offsets[row] = result_size;
            }

            return ColumnArray::create(std::move(result_data), std::move(result_offsets_column));
        }

        MutableColumnPtr result_data;
        ColumnArray::ColumnOffsets::MutablePtr result_offsets_column;
        size_t source_begin = 0;
        size_t copy_begin = 0;

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const size_t source_end = source_offsets[row];
            const size_t array_size = source_end - source_begin;
            const auto remove_position = get_remove_position_for_row(row, array_size);

            if (!remove_position)
            {
                result_size += array_size;
            }
            else
            {
                if (!result_data)
                {
                    result_data = source_data.cloneEmpty();
                    /// At most one element is removed per row, so this is a safe lower bound for the result.
                    const size_t reserve_size = source_data.size() > input_rows_count
                        ? source_data.size() - input_rows_count
                        : 0;
                    result_data->reserve(reserve_size);

                    result_offsets_column = ColumnArray::ColumnOffsets::create(input_rows_count);
                    auto & result_offsets = result_offsets_column->getData();
                    /// Before the first removal, all preceding rows are unchanged.
                    std::copy(source_offsets.begin(), source_offsets.begin() + row, result_offsets.begin());
                }

                const size_t absolute_remove_position = source_begin + *remove_position;
                if (absolute_remove_position > copy_begin)
                {
                    result_data->insertRangeFrom(
                        source_data,
                        copy_begin,
                        absolute_remove_position - copy_begin);
                }

                copy_begin = absolute_remove_position + 1;
                result_size += array_size - 1;
            }

            if (result_offsets_column)
                result_offsets_column->getData()[row] = result_size;
            source_begin = source_end;
        }

        if (!result_data)
            return arguments[0].column;

        if (copy_begin < source_data.size())
            result_data->insertRangeFrom(source_data, copy_begin, source_data.size() - copy_begin);

        return ColumnArray::create(std::move(result_data), std::move(result_offsets_column));
    }
};

}

REGISTER_FUNCTION(ArrayRemoveAt)
{
    FunctionDocumentation::Description description = R"(
Removes the element at the specified index from an array.
Indexes are 1-based. Negative indexes count from the end of the array.
If the index is outside the array bounds, the array is returned unchanged.
Index 0 is invalid.
)";
    FunctionDocumentation::Syntax syntax = "arrayRemoveAt(arr, index)";
    FunctionDocumentation::Arguments arguments = {
        {"arr", "Source array.", {"Array(T)"}},
        {"index", "Non-Nullable integer index of the element to remove. Negative indexes count from the end.", {"(U)Int8/16/32/64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns the source array without the element at `index`, or the original array if `index` is out of bounds.",
        {"Array(T)"}
    };
    FunctionDocumentation::Examples examples = {
        {"Positive index", "SELECT arrayRemoveAt([1, 2, 3, 4], 2)", "[1,3,4]"},
        {"Negative index", "SELECT arrayRemoveAt([1, 2, 3, 4], -1)", "[1,2,3]"},
        {"Out of bounds", "SELECT arrayRemoveAt([1, 2, 3, 4], 10)", "[1,2,3,4]"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 10};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayRemoveAt>(documentation);
}

}
