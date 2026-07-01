#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/array/FunctionArrayMapped.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
}

/** Higher-order functions that pack the values produced by a lambda expression into a compact bit representation.
  *
  *  arrayPackBitsToUInt64(f, arr)          - pack one bit per element into a UInt64 (only the first 64 elements are used).
  *  arrayPackBitsToString(f, arr)          - pack one bit per element into a String of ceil(size / 8) bytes.
  *  arrayPackBitsToFixedString(f, n, arr)  - pack one bit per element into a FixedString of `n` bytes.
  *
  *  arrayPackBitGroupsToUInt64(f, g, arr)         - the lambda returns a number whose low `g` bits form a group; the
  *  arrayPackBitGroupsToString(f, g, arr)           groups are packed contiguously, one after another.
  *  arrayPackBitGroupsToFixedString(f, n, g, arr)
  *
  * Bits are written most-significant-bit first and groups follow the order of the array elements (the first element
  * occupies the most significant bits of the result). For the `UInt64` variants the result is a number, so only the
  * leading whole groups that fit into 64 bits are kept. For the `String`/`FixedString` variants the bit stream is laid
  * out across bytes, and a trailing partial byte keeps its bits in the most significant positions.
  */
template <typename ResultType, bool PackGroups = false>
struct ArrayPackBitsImpl
{
    static bool needBoolean() { return false; }
    static bool needExpression() { return true; }
    static bool needOneArray() { return true; }

    /// The result is a FixedString whose size is taken from a constant argument.
    static constexpr bool return_fixed_string = std::is_same_v<ResultType, ColumnFixedString>;

    /// Fixed parameters precede the array. When present, the FixedString size comes first, the group size second.
    static constexpr size_t num_fixed_params = (return_fixed_string ? 1 : 0) + (PackGroups ? 1 : 0);

    static DataTypePtr getReturnType(const DataTypePtr & expression_return, const DataTypePtr & /*array_element*/, UInt64 fixed_string_size = 0)
    {
        /// The lambda result is interpreted as integer bits via IColumn::getUInt / getBool. Decimal and DateTime64
        /// columns do not implement these accessors (they would throw at execution), and floating-point results have
        /// no meaningful bit interpretation, so the result type is restricted to integers.
        WhichDataType which(expression_return);
        if (!which.isInt() && !which.isUInt())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The lambda expression for a bit-packing array function must return an integer, got {}",
                expression_return->getName());

        if constexpr (std::is_same_v<ResultType, ColumnUInt64>)
            return std::make_shared<DataTypeUInt64>();
        else if constexpr (std::is_same_v<ResultType, ColumnString>)
            return std::make_shared<DataTypeString>();
        else
            return std::make_shared<DataTypeFixedString>(fixed_string_size);
    }

    static void checkArguments(const String & name, const ColumnWithTypeAndName * fixed_arguments)
        requires(num_fixed_params != 0)
    {
        if (!fixed_arguments)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected fixed arguments for function {}", name);

        auto get_constant_integer = [&](size_t index, const char * what) -> Int64
        {
            const ColumnWithTypeAndName & argument = fixed_arguments[index];

            WhichDataType which(argument.type);
            if (!which.isInt() && !which.isUInt())
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "The {} argument of function {} must have an integer type, got {}",
                    what,
                    name,
                    argument.type->getName());

            if (!argument.column || !isColumnConst(*argument.column))
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "The {} argument of function {} must be a constant", what, name);

            return argument.column->getInt(0);
        };

        if constexpr (return_fixed_string)
        {
            const Int64 fixed_string_size = get_constant_integer(0, "FixedString size");
            if (fixed_string_size < 1)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "The FixedString size argument of function {} must be at least 1, got {}",
                    name,
                    fixed_string_size);
        }

        if constexpr (PackGroups)
        {
            const Int64 group_size = get_constant_integer(return_fixed_string ? 1 : 0, "group size");
            if (group_size < 1 || group_size > 64)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "The group size argument of function {} must be between 1 and 64, got {}",
                    name,
                    group_size);
        }
    }

    static ColumnPtr
    execute(const ColumnArray & array, ColumnPtr mapped, const ColumnWithTypeAndName * fixed_arguments [[maybe_unused]] = nullptr)
    {
        const ColumnArray::Offsets & offsets = array.getOffsets();
        const size_t num_rows = offsets.size();

        /// Number of bits taken from each lambda result. Without grouping every element contributes a single bit.
        UInt64 bits_per_element = 1;
        UInt64 group_mask = 1;
        if constexpr (PackGroups)
        {
            bits_per_element = fixed_arguments[return_fixed_string ? 1 : 0].column->getUInt(0);
            group_mask = bits_per_element >= 64 ? ~static_cast<UInt64>(0) : ((static_cast<UInt64>(1) << bits_per_element) - 1);
        }

        [[maybe_unused]] UInt64 fixed_string_size = 0;
        if constexpr (return_fixed_string)
            fixed_string_size = fixed_arguments[0].column->getUInt(0);

        auto get_value = [&](size_t index) -> UInt64
        {
            if constexpr (PackGroups)
                return mapped->getUInt(index) & group_mask;
            else
                return mapped->getBool(index) ? 1 : 0;
        };

        if constexpr (std::is_same_v<ResultType, ColumnUInt64>)
        {
            auto column = ColumnUInt64::create();
            column->reserve(num_rows);
            ColumnUInt64::Container & data = column->getData();

            /// The result holds 64 bits, so only the leading whole groups that fit are kept.
            const size_t max_groups = static_cast<size_t>(64 / bits_per_element);

            size_t prev_offset = 0;
            for (size_t row = 0; row < num_rows; ++row)
            {
                const size_t row_size = offsets[row] - prev_offset;
                const size_t num_groups = std::min(row_size, max_groups);

                UInt64 result = 0;
                for (size_t j = 0; j < num_groups; ++j)
                {
                    /// Shift only between groups; shifting an empty result by `bits_per_element == 64` would be UB.
                    if (j != 0)
                        result <<= bits_per_element;
                    result |= get_value(prev_offset + j);
                }

                data.push_back(result);
                prev_offset = offsets[row];
            }

            return column;
        }
        else /// ColumnString or ColumnFixedString
        {
            auto column = [&]
            {
                if constexpr (return_fixed_string)
                    return ColumnFixedString::create(fixed_string_size);
                else
                    return ColumnString::create();
            }();
            column->reserve(num_rows);

            std::string buffer;
            size_t prev_offset = 0;
            for (size_t row = 0; row < num_rows; ++row)
            {
                const size_t row_size = offsets[row] - prev_offset;

                /// A String packs every element; a FixedString keeps only as many leading whole groups as fit into
                /// `fixed_string_size` bytes, the remaining bytes stay zero.
                size_t num_groups = row_size;
                if constexpr (return_fixed_string)
                    num_groups = std::min<size_t>(row_size, static_cast<size_t>(fixed_string_size * 8 / bits_per_element));

                buffer.clear();
                UInt8 current_byte = 0;
                size_t filled_bits = 0;
                for (size_t j = 0; j < num_groups; ++j)
                {
                    const UInt64 value = get_value(prev_offset + j);
                    for (size_t bit = bits_per_element; bit-- > 0;)
                    {
                        current_byte = static_cast<UInt8>((current_byte << 1) | ((value >> bit) & 1));
                        if (++filled_bits == 8)
                        {
                            buffer.push_back(static_cast<char>(current_byte));
                            current_byte = 0;
                            filled_bits = 0;
                        }
                    }
                }
                /// A trailing partial byte keeps its bits in the most significant positions.
                if (filled_bits != 0)
                    buffer.push_back(static_cast<char>(current_byte << (8 - filled_bits)));

                /// For FixedString a shorter buffer is zero-padded to the declared size by insertData.
                column->insertData(buffer.data(), buffer.size());
                prev_offset = offsets[row];
            }

            return column;
        }
    }
};

struct NameArrayPackBitsToUInt64
{
    static constexpr auto name = "arrayPackBitsToUInt64";
};
using FunctionArrayPackBitsToUInt64 = FunctionArrayMapped<ArrayPackBitsImpl<ColumnUInt64>, NameArrayPackBitsToUInt64>;

struct NameArrayPackBitsToString
{
    static constexpr auto name = "arrayPackBitsToString";
};
using FunctionArrayPackBitsToString = FunctionArrayMapped<ArrayPackBitsImpl<ColumnString>, NameArrayPackBitsToString>;

struct NameArrayPackBitsToFixedString
{
    static constexpr auto name = "arrayPackBitsToFixedString";
};
using FunctionArrayPackBitsToFixedString = FunctionArrayMapped<ArrayPackBitsImpl<ColumnFixedString>, NameArrayPackBitsToFixedString>;

struct NameArrayPackBitGroupsToUInt64
{
    static constexpr auto name = "arrayPackBitGroupsToUInt64";
};
using FunctionArrayPackBitGroupsToUInt64 = FunctionArrayMapped<ArrayPackBitsImpl<ColumnUInt64, true>, NameArrayPackBitGroupsToUInt64>;

struct NameArrayPackBitGroupsToString
{
    static constexpr auto name = "arrayPackBitGroupsToString";
};
using FunctionArrayPackBitGroupsToString = FunctionArrayMapped<ArrayPackBitsImpl<ColumnString, true>, NameArrayPackBitGroupsToString>;

struct NameArrayPackBitGroupsToFixedString
{
    static constexpr auto name = "arrayPackBitGroupsToFixedString";
};
using FunctionArrayPackBitGroupsToFixedString
    = FunctionArrayMapped<ArrayPackBitsImpl<ColumnFixedString, true>, NameArrayPackBitGroupsToFixedString>;

REGISTER_FUNCTION(ArrayPackBits)
{
    FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;

    FunctionDocumentation::Description description_to_uint64 = R"(
Applies a lambda function to each element of the array and packs the resulting bits into a `UInt64`.
For each element the lambda returns an integer that is treated as a single bit (zero or non-zero).
Bits are written most-significant-bit first, so the first element occupies the most significant bit; only the first 64 elements are used.
If the array has fewer than 64 elements the result is the packed bits as a number (the unused high bits are zero).
    )";
    FunctionDocumentation::Syntax syntax_to_uint64 = "arrayPackBitsToUInt64(f, arr)";
    FunctionDocumentation::Arguments arguments_to_uint64 = {
        {"f", "Lambda function producing the bit for each element.", {"Lambda function"}},
        {"arr", "The array to pack.", {"Array(T)"}},
    };
    FunctionDocumentation::ReturnedValue returned_value_to_uint64 = {"Returns the packed bits.", {"UInt64"}};
    FunctionDocumentation::Examples examples_to_uint64 = {
        {"Example", "SELECT arrayPackBitsToUInt64(x -> x, [1, 0, 0, 0, 0, 0])", "32"},
    };
    factory.registerFunction<FunctionArrayPackBitsToUInt64>(
        {description_to_uint64, syntax_to_uint64, arguments_to_uint64, {}, returned_value_to_uint64, examples_to_uint64, introduced_in, category});

    FunctionDocumentation::Description description_to_string = R"(
Applies a lambda function to each element of the array and packs the resulting bits into a `String`.
For each element the lambda returns an integer that is treated as a single bit (zero or non-zero).
Bits are written most-significant-bit first; the length of the result is `ceil(size_of_array / 8)` bytes.
    )";
    FunctionDocumentation::Syntax syntax_to_string = "arrayPackBitsToString(f, arr)";
    FunctionDocumentation::Arguments arguments_to_string = {
        {"f", "Lambda function producing the bit for each element.", {"Lambda function"}},
        {"arr", "The array to pack.", {"Array(T)"}},
    };
    FunctionDocumentation::ReturnedValue returned_value_to_string = {"Returns the packed bits.", {"String"}};
    FunctionDocumentation::Examples examples_to_string = {
        {"Example", "SELECT arrayPackBitsToString(x -> x, [0, 0, 1, 1, 0, 0, 0, 0])", "0"},
    };
    factory.registerFunction<FunctionArrayPackBitsToString>(
        {description_to_string, syntax_to_string, arguments_to_string, {}, returned_value_to_string, examples_to_string, introduced_in, category});

    FunctionDocumentation::Description description_to_fixed_string = R"(
Applies a lambda function to each element of the array and packs the resulting bits into a `FixedString(n)`.
For each element the lambda returns an integer that is treated as a single bit (zero or non-zero).
Bits are written most-significant-bit first. If the array has more than `n * 8` elements the rest are ignored;
if it has fewer, the result is zero-padded to `n` bytes. The size `n` must be a positive constant.
    )";
    FunctionDocumentation::Syntax syntax_to_fixed_string = "arrayPackBitsToFixedString(f, n, arr)";
    FunctionDocumentation::Arguments arguments_to_fixed_string = {
        {"f", "Lambda function producing the bit for each element.", {"Lambda function"}},
        {"n", "The size of the resulting `FixedString` in bytes.", {"(U)Int*"}},
        {"arr", "The array to pack.", {"Array(T)"}},
    };
    FunctionDocumentation::ReturnedValue returned_value_to_fixed_string = {"Returns the packed bits.", {"FixedString(n)"}};
    FunctionDocumentation::Examples examples_to_fixed_string = {
        {"Example", "SELECT arrayPackBitsToFixedString(x -> x, 2, [0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1])", "01"},
    };
    factory.registerFunction<FunctionArrayPackBitsToFixedString>(
        {description_to_fixed_string, syntax_to_fixed_string, arguments_to_fixed_string, {}, returned_value_to_fixed_string, examples_to_fixed_string, introduced_in, category});

    FunctionDocumentation::Description description_groups_to_uint64 = R"(
The same as `arrayPackBitsToUInt64`, but the lambda returns an integer whose low `g` bits form a group, and the groups
are packed contiguously (most-significant-bit first). Because the result is a `UInt64`, only the leading whole groups
that fit into 64 bits are kept. The group size `g` must be a constant between 1 and 64.
    )";
    FunctionDocumentation::Syntax syntax_groups_to_uint64 = "arrayPackBitGroupsToUInt64(f, g, arr)";
    FunctionDocumentation::Arguments arguments_groups_to_uint64 = {
        {"f", "Lambda function producing the group value for each element.", {"Lambda function"}},
        {"g", "The number of bits per group.", {"(U)Int*"}},
        {"arr", "The array to pack.", {"Array(T)"}},
    };
    FunctionDocumentation::ReturnedValue returned_value_groups_to_uint64 = {"Returns the packed bits.", {"UInt64"}};
    FunctionDocumentation::Examples examples_groups_to_uint64 = {
        {"Example", "SELECT arrayPackBitGroupsToUInt64(x -> x, 4, [1, 2, 3])", "291"},
    };
    factory.registerFunction<FunctionArrayPackBitGroupsToUInt64>(
        {description_groups_to_uint64, syntax_groups_to_uint64, arguments_groups_to_uint64, {}, returned_value_groups_to_uint64, examples_groups_to_uint64, introduced_in, category});

    FunctionDocumentation::Description description_groups_to_string = R"(
The same as `arrayPackBitsToString`, but the lambda returns an integer whose low `g` bits form a group, and the groups
are packed contiguously (most-significant-bit first). The length of the result is `ceil(size_of_array * g / 8)` bytes.
The group size `g` must be a constant between 1 and 64.
    )";
    FunctionDocumentation::Syntax syntax_groups_to_string = "arrayPackBitGroupsToString(f, g, arr)";
    FunctionDocumentation::Arguments arguments_groups_to_string = {
        {"f", "Lambda function producing the group value for each element.", {"Lambda function"}},
        {"g", "The number of bits per group.", {"(U)Int*"}},
        {"arr", "The array to pack.", {"Array(T)"}},
    };
    FunctionDocumentation::ReturnedValue returned_value_groups_to_string = {"Returns the packed bits.", {"String"}};
    FunctionDocumentation::Examples examples_groups_to_string = {
        {"Example", "SELECT arrayPackBitGroupsToString(x -> x, 4, [3, 0, 3, 1])", "01"},
    };
    factory.registerFunction<FunctionArrayPackBitGroupsToString>(
        {description_groups_to_string, syntax_groups_to_string, arguments_groups_to_string, {}, returned_value_groups_to_string, examples_groups_to_string, introduced_in, category});

    FunctionDocumentation::Description description_groups_to_fixed_string = R"(
The same as `arrayPackBitsToFixedString`, but the lambda returns an integer whose low `g` bits form a group, and the groups
are packed contiguously (most-significant-bit first) into a `FixedString(n)`. Only the leading whole groups that fit into
`n` bytes are kept, and the result is zero-padded to `n` bytes. Both the size `n` and the group size `g` must be positive
constants, and `g` must not exceed 64.
    )";
    FunctionDocumentation::Syntax syntax_groups_to_fixed_string = "arrayPackBitGroupsToFixedString(f, n, g, arr)";
    FunctionDocumentation::Arguments arguments_groups_to_fixed_string = {
        {"f", "Lambda function producing the group value for each element.", {"Lambda function"}},
        {"n", "The size of the resulting `FixedString` in bytes.", {"(U)Int*"}},
        {"g", "The number of bits per group.", {"(U)Int*"}},
        {"arr", "The array to pack.", {"Array(T)"}},
    };
    FunctionDocumentation::ReturnedValue returned_value_groups_to_fixed_string = {"Returns the packed bits.", {"FixedString(n)"}};
    FunctionDocumentation::Examples examples_groups_to_fixed_string = {
        {"Example", "SELECT arrayPackBitGroupsToFixedString(x -> x, 2, 4, [3, 0, 3, 1])", "01"},
    };
    factory.registerFunction<FunctionArrayPackBitGroupsToFixedString>(
        {description_groups_to_fixed_string, syntax_groups_to_fixed_string, arguments_groups_to_fixed_string, {}, returned_value_groups_to_fixed_string, examples_groups_to_fixed_string, introduced_in, category});
}

}
