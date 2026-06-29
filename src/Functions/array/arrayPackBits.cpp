#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/callOnTypeIndex.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/array/FunctionArrayMapped.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
}

/** Higher-order functions that pack the bits produced by a lambda expression into a compact representation.
  *
  *  arrayPackBitsToUInt64(f, arr)          - pack one bit per element into a UInt64 (only the first 64 bits are used).
  *  arrayPackBitsToString(f, arr)          - pack one bit per element into a String of ceil(size / 8) bytes.
  *  arrayPackBitsToFixedString(f, n, arr)  - pack one bit per element into a FixedString of `n` bytes.
  *
  *  arrayPackBitGroupsToUInt64(f, g, arr)         - the same, but each group of `g` consecutive bits is written to a
  *  arrayPackBitGroupsToString(f, g, arr)           separate byte of the result.
  *  arrayPackBitGroupsToFixedString(f, n, g, arr)
  *
  * The bit order inside a group is most-significant-bit first; the byte order of the result follows the order of
  * the array elements. A group occupies a full byte, so for `arrayPackBit*ToUInt64` at most 8 groups are packed.
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
        auto is_number = [&](const auto & types) -> bool
        {
            using Types = std::decay_t<decltype(types)>;
            using DataType = typename Types::LeftType;
            return IsDataTypeDecimalOrNumber<DataType>;
        };

        if (!callOnIndexAndDataType<void>(expression_return->getTypeId(), is_number))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The lambda expression for a bit-packing array function must return a number, got {}",
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

        for (size_t i = 0; i < num_fixed_params; ++i)
        {
            const ColumnWithTypeAndName & argument = fixed_arguments[i];

            WhichDataType which(argument.type);
            if (!which.isInt() && !which.isUInt())
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Argument #{} of function {} must have an integer type, got {}",
                    i + 2,
                    name,
                    argument.type->getName());

            if (!argument.column || !isColumnConst(*argument.column))
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Argument #{} of function {} must be a constant", i + 2, name);

            if (argument.column->getUInt(0) == 0)
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN, "Argument #{} of function {} must be greater than zero", i + 2, name);
        }
    }

    static ColumnPtr
    execute(const ColumnArray & array, ColumnPtr mapped, const ColumnWithTypeAndName * fixed_arguments [[maybe_unused]] = nullptr)
    {
        const ColumnArray::Offsets & offsets = array.getOffsets();
        const size_t num_rows = offsets.size();

        /// Number of consecutive bits written into one byte of the result.
        /// Without `PackGroups` every bit is written individually, so a byte holds 8 bits.
        UInt64 group_size = 8;
        if constexpr (PackGroups)
            group_size = fixed_arguments[return_fixed_string ? 1 : 0].column->getUInt(0);

        [[maybe_unused]] UInt64 fixed_string_size = 0;
        if constexpr (return_fixed_string)
            fixed_string_size = fixed_arguments[0].column->getUInt(0);

        if constexpr (std::is_same_v<ResultType, ColumnUInt64>)
        {
            auto column = ColumnUInt64::create();
            column->reserve(num_rows);
            ColumnUInt64::Container & data = column->getData();

            size_t prev_offset = 0;
            for (size_t row = 0; row < num_rows; ++row)
            {
                const size_t row_size = offsets[row] - prev_offset;
                /// A group occupies a byte, and at most 8 bytes fit into a UInt64.
                const size_t num_groups = std::min<size_t>((row_size + group_size - 1) / group_size, 8);
                const size_t process_size = std::min(num_groups * group_size, row_size);

                UInt64 result = 0;
                UInt64 bit_group = 0;
                for (size_t j = 0; j < process_size; ++j)
                {
                    bit_group = (bit_group << 1) | static_cast<UInt64>(mapped->getBool(prev_offset + j));

                    const size_t bits_in_group = (j + 1) % group_size;
                    if (bits_in_group == 0 || j + 1 == process_size)
                    {
                        const size_t shift = 8 - bits_in_group; /// `bits_in_group == 0` means a full group, shift by 8.
                        result = (result << shift) | bit_group;
                        bit_group = 0;
                    }
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

                size_t num_bytes;
                if constexpr (return_fixed_string)
                    num_bytes = fixed_string_size;
                else
                    num_bytes = (row_size + group_size - 1) / group_size; /// ceil(row_size / group_size)
                const size_t process_size = std::min(num_bytes * group_size, row_size);

                buffer.clear();
                UInt64 bit_group = 0;
                for (size_t j = 0; j < process_size; ++j)
                {
                    bit_group = (bit_group << 1) | static_cast<UInt64>(mapped->getBool(prev_offset + j));

                    const size_t bits_in_group = (j + 1) % group_size;
                    if (bits_in_group == 0 || j + 1 == process_size)
                    {
                        const size_t shift = 8 - bits_in_group;
                        const UInt8 byte = bits_in_group == 0 ? static_cast<UInt8>(bit_group) : static_cast<UInt8>(bit_group << shift);
                        buffer.push_back(static_cast<char>(byte));
                        bit_group = 0;
                    }
                }

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
For each element the lambda returns a value that is treated as a single bit (zero or non-zero).
Bits are written most-significant-bit first; a byte holds 8 bits, so only the first 64 array elements are used.
If the array has fewer than 64 elements the remaining bits are zero.
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
For each element the lambda returns a value that is treated as a single bit (zero or non-zero).
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
For each element the lambda returns a value that is treated as a single bit (zero or non-zero).
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
The same as `arrayPackBitsToUInt64`, but every group of `g` consecutive bits is written into a separate byte of the result.
A group occupies a full byte, so at most 8 groups (8 bytes) are packed. The group size `g` must be a positive constant.
    )";
    FunctionDocumentation::Syntax syntax_groups_to_uint64 = "arrayPackBitGroupsToUInt64(f, g, arr)";
    FunctionDocumentation::Arguments arguments_groups_to_uint64 = {
        {"f", "Lambda function producing the bit for each element.", {"Lambda function"}},
        {"g", "The number of bits per group.", {"(U)Int*"}},
        {"arr", "The array to pack.", {"Array(T)"}},
    };
    FunctionDocumentation::ReturnedValue returned_value_groups_to_uint64 = {"Returns the packed bits.", {"UInt64"}};
    FunctionDocumentation::Examples examples_groups_to_uint64 = {
        {"Example", "SELECT arrayPackBitGroupsToUInt64(x -> x, 1, [1, 1])", "257"},
    };
    factory.registerFunction<FunctionArrayPackBitGroupsToUInt64>(
        {description_groups_to_uint64, syntax_groups_to_uint64, arguments_groups_to_uint64, {}, returned_value_groups_to_uint64, examples_groups_to_uint64, introduced_in, category});

    FunctionDocumentation::Description description_groups_to_string = R"(
The same as `arrayPackBitsToString`, but every group of `g` consecutive bits is written into a separate byte of the result.
The length of the result is `ceil(size_of_array / g)` bytes. The group size `g` must be a positive constant.
    )";
    FunctionDocumentation::Syntax syntax_groups_to_string = "arrayPackBitGroupsToString(f, g, arr)";
    FunctionDocumentation::Arguments arguments_groups_to_string = {
        {"f", "Lambda function producing the bit for each element.", {"Lambda function"}},
        {"g", "The number of bits per group.", {"(U)Int*"}},
        {"arr", "The array to pack.", {"Array(T)"}},
    };
    FunctionDocumentation::ReturnedValue returned_value_groups_to_string = {"Returns the packed bits.", {"String"}};
    FunctionDocumentation::Examples examples_groups_to_string = {
        {"Example", "SELECT arrayPackBitGroupsToString(x -> x, 6, [1, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0])", "10"},
    };
    factory.registerFunction<FunctionArrayPackBitGroupsToString>(
        {description_groups_to_string, syntax_groups_to_string, arguments_groups_to_string, {}, returned_value_groups_to_string, examples_groups_to_string, introduced_in, category});

    FunctionDocumentation::Description description_groups_to_fixed_string = R"(
The same as `arrayPackBitsToFixedString`, but every group of `g` consecutive bits is written into a separate byte of the result.
The result has `n` bytes. Both the size `n` and the group size `g` must be positive constants.
    )";
    FunctionDocumentation::Syntax syntax_groups_to_fixed_string = "arrayPackBitGroupsToFixedString(f, n, g, arr)";
    FunctionDocumentation::Arguments arguments_groups_to_fixed_string = {
        {"f", "Lambda function producing the bit for each element.", {"Lambda function"}},
        {"n", "The size of the resulting `FixedString` in bytes.", {"(U)Int*"}},
        {"g", "The number of bits per group.", {"(U)Int*"}},
        {"arr", "The array to pack.", {"Array(T)"}},
    };
    FunctionDocumentation::ReturnedValue returned_value_groups_to_fixed_string = {"Returns the packed bits.", {"FixedString(n)"}};
    FunctionDocumentation::Examples examples_groups_to_fixed_string = {
        {"Example", "SELECT arrayPackBitGroupsToFixedString(x -> x, 2, 6, [1, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0])", "10"},
    };
    factory.registerFunction<FunctionArrayPackBitGroupsToFixedString>(
        {description_groups_to_fixed_string, syntax_groups_to_fixed_string, arguments_groups_to_fixed_string, {}, returned_value_groups_to_fixed_string, examples_groups_to_fixed_string, introduced_in, category});
}

}
