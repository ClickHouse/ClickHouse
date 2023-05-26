#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/array/FunctionArrayMapped.h>
#include <Common/logger_useful.h>


namespace DB
{
enum class ArrayPackBitsResultType
{
    uin64,
    string,
    fixed_string,
};

template <ArrayPackBitsResultType result_type, bool pack_groups = false>
struct ArrayPackBitsImpl
{
    static bool needBoolean() { return false; }
    static bool needExpression() { return true; }
    static bool needOneArray() { return true; }
    static constexpr bool return_fixed_string = result_type == ArrayPackBitsResultType::fixed_string;
    static constexpr int num_fixed_params = pack_groups ? 1 : 0 + return_fixed_string;

    static DataTypePtr
    getReturnType(const DataTypePtr & expression_return, const DataTypePtr & /*array_element*/, const UInt64 fixed_string_size = 0)
    {
        auto call = [&](const auto & types)
        {
            using Types = std::decay_t<decltype(types)>;
            using DataType = typename Types::LeftType;
            if constexpr (IsDataTypeDecimalOrNumber<DataType>)
            {
                return true;
            }
            return false;
        };
        if (!callOnIndexAndDataType<void>(expression_return->getTypeId(), call))
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "array pack bits function cannot be performed on type {}",
                expression_return->getName());
        }
        if constexpr (result_type == ArrayPackBitsResultType::uin64)
        {
            return std::make_shared<DataTypeUInt64>();
        }
        else if constexpr (result_type == ArrayPackBitsResultType::string)
        {
            return std::make_shared<DataTypeString>();
        }
        else if constexpr (result_type == ArrayPackBitsResultType::fixed_string)
        {
            return std::make_shared<DataTypeFixedString>(fixed_string_size);
        }
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ArrayPackBits is not implemented for {} as result type", result_type);
    }

    static void checkArguments(const String & name, const ColumnWithTypeAndName * fixed_arguments)
        requires(num_fixed_params != 0)
    {
        if (!fixed_arguments)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected fixed arguments to get the fixed size for fixed string result");

        WhichDataType which(fixed_arguments[0].type.get());
        if (!which.isUInt() && !which.isInt())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of limit argument of function {} (must be UInt or Int)",
                fixed_arguments[0].type->getName(),
                name);
    }

    static ColumnPtr
    execute(const ColumnArray & array, ColumnPtr mapped, const ColumnWithTypeAndName * arguments [[maybe_unused]] = nullptr)
    {
        const auto & offsets = array.getOffsets();
        auto * log = &Poco::Logger::get("ArrayPackBitsImpl");
        if constexpr (result_type == ArrayPackBitsResultType::uin64)
        {
            auto out_column = ColumnUInt64::create();
            out_column->reserve(offsets.size());
            size_t pos = 0;
            for (uint64_t offset : offsets)
            {
                auto max_offset = offset;
                auto pack_size = 8ULL;
                if constexpr (pack_groups)
                {
                    pack_size = std::min(pack_size, typeid_cast<const ColumnConst *>(arguments[1].column.get())->getValue<UInt64>());
                    max_offset = std::min(max_offset, pack_size * 8);
                }
                uint64_t bit64 = 0;
                uint8_t bit = 0;
                for (; pos < max_offset; ++pos)
                {
                    uint8_t one_bit = static_cast<uint8_t>(mapped->getBool(pos));
                    bit = bit << 1 | one_bit;
                    if ((pos + 1) % pack_size == 0 || pos == max_offset - 1)
                    {
                        bit64 = bit64 << 8 | bit;
                        bit = 0;
                    }
                }
                out_column->insert(bit64);
            }
            return out_column;
        }
        else if constexpr (result_type == ArrayPackBitsResultType::string)
        {
            auto out_column = ColumnString::create();
            out_column->reserve(offsets.size());
            size_t pos = 0;
            for (uint64_t offset : offsets)
            {
                auto max_offset = offset;
                auto pack_size = 8ULL;
                if constexpr (pack_groups)
                {
                    pack_size = std::min(pack_size, typeid_cast<const ColumnConst *>(arguments[1].column.get())->getValue<UInt64>());
                    max_offset = std::min(max_offset, pack_size * 8);
                }
                std::string bit_string;
                uint8_t bit = 0;
                for (; pos < max_offset; ++pos)
                {
                    uint8_t one_bit = static_cast<uint8_t>(mapped->getBool(pos));
                    bit = bit << 1 | one_bit;
                    if ((pos + 1) % pack_size == 0 || pos == max_offset - 1)
                    {
                        bit_string += bit;
                        bit = 0;
                    }
                }
                out_column->insert(bit_string);
            }
            return out_column;
        }
        else if constexpr (result_type == ArrayPackBitsResultType::fixed_string)
        {
            const auto fixed_string_size = typeid_cast<const ColumnConst *>(arguments[1].column.get())->getValue<UInt64>();
            auto out_column = ColumnFixedString::create(fixed_string_size);
            out_column->reserve(offsets.size());
            size_t pos = 0;
            for (uint64_t offset : offsets)
            {
                auto max_offset = std::min(offset, fixed_string_size * 8);
                auto pack_size = 8ULL;
                if constexpr (pack_groups)
                {
                    pack_size = std::min(pack_size, typeid_cast<const ColumnConst *>(arguments[1].column.get())->getValue<UInt64>());
                    max_offset = std::min(max_offset, pack_size * 8);
                }
                std::string bit_string;
                uint8_t bit = 0;
                for (; pos < max_offset; ++pos)
                {
                    uint8_t one_bit = static_cast<uint8_t>(mapped->getBool(pos));
                    bit = bit << 1 | one_bit;
                    if ((pos + 1) % pack_size == 0 || pos == max_offset - 1)
                    {
                        bit_string += bit;
                        bit = 0;
                    }
                }
                out_column->insert(bit_string);
            }
            return out_column;
        }
    }
};

struct NameArrayPackBitsToUInt64
{
    static constexpr auto name = "arrayPackBitsToUInt64";
};

using FunctionArrayPackBitsToUInt64 = FunctionArrayMapped<ArrayPackBitsImpl<ArrayPackBitsResultType::uin64>, NameArrayPackBitsToUInt64>;

struct NameArrayPackBitsToFixedString
{
    static constexpr auto name = "arrayPackBitsToFixedString";
};
using FunctionArrayPackBitsToFixedString
    = FunctionArrayMapped<ArrayPackBitsImpl<ArrayPackBitsResultType::fixed_string>, NameArrayPackBitsToFixedString>;

struct NameArrayPackBitsToString
{
    static constexpr auto name = "arrayPackBitsToString";
};
using FunctionArrayPackBitsToString = FunctionArrayMapped<ArrayPackBitsImpl<ArrayPackBitsResultType::string>, NameArrayPackBitsToString>;

constexpr bool group_pack = true;
struct NameArrayPackBitGroupsToUInt64
{
    static constexpr auto name = "arrayPackBitGroupsToUInt64";
};
using FunctionArrayPackBitGroupsToUInt64
    = FunctionArrayMapped<ArrayPackBitsImpl<ArrayPackBitsResultType::uin64, group_pack>, NameArrayPackBitGroupsToUInt64>;

struct NameArrayPackBitGroupsToFixedString
{
    static constexpr auto name = "arrayPackBitGroupsToFixedString";
};
using FunctionArrayPackBitGroupsToFixedString
    = FunctionArrayMapped<ArrayPackBitsImpl<ArrayPackBitsResultType::fixed_string, group_pack>, NameArrayPackBitGroupsToFixedString>;

struct NameArrayPackBitGroupsToString
{
    static constexpr auto name = "arrayPackBitGroupsToString";
};
using FunctionArrayPackBitGroupsToString
    = FunctionArrayMapped<ArrayPackBitsImpl<ArrayPackBitsResultType::string, group_pack>, NameArrayPackBitGroupsToString>;

REGISTER_FUNCTION(ArrayPackBits)
{
    factory.registerFunction<FunctionArrayPackBitsToUInt64>();
    factory.registerFunction<FunctionArrayPackBitsToFixedString>();
    factory.registerFunction<FunctionArrayPackBitsToString>();
    factory.registerFunction<FunctionArrayPackBitGroupsToUInt64>();
    factory.registerFunction<FunctionArrayPackBitGroupsToFixedString>();
    factory.registerFunction<FunctionArrayPackBitGroupsToString>();
}
}
