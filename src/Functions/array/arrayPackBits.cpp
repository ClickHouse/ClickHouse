#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/array/FunctionArrayMapped.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT ;
}

template <typename ResultType>
struct ArrayAggregatePackImpl;

template <>
struct ArrayAggregatePackImpl<ColumnUInt64>
{
    using PackType = uint64_t;
};

template <>
struct ArrayAggregatePackImpl<ColumnString>
{
    using PackType = std::string;
};

template <>
struct ArrayAggregatePackImpl<ColumnFixedString>
{
    using PackType = std::string;
};

template <typename ResultType, bool PackGroups = false>
struct ArrayPackBitsImpl
{
    static bool needBoolean() { return false; }
    static bool needExpression() { return true; }
    static bool needOneArray() { return true; }
    static constexpr bool return_fixed_string = std::is_same_v<ResultType, ColumnFixedString>;
    static constexpr int num_fixed_params = (PackGroups ? 1 : 0) + return_fixed_string;

    static DataTypePtr getReturnType(const DataTypePtr & expression_return, const DataTypePtr &, const UInt64 fixed_string_size = 0)
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
        if constexpr (std::is_same_v<ResultType, ColumnUInt64>)
        {
            return std::make_shared<DataTypeUInt64>();
        }
        else if constexpr (std::is_same_v<ResultType, ColumnString>)
        {
            return std::make_shared<DataTypeString>();
        }
        else if constexpr (std::is_same_v<ResultType, ColumnFixedString>)
        {
            return std::make_shared<DataTypeFixedString>(fixed_string_size);
        }
    }

    static void checkArguments(const String & name, const ColumnWithTypeAndName * fixed_arguments)
        requires(num_fixed_params != 0)
    {
        if (!fixed_arguments)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected fixed arguments for function {}", name);
        {
            WhichDataType which(fixed_arguments[0].type.get());
            if (!which.isUInt() && !which.isInt())
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of limit argument of function {} (must be UInt or Int)",
                    fixed_arguments[0].type->getName(),
                    name);
        }

        if (num_fixed_params > 1)
        {
            WhichDataType which(fixed_arguments[1].type.get());
            if (!which.isUInt() && !which.isInt())
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of limit argument of function {} (must be UInt or Int)",
                    fixed_arguments[1].type->getName(),
                    name);
        }
    }

    static ColumnPtr
    execute(const ColumnArray & array, ColumnPtr mapped, const ColumnWithTypeAndName * arguments [[maybe_unused]] = nullptr)
    {
        const auto & offsets = array.getOffsets();
        auto process_offsets = [&](auto & out_column, uint64_t max_result_size = std::numeric_limits<uint64_t>::max() / 8)
        {
            uint64_t pack_group_size = 8;
            if constexpr (PackGroups)
            {
                if constexpr (std::is_same_v<ResultType, ColumnFixedString>)
                {
                    pack_group_size = typeid_cast<const ColumnConst *>(arguments[2].column.get())->getValue<UInt64>();
                }
                else
                {
                    pack_group_size = typeid_cast<const ColumnConst *>(arguments[1].column.get())->getValue<UInt64>();
                }
            }

            size_t pos = 0;
            for (uint64_t offset : offsets)
            {
                if constexpr (std::is_same_v<ResultType, ColumnString>)
                {
                    max_result_size = static_cast<uint64_t>(std::ceil(static_cast<double>(offset) / pack_group_size));
                }
                else if constexpr (std::is_same_v<ResultType, ColumnUInt64>)
                {
                    max_result_size = std::min(static_cast<uint64_t>(std::ceil(static_cast<double>(offset) / pack_group_size)), 8UL);
                }

                uint64_t max_offset = std::min(max_result_size * pack_group_size, offset);
                typename ArrayAggregatePackImpl<ResultType>::PackType result = {};
                uint8_t bit_group = 0;

                for (; pos < max_offset; ++pos)
                {
                    uint64_t one_bit = static_cast<uint64_t>(mapped->getBool(pos));
                    bit_group = bit_group << 1 | one_bit;

                    if (((pos + 1) % pack_group_size == 0 || pos + 1 == max_offset))
                    {
                        uint8_t bit_shift = 8 - ((pos + 1) % pack_group_size);
                        if constexpr (std::is_same_v<ResultType, ColumnUInt64>)
                        {
                            result = result << bit_shift | bit_group;
                        }
                        else if constexpr (std::is_same_v<ResultType, ColumnString> || std::is_same_v<ResultType, ColumnFixedString>)
                        {
                            if (bit_shift == 8)
                                result += bit_group;
                            else
                                result += bit_group << bit_shift;
                        }
                        bit_group = 0;
                    }
                }
                out_column->insert(result);
            }
        };


        if constexpr (std::is_same_v<ResultType, ColumnUInt64> || std::is_same_v<ResultType, ColumnString>)
        {
            auto out_column = ResultType::create();
            out_column->reserve(offsets.size());
            process_offsets(out_column);
            return out_column;
        }
        else if constexpr (std::is_same_v<ResultType, ColumnFixedString>)
        {
            const auto fixed_string_size = typeid_cast<const ColumnConst *>(arguments[1].column.get())->getValue<UInt64>();
            auto out_column = ResultType::create(fixed_string_size);
            out_column->reserve(offsets.size());
            process_offsets(out_column, fixed_string_size);
            return out_column;
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


constexpr bool group_pack = true;
struct NameArrayPackBitGroupsToUInt64
{
    static constexpr auto name = "arrayPackBitGroupsToUInt64";
};
using FunctionArrayPackBitGroupsToUInt64 = FunctionArrayMapped<ArrayPackBitsImpl<ColumnUInt64, group_pack>, NameArrayPackBitGroupsToUInt64>;

struct NameArrayPackBitGroupsToString
{
    static constexpr auto name = "arrayPackBitGroupsToString";
};
using FunctionArrayPackBitGroupsToString = FunctionArrayMapped<ArrayPackBitsImpl<ColumnString, group_pack>, NameArrayPackBitGroupsToString>;

struct NameArrayPackBitGroupsToFixedString
{
    static constexpr auto name = "arrayPackBitGroupsToFixedString";
};
using FunctionArrayPackBitGroupsToFixedString
    = FunctionArrayMapped<ArrayPackBitsImpl<ColumnFixedString, group_pack>, NameArrayPackBitGroupsToFixedString>;

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
