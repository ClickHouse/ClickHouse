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
    static constexpr auto num_fixed_params = pack_groups ? 1 : 0;

    static DataTypePtr getReturnType(const DataTypePtr & expression_return, const DataTypePtr & /*array_element*/)
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
            auto string_size = 20; // should from args
            return std::make_shared<DataTypeFixedString>(string_size);
        }
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ArrayPackBits is not implemented for {} as result type", result_type);
    }

    static void checkArguments(const String & name, const ColumnWithTypeAndName * fixed_arguments)
            requires(pack_groups)
    {
        if (!fixed_arguments)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected fixed arguments to get the limit for partial array sort");

        WhichDataType which(fixed_arguments[0].type.get());
        if (!which.isUInt() && !which.isInt())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of limit argument of function {} (must be UInt or Int)",
                fixed_arguments[0].type->getName(),
                name);
    }

    static ColumnPtr
    execute(const ColumnArray & array, ColumnPtr mapped, const ColumnWithTypeAndName * fixed_arguments [[maybe_unused]] = nullptr)
    {
        const auto & offsets = array.getOffsets();
        auto out_column = ColumnUInt64::create(offsets.size());
        ColumnUInt64::Container & out_counts = out_column->getData();

        size_t pos = 0;
        auto * log = &Poco::Logger::get("ArrayPackBitsToUint64Impl");
        LOG_TRACE(log, "offset.size = {}", offsets.size());
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            Int64 bit = 0;
            LOG_TRACE(log, "pos.size = {}", offsets[i]);
            for (; pos < std::min(offsets[i], 64ULL); ++pos)
            {
                bit |= static_cast<UInt64>(mapped->getBool(pos)) << pos;
                LOG_TRACE(log, "pos = {}, bit = {}", pos, bit);
            }
            LOG_TRACE(log, "bit = {}", bit);
            out_counts[i] = static_cast<UInt64>(bit);
        }
        return out_column;
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
