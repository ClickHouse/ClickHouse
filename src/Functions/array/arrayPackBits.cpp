#include <Columns/ColumnDecimal.h>
#include <Functions/FunctionFactory.h>
#include <Functions/array/FunctionArrayMapped.h>
#include <Common/logger_useful.h>

namespace DB
{
// articat for arrayPackBitsToUInt64
struct ArrayPackBitsToUint64Impl
{
    static bool needBoolean() { return false; }
    static bool needExpression() { return true; }
    static bool needOneArray() { return true; }


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
        return std::make_shared<DataTypeUInt64>();
    }

    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
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

using FunctionArrayPackBitsToUInt64 = FunctionArrayMapped<ArrayPackBitsToUint64Impl, NameArrayPackBitsToUInt64>;

REGISTER_FUNCTION(ArrayPackBitsToUInt64)
{
    factory.registerFunction<FunctionArrayPackBitsToUInt64>();
}
}
