#include <Columns/ColumnsDateTime.h>
#include <Common/ColumnsHashing.h>
#include <Common/Exception.h>
#include <Common/HashTable/Hash.h>
#include <Common/HashValueIdGenerator.h>
#include <Common/typeid_cast.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

#define APPLY_FOR_NUMBER_COLUMN(Type, col, null_map) \
    else if (const auto * col##Type = typeid_cast<const Column##Type *>(nested_col)) \
    { \
        if (null_map) \
            computeValueIdForNumber<true, Type, Column##Type>(null_map, col##Type, value_ids); \
        else \
            computeValueIdForNumber<false, Type, Column##Type>(null_map, col##Type, value_ids); \
    }

void HashValueIdGenerator::computeValueId(const IColumn * col, std::vector<UInt64> & value_ids)
{
    using Date = UInt16;
    using Date32 = UInt32;
    using DateTime = UInt32;
    const ColumnUInt8 * null_map = nullptr;
    const IColumn * nested_col = col;
    if (col->isNullable())
    {
        const auto * nullable_col = typeid_cast<const ColumnNullable *>(col);
        null_map = &nullable_col->getNullMapColumn();
        nested_col = nullable_col->getNestedColumnPtr().get();
    }
    if (const auto * str_col = typeid_cast<const ColumnString *>(nested_col))
    {
#if defined(__AVX512F__) && defined(__AVX512BW__)
        const auto & offsets = str_col->getOffsets();
        if (!str_col->empty() && offsets.back() / str_col->size() <= 9)
        {
            if (null_map)
                computeValueIdForShortString<true>(null_map, str_col, value_ids);
            else
                computeValueIdForShortString<false>(null_map, str_col, value_ids);

        }
        else
#endif
        {
            if (null_map)
                computeValueIdForString<true>(null_map, str_col, value_ids);
            else
                computeValueIdForString<false>(null_map, str_col, value_ids);
        }
    }
    else if (const auto * fixed_str_col = typeid_cast<const ColumnFixedString *>(nested_col))
    {
        if (null_map)
            computeValueIdForFixedString<true>(null_map, fixed_str_col, value_ids);
        else
            computeValueIdForFixedString<false>(null_map, fixed_str_col, value_ids);
    }
    else if (const auto * low_card_col = typeid_cast<const ColumnLowCardinality *>(col))
    {
        computeValueIdForLowCardinality(low_card_col, value_ids);
    }
    APPLY_FOR_NUMBER_COLUMN(UInt8, nested_col, null_map)
    APPLY_FOR_NUMBER_COLUMN(UInt16, nested_col, null_map)
    APPLY_FOR_NUMBER_COLUMN(UInt32, nested_col, null_map)
    APPLY_FOR_NUMBER_COLUMN(UInt64, nested_col, null_map)
    APPLY_FOR_NUMBER_COLUMN(UInt128, nested_col, null_map)
    APPLY_FOR_NUMBER_COLUMN(UInt256, nested_col, null_map)
    APPLY_FOR_NUMBER_COLUMN(Int8, nested_col, null_map)
    APPLY_FOR_NUMBER_COLUMN(Int16, nested_col, null_map)
    APPLY_FOR_NUMBER_COLUMN(Int32, nested_col, null_map)
    APPLY_FOR_NUMBER_COLUMN(Int64, nested_col, null_map)
    APPLY_FOR_NUMBER_COLUMN(Int128, nested_col, null_map)
    APPLY_FOR_NUMBER_COLUMN(Int256, nested_col, null_map)
    APPLY_FOR_NUMBER_COLUMN(Float32, nested_col, null_map)
    APPLY_FOR_NUMBER_COLUMN(Float64, nested_col, null_map)
    APPLY_FOR_NUMBER_COLUMN(UUID, nested_col, null_map)
    APPLY_FOR_NUMBER_COLUMN(IPv4, nested_col, null_map)
    APPLY_FOR_NUMBER_COLUMN(IPv6, nested_col, null_map)
    APPLY_FOR_NUMBER_COLUMN(Date, nested_col, null_map)
    APPLY_FOR_NUMBER_COLUMN(Date32, nested_col, null_map)
    APPLY_FOR_NUMBER_COLUMN(DateTime, nested_col, null_map)
    APPLY_FOR_NUMBER_COLUMN(DateTime64, nested_col, null_map)
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported column type: {}", col->getName());
    }
}

void HashValueIdGenerator::computeValueIdForLowCardinality(const ColumnLowCardinality * col, std::vector<UInt64> & value_ids)
{
    const auto & indexes_col = col->getIndexes();
    size_t str_len = col->getSizeOfIndexType();
    const UInt8 * pos = nullptr;
    UInt64 * value_id = value_ids.data();
    switch (str_len)
    {
        case sizeof(UInt8):
            pos = reinterpret_cast<const UInt8 *>(assert_cast<const ColumnUInt8 *>(&indexes_col)->getData().data());
            break;
        case sizeof(UInt16):
            pos = reinterpret_cast<const UInt8 *>(assert_cast<const ColumnUInt16 *>(&indexes_col)->getData().data());
            break;
        case sizeof(UInt32):
            pos = reinterpret_cast<const UInt8 *>(assert_cast<const ColumnUInt32 *>(&indexes_col)->getData().data());
            break;
        case sizeof(UInt64):
            pos = reinterpret_cast<const UInt8 *>(assert_cast<const ColumnUInt64 *>(&indexes_col)->getData().data());
            break;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected size of index type for low cardinality column.");
    }
    if (state->hash_mode == AdaptiveKeysHolder::State::VALUE_ID)
    {
        for (size_t i = 0, n = col->size(); i < n; ++i)
        {
            UInt64 current_col_value_id = 0;
            assignValueIdDynamically(pos, str_len, current_col_value_id);
            *value_id = *value_id * max_distinct_values + current_col_value_id;
            value_id++;
            pos += str_len;
        }
    }
    if (current_assigned_value_id > max_distinct_values)
    {
        state->hash_mode = AdaptiveKeysHolder::State::HASH;
    }
}

}
