#include <memory>
#include <Columns/ColumnsDateTime.h>
#include <Common/ColumnsHashing.h>
#include <Common/Exception.h>
#include <Common/HashTable/Hash.h>
#include <Common/HashValueIdGenerator.h>
#include <Common/typeid_cast.h>
#include "typeid_cast.h"
#include <DataTypes/IDataType.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Check whether we could use range mode to generate value id by sampling.
void StringHashValueIdGenerator::tryInitialize(const IColumn *col)
{
    is_nullable = col->isNullable();
    const auto * nested_col = col;
    if (is_nullable)
    {
        const auto * null_col = typeid_cast<const ColumnNullable *>(col);
        if (!null_col)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} is nullable but not ColumnNullable", col->getName());
        nested_col = &(null_col->getNestedColumn());
    }
    const auto * str_col = typeid_cast<const ColumnString *>(nested_col);
    const auto & offsets = str_col->getOffsets();
    IColumn::Offset prev_offset = 0;
    const auto & chars = str_col->getChars();
    const UInt8 * char_pos = chars.data();

    enable_range_mode = true;
    size_t i = 0;
    size_t n = std::min(max_sample_rows, str_col->size());
    alignas(64) UInt64 str_lens[8] = {0};
    for (i = 1; i + 8 < n; i += 8)
    {
        bool is_all_short_string = false;
        computeOneBatchStringLength(i, offsets, str_lens, is_all_short_string);
        for (UInt64 str_len : str_lens)
        {
            if (!str_len)
            {
                continue;
            }
            if (str_len > 9)
            {
                enable_range_mode = false;
                break;
            }
            auto val = str2Int64(char_pos, str_len - 1);
            if (val > range_max)
                range_max = val;
            if (val < range_min)
                range_min = val;
            if (range_max - range_min + 1 + is_nullable > max_distinct_values)
            {
                enable_range_mode = false;
                break;
            }
            char_pos += str_len;
        }
        if (!enable_range_mode)
            break;
        prev_offset = offsets[i + 7];
    }
    if (enable_range_mode)
    {
        for (; i < n; i++)
        {
            auto str_len = offsets[i] - prev_offset;
            if (!str_len)
                continue;
            if (str_len > 9)
            {
                enable_range_mode = false;
                break;
            }
            auto val = str2Int64(char_pos, str_len - 1);
            if (val > range_max)
                range_max = val;
            if (val < range_min)
                range_min = val;
            if (range_max - range_min + 1 + is_nullable > max_distinct_values)
            {
                enable_range_mode = false;
                break;
            }
            prev_offset = offsets[i];
            char_pos += str_len;
        }
    }
    if (enable_range_mode)
    {
        allocated_value_id = range_max - range_min + 1 + is_nullable;
    }
    else
    {
        allocated_value_id = is_nullable;
    }
}

void FixedStringHashValueIdGenerator::tryInitialize(const IColumn *col)
{
    is_nullable = col->isNullable();
    const auto * nested_col = col;
    if (is_nullable)
    {
        const auto * null_col = typeid_cast<const ColumnNullable *>(col);
        if (!null_col)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} is nullable but not ColumnNullable", col->getName());
        nested_col = &(null_col->getNestedColumn());
    }
    const auto * str_col = typeid_cast<const ColumnFixedString *>(nested_col);
    const auto & chars = str_col->getChars();
    const UInt8 * char_pos = chars.data();

    enable_range_mode = true;
    auto str_len = str_col->getN();
    if (str_len > 8)
    {
        enable_range_mode = false;
    }
    else
    {
        size_t i = 0;
        size_t n = std::min(max_sample_rows, str_col->size());
        for (i = 0; i < n ; i++)
        {
            auto val = str2Int64(char_pos, str_len);
            if (val > range_max)
                range_max = val;
            if (val < range_min)
                range_min = val;
            if (range_max - range_min + 1 + is_nullable > max_distinct_values)
            {
                enable_range_mode = false;
                break;
            }
            char_pos += str_len;
        }
    }

    if (enable_range_mode)
    {
        allocated_value_id = range_max - range_min + 1 + is_nullable;
    }
    else
    {
        allocated_value_id = is_nullable;
    }

}

HashValueIdGeneratorFactory & HashValueIdGeneratorFactory::instance()
{
    static HashValueIdGeneratorFactory instance;
    return instance;
}

std::unique_ptr<IHashValueIdGenerator> HashValueIdGeneratorFactory::getGenerator(AdaptiveKeysHolder::State * state_, size_t max_distinct_values_, const IColumn * col)
{
    const auto * nested_col = col;
    if (col->isNullable())
    {
        const auto * null_col = typeid_cast<const ColumnNullable *>(col);
        if (!null_col)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} is nullable but not ColumnNullable", col->getName());
        nested_col = &(null_col->getNestedColumn());
    }

    using Date = UInt16;
    using Date32 = UInt32;
    using DateTime = UInt32;
#define APPLY_ON_NUMBER_COLUMN(type, nested_col, col) \
    else if (const auto * col##type = typeid_cast<const Column##type *>(nested_col)) \
    { \
        return std::make_unique<NumericHashValueIdGenerator<type, Column##type>>(col, state_, max_distinct_values_); \
    }

    WhichDataType which_type(nested_col->getDataType());
    if (which_type.isString())
    {
        return std::make_unique<StringHashValueIdGenerator>(col, state_, max_distinct_values_);
    }
    else if (which_type.isFixedString())
    {
        return std::make_unique<FixedStringHashValueIdGenerator>(col, state_, max_distinct_values_);
    }
    else if (const auto * low_card_col = typeid_cast<const ColumnLowCardinality *>(col))
    {
        return std::make_unique<LowCardinalityHashValueIdGenerator>(col, state_, max_distinct_values_);
    }
    APPLY_ON_NUMBER_COLUMN(UInt8, nested_col, col)
    APPLY_ON_NUMBER_COLUMN(UInt16, nested_col, col)
    APPLY_ON_NUMBER_COLUMN(UInt32, nested_col, col)
    APPLY_ON_NUMBER_COLUMN(UInt64, nested_col, col)
    APPLY_ON_NUMBER_COLUMN(UInt128, nested_col, col)
    APPLY_ON_NUMBER_COLUMN(UInt256, nested_col, col)
    APPLY_ON_NUMBER_COLUMN(Int8, nested_col, col)
    APPLY_ON_NUMBER_COLUMN(Int16, nested_col, col)
    APPLY_ON_NUMBER_COLUMN(Int32, nested_col, col)
    APPLY_ON_NUMBER_COLUMN(Int64, nested_col, col)
    APPLY_ON_NUMBER_COLUMN(Int128, nested_col, col)
    APPLY_ON_NUMBER_COLUMN(Int256, nested_col, col)
    APPLY_ON_NUMBER_COLUMN(Float32, nested_col, col)
    APPLY_ON_NUMBER_COLUMN(Float64, nested_col, col)
    APPLY_ON_NUMBER_COLUMN(UUID, nested_col, col)
    APPLY_ON_NUMBER_COLUMN(IPv4, nested_col, col)
    APPLY_ON_NUMBER_COLUMN(IPv6, nested_col, col)
    APPLY_ON_NUMBER_COLUMN(Date, nested_col, col)
    APPLY_ON_NUMBER_COLUMN(Date32, nested_col, col)
    APPLY_ON_NUMBER_COLUMN(DateTime, nested_col, col)
    APPLY_ON_NUMBER_COLUMN(DateTime64, nested_col, col)
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown column type: {}", col->getName());
}

}
