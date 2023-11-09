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
void StringHashValueIdGenerator::setup(const IColumn *col)
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
    for (; i < n; i++)
    {
        auto str_len = offsets[i] - prev_offset;
        if (str_len == 1)
        {
            char_pos += str_len;
            prev_offset = offsets[i];
            continue;
        }
        if (str_len > 9)
        {
            enable_range_mode = false;
            break;
        }
        UInt64 val = 0;
        shortStringToUInt64(char_pos, str_len - 1, val);
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
    enable_range_mode = enable_range_mode && range_max > range_min && range_max - range_min + 1 + is_nullable <= max_distinct_values;
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
        return std::make_unique<NumericHashValueIdGenerator<type, Column##type, false>>(col, state_, max_distinct_values_); \
    }
#define APPLY_ON_BASIC_NUMBER_COLUMN(type, nested_col, col) \
    else if (const auto * col##type = typeid_cast<const Column##type *>(nested_col)) \
    { \
        return std::make_unique<NumericHashValueIdGenerator<type, Column##type, true>>(col, state_, max_distinct_values_); \
    }

    WhichDataType which_type(nested_col->getDataType());
    if (which_type.isString())
    {
        return std::make_unique<StringHashValueIdGenerator>(col, state_, max_distinct_values_);
    }
    else if (which_type.isFixedString())
    {
        const auto * fixed_str_col = typeid_cast<const ColumnFixedString *>(nested_col);
        if (fixed_str_col->getN() == 1)
            return std::make_unique<FixedStringHashValueIdGenerator<1>>(col, state_, max_distinct_values_);
        else if (fixed_str_col->getN() == 2)
            return std::make_unique<FixedStringHashValueIdGenerator<2>>(col, state_, max_distinct_values_);
        else if (fixed_str_col->getN() == 3)
            return std::make_unique<FixedStringHashValueIdGenerator<3>>(col, state_, max_distinct_values_);
        else if (fixed_str_col->getN() == 4)
            return std::make_unique<FixedStringHashValueIdGenerator<4>>(col, state_, max_distinct_values_);
        else if (fixed_str_col->getN() == 5)
            return std::make_unique<FixedStringHashValueIdGenerator<5>>(col, state_, max_distinct_values_);
        else if (fixed_str_col->getN() == 6)
            return std::make_unique<FixedStringHashValueIdGenerator<6>>(col, state_, max_distinct_values_);
        else if (fixed_str_col->getN() == 7)
            return std::make_unique<FixedStringHashValueIdGenerator<7>>(col, state_, max_distinct_values_);
        else if (fixed_str_col->getN() == 8)
            return std::make_unique<FixedStringHashValueIdGenerator<8>>(col, state_, max_distinct_values_);
        return std::make_unique<FixedStringHashValueIdGenerator<0>>(col, state_, max_distinct_values_);
    }
    APPLY_ON_BASIC_NUMBER_COLUMN(UInt8, nested_col, col)
    APPLY_ON_BASIC_NUMBER_COLUMN(UInt16, nested_col, col)
    APPLY_ON_BASIC_NUMBER_COLUMN(UInt32, nested_col, col)
    APPLY_ON_BASIC_NUMBER_COLUMN(UInt64, nested_col, col)
    APPLY_ON_NUMBER_COLUMN(UInt128, nested_col, col)
    APPLY_ON_NUMBER_COLUMN(UInt256, nested_col, col)
    APPLY_ON_BASIC_NUMBER_COLUMN(Int8, nested_col, col)
    APPLY_ON_BASIC_NUMBER_COLUMN(Int16, nested_col, col)
    APPLY_ON_BASIC_NUMBER_COLUMN(Int32, nested_col, col)
    APPLY_ON_BASIC_NUMBER_COLUMN(Int64, nested_col, col)
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
