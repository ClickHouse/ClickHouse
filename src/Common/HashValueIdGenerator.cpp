#include <memory>
#include <Columns/ColumnsDateTime.h>
#include <Common/ColumnsHashing.h>
#include <Common/Exception.h>
#include <Common/HashTable/Hash.h>
#include <Common/HashValueIdGenerator.h>
#include <Common/typeid_cast.h>
#include "logger_useful.h"
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
    allocated_value_id = is_nullable;
    /// String column does not support range mode.
    enable_range_mode = false;
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
        return std::make_unique<FixedStringHashValueIdGenerator>(col, state_, max_distinct_values_);
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
