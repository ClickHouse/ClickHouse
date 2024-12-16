#include "Common/ErrorCodes.h"
#include "Common/logger_useful.h"
#include "Columns/ColumnNullable.h"
#include "Columns/ColumnsDateTime.h"
#include "DataTypes/DataTypeNullable.h"


#include "Storages/ObjectStorage/DataLakes/Iceberg/PartitionPruning.h"


namespace DB::ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int BAD_ARGUMENTS;
}

using namespace DB;

namespace Iceberg
{

Iceberg::PartitionTransform getTransform(const String & transform_name)
{
    if (transform_name == "year")
    {
        return Iceberg::PartitionTransform::Year;
    }
    else if (transform_name == "month")
    {
        return Iceberg::PartitionTransform::Month;
    }
    else if (transform_name == "day")
    {
        return Iceberg::PartitionTransform::Day;
    }
    else if (transform_name == "hour")
    {
        return Iceberg::PartitionTransform::Hour;
    }
    else if (transform_name == "identity")
    {
        return Iceberg::PartitionTransform::Identity;
    }
    else if (transform_name == "void")
    {
        return Iceberg::PartitionTransform::Void;
    }
    else
    {
        return Iceberg::PartitionTransform::Unsupported;
    }
}

DateLUTImpl::Values getValues(Int32 value, Iceberg::PartitionTransform transform)
{
    if (transform == Iceberg::PartitionTransform::Year)
    {
        return DateLUT::instance().lutIndexByYearSinceEpochStartsZeroIndexing(value);
    }
    else if (transform == Iceberg::PartitionTransform::Month)
    {
        return DateLUT::instance().lutIndexByMonthSinceEpochStartsZeroIndexing(static_cast<UInt32>(value));
    }
    else if (transform == Iceberg::PartitionTransform::Day)
    {
        return DateLUT::instance().getValues(static_cast<ExtendedDayNum>(value));
    }
    else if (transform == Iceberg::PartitionTransform::Hour)
    {
        DateLUTImpl::Values values = DateLUT::instance().getValues(static_cast<ExtendedDayNum>(value / 24));
        values.date += (value % 24) * 3600;
        return values;
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported partition transform for get day function: {}", transform);
}

Int64 getTime(Int32 value, Iceberg::PartitionTransform transform)
{
    DateLUTImpl::Values values = getValues(value, transform);
    return values.date;
}

Int16 getDay(Int32 value, Iceberg::PartitionTransform transform)
{
    DateLUTImpl::Time got_time = getTime(value, transform);
    // LOG_DEBUG(&Poco::Logger::get("Get field"), "Time: {}", got_time);
    return DateLUT::instance().toDayNum(got_time);
}

Range getPartitionRange(
    Iceberg::PartitionTransform partition_transform, UInt32 index, ColumnPtr partition_column, DataTypePtr column_data_type)
{
    if (partition_transform == Iceberg::PartitionTransform::Unsupported)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported partition transform: {}", partition_transform);
    }
    auto column = dynamic_cast<const ColumnNullable *>(partition_column.get())->getNestedColumnPtr();
    if (partition_transform == Iceberg::PartitionTransform::Identity)
    {
        Field entry = (*column.get())[index];
        return Range{entry, true, entry, true};
    }
    auto [nested_data_type, value] = [&]() -> std::pair<DataTypePtr, Int32>
    {
        if (column->getDataType() == TypeIndex::Int32)
        {
            const auto * casted_innner_column = assert_cast<const ColumnInt32 *>(column.get());
            Int32 begin_value = static_cast<Int32>(casted_innner_column->getInt(index));
            LOG_DEBUG(
                &Poco::Logger::get("Partition"), "Partition value: {}, transform: {}, column_type: int", begin_value, partition_transform);
            return {dynamic_cast<const DataTypeNullable *>(column_data_type.get())->getNestedType(), begin_value};
        }
        else if (column->getDataType() == TypeIndex::Date && (partition_transform == Iceberg::PartitionTransform::Day))
        {
            const auto * casted_innner_column = assert_cast<const ColumnDate *>(column.get());
            Int32 begin_value = static_cast<Int32>(casted_innner_column->getInt(index));
            LOG_DEBUG(
                &Poco::Logger::get("Partition"), "Partition value: {}, transform: {}, column type: date", begin_value, partition_transform);
            return {dynamic_cast<const DataTypeNullable *>(column_data_type.get())->getNestedType(), begin_value};
        }
        else
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported partition column type: {}", column->getFamilyName());
        }
    }();
    if (WhichDataType(nested_data_type).isDate() && (partition_transform != Iceberg::PartitionTransform::Hour))
    {
        const UInt16 begin_range_value = getDay(value, partition_transform);
        const UInt16 end_range_value = getDay(value + 1, partition_transform);
        LOG_DEBUG(&Poco::Logger::get("Partition"), "Range begin: {}, range end {}", begin_range_value, end_range_value);
        return Range{begin_range_value, true, end_range_value, false};
    }
    else if (WhichDataType(nested_data_type).isDateTime64())
    {
        const UInt64 begin_range_value = getTime(value, partition_transform);
        const UInt64 end_range_value = getTime(value + 1, partition_transform);
        return Range{begin_range_value, true, end_range_value, false};
    }
    else
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Partition transform {} is not supported for the type: {}", partition_transform, nested_data_type);
    }
}


} // namespace Iceberg
