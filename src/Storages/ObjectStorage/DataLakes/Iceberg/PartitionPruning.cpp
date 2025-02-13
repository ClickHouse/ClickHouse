#include "Common/ErrorCodes.h"
#include "Common/logger_useful.h"
#include "Columns/ColumnNullable.h"
#include "Columns/ColumnsDateTime.h"
#include "DataTypes/DataTypeNullable.h"


#include "Storages/ObjectStorage/DataLakes/Iceberg/PartitionPruning.h"


namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

using namespace DB;

namespace Iceberg
{

Iceberg::PartitionTransform getTransform(const String & transform_name)
{
    if (transform_name == "year" || transform_name == "years")
    {
        return Iceberg::PartitionTransform::Year;
    }
    else if (transform_name == "month" || transform_name == "months")
    {
        return Iceberg::PartitionTransform::Month;
    }
    else if (transform_name == "day" || transform_name == "date" || transform_name == "days" || transform_name == "dates")
    {
        return Iceberg::PartitionTransform::Day;
    }
    else if (transform_name == "hour" || transform_name == "hours")
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

// This function is used to convert the value to the start of the corresponding time period (internal ClickHouse representation)
DateLUTImpl::Values getDateLUTImplValues(Int32 value, Iceberg::PartitionTransform transform)
{
    switch (transform)
    {
        case Iceberg::PartitionTransform::Year:
            return DateLUT::instance().lutIndexByYearSinceEpochStartsZeroIndexing(value);
        case Iceberg::PartitionTransform::Month:
            return DateLUT::instance().lutIndexByMonthSinceEpochStartsZeroIndexing(static_cast<UInt32>(value));
        case Iceberg::PartitionTransform::Day:
            return DateLUT::instance().getValues(static_cast<ExtendedDayNum>(value));
        case Iceberg::PartitionTransform::Hour:
        {
            DateLUTImpl::Values values = DateLUT::instance().getValues(static_cast<ExtendedDayNum>(value / 24));
            values.date += (value % 24) * 3600;
            return values;
        }
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported partition transform for get values function: {}", transform);
    }
}

// This function is used to convert the value to the start of the corresponding time period (in seconds)
Int64 getTime(Int32 value, Iceberg::PartitionTransform transform)
{
    DateLUTImpl::Values values = getDateLUTImplValues(value, transform);
    return values.date;
}

// This function is used to convert the value to the start of the corresponding date period (in days)
Int16 getDay(Int32 value, Iceberg::PartitionTransform transform)
{
    DateLUTImpl::Time got_time = getTime(value, transform);
    return DateLUT::instance().toDayNum(got_time);
}


Range getPartitionRange(
    Iceberg::PartitionTransform partition_transform, size_t index, ColumnPtr partition_column, DataTypePtr column_data_type)
{
    if (partition_transform == Iceberg::PartitionTransform::Unsupported)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported partition transform: {}", partition_transform);
    }
    if (partition_transform == Iceberg::PartitionTransform::Identity)
    {
        Field entry = (*partition_column.get())[index];
        return Range{entry, true, entry, true};
    }
    if (partition_column->getDataType() != TypeIndex::Int32)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported partition column type: {}", partition_column->getFamilyName());
    }

    auto nested_data_type = removeNullable(column_data_type);

    const auto * casted_innner_column = assert_cast<const ColumnInt32 *>(partition_column.get());
    Int32 value = casted_innner_column->getElement(index);

    if ((WhichDataType(nested_data_type).isDate() || WhichDataType(nested_data_type).isDate32())
        && (partition_transform != Iceberg::PartitionTransform::Hour))
    {
        const UInt16 begin_range_value = getDay(value, partition_transform);
        const UInt16 end_range_value = getDay(value + 1, partition_transform);
        return Range{begin_range_value, true, end_range_value, false};
    }
    else if (WhichDataType(nested_data_type).isDateTime64() || WhichDataType(nested_data_type).isDateTime())
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


}
