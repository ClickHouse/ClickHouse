#pragma once

#include <vector>
#include <Core/NamesAndTypes.h>
#include <Core/Range.h>

namespace Iceberg
{

enum class PartitionTransform
{
    Year,
    Month,
    Day,
    Hour,
    Identity,
    Void,
    Unsupported
};

struct SpecificSchemaPartitionInfo
{
    std::vector<std::vector<DB::Range>> ranges;
    DB::NamesAndTypesList partition_names_and_types;
};

Iceberg::PartitionTransform getTransform(const String & transform_name);

DateLUTImpl::Values getValues(Int32 value, Iceberg::PartitionTransform transform);

Int64 getTime(Int32 value, Iceberg::PartitionTransform transform);

Int16 getDay(Int32 value, Iceberg::PartitionTransform transform);

DB::Range getPartitionRange(
    Iceberg::PartitionTransform partition_transform, UInt32 index, DB::ColumnPtr partition_column, DB::DataTypePtr column_data_type);

}
