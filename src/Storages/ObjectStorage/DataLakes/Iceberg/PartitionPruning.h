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

// This function is used to convert the entry `partition_column[index]` to the range which type coincides with the
// `column_data_type` and represents the time period corresponding to the entry's value under the `partition_transform`
// Examples:
// partition_transform = Iceberg::PartitionTransform::Year
// index = 3
// partition_column = [1970, 1971, 1972, 1973, 1974]
// column_data_type = Date
// Range: {1096, true, 1461, false} // 1096 is the start of the year 1973 (in days), 1461 is the start of the year 1974 (in days) (exclusive end of the year 1973)
// partition_transform = Iceberg::PartitionTransform::Month
// index = 3
// partition_column = [1970-01, 1970-02, 1970-03, 1970-04, 1970-05]
// column_data_type = DateTime64
// Range: {7776000, true, 10368000, false} // 7776000 is the start of the year 1970-04 (in seconds), 10368000 is the start of the month 1970-05 (in seconds) (exclusive end of the month 1970-04)
DB::Range getPartitionRange(
    Iceberg::PartitionTransform partition_transform, size_t index, DB::ColumnPtr partition_column, DB::DataTypePtr column_data_type);
}
