#pragma once

#include <DataTypes/IDataType.h>


namespace DB
{

/// Returns the scale used for timestamps in the results of PromQL evaluation and for parsing PromQL queries.
/// It's the scale of the timestamp column of the TimeSeries table, but not less than 3 (milliseconds).
UInt32 getPromQLResultTimestampScale(const DataTypePtr & table_timestamp_type);

/// Returns the data type used for timestamps in the results of PromQL evaluation.
/// It's always DateTime64 with the scale returned by getPromQLResultTimestampScale()
/// and with the same time zone as the timestamp column of the TimeSeries table (if it has one).
DataTypePtr getPromQLResultTimestampType(const DataTypePtr & table_timestamp_type);

/// Returns the data type used for values in the results of PromQL evaluation. It's always Float64.
DataTypePtr getPromQLResultValueType();

}
