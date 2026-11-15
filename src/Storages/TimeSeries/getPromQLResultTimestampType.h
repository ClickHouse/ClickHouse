#pragma once

#include <DataTypes/IDataType.h>


namespace DB
{

/// Returns the scale of the timestamps in the results of PromQL evaluation.
/// It's the scale of the timestamp column of the TimeSeries table, but not less than 3 (milliseconds).
/// The same scale is used for all timestamps and durations while parsing and evaluating the query
/// (see `PrometheusQueryEvaluationSettings::time_scale`).
UInt32 getPromQLResultTimestampScale(const DataTypePtr & table_timestamp_type);

/// Returns the time zone of the timestamps in the results of PromQL evaluation. It is the time zone specified in the types of the
/// `DateTime` or `DateTime64` parameters of the evaluation (the evaluation time, or the start and the end of the range) if they all
/// specify the same time zone; otherwise it is the time zone of the timestamp column of the TimeSeries table. An empty string means
/// no time zone is specified (for example, the column has type UInt32), then the server's time zone is used.
String getPromQLResultTimeZone(const DataTypePtr & table_timestamp_type, const DataTypes & parameter_types = {});

/// Returns the data type of the timestamps in the results of PromQL evaluation: DateTime64 with the scale `time_scale`
/// (see getPromQLResultTimestampScale()) and the time zone `time_zone` (see getPromQLResultTimeZone()).
DataTypePtr getPromQLResultTimestampType(UInt32 time_scale, const String & time_zone);

}
