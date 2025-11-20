#pragma once

#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

/// Returns the scale used in a specified time type if it's a decimal, or 0 otherwise.
UInt32 getTimeseriesTimeScale(const DataTypePtr & time_or_duration_type);

/// Returns the timezone used in a specified time type or an empty string.
String getTimeseriesTimezone(const DataTypePtr & time_type);

/// Returns "DateTime64(scale, 'timezone')" if scale > 0; otherwise returns "DateTime('timezone')".
DataTypePtr makeTimeseriesTimeDataType(UInt32 scale, const String & timezone);

/// Extracts a timestamp from a Field.
DecimalField<DateTime64> getTimeseriesTime(const Field & field, UInt32 default_scale);
DecimalField<DateTime64> getTimeseriesTime(const Field & field, const DataTypePtr & type, UInt32 default_scale);

/// Extracts a duration from a Field.
DecimalField<Decimal64> getTimeseriesDuration(const Field & field, UInt32 default_scale);
DecimalField<Decimal64> getTimeseriesDuration(const Field & field, const DataTypePtr & type, UInt32 default_scale);

/// Converts a time to SQL.
ASTPtr timeseriesTimeToAST(const DecimalField<DateTime64> & time);
ASTPtr timeseriesTimeToAST(const DecimalField<DateTime64> & time, const DataTypePtr & time_type);

/// Converts a duration to SQL.
ASTPtr timeseriesDurationToAST(const DecimalField<Decimal64> & duration);

/// Adds a duration to a timestamp.
DecimalField<DateTime64> addTimeseriesDuration(const DecimalField<DateTime64> & time, const DecimalField<Decimal64> & duration);

/// Subtracts a duration from a timestamp.
DecimalField<DateTime64> subtractTimeseriesDuration(const DecimalField<DateTime64> & time, const DecimalField<Decimal64> & duration);

/// Calculates the duration between [min_time] and [max_time].
/// The function basically returns (max_time - min_time).
DecimalField<Decimal64> getTimeseriesDuration(const DecimalField<DateTime64> & min_time, const DecimalField<DateTime64> & max_time);

/// Returns the number of steps between `start_time` and `end_time`, including both `start_time` and `end_time`.
size_t countTimeseriesSteps(const DecimalField<DateTime64> & start_time, const DecimalField<DateTime64> & end_time, const DecimalField<Decimal64> & step);

/// Increases a timestamp to make it divisible by `step`.
DecimalField<DateTime64> roundUpTimeseriesTime(const DecimalField<DateTime64> & time, const DecimalField<Decimal64> & step);

/// Decreases a timestamp to make it divisible by `step`.
DecimalField<DateTime64> roundDownTimeseriesTime(const DecimalField<DateTime64> & time, const DecimalField<Decimal64> & step);

}
