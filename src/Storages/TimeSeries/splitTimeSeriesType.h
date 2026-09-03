#pragma once

#include <DataTypes/IDataType.h>


namespace DB
{

/// Splits data type `Array(Tuple(<timestamp_type>, <scalar_type>))` into <timestamp_type> and <scalar_type>.
std::pair<DataTypePtr, DataTypePtr> splitTimeSeriesType(const DataTypePtr & time_series_type);

}
