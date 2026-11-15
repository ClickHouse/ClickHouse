#pragma once

#include <Columns/IColumn.h>
#include <Storages/BigQuery/BigQuerySchema.h>

#include <Poco/Dynamic/Var.h>

namespace DB
{

/// Insert one cell of a `tabledata.list` response (the "v" value) into a column.
/// `type` must be the ClickHouse type the field was mapped to (`field.data_type`).
/// TIMESTAMP values are expected as int64 microseconds (formatOptions.useInt64Timestamp=true).
void insertBigQueryValue(IColumn & column, const DataTypePtr & type, const BigQueryField & field, const Poco::Dynamic::Var & value);

/// Convert one cell of a column into the JSON value for a `tabledata.insertAll` request.
Poco::Dynamic::Var bigQueryJSONValue(const BigQueryField & field, const DataTypePtr & type, const IColumn & column, size_t row);

}
