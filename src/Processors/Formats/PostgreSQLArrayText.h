#pragma once

#include <Columns/IColumn_fwd.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatSettings.h>

namespace DB
{

class WriteBuffer;

/// Serialize row `row` of an `Array(...)` column as a PostgreSQL array literal (`{...}`, nested as
/// `{{...},{...}}`, `NULL` for a null element). ClickHouse's own `serializeText` cannot be used because it
/// encloses arrays in `[]` and quotes elements in a way PostgreSQL does not understand. This spelling is
/// what a PostgreSQL client (including ClickHouse itself, via `postgresql(..., 'arr_table')`, which parses
/// the response with `pqxx::array_parser`) expects. Every non-null scalar element is double-quoted with `"`
/// and `\` escaped: the array parser strips the quotes before handing the content to the per-element parser,
/// so quoting numbers and dates is harmless while quoting text is required.
void writePostgreSQLArrayText(
    const IColumn & column, const IDataType & type, size_t row, WriteBuffer & out, const FormatSettings & settings);

}
