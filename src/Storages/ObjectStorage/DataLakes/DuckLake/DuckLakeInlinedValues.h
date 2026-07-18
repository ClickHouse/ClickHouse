#pragma once

#include "config.h"

#if USE_PARQUET

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>

#include <optional>
#include <vector>

namespace DB
{

namespace DuckLake
{

/// Convert one SQL text value produced by a DuckLake catalog backend into a Field of
/// `type` (which must be non-nullable; SQL NULLs are handled by the caller).
/// Backend encodings handled: postgres bytea hex for strings/blobs (top-level columns
/// only), postgres t/f booleans and Infinity/NaN floats, timestamptz offsets, and the
/// DuckDB literal syntax for nested values ({'x': 1}, [1, 2], {k=v}), including
/// '::type' cast suffixes on nested scalars.
/// Throws BAD_ARGUMENTS on malformed values.
Field parseInlinedValue(const String & value, const DataTypePtr & type, bool postgres_backend);

/// Build a ClickHouse column of `type` from per-row values (nullopt = SQL NULL).
/// Throws BAD_ARGUMENTS on NULLs in a non-nullable column or malformed values.
ColumnPtr buildInlinedColumn(
    const std::vector<std::optional<String>> & values,
    const DataTypePtr & type,
    bool postgres_backend);

}

}

#endif
