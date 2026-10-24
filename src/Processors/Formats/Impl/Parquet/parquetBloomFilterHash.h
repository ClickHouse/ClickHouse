#pragma once

#include <config.h>

#include <Core/Field.h>
#include <Columns/ColumnString.h>

#if USE_PARQUET

#include <Processors/Formats/Impl/Parquet/ThriftUtil.h>

namespace DB
{

/// Hashes values the way Parquet bloom filters do: `XXH64` with seed 0 of the value represented as
/// the column's physical type. `schema_element` describes a primitive column.

/*
 * Try to hash a ClickHouse field, nullopt in case it can't be done
 * */
std::optional<uint64_t> parquetTryHashField(const Field & field, const Parquet::parq::SchemaElement & schema_element);


/*
 * Try to hash elements in a ClickHouse column; Will return std::nullopt in case one of them can't be hashed
 * */
std::optional<std::vector<uint64_t>> parquetTryHashColumn(const IColumn * data_column, const Parquet::parq::SchemaElement & schema_element);

}

#endif
