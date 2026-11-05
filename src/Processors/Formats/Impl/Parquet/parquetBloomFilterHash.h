#pragma once

#include <config.h>

#include <Core/Field.h>
#include <Columns/ColumnString.h>

#if USE_PARQUET

#include <parquet/encoding.h>
#include <parquet/schema.h>

namespace DB
{

/*
 * Try to hash a ClickHouse field, nullopt in case it can't be done
 * */
std::optional<uint64_t> parquetTryHashField(const Field & field, const parquet::ColumnDescriptor * parquet_column_descriptor);


/*
 * Try to hash elements in a ClickHouse column; Will return std::nullopt in case one of them can't be hashed
 * */
std::optional<std::vector<uint64_t>> parquetTryHashColumn(const IColumn * data_column, const parquet::ColumnDescriptor * parquet_column_descriptor);

/// Everything besides the values that the two functions above read from the descriptor: values hash
/// the same way for two descriptors of the same kind. Must change whenever their inputs do.
String parquetHashKind(const parquet::ColumnDescriptor * parquet_column_descriptor);

}

#endif
