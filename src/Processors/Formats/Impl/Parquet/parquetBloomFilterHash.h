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

/*
 * The INT32/INT64 physical value that `parquetTryHashField` hashes for an integer constant, sign-extended
 * to 64 bits for INT32; nullopt if it would not hash the constant as an integer.
 * */
std::optional<int64_t> parquetTryPhysicalInteger(const Field & field, const parquet::ColumnDescriptor * parquet_column_descriptor);

}

#endif
