#pragma once

#include <config.h>

#if USE_PARQUET

#include <Processors/Formats/Impl/ArrowFieldIndexUtil.h>

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

}

#endif
