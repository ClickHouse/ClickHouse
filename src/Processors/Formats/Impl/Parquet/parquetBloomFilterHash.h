#pragma once

#include <config.h>

#include <Core/Field.h>
#include <Columns/ColumnString.h>
#include <DataTypes/IDataType_fwd.h>

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

/// Whether a query constant of `requested_type` hashes to the digest of the stored value it has to
/// match. Constants are hashed in the requested type, while dictionary values are hashed in
/// `decoded_type` and the file's bloom filter holds digests of the parquet physical values.
bool parquetHashFilterOutputTypeIsExact(
    const DataTypePtr & decoded_type, const DataTypePtr & requested_type, parquet::Type::type physical_type);

}

#endif
