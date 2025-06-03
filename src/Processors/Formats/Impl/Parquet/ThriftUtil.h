#pragma once

#include <generated/parquet_types.h> // in contrib/arrow/cpp/src/ , generated from parquet.thrift
#include <IO/WriteBuffer.h>

namespace DB::Parquet
{

/// Returns number of bytes written.
template <typename T>
size_t serializeThriftStruct(const T & obj, WriteBuffer & out);

extern template size_t serializeThriftStruct<parquet::format::PageHeader>(const parquet::format::PageHeader &, WriteBuffer & out);
extern template size_t serializeThriftStruct<parquet::format::ColumnChunk>(const parquet::format::ColumnChunk &, WriteBuffer & out);
extern template size_t serializeThriftStruct<parquet::format::FileMetaData>(const parquet::format::FileMetaData &, WriteBuffer & out);

}
