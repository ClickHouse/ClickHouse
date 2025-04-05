#pragma once

#include <generated/parquet_types.h> // in contrib/arrow/cpp/src/ , generated from parquet.thrift
#include <IO/WriteBuffer.h>
#include <sstream>

namespace DB::Parquet
{

/// Namespace with structs generated from parquet.thrift
namespace parq = parquet::format;

/// Returns number of bytes written.
/// The template must be explicitly instantiated in ThriftUtil.{h,cpp}.
template <typename T>
size_t serializeThriftStruct(const T & obj, WriteBuffer & out);

/// Returns number of bytes read (<= limit).
/// The struct is not necessarily fully overwritten, the caller must ensure that `out` is
/// default-initialized or cleared before the call.
/// The template must be explicitly instantiated in ThriftUtil.{h,cpp}.
template <typename T>
size_t deserializeThriftStruct(T & out, const char * buf, size_t limit);

template <typename T>
std::string thriftToString(const T &obj)
{
    std::stringstream ss;
    ss << obj;
    return ss.str();
}


extern template size_t serializeThriftStruct<parq::PageHeader>(const parq::PageHeader &, WriteBuffer & out);
extern template size_t serializeThriftStruct<parq::ColumnChunk>(const parq::ColumnChunk &, WriteBuffer & out);
extern template size_t serializeThriftStruct<parq::FileMetaData>(const parq::FileMetaData &, WriteBuffer & out);
extern template size_t serializeThriftStruct<parq::ColumnIndex>(const parq::ColumnIndex &, WriteBuffer & out);
extern template size_t serializeThriftStruct<parq::OffsetIndex>(const parq::OffsetIndex &, WriteBuffer & out);
extern template size_t serializeThriftStruct<parq::BloomFilterHeader>(const parq::BloomFilterHeader &, WriteBuffer & out);

extern template size_t deserializeThriftStruct<parq::FileMetaData>(parq::FileMetaData &, const char *, size_t);
extern template size_t deserializeThriftStruct<parq::PageHeader>(parq::PageHeader &, const char *, size_t);
extern template size_t deserializeThriftStruct<parq::BloomFilterHeader>(parq::BloomFilterHeader &, const char *, size_t);
extern template size_t deserializeThriftStruct<parq::ColumnIndex>(parq::ColumnIndex &, const char *, size_t);
extern template size_t deserializeThriftStruct<parq::OffsetIndex>(parq::OffsetIndex &, const char *, size_t);

}
