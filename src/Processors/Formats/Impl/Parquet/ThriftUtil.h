#pragma once

#include <base/defines.h>
#include <base/types.h>
#include <generated/parquet_types.h> // in contrib/arrow/cpp/src/ , generated from parquet.thrift

#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace DB
{
    class WriteBuffer;
}

namespace DB::Parquet
{

/// Namespace with structs generated from parquet.thrift
namespace parq = parquet::format;

/// Parquet file footer whose column chunks are parsed on demand, see deserializeColumnChunks.
struct FileFooter
{
    /// All of FileMetaData except `row_groups[i].columns`, which are empty.
    parq::FileMetaData metadata;
    /// Serialized FileMetaData.
    std::string serialized;
    /// For each row group: where each serialized ColumnChunk starts in `serialized`, then where the last one ends.
    std::vector<std::vector<UInt32>> column_chunk_offsets;

    size_t numColumnChunks(size_t row_group_idx) const;
};

/// One Parquet column's chunks in every row group of a file.
struct FileColumnChunks
{
    /// [row group]; default-constructed where the row group has no such chunk.
    std::vector<parq::ColumnChunk> chunks;
    /// What `chunks` allocates, for ParquetMetadataCache's weight.
    size_t memory_bytes = 0;
};
using FileColumnChunksPtr = std::shared_ptr<const FileColumnChunks>;

/// Offset indexes of a file's column chunks, read from the file and validated, that ParquetMetadataCache keeps with the
/// cached footer (see `ParquetMetadataCache::keepOffsetIndexes`), so that a later read of the file does not read them again.
/// Only grows. Shared by the cache cells of a file version that replace each other.
class KeptOffsetIndexes
{
public:
    using OffsetIndexPtr = std::shared_ptr<const parq::OffsetIndex>;

    struct Entry
    {
        size_t row_group_idx = 0;
        /// Parquet column index.
        size_t column_idx = 0;
        OffsetIndexPtr offset_index;
    };

    explicit KeptOffsetIndexes(size_t max_bytes_) : max_bytes(max_bytes_) {}
    ~KeptOffsetIndexes();
    KeptOffsetIndexes(const KeptOffsetIndexes &) = delete;
    KeptOffsetIndexes & operator=(const KeptOffsetIndexes &) = delete;

    /// nullptr if not kept.
    OffsetIndexPtr get(size_t row_group_idx, size_t column_idx) const;
    /// Keeps the entries not kept yet, while all kept entries take at most `max_bytes`. Returns whether any was added.
    bool add(const std::vector<Entry> & entries);
    size_t bytes() const;
    size_t remainingBytes() const;
    /// What keeping `offset_index` takes.
    static size_t entryBytes(const parq::OffsetIndex & offset_index);

private:
    static UInt64 makeKey(size_t row_group_idx, size_t column_idx) { return (UInt64(row_group_idx) << 32) | column_idx; }

    const size_t max_bytes;
    mutable std::mutex mutex;
    std::unordered_map<UInt64, OffsetIndexPtr> offset_indexes TSA_GUARDED_BY(mutex);
    size_t total_bytes TSA_GUARDED_BY(mutex) = 0;
};
using KeptOffsetIndexesPtr = std::shared_ptr<KeptOffsetIndexes>;

/// Like deserializeThriftStruct<parq::FileMetaData>, but skips the elements of `RowGroup::columns`
/// and records their offsets in `column_chunk_offsets` (see FileFooter).
void deserializeFileMetaDataWithoutColumnChunks(
    parq::FileMetaData & out, const char * buf, size_t limit, std::vector<std::vector<UInt32>> & column_chunk_offsets);

/// Parses the column chunks `column_indices` (sorted, unique) of every row group:
/// `out[k].chunks[i]` is column chunk `column_indices[k]` of row group `i`.
void deserializeColumnChunks(
    const char * buf, size_t limit, const std::vector<std::vector<UInt32>> & column_chunk_offsets,
    const std::vector<size_t> & column_indices, std::vector<FileColumnChunks> & out);

/// All templates are explicitly instantiated, feel free to add more types.

/// Returns number of bytes written.
template <typename T>
size_t serializeThriftStruct(const T & obj, WriteBuffer & out);

/// Returns number of bytes read (<= limit).
/// The struct is not necessarily fully overwritten, the caller must ensure that `out` is
/// default-initialized or cleared before the call.
template <typename T>
size_t deserializeThriftStruct(T & out, const char * buf, size_t limit);

template <typename T>
std::string thriftToString(const T &obj);


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

extern template std::string thriftToString<parq::Encoding>(const parq::Encoding &);
extern template std::string thriftToString<parq::PageHeader>(const parq::PageHeader &);
extern template std::string thriftToString<parq::CompressionCodec>(const parq::CompressionCodec &);
extern template std::string thriftToString<parq::SchemaElement>(const parq::SchemaElement &);
extern template std::string thriftToString<parq::PageType::type>(const parq::PageType::type &);
extern template std::string thriftToString<parq::PageLocation>(const parq::PageLocation &);
extern template std::string thriftToString<parq::Type::type>(const parq::Type::type &);

}
