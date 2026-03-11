#pragma once

#include <DataTypes/Serializations/SerializationTuple.h>
#include <rocksdb/db.h>
#include <rocksdb/iterator.h>

namespace DB
{

class IMergeTreeDataPart;
class IDataPartStorage;
class WriteBuffer;
class ReadBuffer;
class ReadBufferFromFileBase;
class SeekableReadBuffer;
class SSTFileWriter;
class SSTFileReader;

/// SortedStringKV: KV pairs stored in SST files with offset-based seeking
///
/// File layout:
///   column.offsets.bin  - per-granule row offsets for seeking
///   column.sst          - SST file with all KV pairs (global per part)
///
/// The SST stream is managed externally by MergeTreeDataPartWriter and injected via getters.
class SerializationSortedStringKV final : public SerializationTuple
{
public:
    /// Write state: holds reference to SSTFileWriter for streaming KV pairs
    struct SerializeBinaryBulkStateSST : public SerializeBinaryBulkState
    {
        SSTFileWriter * sst_file_writer = nullptr;
    };

    /// Read state: SST reader with lazy initialization (clone-safe)
    struct DeserializeBinaryBulkStateSortedStringKV : public DeserializeBinaryBulkState
    {
        std::shared_ptr<SSTFileReader> sst_file_reader;
        std::unique_ptr<rocksdb::Iterator> sst_file_iterator;
        UInt64 current_row_position = 0;

        DeserializeBinaryBulkStatePtr clone() const override
        {
            /// Return a fresh state — iterator and reader are NOT carried over.
            /// The cloned state will lazily re-init from the SST stream getter,
            /// starting from SeekToFirst with current_row_position = 0.
            return std::make_shared<DeserializeBinaryBulkStateSortedStringKV>();
        }
    };

    SerializationSortedStringKV(const ElementSerializations & elems_, bool has_explicit_names_)
        : SerializationTuple(elems_, has_explicit_names_)
    {
    }

    void enumerateStreams(
        EnumerateStreamsSettings & settings,
        const StreamCallback & callback,
        const SubstreamData & data) const override;

    void serializeBinaryBulkStatePrefix(
        const IColumn & column,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkWithMultipleStreams(
        const IColumn & column,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkStateSuffix(
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsDeserializeStatesCache * cache) const override;

    void deserializeBinaryBulkWithMultipleStreams(
        ColumnPtr & column,
        size_t rows_offset,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const override;

};

}
