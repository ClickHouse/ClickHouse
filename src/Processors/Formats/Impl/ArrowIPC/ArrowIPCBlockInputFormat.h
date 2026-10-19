#pragma once

#include "config.h"

#if USE_ARROW

#include <Core/BlockMissingValues.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/Impl/ArrowIPC/MessageReader.h>
#include <Processors/Formats/Impl/ArrowIPC/SchemaConverter.h>
#include <Processors/Formats/Impl/ArrowIPC/RecordBatchDecoder.h>
#include <Processors/Formats/Impl/ArrowGeoTypes.h>
#include <Formats/FormatSettings.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace DB
{

class ReadBuffer;
class SeekableReadBuffer;
struct ColumnWithTypeAndName;

/// Native ClickHouse reader for the `Arrow` (file) and `ArrowStream` (stream) IPC formats.
///
/// Does not use the Apache Arrow C++ library: it parses the IPC metadata (FlatBuffers) directly and
/// decodes record-batch buffers straight into ClickHouse columns. Selected via
/// `input_format_arrow_use_native_reader`. The streaming format is read sequentially; the file format
/// uses the footer for random access to record batches (seeking the input, or loading it into memory
/// when the input is not seekable).
class ArrowIPCBlockInputFormat final : public IInputFormat
{
public:
    ArrowIPCBlockInputFormat(ReadBuffer & in_, SharedHeader header_, bool stream_, const FormatSettings & format_settings_);
    ~ArrowIPCBlockInputFormat() override;

    String getName() const override { return "ArrowIPCBlockInputFormat"; }

    void resetParser() override;

    const BlockMissingValues * getMissingValues() const override;

    size_t getApproxBytesReadForChunk() const override { return approx_bytes_read_for_chunk; }

private:
    Chunk read() override;

    void onCancel() noexcept override { is_stopped = 1; }

    void prepareReader();
    void prepareStreamReader();
    void prepareFileReader();
    void collectDictionaryFields(const ArrowIPC::ArrowFields & fields);
    /// Fills `reachable_dictionary_ids` with the Arrow dictionary ids referenced by the requested top-level
    /// fields (per `requested_top_level_fields`), so the reader skips the DictionaryBatch bodies of
    /// dictionaries used only by unrequested columns. Requires `arrow_schema` and the requested-fields set.
    void computeReachableDictionaryIds();
    Chunk buildChunk(ArrowIPC::RecordBatchDecoder::DecodedColumns & decoded, size_t num_rows);
    /// Reinterprets the raw bytes of decoded fixed_size_binary / binary leaves as UUID / IPv6 / big integer
    /// in place when the requested header type asks for it (recursing through Nullable/Array/Tuple/Map), so
    /// the raw 16/32 bytes are reinterpreted rather than text-parsed by the subsequent cast.
    static void reinterpretRawByteColumns(ColumnWithTypeAndName & column, const DataTypePtr & to_type);
    /// Parses the WKB/WKT binary values of a decoded (possibly Nullable) String column into a geo column.
    static ColumnPtr decodeGeoColumn(const ColumnPtr & source, const GeoColumnMetadata & geo_metadata, bool precise_float_parsing);
    Chunk readStream();
    Chunk readFile();

    const bool stream;

    std::optional<ArrowIPC::MessageReader> message_reader;
    std::optional<ArrowIPC::ArrowSchema> arrow_schema;
    /// GeoParquet geometry columns (by field name), parsed from the schema-level "geo" metadata.
    /// Plain `std::unordered_map`: assigned from `parseGeoMetadataEncoding` (shared API) and bounded by the
    /// schema's geo-column count.
    std::unordered_map<String, GeoColumnMetadata> geo_columns; // STYLE_CHECK_ALLOW_STD_CONTAINERS
    ArrowIPC::DictionaryRegistry dictionaries;
    /// For each Arrow dictionary id, the field describing its value type (used to decode dictionary batches).
    UnorderedMapWithMemoryTracking<Int64, ArrowIPC::ArrowField> dictionary_value_fields;
    std::unique_ptr<ArrowIPC::RecordBatchDecoder> decoder;
    /// Top-level Arrow field names the requested header needs (normalized for case-insensitive matching).
    /// The decoder skips every other column so a SELECT of a subset of columns does not decode — or fail
    /// on — the unrequested ones. Empty until `prepareReader` runs.
    UnorderedSetWithMemoryTracking<String> requested_top_level_fields;
    /// Each requested column's target ClickHouse type, keyed by its normalized name (including dotted
    /// subcolumn names like `t.d`). Passed to the decoder for the recursive `date32` numeric type hint: a
    /// `date32` mapped (at any nesting) to a numeric target is read as the raw `Int32` day number without
    /// the `Date32` range check, matching the Apache Arrow library reader's numeric type-hint behavior.
    UnorderedMapWithMemoryTracking<String, DataTypePtr> requested_field_target_types;
    /// Arrow dictionary ids referenced by the requested top-level fields (computed by
    /// `computeReachableDictionaryIds`). A DictionaryBatch whose id is not here belongs only to unrequested
    /// columns and its body is skipped rather than decoded, so a subset read does not pay for — or fail on —
    /// an unrequested column's dictionary.
    UnorderedSetWithMemoryTracking<Int64> reachable_dictionary_ids;
    bool prepared = false;
    PODArray<char> body_buffer;

    /// File format: random access to record batches via the footer.
    SeekableReadBuffer * seekable = nullptr;
    String file_data;                       /// Owns the bytes when the input had to be loaded into memory.
    std::unique_ptr<ReadBuffer> memory_buffer;
    struct BlockInfo { Int64 offset = 0; Int64 metadata_length = 0; Int64 body_length = 0; };
    VectorWithMemoryTracking<BlockInfo> record_batch_blocks;
    size_t record_batch_current = 0;

    BlockMissingValues block_missing_values;
    size_t approx_bytes_read_for_chunk = 0;

    const FormatSettings format_settings;

    std::atomic<int> is_stopped{0};
};

}

#endif
