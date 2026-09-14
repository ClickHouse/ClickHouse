#pragma once

#include "config.h"

#if USE_ARROW

#include <Formats/FormatSettings.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/Impl/ArrowIPC/MessageWriter.h>
#include <Processors/Formats/Impl/ArrowIPC/RecordBatchEncoder.h>
#include <Processors/Formats/Impl/ArrowIPC/SchemaConverter.h>
#include <Core/Names.h>
#include <Columns/IColumn.h>
#include <Common/StringHashForHeterogeneousLookup.h>
#include <Common/StringWithMemoryTracking.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace DB
{

/// Native ClickHouse writer for the `Arrow` (file) and `ArrowStream` (stream) IPC formats.
///
/// Encodes ClickHouse columns directly into Arrow IPC record-batch buffers and builds the FlatBuffers
/// metadata without the Apache Arrow C++ library. Selected via `output_format_arrow_use_native_writer`.
/// `LowCardinality` is written as its full column, or — when `output_format_arrow_low_cardinality_as_dictionary`
/// is on — as an Arrow dictionary-encoded column (a single dictionary per id, extended across batches via deltas).
class ArrowIPCBlockOutputFormat final : public IOutputFormat
{
public:
    ArrowIPCBlockOutputFormat(WriteBuffer & out_, SharedHeader header_, bool stream_, const FormatSettings & format_settings_);

    String getName() const override { return "ArrowIPCBlockOutputFormat"; }

private:
    void consume(Chunk) override;
    void finalizeImpl() override;
    void resetFormatterImpl() override;

    void writeSchemaIfNeeded();
    /// Writes one encapsulated message for an encoded batch (a record batch, or a dictionary batch
    /// when `dictionary_id` is set), returning its location for recording an Arrow file `Block`.
    ArrowIPC::MessageWriter::WrittenMessage writeBatchMessage(
        const ArrowIPC::RecordBatchEncoder::EncodedBatch & batch,
        std::optional<Int64> dictionary_id = std::nullopt,
        bool is_delta = false);

    /// Recursively replaces every dictionary-encoded `LowCardinality` node of `column`/`type` (per `plan`)
    /// with its integer index column, accumulating the per-id dictionary and emitting `DictionaryBatch`
    /// deltas, and returns the substituted (column, type) for the record batch. Handles nested dictionaries
    /// inside `Array`/`Tuple`/`Map`.
    std::pair<ColumnPtr, DataTypePtr> substituteDictionaries(
        const ColumnPtr & column, const DataTypePtr & type, const ArrowIPC::DictPlan & plan);
    /// Encodes one `LowCardinality` column against its accumulated dictionary id and returns its index
    /// column (Nullable-wrapped when the dictionary value type is nullable) and that index column's type.
    std::pair<ColumnPtr, DataTypePtr> encodeDictionaryColumn(
        const ColumnPtr & low_cardinality_column, const DataTypePtr & low_cardinality_type, const ArrowIPC::OutputDictionary & dict);

    const bool stream;
    const FormatSettings format_settings;

    Names column_names;
    DataTypes column_types;

    std::optional<ArrowIPC::MessageWriter> message_writer;
    std::unique_ptr<ArrowIPC::RecordBatchEncoder> encoder;
    bool schema_written = false;
    ArrowIPC::ArrowFileBlocks dictionary_blocks;
    ArrowIPC::ArrowFileBlocks record_blocks;

    /// Dictionary-encoded output (`output_format_arrow_low_cardinality_as_dictionary`): the per-column
    /// plan of which `LowCardinality` nodes (top-level or nested) are dictionary-encoded, and the per-id
    /// accumulated dictionary (extended across batches via Arrow dictionary deltas).
    /// Compares dictionary keys as `string_view` so a lookup by `string_view` does not allocate a key.
    struct DictKeyEqual
    {
        using is_transparent = void;
        bool operator()(std::string_view a, std::string_view b) const { return a == b; }
    };

    struct DictionaryColumnState
    {
        MutableColumnPtr values;                              /// accumulated dictionary values (full nested type)
        /// The key owns a tracked copy of the value, so the dedup memory is enforced by `max_memory_usage`;
        /// lookups go through the transparent `string_view` overloads and do not allocate.
        UnorderedMapWithMemoryTracking<StringWithMemoryTracking, Int64, StringHashForHeterogeneousLookup, DictKeyEqual> value_to_index;
        bool emitted = false;
    };
    ArrowIPC::DictPlans column_dict_plans;
    VectorWithMemoryTracking<DictionaryColumnState> dictionary_states;
};

}

#endif
