#pragma once

#include "config.h"

#if USE_ARROW

#include <Processors/Formats/Impl/ArrowIPC/FlatBuffersCommon.h>
#include <DataTypes/IDataType.h>
#include <Core/Names.h>
#include <Formats/FormatSettings.h>
#include <Common/MapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

#include <map>
#include <optional>
#include <vector>

namespace DB
{
class SeekableReadBuffer;
}

namespace DB::ArrowIPC
{

/// Arrow-free description of an Arrow logical type, parsed from the IPC Schema FlatBuffer.
/// This is the input to both the CH-type mapping (schema inference / header building) and the
/// record-batch decoder's per-field buffer walk.
enum class TypeKind : UInt8
{
    Null,
    Int,
    FloatingPoint,
    Bool,
    Decimal,
    Date,
    Time,
    Timestamp,
    Duration,
    Interval,
    Utf8,
    LargeUtf8,
    Binary,
    LargeBinary,
    BinaryView,
    Utf8View,
    FixedSizeBinary,
    List,
    LargeList,
    FixedSizeList,
    Struct,
    Map,
    Union,
    /// An Arrow type the native reader does not support. Parsed as a placeholder (rather than failing
    /// during schema parsing) so schema inference can drop it with
    /// `input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference`; decoding or requesting
    /// such a column still fails with a clear error.
    Unsupported,
};

struct ArrowField;

struct ArrowType
{
    TypeKind kind = TypeKind::Null;

    /// Int
    int bit_width = 0;
    bool is_signed = false;

    /// FloatingPoint precision: matches flatbuf::Precision (HALF=0, SINGLE=1, DOUBLE=2).
    int float_precision = 0;

    /// Decimal
    int precision = 0;
    int scale = 0;
    int decimal_bit_width = 128;

    /// Date unit (flatbuf::DateUnit), Time/Timestamp/Duration unit (flatbuf::TimeUnit), Interval unit.
    int unit = 0;
    int time_bit_width = 32;
    std::string timezone;

    /// FixedSizeBinary byte width / FixedSizeList element count.
    int byte_width = 0;
    int list_size = 0;

    /// Union
    int union_mode = 0;
    VectorWithMemoryTracking<int> union_type_ids;

    /// Children (List/LargeList/FixedSizeList element, Struct/Union fields, Map's key_value struct).
    VectorWithMemoryTracking<ArrowField> children;

    /// The Arrow type name for a `TypeKind::Unsupported` placeholder (used in the error message).
    std::string unsupported_type_name;

    /// Physical buffer layout of a `TypeKind::Unsupported` field, when it is known. The reader cannot
    /// decode these types, but knowing the layout lets `skipField` advance the node/buffer cursors past an
    /// unrequested column of such a type (subset-of-columns support), instead of failing a `SELECT` of the
    /// other columns. `Unknown` means the layout is not known and the column cannot be skipped.
    enum class SkipLayout : UInt8
    {
        Unknown,        /// cannot be skipped
        ListView,       /// validity + offsets + sizes buffers, then one child (same buffer count for LargeListView)
        RunEndEncoded,  /// no buffers (not even validity), then two children (run_ends, values)
    };
    SkipLayout skip_layout = SkipLayout::Unknown;
};

struct DictionaryEncoding
{
    Int64 id = -1;
    int index_bit_width = 32;
    bool index_is_signed = true;

    bool operator==(const DictionaryEncoding &) const = default;
};

using KeyValueMetadata = MapWithMemoryTracking<std::string, std::string>;

struct ArrowField
{
    std::string name;
    bool nullable = false;
    ArrowType type;
    std::optional<DictionaryEncoding> dictionary;
    KeyValueMetadata custom_metadata;
};

using ArrowFields = VectorWithMemoryTracking<ArrowField>;

struct ArrowSchema
{
    ArrowFields fields;
    KeyValueMetadata custom_metadata;
    bool little_endian = true;
};

/// Parses the IPC Schema FlatBuffer into the Arrow-free description above.
ArrowSchema parseSchema(const flatbuf::Schema & schema);

/// Whether a fixed_size_binary(16) field is flagged as the Arrow UUID extension type.
bool isUUIDField(const ArrowField & field);

/// A record-batch / dictionary-batch location inside an Arrow file.
struct ArrowFileBlock
{
    Int64 offset = 0;
    Int32 metadata_length = 0; /// the block's metadata section size; bounds the message metadata read
    Int64 body_length = 0;
};

using ArrowFileBlocks = VectorWithMemoryTracking<ArrowFileBlock>;

/// The parsed footer of an Arrow file: schema plus the locations of all dictionary and record batches.
struct ArrowFileFooter
{
    ArrowSchema schema;
    ArrowFileBlocks dictionary_blocks;
    ArrowFileBlocks record_batch_blocks;
};

/// Verifies the Arrow file magic and reads its footer. `file_size` is the total input size (the
/// input must support `SEEK_SET`, but not necessarily `SEEK_END`).
ArrowFileFooter readArrowFileFooter(SeekableReadBuffer & in, size_t file_size);

/// Builds the Arrow file `Footer` FlatBuffer (schema + dictionary/record-batch blocks) into `builder`.
void buildFooter(
    flatbuffers::FlatBufferBuilder & builder,
    const Names & names,
    const DataTypes & types,
    const FormatSettings & settings,
    const ArrowFileBlocks & dictionary_blocks,
    const ArrowFileBlocks & record_blocks);

/// Maps a parsed Arrow field to the ClickHouse data type used for schema inference / the natural
/// decode type. `make_nullable` forces a Nullable wrapper (used for schema_inference_make_columns_nullable).
/// `allow_null_type` maps an Arrow `null`-typed field to the all-null `Nullable(Nothing)` instead of
/// throwing — the data reader enables it (matching the library's `allow_arrow_null_type`), schema
/// inference leaves it off.
DataTypePtr fieldToCHType(
    const ArrowField & field, const FormatSettings & settings, bool make_nullable, bool allow_null_type = false);

/// Builds the IPC `Schema` message FlatBuffer for a ClickHouse header into `builder` (left Finished),
/// mirroring the type choices of the native writer's record-batch encoder.
void buildSchemaMessage(
    flatbuffers::FlatBufferBuilder & builder, const Names & names, const DataTypes & types, const FormatSettings & settings);

/// Description of a dictionary-encoded output node (`output_format_arrow_low_cardinality_as_dictionary`).
struct OutputDictionary
{
    Int64 id = 0;
    int index_bit_width = 32;
    bool index_is_signed = true;
};

/// Recursive plan describing which `LowCardinality` nodes of a column's type tree are written as Arrow
/// dictionaries — top-level *or* nested inside `Array`/`Tuple`/`Map` — when
/// `output_format_arrow_low_cardinality_as_dictionary` is on, with ids/index types assigned in a
/// deterministic DFS order. It is the single source of truth for dictionary ids: the schema builder and
/// the output format both derive it from the same types+settings and read ids by position, so they agree
/// without any traversal-order coordination. `children` mirror the type's children in the order the
/// schema builder recurses (`Array` -> [item]; `Tuple` -> [elements...]; `Map` -> [key, value]); `Variant`
/// is treated as a dictionary boundary (no nested dictionaries inside it) and other types have no children.
struct DictPlan
{
    std::optional<OutputDictionary> here; /// set when this type node is a dictionary-encoded `LowCardinality`
    VectorWithMemoryTracking<DictPlan> children;

    /// True if this node or any descendant is dictionary-encoded (lets the writer skip untouched columns).
    bool hasAnyDictionary() const;
};

using DictPlans = VectorWithMemoryTracking<DictPlan>;

/// Builds the dictionary plan (see `DictPlan`) for each top-level column, with ids assigned sequentially
/// across all columns in DFS order. Both the schema builder and the output format use this.
DictPlans assignOutputDictionaries(const DataTypes & types, const FormatSettings & settings);

}

#endif
