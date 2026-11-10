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
    /// Lists described by an offset and a size per row: the ranges can overlap and come in any order.
    ListView,
    LargeListView,
    Struct,
    Map,
    Union,
    /// Values stored once per run: children `run_ends` (Int16/Int32/Int64, the exclusive end of each run) and `values`.
    RunEndEncoded,
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

    /// Struct: the field names are empty or repeated, which Arrow allows but a named `Tuple` cannot have. Such a
    /// struct is read as an unnamed `Tuple`, and its fields are matched with the requested type and identified by
    /// position instead of by name.
    bool positional_fields = false;

    /// The Arrow type name for a `TypeKind::Unsupported` placeholder (used in the error message).
    std::string unsupported_type_name;
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

/// Parses the IPC Schema FlatBuffer into the Arrow-free description above. `version` is the metadata
/// version of the message (or file footer) the schema came from: the union buffer layout differs between
/// V4 and V5, and only the V5 one is implemented, so a union field under V4 is rejected here.
ArrowSchema parseSchema(const flatbuf::Schema & schema, flatbuf::MetadataVersion version);

/// Whether a fixed_size_binary(16) field is flagged as the Arrow UUID extension type.
bool isUUIDField(const ArrowField & field);

/// The ClickHouse type name a field carries when the writer had no Arrow mapping for it and wrote it as an
/// opaque column, or empty when the field is not one. The field's Arrow type says which encoding it holds:
/// `Utf8` is the text form, `Binary` is `serializeBinary`.
std::string_view opaqueFieldTypeName(const ArrowField & field);

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

/// Whether the values of a field can be null, i.e. whether its natural ClickHouse type is `Nullable` when following the
/// metadata. A `RunEndEncoded` field has no validity bitmap, so its own nullability flag says nothing: its nulls are the
/// nulls of its values.
bool isNullableField(const ArrowField & field);

/// Maps a parsed Arrow field to the ClickHouse data type used for schema inference / the natural
/// decode type. `make_nullable` forces a Nullable wrapper (used for schema_inference_make_columns_nullable).
/// `allow_null_type` maps an Arrow `null`-typed field to the all-null `Nullable(Nothing)` instead of
/// throwing `UNKNOWN_TYPE`. The data reader and schema inference both enable it.
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
