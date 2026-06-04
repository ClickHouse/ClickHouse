#pragma once

#include "config.h"

#if USE_ARROW

#include <Processors/Formats/Impl/ArrowIPC/FlatBuffersCommon.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatSettings.h>

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
enum class TypeKind : uint8_t
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
    FixedSizeBinary,
    List,
    LargeList,
    FixedSizeList,
    Struct,
    Map,
    Union,
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
    std::vector<int> union_type_ids;

    /// Children (List/LargeList/FixedSizeList element, Struct/Union fields, Map's key_value struct).
    std::vector<ArrowField> children;
};

struct DictionaryEncoding
{
    int64_t id = -1;
    int index_bit_width = 32;
    bool index_is_signed = true;
    bool is_ordered = false;
};

using KeyValueMetadata = std::map<std::string, std::string>;

struct ArrowField
{
    std::string name;
    bool nullable = false;
    ArrowType type;
    std::optional<DictionaryEncoding> dictionary;
    KeyValueMetadata custom_metadata;
};

struct ArrowSchema
{
    std::vector<ArrowField> fields;
    KeyValueMetadata custom_metadata;
    bool little_endian = true;
};

/// Parses the IPC Schema FlatBuffer into the Arrow-free description above.
ArrowSchema parseSchema(const flatbuf::Schema & schema);

/// A record-batch / dictionary-batch location inside an Arrow file (absolute offset + body length).
struct ArrowFileBlock
{
    int64_t offset = 0;
    int64_t body_length = 0;
};

/// The parsed footer of an Arrow file: schema plus the locations of all dictionary and record batches.
struct ArrowFileFooter
{
    ArrowSchema schema;
    std::vector<ArrowFileBlock> dictionary_blocks;
    std::vector<ArrowFileBlock> record_batch_blocks;
};

/// Verifies the Arrow file magic and reads its footer. `file_size` is the total input size (the
/// input must support `SEEK_SET`, but not necessarily `SEEK_END`).
ArrowFileFooter readArrowFileFooter(SeekableReadBuffer & in, size_t file_size);

/// Maps a parsed Arrow field to the ClickHouse data type used for schema inference / the natural
/// decode type. `make_nullable` forces a Nullable wrapper (used for schema_inference_make_columns_nullable).
DataTypePtr fieldToCHType(const ArrowField & field, const FormatSettings & settings, bool make_nullable);

}

#endif
