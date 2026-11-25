#pragma once

#include <cstdint>

namespace DB
{

/// The enums of `FormatSettings` that are also setting types. `FormatSettings` inherits them, so they are still named
/// `FormatSettings::X`; they are here so that `Core/SettingsEnums.h` does not need `Formats/FormatSettings.h`.
struct FormatSettingsEnums
{
    enum class AggregateFunctionInputFormat : uint8_t
    {
        State,
        Value,
        Array,
    };

    enum class DateTimeInputFormat : uint8_t
    {
        Basic, /// Default format for fast parsing: YYYY-MM-DD hh:mm:ss (ISO-8601 without fractional part and timezone) or NNNNNNNNNN unix timestamp.
        BestEffort, /// Use sophisticated rules to parse whatever possible.
        BestEffortUS /// Use sophisticated rules to parse American style: mm/dd/yyyy
    };

    enum class DateTimeOutputFormat : uint8_t
    {
        Simple,
        ISO,
        UnixTimestamp
    };

    enum class EscapingRule : uint8_t
    {
        None,
        Escaped,
        Quoted,
        CSV,
        JSON,
        XML,
        Raw
    };

    enum class IntervalOutputFormat : uint8_t
    {
        Kusto,
        Numeric
    };

    enum class DateTimeOverflowBehavior : uint8_t
    {
        Ignore,
        Throw,
        Saturate
    };

    enum class InputFormatColumnMatchingCaseSensitivity : uint8_t
    {
        MATCH_CASE, /// Matches case-sensitively
        IGNORE_CASE, /// Matches case-insensitively
        AUTO, /// First tries to match case-sensitively, if fails, tries to match case-insensitively
    };

    enum class ArrowCompression : uint8_t
    {
        NONE,
        LZ4_FRAME,
        ZSTD
    };

    /// What to do with a column whose type has no first-class Arrow mapping.
    enum class ArrowUnsupportedTypes : uint8_t
    {
        /// Reject the query.
        THROW,
        /// Write the text representation of each value (`serializeText`) as an Arrow `Utf8` column.
        TEXT,
        /// Write the binary representation of each value (`serializeBinary`) as an Arrow `Binary` column.
        BINARY
    };

    enum class ParquetVersion : uint8_t
    {
        V1_0,
        V2_4,
        V2_6,
        V2_LATEST,
    };

    enum class ParquetCompression : uint8_t
    {
        NONE,
        SNAPPY,
        ZSTD,
        LZ4,
        GZIP,
        BROTLI,
    };

    enum class ORCCompression : uint8_t
    {
        NONE,
        LZ4,
        SNAPPY,
        ZSTD,
        ZLIB,
    };

    /// For capnProto format we should determine how to
    /// compare ClickHouse Enum and Enum from schema.
    enum class CapnProtoEnumComparingMode : uint8_t
    {
        BY_NAMES, // Names in enums should be the same, values can be different.
        BY_NAMES_CASE_INSENSITIVE, // Case-insensitive name comparison.
        BY_VALUES, // Values should be the same, names can be different.
    };

    enum class MsgPackUUIDRepresentation : uint8_t
    {
        STR, // Output UUID as a string of 36 characters.
        BIN, // Output UUID as 16-bytes binary.
        EXT, // Output UUID as ExtType = 2
    };

    enum class UnsupportedGeometryHandling { Throw, Null };
};

}
