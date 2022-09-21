#pragma once

#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Formats/FormatSettings.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>
#include <IO/Progress.h>
#include <Core/NamesAndTypes.h>
#include <Common/Stopwatch.h>
#include <utility>

namespace DB
{

namespace JSONUtils
{
    std::pair<bool, size_t> fileSegmentationEngineJSONEachRow(ReadBuffer & in, DB::Memory<> & memory, size_t min_chunk_size);
    std::pair<bool, size_t>
    fileSegmentationEngineJSONCompactEachRow(ReadBuffer & in, DB::Memory<> & memory, size_t min_chunk_size, size_t min_rows);

    /// Parse JSON from string and convert it's type to ClickHouse type. Make the result type always Nullable.
    /// JSON array with different nested types is treated as Tuple.
    /// If cannot convert (for example when field contains null), return nullptr.
    DataTypePtr getDataTypeFromField(const String & field, const FormatSettings & settings);

    /// Read row in JSONEachRow format and try to determine type for each field.
    /// Return list of names and types.
    /// If cannot determine the type of some field, return nullptr for it.
    NamesAndTypesList readRowAndGetNamesAndDataTypesForJSONEachRow(ReadBuffer & in, const FormatSettings & settings, bool json_strings);

    /// Read row in JSONCompactEachRow format and try to determine type for each field.
    /// If cannot determine the type of some field, return nullptr for it.
    DataTypes readRowAndGetDataTypesForJSONCompactEachRow(ReadBuffer & in, const FormatSettings & settings, bool json_strings);

    bool nonTrivialPrefixAndSuffixCheckerJSONEachRowImpl(ReadBuffer & buf);

    bool readField(
        ReadBuffer & in,
        IColumn & column,
        const DataTypePtr & type,
        const SerializationPtr & serialization,
        const String & column_name,
        const FormatSettings & format_settings,
        bool yield_strings);

    Strings makeNamesValidJSONStrings(const Strings & names, const FormatSettings & settings, bool validate_utf8);

    /// Functions helpers for writing JSON data to WriteBuffer.

    void writeFieldDelimiter(WriteBuffer & out, size_t new_lines = 1);

    void writeFieldCompactDelimiter(WriteBuffer & out);

    void writeObjectStart(WriteBuffer & out, size_t indent = 0, const char * title = nullptr);

    void writeCompactObjectStart(WriteBuffer & out, size_t indent = 0, const char * title = nullptr);

    void writeObjectEnd(WriteBuffer & out, size_t indent = 0);

    void writeCompactObjectEnd(WriteBuffer & out);

    void writeArrayStart(WriteBuffer & out, size_t indent = 0, const char * title = nullptr);

    void writeCompactArrayStart(WriteBuffer & out, size_t indent = 0, const char * title = nullptr);

    void writeArrayEnd(WriteBuffer & out, size_t indent = 0);

    void writeCompactArrayEnd(WriteBuffer & out);

    void writeFieldFromColumn(
        const IColumn & column,
        const ISerialization & serialization,
        size_t row_num,
        bool yield_strings,
        const FormatSettings & settings,
        WriteBuffer & out,
        const std::optional<String> & name = std::nullopt,
        size_t indent = 0,
        const char * title_after_delimiter = " ");

    void writeColumns(
        const Columns & columns,
        const Names & names,
        const Serializations & serializations,
        size_t row_num,
        bool yield_strings,
        const FormatSettings & settings,
        WriteBuffer & out,
        size_t indent = 0);

    void writeCompactColumns(
        const Columns & columns,
        const Serializations & serializations,
        size_t row_num,
        bool yield_strings,
        const FormatSettings & settings,
        WriteBuffer & out);

    void writeMetadata(const Names & names, const DataTypes & types, const FormatSettings & settings, WriteBuffer & out);

    void writeAdditionalInfo(
        size_t rows,
        size_t rows_before_limit,
        bool applied_limit,
        const Stopwatch & watch,
        const Progress & progress,
        bool write_statistics,
        WriteBuffer & out);

    void skipColon(ReadBuffer & in);
    void skipComma(ReadBuffer & in);

    String readFieldName(ReadBuffer & in);

    void skipArrayStart(ReadBuffer & in);
    void skipArrayEnd(ReadBuffer & in);
    bool checkAndSkipArrayStart(ReadBuffer & in);
    bool checkAndSkipArrayEnd(ReadBuffer & in);

    void skipObjectStart(ReadBuffer & in);
    void skipObjectEnd(ReadBuffer & in);
    bool checkAndSkipObjectEnd(ReadBuffer & in);

    NamesAndTypesList readMetadata(ReadBuffer & in);
    NamesAndTypesList readMetadataAndValidateHeader(ReadBuffer & in, const Block & header);

    bool skipUntilFieldInObject(ReadBuffer & in, const String & desired_field_name);
    void skipTheRestOfObject(ReadBuffer & in);
}

}
