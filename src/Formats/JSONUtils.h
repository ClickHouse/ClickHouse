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
    DataTypePtr getDataTypeFromField(const String & field);

    /// Read row in JSONEachRow format and try to determine type for each field.
    /// Return list of names and types.
    /// If cannot determine the type of some field, return nullptr for it.
    NamesAndTypesList readRowAndGetNamesAndDataTypesForJSONEachRow(ReadBuffer & in, bool json_strings);

    /// Read row in JSONCompactEachRow format and try to determine type for each field.
    /// If cannot determine the type of some field, return nullptr for it.
    DataTypes readRowAndGetDataTypesForJSONCompactEachRow(ReadBuffer & in, bool json_strings);

    bool nonTrivialPrefixAndSuffixCheckerJSONEachRowImpl(ReadBuffer & buf);

    bool readField(
        ReadBuffer & in,
        IColumn & column,
        const DataTypePtr & type,
        const SerializationPtr & serialization,
        const String & column_name,
        const FormatSettings & format_settings,
        bool yield_strings);

    DataTypePtr getCommonTypeForJSONFormats(const DataTypePtr & first, const DataTypePtr & second, bool allow_bools_as_numbers);

    void makeNamesAndTypesWithValidUTF8(NamesAndTypes & fields, const FormatSettings & settings, bool & need_validate_utf8);

    /// Functions helpers for writing JSON data to WriteBuffer.

    void writeFieldDelimiter(WriteBuffer & out, size_t new_lines = 1);

    void writeFieldCompactDelimiter(WriteBuffer & out);

    void writeObjectStart(WriteBuffer & out, size_t indent = 0, const char * title = nullptr);

    void writeObjectEnd(WriteBuffer & out, size_t indent = 0);

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
        size_t indent = 0);

    void writeColumns(
        const Columns & columns,
        const NamesAndTypes & fields,
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

    void writeMetadata(const NamesAndTypes & fields, const FormatSettings & settings, WriteBuffer & out);

    void writeAdditionalInfo(
        size_t rows,
        size_t rows_before_limit,
        bool applied_limit,
        const Stopwatch & watch,
        const Progress & progress,
        bool write_statistics,
        WriteBuffer & out);
}

}
