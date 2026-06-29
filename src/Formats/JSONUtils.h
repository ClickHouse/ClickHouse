#pragma once

#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Formats/FormatSettings.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>
#include <IO/Progress.h>
#include <Core/NamesAndTypes.h>
#include <Common/Stopwatch.h>
#include <functional>
#include <utility>

namespace DB
{

class Block;
struct JSONInferenceInfo;

namespace JSONUtils
{
    std::pair<bool, size_t> fileSegmentationEngineJSONEachRow(ReadBuffer & in, DB::Memory<> & memory, size_t min_bytes, size_t max_rows);
    std::pair<bool, size_t> fileSegmentationEngineJSONCompactEachRow(ReadBuffer & in, DB::Memory<> & memory, size_t min_bytes, size_t min_rows, size_t max_rows);

    void skipRowForJSONEachRow(ReadBuffer & in);
    void skipRowForJSONCompactEachRow(ReadBuffer & in);

    /// Read row in JSONEachRow format and try to determine type for each field.
    /// Return list of names and types.
    /// If cannot determine the type of some field, return nullptr for it.
    NamesAndTypesList readRowAndGetNamesAndDataTypesForJSONEachRow(ReadBuffer & in, const FormatSettings & settings, JSONInferenceInfo * inference_info);

    /// Read row in JSONCompactEachRow format and try to determine type for each field.
    /// If cannot determine the type of some field, return nullptr for it.
    DataTypes readRowAndGetDataTypesForJSONCompactEachRow(ReadBuffer & in, const FormatSettings & settings, JSONInferenceInfo * inference_info);

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
        const char * title_after_delimiter = " ",
        bool pretty_json = false);

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
        size_t rows_before_aggregation,
        bool applied_aggregation,
        const Stopwatch & watch,
        const Progress & progress,
        bool write_statistics,
        WriteBuffer & out);

    void writeException(const String & exception_message, WriteBuffer & out, const FormatSettings & settings, size_t indent = 0);

    void skipColon(ReadBuffer & in);
    void skipComma(ReadBuffer & in);
    bool checkAndSkipComma(ReadBuffer & in);

    String readFieldName(ReadBuffer & in, const FormatSettings::JSON & settings);

    void skipArrayStart(ReadBuffer & in);
    void skipArrayEnd(ReadBuffer & in);
    bool checkAndSkipArrayStart(ReadBuffer & in);
    bool checkAndSkipArrayEnd(ReadBuffer & in);

    void skipObjectStart(ReadBuffer & in);
    void skipObjectEnd(ReadBuffer & in);
    bool checkAndSkipObjectStart(ReadBuffer & in);
    bool checkAndSkipObjectEnd(ReadBuffer & in);

    NamesAndTypesList readMetadata(ReadBuffer & in, const FormatSettings::JSON & settings);
    bool tryReadMetadata(ReadBuffer & in, NamesAndTypesList & names_and_types, const FormatSettings::JSON & settings);
    NamesAndTypesList readMetadataAndValidateHeader(ReadBuffer & in, const Block & header, const FormatSettings::JSON & settings);
    void validateMetadataByHeader(const NamesAndTypesList & names_and_types_from_metadata, const Block & header);

    bool skipUntilFieldInObject(ReadBuffer & in, const String & desired_field_name, const FormatSettings::JSON & settings);
    void skipTheRestOfObject(ReadBuffer & in, const FormatSettings::JSON & settings);

    template <typename ReturnType>
    using NestedDeserialize = std::function<ReturnType(IColumn &, ReadBuffer &)>;

    template <typename ReturnType, bool default_column_return_value = true>
    ReturnType deserializeEmpyStringAsDefaultOrNested(IColumn & column, ReadBuffer & istr, const NestedDeserialize<ReturnType> & deserialize_nested);

    extern template void deserializeEmpyStringAsDefaultOrNested<void, true>(IColumn & column, ReadBuffer & istr, const NestedDeserialize<void> & deserialize_nested);
    extern template bool deserializeEmpyStringAsDefaultOrNested<bool, true>(IColumn & column, ReadBuffer & istr, const NestedDeserialize<bool> & deserialize_nested);
    extern template bool deserializeEmpyStringAsDefaultOrNested<bool, false>(IColumn & column, ReadBuffer & istr, const NestedDeserialize<bool> & deserialize_nested);
}

}
