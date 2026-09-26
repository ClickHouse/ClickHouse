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
#include <optional>
#include <string_view>
#include <utility>

namespace DB
{

class Block;
struct JSONInferenceInfo;

namespace JSONUtils
{
    std::pair<bool, size_t> fileSegmentationEngineJSONEachRow(ReadBuffer & in, DB::Memory<> & memory, size_t min_bytes, size_t max_rows, size_t max_row_size = 0);
    std::pair<bool, size_t> fileSegmentationEngineJSONCompactEachRow(ReadBuffer & in, DB::Memory<> & memory, size_t min_bytes, size_t min_rows, size_t max_rows, size_t max_row_size = 0);

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

    /// Returns true if the JSON keys derived from `names` (via `makeNamesValidJSONStrings`) would
    /// contain bytes that are not valid UTF-8 under the given settings. This can happen only when
    /// UTF-8 validation is off (`validate_utf8 == false`) and a name contains raw non-UTF-8 bytes
    /// (for example a quoted alias like `` `a\xFFb` ``), because `writeJSONString` passes such bytes
    /// through verbatim. The names come from the header and are therefore known before the first row,
    /// so text framings (see `IFramingFormat::requiresTextPayload`) can reject or base64-encode the
    /// output accordingly.
    bool namesMayProduceRawBytesInJSON(const Strings & names, const FormatSettings & settings, bool validate_utf8);

    /// Returns true if the `meta.type` strings that the full-document JSON formats (`JSON`,
    /// `JSONStrings`, `JSONCompact`, `JSONCompactStrings`, `JSONColumnsWithMetadata`) serialize via
    /// `writeMetadata` may contain bytes that are not valid UTF-8. These formats always request
    /// UTF-8 validation for the column names, but the type-name strings are only routed through
    /// `WriteBufferValidUTF8` when the output adaptor installs the validating buffer, which it does
    /// only if at least one column's value type may itself emit invalid UTF-8 (see
    /// `OutputFormatWithUTF8ValidationAdaptorBase`). When every value type is guaranteed valid
    /// UTF-8, that buffer is skipped, so a non-UTF-8 type name (for example a named `Tuple` element
    /// or an `Enum` value with arbitrary bytes) leaks into `meta.type` verbatim. The type names come
    /// from the header, so text framings can reject or base64-encode the output accordingly.
    bool metadataTypeNamesMayProduceRawBytesInJSON(const Block & header, const FormatSettings & settings);

    /// Returns true if the JSON object keys synthesized from named `Tuple` element names during row
    /// serialization may contain bytes that are not valid UTF-8. When
    /// `output_format_json_named_tuples_as_objects` is on (the default),
    /// `SerializationTuple::serializeTextJSON` writes `getElementName()` of every element as a JSON
    /// key - verbatim, without the `makeNamesValidJSONStrings` sanitization the top-level column
    /// names get. The only sanitization such keys can receive is the whole-output
    /// `WriteBufferValidUTF8` of `OutputFormatWithUTF8ValidationAdaptorBase`, which is installed
    /// only when `validate_utf8` is on and at least one column's value type may itself emit invalid
    /// UTF-8 - and `DataTypeTuple::textCanContainOnlyValidUTF8` inspects only the element value
    /// types, not the element names, so a `Tuple` of clean value types with a non-UTF-8 element
    /// name skips the buffer even with validation on. The element names come from the header
    /// (walked recursively through `Array`, `Map`, `Nullable`, nested `Tuple`, etc. via
    /// `IDataType::forEachChild`), so text framings can reject or base64-encode accordingly.
    /// Pass `validate_utf8 = false` when the format does not install the validating buffer at all
    /// (for example `CustomSeparated` with the `JSON` escaping rule).
    bool tupleElementNamesMayProduceRawBytesInJSON(const Block & header, const FormatSettings & settings, bool validate_utf8);

    /// Returns true if the `Bool` representations (`bool_true_representation` /
    /// `bool_false_representation`) may leak bytes that are not valid UTF-8 into the output of the
    /// `*Strings*` JSON variants (`JSONStrings`, `JSONStringsEachRow`, `JSONCompactStrings*`). Those
    /// variants serialize every value through the plain `serializeText` kind and embed the result
    /// with `writeJSONString`, which escapes control characters but passes non-UTF-8 bytes through
    /// verbatim - and `SerializationBool` writes the representations verbatim in that kind. Like the
    /// `Tuple` element names (see above), the only sanitization is the whole-output
    /// `WriteBufferValidUTF8`, which is installed only when `validate_utf8` is on and at least one
    /// column's value type may itself emit invalid UTF-8 - the `Bool` value type is "clean", so a
    /// header of clean value types skips the buffer even with validation on. The representations
    /// come from the settings, so text framings can reject the output accordingly.
    bool boolRepresentationsMayProduceRawBytesInJSONStrings(const Block & header, const FormatSettings & settings, bool validate_utf8);

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
        std::optional<std::string_view> name = std::nullopt,
        size_t indent = 0,
        std::string_view title_after_delimiter = " ",
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
