#pragma once

#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <Formats/FormatSettings.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/PeekableReadBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/Progress.h>
#include <Core/NamesAndTypes.h>
#include <Common/assert_cast.h>
#include <Common/Stopwatch.h>
#include <utility>

namespace DB
{

class Block;
struct JSONInferenceInfo;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

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
    ReturnType deserializeEmpyStringAsDefaultOrNested(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, auto && deserializer)
    {
        static constexpr auto throw_exception = std::is_same_v<ReturnType, void>;

        static constexpr auto EMPTY_STRING = "\"\"";
        static constexpr auto EMPTY_STRING_LENGTH = std::string_view(EMPTY_STRING).length();

        auto do_deserialize_nested = [](IColumn & nested_column, ReadBuffer & buf, auto && check_for_empty_string, auto && deserialize, const SerializationPtr & nested_column_serialization) -> ReturnType
        {
            if (check_for_empty_string(buf))
            {
                nested_column.insertDefault();
                return ReturnType(true);
            }
            return deserialize(nested_column, buf, nested_column_serialization);
        };

        auto deserialize_nested_impl = [&settings](IColumn & nested_column, ReadBuffer & buf, const SerializationPtr & nested_column_serialization) -> ReturnType
        {
            if constexpr (throw_exception)
            {
                if (settings.null_as_default && !isColumnNullableOrLowCardinalityNullable(nested_column))
                    SerializationNullable::deserializeNullAsDefaultOrNestedTextJSON(nested_column, buf, settings, nested_column_serialization);
                else
                    nested_column_serialization->deserializeTextJSON(nested_column, buf, settings);
            }
            else
            {
                if (settings.null_as_default && !isColumnNullableOrLowCardinalityNullable(nested_column))
                    return SerializationNullable::tryDeserializeNullAsDefaultOrNestedTextJSON(nested_column, buf, settings, nested_column_serialization);
                return nested_column_serialization->tryDeserializeTextJSON(nested_column, buf, settings);
            }
        };

        auto deserialize_nested = [&do_deserialize_nested, &deserialize_nested_impl](IColumn & nested_column, ReadBuffer & buf, const SerializationPtr & nested_column_serialization) -> ReturnType
        {
            if (buf.eof() || *buf.position() != EMPTY_STRING[0])
                return deserialize_nested_impl(nested_column, buf, nested_column_serialization);

            if (buf.available() >= EMPTY_STRING_LENGTH)
            {
                /// We have enough data in buffer to check if we have an empty string.
                auto check_for_empty_string = [](ReadBuffer & buf_) -> bool
                {
                    auto * pos = buf_.position();
                    if (checkString(EMPTY_STRING, buf_))
                        return true;
                    buf_.position() = pos;
                    return false;
                };

                return do_deserialize_nested(nested_column, buf, check_for_empty_string, deserialize_nested_impl, nested_column_serialization);
            }

            /// We don't have enough data in buffer to check if we have an empty string.
            /// Use PeekableReadBuffer to make a checkpoint before checking for an
            /// empty string and rollback if check was failed.

            auto check_for_empty_string = [](ReadBuffer & buf_) -> bool
            {
                auto & peekable_buf = assert_cast<PeekableReadBuffer &>(buf_);
                peekable_buf.setCheckpoint();
                SCOPE_EXIT(peekable_buf.dropCheckpoint());
                if (checkString(EMPTY_STRING, peekable_buf))
                    return true;
                peekable_buf.rollbackToCheckpoint();
                return false;
            };

            auto deserialize_nested_impl_with_check = [&deserialize_nested_impl](IColumn & nested_column_, ReadBuffer & buf_, const SerializationPtr & nested_column_serialization_) -> ReturnType
            {
                auto & peekable_buf = assert_cast<PeekableReadBuffer &>(buf_);

                auto enforceNoUnreadData = [&peekable_buf]() -> void
                {
                    if (unlikely(peekable_buf.hasUnreadData()))
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Incorrect state while parsing JSON: PeekableReadBuffer has unread data in own memory: {}", String(peekable_buf.position(), peekable_buf.available()));
                };

                if constexpr (throw_exception)
                {
                    deserialize_nested_impl(nested_column_, peekable_buf, nested_column_serialization_);
                    enforceNoUnreadData();
                }
                else
                {
                    bool res = deserialize_nested_impl(nested_column_, peekable_buf, nested_column_serialization_);
                    enforceNoUnreadData();
                    return res;
                }
            };

            PeekableReadBuffer peekable_buf(buf, true);
            return do_deserialize_nested(nested_column, peekable_buf, check_for_empty_string, deserialize_nested_impl_with_check, nested_column_serialization);
        };

        return deserializer(column, istr, deserialize_nested);
    }
}

}
