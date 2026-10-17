#pragma once

#include <Processors/Formats/ISchemaReader.h>
#include <Processors/Formats/Impl/JSONEachRowRowInputFormat.h>


namespace DB
{

class ReadBuffer;


class JSONRowInputFormat final : public JSONEachRowRowInputFormat
{
public:
    JSONRowInputFormat(
        ReadBuffer & in_,
        SharedHeader header_,
        Params params_,
        const FormatSettings & format_settings_);

    String getName() const override { return "JSONRowInputFormat"; }

    void setReadBuffer(ReadBuffer & in_) override;
    void resetReadBuffer() override;

private:
    JSONRowInputFormat(
        std::unique_ptr<PeekableReadBuffer> buf,
        SharedHeader header_,
        Params params_,
        const FormatSettings & format_settings_);

    void readPrefix() override;
    void readSuffix() override;

    const bool validate_types_from_metadata;
    bool parse_as_json_each_row = false;
    std::unique_ptr<PeekableReadBuffer> peekable_buf;
    std::exception_ptr reading_metadata_exception;
};

class JSONRowSchemaReader final : public JSONEachRowSchemaReader
{
public:
    JSONRowSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_, bool fallback_to_json_each_row_);

    NamesAndTypesList readSchema() override;

    bool hasStrictOrderOfColumns() const override { return false; }

    /// When the input carries a `meta` section the types come from it, and the parser validates them
    /// against the destination exactly iff `input_format_json_validate_types_from_metadata` is enabled;
    /// otherwise those declared types are ignored. Without a `meta` section the format falls back to
    /// `JSONEachRow` and the types are inferred (and widened) from the values. Both properties are
    /// therefore known only after `readSchema` has inspected the data.
    bool hasExactTypesFromData() const override
    {
        return read_metadata_from_input && format_settings.json.validate_types_from_metadata;
    }
    bool schemaDescribesParsedData() const override
    {
        return !read_metadata_from_input || format_settings.json.validate_types_from_metadata;
    }

private:
    JSONRowSchemaReader(std::unique_ptr<PeekableReadBuffer> buf, const FormatSettings & format_settings_, bool fallback_to_json_each_row_);

    std::unique_ptr<PeekableReadBuffer> peekable_buf;
    bool fallback_to_json_each_row;
    /// Set by `readSchema` when the input actually contained a `meta` section (rather than falling
    /// back to `JSONEachRow`), so the two properties above can mirror the real parser.
    bool read_metadata_from_input = false;
};

}
