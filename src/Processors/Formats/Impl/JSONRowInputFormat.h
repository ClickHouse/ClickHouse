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
    /// Cap only on the no-metadata fallback, where readPrefix delegates to
    /// JSONEachRowRowInputFormat and rows go through the base readRow path. The
    /// metadata/array-framed path is not a single self-delimited object per row.
    bool applyRowSizeLimit() const override { return parse_as_json_each_row; }

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

private:
    JSONRowSchemaReader(std::unique_ptr<PeekableReadBuffer> buf, const FormatSettings & format_settings_, bool fallback_to_json_each_row_);

    /// Cap only when inference can fall back to JSONEachRowSchemaReader (no metadata);
    /// the direct metadata path does not read rows through the capped base reader.
    bool applyRowSizeLimit() const override { return fallback_to_json_each_row; }

    std::unique_ptr<PeekableReadBuffer> peekable_buf;
    bool fallback_to_json_each_row;
};

}
