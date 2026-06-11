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
    /// Reads through a PeekableReadBuffer with metadata/array framing; the JSONEachRow
    /// per-row segmentation cap is not applied (matches the parallel-parsing coverage,
    /// which is not enabled for the JSON-with-metadata format either).
    bool applyRowSizeLimit() const override { return false; }

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

    /// Reads through a PeekableReadBuffer with metadata/array framing; the per-row segmentation
    /// cap is not applied (matches the input-format side, which also excludes this format).
    bool applyRowSizeLimit() const override { return false; }

    std::unique_ptr<PeekableReadBuffer> peekable_buf;
    bool fallback_to_json_each_row;
};

}
