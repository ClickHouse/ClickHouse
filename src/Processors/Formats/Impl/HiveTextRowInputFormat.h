#pragma once

#include <Common/config.h>

#if USE_HIVE
#include <IO/PeekableReadBuffer.h>
#include <Processors/Formats/Impl/CSVRowInputFormat.h>


namespace DB
{

/// A stream for input data in Hive Text format.
/// Parallel parsing is disabled currently.
class HiveTextRowInputFormat final : public CSVRowInputFormat
{
public:
    HiveTextRowInputFormat(const Block & header_, ReadBuffer & in_, const Params & params_, const FormatSettings & format_settings_);

    String getName() const override { return "HiveTextRowInputFormat"; }

    static FormatSettings updateFormatSettings(const FormatSettings & settings);

private:
    HiveTextRowInputFormat(
        const Block & header_, std::unique_ptr<PeekableReadBuffer> buf_, const Params & params_, const FormatSettings & format_settings_);
};

class HiveTextFormatReader final : public CSVFormatReader
{
public:
    HiveTextFormatReader(std::unique_ptr<PeekableReadBuffer> buf_, const FormatSettings & format_settings_);

    std::vector<String> readNames() override;
    std::vector<String> readTypes() override;
    bool readField(
        IColumn & column,
        const DataTypePtr & type,
        const SerializationPtr & serialization,
        bool is_last_file_column,
        const String & column_name) override;

    void skipFieldDelimiter() override;
    void skipRowEndDelimiter() override;

private:
    std::unique_ptr<PeekableReadBuffer> buf;
    std::vector<String> input_field_names;
};

}

#endif
