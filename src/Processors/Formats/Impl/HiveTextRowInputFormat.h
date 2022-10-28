#pragma once

#include "config.h"

#if USE_HIVE
#include <IO/PeekableReadBuffer.h>
#include <Processors/Formats/Impl/CSVRowInputFormat.h>


namespace DB
{

/// A stream for input data in Hive Text format.
class HiveTextRowInputFormat final : public CSVRowInputFormat
{
public:
    HiveTextRowInputFormat(const Block & header_, ReadBuffer & in_, const Params & params_, const FormatSettings & format_settings_);

    String getName() const override { return "HiveTextRowInputFormat"; }

    void setReadBuffer(ReadBuffer & in_) override;

private:
    HiveTextRowInputFormat(
        const Block & header_, std::unique_ptr<PeekableReadBuffer> buf_, const Params & params_, const FormatSettings & format_settings_);

    std::unique_ptr<PeekableReadBuffer> buf;
};

class HiveTextFormatReader final : public CSVFormatReader
{
public:
    HiveTextFormatReader(PeekableReadBuffer & buf_, const FormatSettings & format_settings_);

    std::vector<String> readNames() override;
    std::vector<String> readTypes() override;

    void setReadBuffer(ReadBuffer & buf_) override;

private:
    PeekableReadBuffer * buf;
    std::vector<String> input_field_names;
};

}

#endif
