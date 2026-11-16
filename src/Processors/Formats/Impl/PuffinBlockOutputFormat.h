#pragma once

#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/Impl/PuffinBlobBuilder.h>

namespace DB
{

class PuffinBlockOutputFormat final : public IOutputFormat
{
public:
    PuffinBlockOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings);

    String getName() const override { return "PuffinBlockOutputFormat"; }

private:
    void consume(Chunk chunk) override;
    void finalizeImpl() override;
    void resetFormatterImpl() override;

    PuffinBlobBuilderPtr builder;
};

class FormatFactory;
void registerOutputFormatPuffin(FormatFactory & factory);

}
