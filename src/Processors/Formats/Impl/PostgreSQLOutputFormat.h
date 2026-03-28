#pragma once

#include <Processors/Formats/IRowOutputFormat.h>

#include <Core/PostgreSQLProtocol.h>
#include <Formats/FormatSettings.h>

namespace DB
{

//// https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.4
class PostgreSQLOutputFormat final : public IOutputFormat
{
public:
    PostgreSQLOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & settings_);

    String getName() const override {return "PostgreSQLOutputFormat";}

    void flushImpl() override;

private:
    void writePrefix() override;
    void consume(Chunk) override;

    FormatSettings format_settings;
    PostgreSQLProtocol::Messaging::MessageTransport message_transport;
    Serializations serializations;
};

}
