#pragma once

#include <Processors/Formats/IRowOutputFormat.h>
#include <Core/Block.h>

#include <Core/MySQLProtocol.h>
#include <Formats/FormatSettings.h>

namespace DB
{

class IColumn;
class IDataType;
class WriteBuffer;
class Context;

/** A stream for outputting data in a binary line-by-line format.
  */
class MySQLOutputFormat final : public IOutputFormat
{
public:
    MySQLOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & settings_);

    String getName() const override { return "MySQLOutputFormat"; }

    void setContext(const Context & context_)
    {
        context = &context_;
        packet_sender = std::make_unique<MySQLProtocol::PacketSender>(out, const_cast<uint8_t &>(context_.mysql.sequence_id)); /// TODO: fix it
        packet_sender->max_packet_size = context_.mysql.max_packet_size;
    }

    void consume(Chunk) override;
    void finalize() override;
    void flush() override;
    void doWritePrefix() override { initialize(); }

    void initialize();

private:

    bool initialized = false;

    const Context * context = nullptr;
    std::unique_ptr<MySQLProtocol::PacketSender> packet_sender;
    FormatSettings format_settings;
    DataTypes data_types;
};

}
