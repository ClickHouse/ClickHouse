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
class MySQLOutputFormat: public IOutputFormat
{
public:
    MySQLOutputFormat(WriteBuffer & out_, const Block & header, const Context & context, const FormatSettings & settings);

    String getName() const override { return "MySQLOutputFormat"; }

    void consume(Chunk) override;
    void finalize() override;

private:

    bool initialized = false;

    const Context & context;
    std::shared_ptr<MySQLProtocol::PacketSender> packet_sender;
    FormatSettings format_settings;
};

}

