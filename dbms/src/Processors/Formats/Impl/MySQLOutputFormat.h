#pragma once

#include "config_core.h"

#if USE_SSL

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
    MySQLOutputFormat(WriteBuffer & out_, const Block & header_, const Context & context_, const FormatSettings & settings_);

    String getName() const override { return "MySQLOutputFormat"; }

    void consume(Chunk) override;
    void finalize() override;
    void flush() override;
    void doWritePrefix() override { initialize(); }

    void initialize();

private:

    bool initialized = false;

    const Context & context;
    MySQLProtocol::PacketSender packet_sender;
    FormatSettings format_settings;
};

}

#endif
