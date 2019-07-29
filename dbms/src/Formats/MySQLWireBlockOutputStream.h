#pragma once

#include <Core/MySQLProtocol.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <Interpreters/Context.h>

namespace DB
{

/** Interface for writing rows in MySQL Client/Server Protocol format.
  */
class MySQLWireBlockOutputStream : public IBlockOutputStream
{
public:
    MySQLWireBlockOutputStream(WriteBuffer & buf, const Block & header, Context & context);

    Block getHeader() const { return header; }

    void write(const Block & block);

    void writePrefix();
    void writeSuffix();

    void flush();
private:
    Block header;
    Context & context;
    MySQLProtocol::PacketSender packet_sender;
    FormatSettings format_settings;
};

using MySQLWireBlockOutputStreamPtr = std::shared_ptr<MySQLWireBlockOutputStream>;

}
