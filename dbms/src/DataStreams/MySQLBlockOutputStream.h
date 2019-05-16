#pragma once

#include "IBlockOutputStream.h"
#include <Core/MySQLProtocol.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <Interpreters/Context.h>

namespace DB
{

/** Interface for writing rows in MySQL Client/Server Protocol format.
  */
class MySQLBlockOutputStream : public IBlockOutputStream
{
public:
    MySQLBlockOutputStream(WriteBuffer & buf, const Block & header, const uint32_t capabilities, size_t & sequence_id);

    Block getHeader() const { return header; }

    void write(const Block & block);

    void writePrefix();
    void writeSuffix();

    void flush();
private:
    Block header;
    uint32_t capabilities;
    std::shared_ptr<MySQLProtocol::PacketSender> packet_sender;
    FormatSettings format_settings;
};

using MySQLBlockOutputStreamPtr = std::shared_ptr<MySQLBlockOutputStream>;

}
