#include <Core/MySQL/PacketsReplication.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Core/MySQL/PacketsProtocolText.h>

namespace DB
{

namespace MySQLProtocol
{

namespace Replication
{

RegisterSlave::RegisterSlave(UInt32 server_id_)
    : server_id(server_id_), slaves_mysql_port(0x00), replication_rank(0x00), master_id(0x00)
{
}

size_t RegisterSlave::getPayloadSize() const
{
    return 1 + 4 + getLengthEncodedStringSize(slaves_hostname) + getLengthEncodedStringSize(slaves_users)
           + getLengthEncodedStringSize(slaves_password) + 2 + 4 + 4;
}

void RegisterSlave::writePayloadImpl(WriteBuffer & buffer) const
{
    buffer.write(ProtocolText::COM_REGISTER_SLAVE);
    buffer.write(reinterpret_cast<const char *>(&server_id), 4);
    writeLengthEncodedString(slaves_hostname, buffer);
    writeLengthEncodedString(slaves_users, buffer);
    writeLengthEncodedString(slaves_password, buffer);
    buffer.write(reinterpret_cast<const char *>(&slaves_mysql_port), 2);
    buffer.write(reinterpret_cast<const char *>(&replication_rank), 4);
    buffer.write(reinterpret_cast<const char *>(&master_id), 4);
}

BinlogDump::BinlogDump(UInt32 binlog_pos_, String binlog_file_name_, UInt32 server_id_)
    : binlog_pos(binlog_pos_), flags(0x00), server_id(server_id_), binlog_file_name(std::move(binlog_file_name_))
{
}

size_t BinlogDump::getPayloadSize() const
{
    return 1 + 4 + 2 + 4 + binlog_file_name.size() + 1;
}

void BinlogDump::writePayloadImpl(WriteBuffer & buffer) const
{
    buffer.write(ProtocolText::COM_BINLOG_DUMP);
    buffer.write(reinterpret_cast<const char *>(&binlog_pos), 4);
    buffer.write(reinterpret_cast<const char *>(&flags), 2);
    buffer.write(reinterpret_cast<const char *>(&server_id), 4);
    buffer.write(binlog_file_name.data(), binlog_file_name.length());
    buffer.write(0x00);
}

}

}
}
