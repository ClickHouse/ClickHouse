#include <Core/MySQL/PacketsReplication.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Core/MySQL/PacketsGeneric.h>

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
    buffer.write(Generic::COM_REGISTER_SLAVE);
    buffer.write(reinterpret_cast<const char *>(&server_id), 4);
    writeLengthEncodedString(slaves_hostname, buffer);
    writeLengthEncodedString(slaves_users, buffer);
    writeLengthEncodedString(slaves_password, buffer);
    buffer.write(reinterpret_cast<const char *>(&slaves_mysql_port), 2);
    buffer.write(reinterpret_cast<const char *>(&replication_rank), 4);
    buffer.write(reinterpret_cast<const char *>(&master_id), 4);
}

BinlogDumpGTID::BinlogDumpGTID(UInt32 server_id_, String gtid_datas_)
    : flags(0x04), server_id(server_id_), gtid_datas(std::move(gtid_datas_))
{
}

size_t BinlogDumpGTID::getPayloadSize() const { return 1 + 2 + 4 + 4 + 0 + 8 + 4 + gtid_datas.size(); }

void BinlogDumpGTID::writePayloadImpl(WriteBuffer & buffer) const
{
    buffer.write(Generic::COM_BINLOG_DUMP_GTID);
    buffer.write(reinterpret_cast<const char *>(&flags), 2);
    buffer.write(reinterpret_cast<const char *>(&server_id), 4);

    // Binlog file.
    UInt32 file_size = 0;
    buffer.write(reinterpret_cast<const char *>(&file_size), 4);
    buffer.write("", 0);

    const UInt64 position = 4;
    buffer.write(reinterpret_cast<const char *>(&position), 8);

    UInt32 gtid_size = gtid_datas.size();
    buffer.write(reinterpret_cast<const char *>(&gtid_size), 4);
    buffer.write(gtid_datas.data(), gtid_datas.size());
}

}

}
}
