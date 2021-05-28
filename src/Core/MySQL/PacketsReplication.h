#pragma once

#include <IO/MySQLPacketPayloadReadBuffer.h>
#include <IO/MySQLPacketPayloadWriteBuffer.h>
#include <Core/MySQL/PacketEndpoint.h>

/// Implementation of MySQL wire protocol.
/// Works only on little-endian architecture.

namespace DB
{

namespace MySQLProtocol
{

namespace Replication
{

/// https://dev.mysql.com/doc/internals/en/com-register-slave.html
class RegisterSlave : public IMySQLWritePacket
{
public:
    UInt32 server_id;
    String slaves_hostname;
    String slaves_users;
    String slaves_password;
    size_t slaves_mysql_port;
    UInt32 replication_rank;
    UInt32 master_id;

protected:
    size_t getPayloadSize() const override;

    void writePayloadImpl(WriteBuffer & buffer) const override;

public:
    RegisterSlave(UInt32 server_id_);
};

/// https://dev.mysql.com/doc/internals/en/com-binlog-dump-gtid.html
class BinlogDumpGTID : public IMySQLWritePacket
{
public:
    UInt16 flags;
    UInt32 server_id;
    String gtid_datas;

protected:
    size_t getPayloadSize() const override;

    void writePayloadImpl(WriteBuffer & buffer) const override;

public:
    BinlogDumpGTID(UInt32 server_id_, String gtid_datas_);
};

}
}

}
