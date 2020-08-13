#pragma once
#include <ext/scope_guard.h>
#include <random>
#include <sstream>
#include <Common/MemoryTracker.h>
#include <Common/OpenSSLHelpers.h>
#include <Common/PODArray.h>
#include <Core/Types.h>
#include <Interpreters/Context.h>
#include <Access/AccessControlManager.h>
#include <Access/User.h>
#include <IO/copyData.h>
#include <IO/LimitReadBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/RandomStream.h>
#include <Poco/SHA1Engine.h>
#include <Core/MySQL/PacketsGeneric.h>
#include <Core/MySQL/PacketPayloadReadBuffer.h>
#include <Core/MySQL/PacketPayloadWriteBuffer.h>
#include <Core/MySQL/PacketEndpoint.h>

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_SSL
#    include <openssl/pem.h>
#    include <openssl/rsa.h>
#endif

/// Implementation of MySQL wire protocol.
/// Works only on little-endian architecture.

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_WRITE_AFTER_END_OF_BUFFER;
    extern const int UNKNOWN_PACKET_FROM_CLIENT;
    extern const int MYSQL_CLIENT_INSUFFICIENT_CAPABILITIES;
    extern const int OPENSSL_ERROR;
    extern const int UNKNOWN_EXCEPTION;
}

namespace MySQLProtocol
{

//const size_t MAX_PACKET_LENGTH = (1 << 24) - 1; // 16 mb
const size_t SCRAMBLE_LENGTH = 20;
const size_t PACKET_HEADER_SIZE = 4;
const size_t SSL_REQUEST_PAYLOAD_SIZE = 32;

enum Command
{
    COM_SLEEP = 0x0,
    COM_QUIT = 0x1,
    COM_INIT_DB = 0x2,
    COM_QUERY = 0x3,
    COM_FIELD_LIST = 0x4,
    COM_CREATE_DB = 0x5,
    COM_DROP_DB = 0x6,
    COM_REFRESH = 0x7,
    COM_SHUTDOWN = 0x8,
    COM_STATISTICS = 0x9,
    COM_PROCESS_INFO = 0xa,
    COM_CONNECT = 0xb,
    COM_PROCESS_KILL = 0xc,
    COM_DEBUG = 0xd,
    COM_PING = 0xe,
    COM_TIME = 0xf,
    COM_DELAYED_INSERT = 0x10,
    COM_CHANGE_USER = 0x11,
    COM_BINLOG_DUMP = 0x12,
    COM_REGISTER_SLAVE = 0x15,
    COM_RESET_CONNECTION = 0x1f,
    COM_DAEMON = 0x1d
};


class ProtocolError : public DB::Exception
{
public:
    using Exception::Exception;
};


namespace Replication
{
    /// https://dev.mysql.com/doc/internals/en/com-register-slave.html
    class RegisterSlave : public IMySQLWritePacket
    {
    public:
        UInt8 header = COM_REGISTER_SLAVE;
        UInt32 server_id;
        String slaves_hostname;
        String slaves_users;
        String slaves_password;
        size_t slaves_mysql_port;
        UInt32 replication_rank;
        UInt32 master_id;

        RegisterSlave(UInt32 server_id_) : server_id(server_id_), slaves_mysql_port(0x00), replication_rank(0x00), master_id(0x00) { }

        void writePayloadImpl(WriteBuffer & buffer) const override
        {
            buffer.write(header);
            buffer.write(reinterpret_cast<const char *>(&server_id), 4);
            writeLengthEncodedString(slaves_hostname, buffer);
            writeLengthEncodedString(slaves_users, buffer);
            writeLengthEncodedString(slaves_password, buffer);
            buffer.write(reinterpret_cast<const char *>(&slaves_mysql_port), 2);
            buffer.write(reinterpret_cast<const char *>(&replication_rank), 4);
            buffer.write(reinterpret_cast<const char *>(&master_id), 4);
        }

    protected:
        size_t getPayloadSize() const override
        {
            return 1 + 4 + getLengthEncodedStringSize(slaves_hostname) + getLengthEncodedStringSize(slaves_users)
                + getLengthEncodedStringSize(slaves_password) + 2 + 4 + 4;
        }
    };

    /// https://dev.mysql.com/doc/internals/en/com-binlog-dump.html
    class BinlogDump : public IMySQLWritePacket
    {
    public:
        UInt8 header = COM_BINLOG_DUMP;
        UInt32 binlog_pos;
        UInt16 flags;
        UInt32 server_id;
        String binlog_file_name;

        BinlogDump(UInt32 binlog_pos_, String binlog_file_name_, UInt32 server_id_)
            : binlog_pos(binlog_pos_), flags(0x00), server_id(server_id_), binlog_file_name(std::move(binlog_file_name_))
        {
        }

        void writePayloadImpl(WriteBuffer & buffer) const override
        {
            buffer.write(header);
            buffer.write(reinterpret_cast<const char *>(&binlog_pos), 4);
            buffer.write(reinterpret_cast<const char *>(&flags), 2);
            buffer.write(reinterpret_cast<const char *>(&server_id), 4);
            buffer.write(binlog_file_name.data(), binlog_file_name.length());
            buffer.write(0x00);
        }

    protected:
        size_t getPayloadSize() const override { return 1 + 4 + 2 + 4 + binlog_file_name.size() + 1; }
    };
}
}

}
