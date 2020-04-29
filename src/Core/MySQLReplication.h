#pragma once
#include <Core/MySQLProtocol.h>
#include <Core/Types.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

/// Implementation of MySQL replication protocol.
/// Works only on little-endian architecture.

namespace DB
{
namespace MySQLReplication
{
    class IBinlogEvent;
    using BinlogEventPtr = std::shared_ptr<IBinlogEvent>;

    /// http://dev.mysql.com/doc/internals/en/binlog-event-type.html
    enum EventType
    {
        UNKNOW_EVENT = 0,
        START_EVENT_V3 = 1,
        QUERY_EVENT = 2,
        STOP_EVENT = 3,
        ROTATE_EVENT = 4,
        INT_VAR_EVENT = 5,
        LOAD_EVENT = 6,
        SLAVE_EVENT = 7,
        CREATE_FILE_EVENT = 8,
        APPEND_BLOCK_EVENT = 9,
        EXEC_LOAD_EVENT = 10,
        DELETE_FILE_EVENT = 11,
        NEW_LOAD_EVENT = 12,
        RAND_EVENT = 13,
        USER_VAR_EVENT = 14,
        FORMAT_DESCRIPTION_EVENT = 15,
        XID_EVENT = 16,
        BEGIN_LOAD_QUERY_EVENT = 17,
        EXECUTE_LOAD_QUERY_EVENT = 18,
        TABLE_MAP_EVENT = 19,
        WRITE_ROWS_EVENT_V0 = 20,
        UPDATE_ROWS_EVENT_V0 = 21,
        DELETE_ROWS_EVENT_V0 = 22,
        WRITE_ROWS_EVENT_V1 = 23,
        UPDATE_ROWS_EVENT_V1 = 24,
        DELETE_ROWS_EVENT_V1 = 25,
        INCIDENT_EVENT = 26,
        HEARTBEAT_EVENT = 27,
        IGNORABLE_EVENT = 28,
        ROWS_QUERY_EVENT = 29,
        WRITE_ROWS_EVENT_V2 = 30,
        UPDATE_ROWS_EVENT_V2 = 31,
        DELETE_ROWS_EVENT_V2 = 32,
        GTID_EVENT = 33,
        ANONYMOUS_GTID_EVENT = 34,
        PREVIOUS_GTIDS_EVENT = 35,
        TRANSACTION_CONTEXT_EVENT = 36,
        VIEW_CHANGE_EVENT = 37,
        XA_PREPARE_LOG_EVENT = 38,

        /// MariaDB specific values. They start at 160.
        MARIA_ANNOTATE_ROWS_EVENT = 160,
        MARIA_BINLOG_CHECKPOINT_EVENT = 161,
        MARIA_GTID_EVENT = 162,
        MARIA_GTID_LIST_EVENT = 163,
        MARIA_START_ENCRYPTION_EVENT = 164,
    };

    class ReplicationError : public DB::Exception
    {
    public:
        using Exception::Exception;
    };

    class EventHeader
    {
    public:
        UInt32 timestamp;
        EventType type = UNKNOW_EVENT;
        UInt32 server_id;
        UInt32 event_size;
        UInt32 log_pos;
        UInt16 flags;

        void dump() const;
        void parse(ReadBuffer & payload);
    };

    class IBinlogEvent
    {
    public:
        virtual ~IBinlogEvent() = default;

        virtual void dump() = 0;
        virtual void parse(ReadBuffer & payload) = 0;

        EventType type() { return header.type; }
        void setHeader(EventHeader header_) { header = header_; }

    protected:
        EventHeader header;
    };

    class FormatDescriptionEvent : public IBinlogEvent
    {
    public:
        UInt16 binlog_version;
        String server_version;
        UInt32 create_timestamp;
        UInt8 event_header_length;
        String event_type_header_length;

        void dump() override;
        void parse(ReadBuffer & payload) override;
    };

    class RotateEvent : public IBinlogEvent
    {
    public:
        UInt64 position;
        String next_binlog;

        void dump() override;
        void parse(ReadBuffer & payload) override;
    };

    class IFlavor
    {
    public:
        virtual String getName() = 0;
        virtual BinlogEventPtr binlogEvent() = 0;
        virtual ~IFlavor() = default;
    };

    class MySQLFlavor : public IFlavor, public MySQLProtocol::ReadPacket
    {
    public:
        BinlogEventPtr event;

        String getName() override { return "MySQL"; }
        void readPayloadImpl(ReadBuffer & payload) override;
        BinlogEventPtr binlogEvent() override { return event; }
    };
}

}
