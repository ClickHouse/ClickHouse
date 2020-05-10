#pragma once
#include <Core/Field.h>
#include <Core/MySQLProtocol.h>
#include <Core/Types.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

#include <map>

/// Implementation of MySQL replication protocol.
/// Works only on little-endian architecture.

namespace DB
{
namespace MySQLReplication
{
    static const int EVENT_VERSION_V4 = 4;
    static const int EVENT_HEADER_LENGTH = 19;
    static const int CHECKSUM_CRC32_SIGNATURE_LENGTH = 4;
    static const int QUERY_EVENT_BEGIN_LENGTH = 74;
    static const int ROWS_HEADER_LEN_V2 = 10;

    class EventBase;
    using BinlogEventPtr = std::shared_ptr<EventBase>;

    inline bool check_string_bit(String s, int k) { return (s[(k / 8)] & (1 << (k % 8))) != 0; }

    enum BinlogChecksumAlg
    {
        BINLOG_CHECKSUM_ALG_OFF = 0,
        BINLOG_CHECKSUM_ALG_CRC32 = 1,
        BINLOG_CHECKSUM_ALG_ENUM_END,
        BINLOG_CHECKSUM_ALG_UNDEF = 255
    };
    String ToString(BinlogChecksumAlg type);

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
    String ToString(EventType type);

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

        void print() const;
        void parse(ReadBuffer & payload);
    };

    class EventBase
    {
    public:
        EventHeader header;

        virtual ~EventBase() = default;
        virtual void print() const = 0;
        virtual void parseHeader(ReadBuffer & payload) { header.parse(payload); }

        virtual void parseEvent(ReadBuffer & payload) { parseImpl(payload); }

        EventType type() const { return header.type; }

    protected:
        virtual void parseImpl(ReadBuffer & payload) = 0;
    };

    class FormatDescriptionEvent : public EventBase
    {
    public:
        UInt16 binlog_version;
        String server_version;
        UInt32 create_timestamp;
        UInt8 event_header_length;
        String event_type_header_length;

        void print() const override;

    protected:
        void parseImpl(ReadBuffer & payload) override;

    private:
        std::vector<UInt8> post_header_lens;
    };

    class RotateEvent : public EventBase
    {
    public:
        UInt64 position;
        String next_binlog;

        void print() const override;

    protected:
        void parseImpl(ReadBuffer & payload) override;
    };

    class QueryEvent : public EventBase
    {
    public:
        UInt32 thread_id;
        UInt32 exec_time;
        UInt8 schema_len;
        UInt16 error_code;
        UInt16 status_len;
        String status;
        String schema;
        String query;

        void print() const override;

    protected:
        void parseImpl(ReadBuffer & payload) override;
    };

    class XIDEvent : public EventBase
    {
    public:
        UInt64 xid;

        void print() const override;

    protected:
        void parseImpl(ReadBuffer & payload) override;
    };

    class TableMapEvent : public EventBase
    {
    public:
        UInt64 table_id;
        UInt16 flags;
        UInt8 schema_len;
        String schema;
        UInt8 table_len;
        String table;
        UInt32 column_count;
        std::vector<UInt8> column_type;
        std::vector<UInt16> column_meta;
        String null_bitmap;

        void print() const override;

    protected:
        void parseImpl(ReadBuffer & payload) override;
        void parseMeta(String meta);
    };

    class RowsEvent : public EventBase
    {
    public:
        UInt64 table_id;
        UInt16 flags;
        UInt16 extra_data_len;
        UInt32 number_columns;
        String schema;
        String table;
        String columns_present_bitmap1;
        String columns_present_bitmap2;
        std::vector<Field> rows;

        RowsEvent(std::shared_ptr<TableMapEvent> table_map_) : table_map(table_map_)
        {
            schema = table_map->schema;
            table = table_map->table;
        }
        void print() const override;

    protected:
        void parseImpl(ReadBuffer & payload) override;
        void parseRow(ReadBuffer & payload, String bitmap);

    private:
        std::shared_ptr<TableMapEvent> table_map;
    };

    class WriteRowsEvent : public RowsEvent
    {
    public:
        WriteRowsEvent(std::shared_ptr<TableMapEvent> table_map_) : RowsEvent(table_map_) { }
    };

    class DeleteRowsEvent : public RowsEvent
    {
    public:
        DeleteRowsEvent(std::shared_ptr<TableMapEvent> table_map_) : RowsEvent(table_map_) { }
    };

    class UpdateRowsEvent : public RowsEvent
    {
    public:
        UpdateRowsEvent(std::shared_ptr<TableMapEvent> table_map_) : RowsEvent(table_map_) { }
    };

    class DryRunEvent : public EventBase
    {
        void print() const override;

    protected:
        void parseImpl(ReadBuffer & payload) override;
    };

    class Position
    {
    public:
        UInt64 binlog_pos;
        String binlog_name;

        Position() : binlog_pos(0), binlog_name("") { }
        void updateLogPos(UInt64 pos) { binlog_pos = pos; }
        void updateLogName(String binlog) { binlog_name = std::move(binlog); }
    };

    class IFlavor : public MySQLProtocol::ReadPacket
    {
    public:
        virtual String getName() const = 0;
        virtual Position getPosition() const = 0;
        virtual BinlogEventPtr readOneEvent() = 0;
        virtual ~IFlavor() = default;
    };

    class MySQLFlavor : public IFlavor
    {
    public:
        BinlogEventPtr event;

        String getName() const override { return "MySQL"; }
        Position getPosition() const override { return position; }
        void readPayloadImpl(ReadBuffer & payload) override;
        BinlogEventPtr readOneEvent() override { return event; }

    private:
        Position position;
        std::shared_ptr<TableMapEvent> table_map;
    };
}

}
