#pragma once
#include <Core/Field.h>
#include <Core/MySQL/PacketsReplication.h>
#include <Core/MySQL/MySQLGtid.h>
#include <common/types.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

#include <map>
#include <boost/dynamic_bitset.hpp>

/// Implementation of MySQL replication protocol.
/// Works only on little-endian architecture.

namespace DB
{

namespace MySQLReplication
{
    static const int EVENT_VERSION_V4 = 4;
    static const int EVENT_HEADER_LENGTH = 19;

    using Bitmap = boost::dynamic_bitset<>;

    inline UInt64 readBits(UInt64 val, UInt8 start, UInt8 size, UInt8 length)
    {
        UInt64 mask = 1;
        val = val >> (length - (start + size));
        return val & ((mask << size) - 1);
    }

    inline void readBigEndianStrict(ReadBuffer & payload, char * to, size_t n)
    {
        payload.readStrict(to, n);
        char *start = to, *end = to + n;
        std::reverse(start, end);
    }

    inline void readTimeFractionalPart(ReadBuffer & payload, UInt32 & factional, UInt16 meta)
    {
        switch (meta)
        {
            case 1:
            {
                readBigEndianStrict(payload, reinterpret_cast<char *>(&factional), 1);
                factional /= 10;
                break;
            }
            case 2:
            {
                readBigEndianStrict(payload, reinterpret_cast<char *>(&factional), 1);
                break;
            }
            case 3:
            {
                readBigEndianStrict(payload, reinterpret_cast<char *>(&factional), 2);
                factional /= 10;
                break;
            }
            case 4:
            {
                readBigEndianStrict(payload, reinterpret_cast<char *>(&factional), 2);
                break;
            }
            case 5:
            {
                readBigEndianStrict(payload, reinterpret_cast<char *>(&factional), 3);
                factional /= 10;
                break;
            }
            case 6:
            {
                readBigEndianStrict(payload, reinterpret_cast<char *>(&factional), 3);
                break;
            }
            default:
                break;
        }
    }

    inline void readBitmap(ReadBuffer & payload, Bitmap & bitmap, size_t bitmap_size)
    {
        String byte_buffer;
        byte_buffer.resize(bitmap_size);
        payload.readStrict(reinterpret_cast<char *>(byte_buffer.data()), bitmap_size);
        bitmap.resize(bitmap_size * 8, false);
        for (size_t i = 0; i < bitmap_size; ++i)
        {
            uint8_t tmp = byte_buffer[i];
            boost::dynamic_bitset<>::size_type bit = i * 8;
            if (tmp == 0)
                continue;
            if ((tmp & 0x01) != 0)
                bitmap.set(bit);
            if ((tmp & 0x02) != 0)
                bitmap.set(bit + 1);
            if ((tmp & 0x04) != 0)
                bitmap.set(bit + 2);
            if ((tmp & 0x08) != 0)
                bitmap.set(bit + 3);
            if ((tmp & 0x10) != 0)
                bitmap.set(bit + 4);
            if ((tmp & 0x20) != 0)
                bitmap.set(bit + 5);
            if ((tmp & 0x40) != 0)
                bitmap.set(bit + 6);
            if ((tmp & 0x80) != 0)
                bitmap.set(bit + 7);
        }
    }

    class EventBase;
    using BinlogEventPtr = std::shared_ptr<EventBase>;

    enum BinlogChecksumAlg
    {
        BINLOG_CHECKSUM_ALG_OFF = 0,
        BINLOG_CHECKSUM_ALG_CRC32 = 1,
        BINLOG_CHECKSUM_ALG_ENUM_END,
        BINLOG_CHECKSUM_ALG_UNDEF = 255
    };

    inline String to_string(BinlogChecksumAlg type)
    {
        switch (type)
        {
            case BINLOG_CHECKSUM_ALG_OFF:
                return "BINLOG_CHECKSUM_ALG_OFF";
            case BINLOG_CHECKSUM_ALG_CRC32:
                return "BINLOG_CHECKSUM_ALG_CRC32";
            case BINLOG_CHECKSUM_ALG_ENUM_END:
                return "BINLOG_CHECKSUM_ALG_ENUM_END";
            case BINLOG_CHECKSUM_ALG_UNDEF:
                return "BINLOG_CHECKSUM_ALG_UNDEF";
        }
        return std::string("Unknown checksum alg: ") + std::to_string(static_cast<int>(type));
    }

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

    inline String to_string(EventType type)
    {
        switch (type)
        {
            case START_EVENT_V3:
                return "StartEventV3";
            case QUERY_EVENT:
                return "QueryEvent";
            case STOP_EVENT:
                return "StopEvent";
            case ROTATE_EVENT:
                return "RotateEvent";
            case INT_VAR_EVENT:
                return "IntVarEvent";
            case LOAD_EVENT:
                return "LoadEvent";
            case SLAVE_EVENT:
                return "SlaveEvent";
            case CREATE_FILE_EVENT:
                return "CreateFileEvent";
            case APPEND_BLOCK_EVENT:
                return "AppendBlockEvent";
            case EXEC_LOAD_EVENT:
                return "ExecLoadEvent";
            case DELETE_FILE_EVENT:
                return "DeleteFileEvent";
            case NEW_LOAD_EVENT:
                return "NewLoadEvent";
            case RAND_EVENT:
                return "RandEvent";
            case USER_VAR_EVENT:
                return "UserVarEvent";
            case FORMAT_DESCRIPTION_EVENT:
                return "FormatDescriptionEvent";
            case XID_EVENT:
                return "XIDEvent";
            case BEGIN_LOAD_QUERY_EVENT:
                return "BeginLoadQueryEvent";
            case EXECUTE_LOAD_QUERY_EVENT:
                return "ExecuteLoadQueryEvent";
            case TABLE_MAP_EVENT:
                return "TableMapEvent";
            case WRITE_ROWS_EVENT_V0:
                return "WriteRowsEventV0";
            case UPDATE_ROWS_EVENT_V0:
                return "UpdateRowsEventV0";
            case DELETE_ROWS_EVENT_V0:
                return "DeleteRowsEventV0";
            case WRITE_ROWS_EVENT_V1:
                return "WriteRowsEventV1";
            case UPDATE_ROWS_EVENT_V1:
                return "UpdateRowsEventV1";
            case DELETE_ROWS_EVENT_V1:
                return "DeleteRowsEventV1";
            case INCIDENT_EVENT:
                return "IncidentEvent";
            case HEARTBEAT_EVENT:
                return "HeartbeatEvent";
            case IGNORABLE_EVENT:
                return "IgnorableEvent";
            case ROWS_QUERY_EVENT:
                return "RowsQueryEvent";
            case WRITE_ROWS_EVENT_V2:
                return "WriteRowsEventV2";
            case UPDATE_ROWS_EVENT_V2:
                return "UpdateRowsEventV2";
            case DELETE_ROWS_EVENT_V2:
                return "DeleteRowsEventV2";
            case GTID_EVENT:
                return "GTIDEvent";
            case ANONYMOUS_GTID_EVENT:
                return "AnonymousGTIDEvent";
            case PREVIOUS_GTIDS_EVENT:
                return "PreviousGTIDsEvent";
            case TRANSACTION_CONTEXT_EVENT:
                return "TransactionContextEvent";
            case VIEW_CHANGE_EVENT:
                return "ViewChangeEvent";
            case XA_PREPARE_LOG_EVENT:
                return "XAPrepareLogEvent";
            case MARIA_ANNOTATE_ROWS_EVENT:
                return "MariaAnnotateRowsEvent";
            case MARIA_BINLOG_CHECKPOINT_EVENT:
                return "MariaBinlogCheckpointEvent";
            case MARIA_GTID_EVENT:
                return "MariaGTIDEvent";
            case MARIA_GTID_LIST_EVENT:
                return "MariaGTIDListEvent";
            case MARIA_START_ENCRYPTION_EVENT:
                return "MariaStartEncryptionEvent";
            default:
                break;
        }
        return std::string("Unknown event: ") + std::to_string(static_cast<int>(type));
    }

    enum MySQLEventType
    {
        MYSQL_UNHANDLED_EVENT = 0,
        MYSQL_QUERY_EVENT = 1,
        MYSQL_WRITE_ROWS_EVENT = 2,
        MYSQL_UPDATE_ROWS_EVENT = 3,
        MYSQL_DELETE_ROWS_EVENT = 4,
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

        EventHeader() : timestamp(0), server_id(0), event_size(0), log_pos(0), flags(0) { }
        void dump(WriteBuffer & out) const;
        void parse(ReadBuffer & payload);
    };

    class EventBase
    {
    public:
        EventHeader header;

        EventBase(EventHeader && header_) : header(std::move(header_)) {}

        virtual ~EventBase() = default;
        virtual void dump(WriteBuffer & out) const = 0;
        virtual void parseEvent(ReadBuffer & payload) { parseImpl(payload); }
        virtual MySQLEventType type() const { return MYSQL_UNHANDLED_EVENT; }

    protected:
        virtual void parseImpl(ReadBuffer & payload) = 0;
    };

    class FormatDescriptionEvent : public EventBase
    {
    public:
        FormatDescriptionEvent(EventHeader && header_)
            : EventBase(std::move(header_)), binlog_version(0), create_timestamp(0), event_header_length(0)
        {
        }

    protected:
        UInt16 binlog_version;
        String server_version;
        UInt32 create_timestamp;
        UInt8 event_header_length;
        String event_type_header_length;

        void dump(WriteBuffer & out) const override;
        void parseImpl(ReadBuffer & payload) override;

    private:
        std::vector<UInt8> post_header_lens;
    };

    class RotateEvent : public EventBase
    {
    public:
        UInt64 position;
        String next_binlog;

        RotateEvent(EventHeader && header_) : EventBase(std::move(header_)), position(0) {}
        void dump(WriteBuffer & out) const override;

    protected:
        void parseImpl(ReadBuffer & payload) override;
    };

    enum QueryType
    {
        QUERY_EVENT_DDL = 0,
        QUERY_EVENT_MULTI_TXN_FLAG = 1,
        QUERY_EVENT_XA = 2
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
        QueryType typ = QUERY_EVENT_DDL;

        QueryEvent(EventHeader && header_)
            : EventBase(std::move(header_)), thread_id(0), exec_time(0), schema_len(0), error_code(0), status_len(0)
        {
        }

        void dump(WriteBuffer & out) const override;
        MySQLEventType type() const override { return MYSQL_QUERY_EVENT; }

    protected:
        void parseImpl(ReadBuffer & payload) override;
    };

    class XIDEvent : public EventBase
    {
    public:
        XIDEvent(EventHeader && header_) : EventBase(std::move(header_)), xid(0) {}

    protected:
        UInt64 xid;

        void dump(WriteBuffer & out) const override;
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
        Bitmap null_bitmap;

        TableMapEvent(EventHeader && header_) : EventBase(std::move(header_)), table_id(0), flags(0), schema_len(0), table_len(0), column_count(0) {}
        void dump(WriteBuffer & out) const override;

    protected:
        void parseImpl(ReadBuffer & payload) override;
        void parseMeta(String meta);
    };

    class RowsEvent : public EventBase
    {
    public:
        UInt32 number_columns;
        String schema;
        String table;
        std::vector<Field> rows;

        RowsEvent(std::shared_ptr<TableMapEvent> table_map_, EventHeader && header_)
            : EventBase(std::move(header_)), number_columns(0), table_id(0), flags(0), extra_data_len(0), table_map(table_map_)
        {
            schema = table_map->schema;
            table = table_map->table;
        }

        void dump(WriteBuffer & out) const override;

    protected:
        UInt64 table_id;
        UInt16 flags;
        UInt16 extra_data_len;
        Bitmap columns_present_bitmap1;
        Bitmap columns_present_bitmap2;

        void parseImpl(ReadBuffer & payload) override;
        void parseRow(ReadBuffer & payload, Bitmap & bitmap);

    private:
        std::shared_ptr<TableMapEvent> table_map;
    };

    class WriteRowsEvent : public RowsEvent
    {
    public:
        WriteRowsEvent(std::shared_ptr<TableMapEvent> table_map_, EventHeader && header_) : RowsEvent(table_map_, std::move(header_)) {}
        MySQLEventType type() const override { return MYSQL_WRITE_ROWS_EVENT; }
    };

    class DeleteRowsEvent : public RowsEvent
    {
    public:
        DeleteRowsEvent(std::shared_ptr<TableMapEvent> table_map_, EventHeader && header_) : RowsEvent(table_map_, std::move(header_)) {}
        MySQLEventType type() const override { return MYSQL_DELETE_ROWS_EVENT; }
    };

    class UpdateRowsEvent : public RowsEvent
    {
    public:
        UpdateRowsEvent(std::shared_ptr<TableMapEvent> table_map_, EventHeader && header_) : RowsEvent(table_map_, std::move(header_)) {}
        MySQLEventType type() const override { return MYSQL_UPDATE_ROWS_EVENT; }
    };

    class GTIDEvent : public EventBase
    {
    public:
        UInt8 commit_flag;
        GTID gtid;

        GTIDEvent(EventHeader && header_) : EventBase(std::move(header_)), commit_flag(0) {}
        void dump(WriteBuffer & out) const override;

    protected:
        void parseImpl(ReadBuffer & payload) override;
    };

    class DryRunEvent : public EventBase
    {
    public:
        DryRunEvent(EventHeader && header_) : EventBase(std::move(header_)) {}
        void dump(WriteBuffer & out) const override;

    protected:
        void parseImpl(ReadBuffer & payload) override;
    };

    class Position
    {
    public:
        UInt64 binlog_pos;
        String binlog_name;
        GTIDSets gtid_sets;

        Position() : binlog_pos(0) { }
        void update(BinlogEventPtr event);
        void update(UInt64 binlog_pos_, const String & binlog_name_, const String & gtid_sets_);
        void dump(WriteBuffer & out) const;
    };

    class IFlavor : public MySQLProtocol::IMySQLReadPacket
    {
    public:
        virtual String getName() const = 0;
        virtual Position getPosition() const = 0;
        virtual BinlogEventPtr readOneEvent() = 0;
        virtual void setReplicateDatabase(String db) = 0;
        virtual void setGTIDSets(GTIDSets sets) = 0;
        virtual void setChecksumSignatureLength(size_t checksum_signature_length_) = 0;

        virtual ~IFlavor() override = default;
    };

    class MySQLFlavor : public IFlavor
    {
    public:
        void readPayloadImpl(ReadBuffer & payload) override;
        String getName() const override { return "MySQL"; }
        Position getPosition() const override { return position; }
        BinlogEventPtr readOneEvent() override { return event; }
        void setReplicateDatabase(String db) override { replicate_do_db = std::move(db); }
        void setGTIDSets(GTIDSets sets) override { position.gtid_sets = std::move(sets); }
        void setChecksumSignatureLength(size_t checksum_signature_length_) override { checksum_signature_length = checksum_signature_length_; }

    private:
        Position position;
        BinlogEventPtr event;
        String replicate_do_db;
        std::shared_ptr<TableMapEvent> table_map;
        size_t checksum_signature_length = 4;

        inline bool doReplicate() { return (replicate_do_db.empty() || table_map->schema == replicate_do_db); }
    };
}

}
