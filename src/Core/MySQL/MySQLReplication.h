#pragma once
#include <Core/Field.h>
#include <Core/MySQL/PacketsReplication.h>
#include <Core/MySQL/MySQLGtid.h>
#include <Core/MySQL/MySQLCharset.h>
#include <base/types.h>
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
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
        char *start = to, *end = to + n;
        std::reverse(start, end);
#endif
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
        BINLOG_CHECKSUM_ALG_ENUM_END = 3,
        BINLOG_CHECKSUM_ALG_UNDEF = 255
    };

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

    enum MySQLEventType
    {
        MYSQL_UNHANDLED_EVENT = 0,
        MYSQL_QUERY_EVENT = 1,
        MYSQL_WRITE_ROWS_EVENT = 2,
        MYSQL_UPDATE_ROWS_EVENT = 3,
        MYSQL_DELETE_ROWS_EVENT = 4,
        MYSQL_UNPARSED_ROWS_EVENT = 100,
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

        explicit EventBase(EventHeader && header_) : header(std::move(header_)) {}

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
        explicit FormatDescriptionEvent(EventHeader && header_)
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

        explicit RotateEvent(EventHeader && header_) : EventBase(std::move(header_)), position(0) {}
        void dump(WriteBuffer & out) const override;

    protected:
        void parseImpl(ReadBuffer & payload) override;
    };

    enum QueryType
    {
        QUERY_EVENT_DDL = 0,
        QUERY_EVENT_MULTI_TXN_FLAG = 1,
        QUERY_EVENT_XA = 2,
        QUERY_SAVEPOINT = 3,
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
        String query_database_name;
        String query_table_name;
        QueryType typ = QUERY_EVENT_DDL;
        bool transaction_complete = true;

        explicit QueryEvent(EventHeader && header_)
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
        explicit XIDEvent(EventHeader && header_) : EventBase(std::move(header_)), xid(0) {}

    protected:
        UInt64 xid;

        void dump(WriteBuffer & out) const override;
        void parseImpl(ReadBuffer & payload) override;
    };

    class TableMapEventHeader
    {
        public:
            UInt64 table_id;
            UInt16 flags;
            UInt8 schema_len;
            String schema;
            UInt8 table_len;
            String table;

        TableMapEventHeader(): table_id(0), flags(0), schema_len(0), table_len(0) {}
        void parse(ReadBuffer & payload);
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
        /// Character set of string columns
        std::vector<UInt32> column_charset;
        /// Character set of string columns,
        /// optimized to minimize space when many
        /// columns have the same charset
        UInt32 default_charset = 255; /// utf8mb4_0900_ai_ci
        std::unordered_map<UInt32, UInt32> default_charset_pairs;
        /// Points to flavor_charset object
        MySQLCharsetPtr charset_ptr;
        Bitmap null_bitmap;

        TableMapEvent(
            EventHeader && header_,
            const TableMapEventHeader & map_event_header,
            const MySQLCharsetPtr & charset_ptr_)
            : EventBase(std::move(header_))
            , column_count(0)
            , charset_ptr(charset_ptr_)
        {
            table_id = map_event_header.table_id;
            flags = map_event_header.flags;
            schema_len = map_event_header.schema_len;
            schema = map_event_header.schema;
            table_len = map_event_header.table_len;
            table = map_event_header.table;
        }
        void dump(WriteBuffer & out) const override;
        UInt32 getColumnCharsetId(UInt32 column_index);
        /// https://mysqlhighavailability.com/more-metadata-is-written-into-binary-log/
        /// https://github.com/mysql/mysql-server/blob/8.0/libbinlogevents/include/rows_event.h#L50
        /// DEFAULT_CHARSET and COLUMN_CHARSET don't appear together, and
        /// ENUM_AND_SET_DEFAULT_CHARSET and ENUM_AND_SET_COLUMN_CHARSET don't appear together.
        enum OptionalMetaType : char
        {
            /// UNSIGNED flag of numeric columns
            SIGNEDNESS = 1,
            /// Character set of string columns, optimized to
            /// minimize space when many columns have the
            /// same charset
            DEFAULT_CHARSET,
            /// Character set of string columns, optimized to
            /// minimize space when columns have many
            /// different charsets
            COLUMN_CHARSET,
            COLUMN_NAME,
            /// String value of SET columns
            SET_STR_VALUE,
            /// String value of ENUM columns
            ENUM_STR_VALUE,
            /// Real type of geometry columns
            GEOMETRY_TYPE,
            /// Primary key without prefix
            SIMPLE_PRIMARY_KEY,
            /// Primary key with prefix
            PRIMARY_KEY_WITH_PREFIX,
            /// Character set of enum and set
            /// columns, optimized to minimize
            /// space when many columns have the
            /// same charset
            ENUM_AND_SET_DEFAULT_CHARSET,
            /// Character set of enum and set
            /// columns, optimized to minimize
            /// space when many columns have the
            /// same charset
            ENUM_AND_SET_COLUMN_CHARSET,
            /// Flag to indicate column visibility attribute
            COLUMN_VISIBILITY
        };

    protected:
        void parseImpl(ReadBuffer & payload) override;
        void parseMeta(String meta);
        void parseOptionalMetaField(ReadBuffer & payload);
    };

    enum RowsEventFlags
    {
        ROWS_END_OF_STATEMENT = 1
    };

    class RowsEventHeader
    {
    public:
        EventType type;
        UInt64 table_id;
        UInt16 flags;

        explicit RowsEventHeader(EventType type_) : type(type_), table_id(0), flags(0) {}
        void parse(ReadBuffer & payload);
    };

    class RowsEvent : public EventBase
    {
    public:
        UInt32 number_columns;
        String schema;
        String table;
        Row rows;

        RowsEvent(std::shared_ptr<TableMapEvent> table_map_, EventHeader && header_, const RowsEventHeader & rows_header)
            : EventBase(std::move(header_)), number_columns(0), table_map(table_map_)
        {
            table_id = rows_header.table_id;
            flags = rows_header.flags;
            schema = table_map->schema;
            table = table_map->table;
        }

        void dump(WriteBuffer & out) const override;

    protected:
        UInt64 table_id;
        UInt16 flags;
        Bitmap columns_present_bitmap1;
        Bitmap columns_present_bitmap2;

        void parseImpl(ReadBuffer & payload) override;
        void parseRow(ReadBuffer & payload, Bitmap & bitmap);

        std::shared_ptr<TableMapEvent> table_map;
    };

    class WriteRowsEvent : public RowsEvent
    {
    public:
        WriteRowsEvent(std::shared_ptr<TableMapEvent> table_map_, EventHeader && header_, const RowsEventHeader & rows_header)
            : RowsEvent(table_map_, std::move(header_), rows_header) {}
        MySQLEventType type() const override { return MYSQL_WRITE_ROWS_EVENT; }
    };

    class DeleteRowsEvent : public RowsEvent
    {
    public:
        DeleteRowsEvent(std::shared_ptr<TableMapEvent> table_map_, EventHeader && header_, const RowsEventHeader & rows_header)
            : RowsEvent(table_map_, std::move(header_), rows_header) {}
        MySQLEventType type() const override { return MYSQL_DELETE_ROWS_EVENT; }
    };

    class UpdateRowsEvent : public RowsEvent
    {
    public:
        UpdateRowsEvent(std::shared_ptr<TableMapEvent> table_map_, EventHeader && header_, const RowsEventHeader & rows_header)
            : RowsEvent(table_map_, std::move(header_), rows_header) {}
        MySQLEventType type() const override { return MYSQL_UPDATE_ROWS_EVENT; }
    };

    class GTIDEvent : public EventBase
    {
    public:
        UInt8 commit_flag;
        GTID gtid;

        explicit GTIDEvent(EventHeader && header_) : EventBase(std::move(header_)), commit_flag(0) {}
        void dump(WriteBuffer & out) const override;

    protected:
        void parseImpl(ReadBuffer & payload) override;
    };

    class DryRunEvent : public EventBase
    {
    public:
        explicit DryRunEvent(EventHeader && header_) : EventBase(std::move(header_)) {}
        void dump(WriteBuffer & out) const override;

    protected:
        void parseImpl(ReadBuffer & payload) override;
    };

    class UnparsedRowsEvent : public RowsEvent
    {
    public:
        UnparsedRowsEvent(const std::shared_ptr<TableMapEvent> & table_map_, EventHeader && header_, const RowsEventHeader & rows_header)
            : RowsEvent(table_map_, std::move(header_), rows_header)
        {
        }

        void dump(WriteBuffer & out) const override;
        MySQLEventType type() const override { return MYSQL_UNPARSED_ROWS_EVENT; }
        std::shared_ptr<RowsEvent> parse();

    protected:
        void parseImpl(ReadBuffer & payload) override;
        std::vector<uint8_t> unparsed_data;
        std::shared_ptr<RowsEvent> parsed_event;
        mutable std::mutex mutex;
    };

    class Position
    {
    public:
        UInt64 binlog_pos;
        String binlog_name;
        GTIDSets gtid_sets;
        UInt32 timestamp;

        Position() : binlog_pos(0), timestamp(0) { }
        void update(BinlogEventPtr event);
        void update(UInt64 binlog_pos_, const String & binlog_name_, const String & gtid_sets_, UInt32 binlog_time_);
        void dump(WriteBuffer & out) const;
        void resetPendingGTID() { pending_gtid.reset(); }

    private:
        std::optional<GTID> pending_gtid;
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

        ~IFlavor() override = default;
    };

    class MySQLFlavor : public IFlavor
    {
    public:
        void readPayloadImpl(ReadBuffer & payload) override;
        String getName() const override { return "MySQL"; }
        Position getPosition() const override { return position; }
        BinlogEventPtr readOneEvent() override { return event; }
        void setReplicateDatabase(String db) override { replicate_do_db = std::move(db); }
        void setReplicateTables(std::unordered_set<String> tables) { replicate_tables = std::move(tables); }
        void setGTIDSets(GTIDSets sets) override { position.gtid_sets = std::move(sets); }
        void setChecksumSignatureLength(size_t checksum_signature_length_) override { checksum_signature_length = checksum_signature_length_; }

    private:
        Position position;
        BinlogEventPtr event;
        String replicate_do_db;
        // only for filter data(Row Event), not include DDL Event
        std::unordered_set<String> replicate_tables;
        std::map<UInt64, std::shared_ptr<TableMapEvent> > table_maps;
        size_t checksum_signature_length = 4;
        MySQLCharsetPtr flavor_charset = std::make_shared<MySQLCharset>();

        bool doReplicate(UInt64 table_id);
        bool doReplicate(const String & db, const String & table_name);
    };
}

}
