#include "MySQLReplication.h"

#include <DataTypes/DataTypeString.h>
#include <Common/FieldVisitors.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_EXCEPTION;
}

namespace MySQLReplication
{
    using namespace MySQLProtocol;

    String ToString(BinlogChecksumAlg type)
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
            default:
                return std::string("Unknown checksum alg: ") + std::to_string(static_cast<int>(type));
        }
    }

    String ToString(EventType type)
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

    /// https://dev.mysql.com/doc/internals/en/binlog-event-header.html
    void EventHeader::parse(ReadBuffer & payload)
    {
        payload.readStrict(reinterpret_cast<char *>(&timestamp), 4);
        payload.readStrict(reinterpret_cast<char *>(&type), 1);
        payload.readStrict(reinterpret_cast<char *>(&server_id), 4);
        payload.readStrict(reinterpret_cast<char *>(&event_size), 4);
        payload.readStrict(reinterpret_cast<char *>(&log_pos), 4);
        payload.readStrict(reinterpret_cast<char *>(&flags), 2);
    }

    void EventHeader::print() const
    {
        std::cerr << "\n=== " << ToString(this->type) << " ===" << std::endl;
        std::cerr << "Timestamp: " << this->timestamp << std::endl;
        std::cerr << "Event Type: " << this->type << std::endl;
        std::cerr << "Server ID: " << this->server_id << std::endl;
        std::cerr << "Event Size: " << this->event_size << std::endl;
        std::cerr << "Log Pos: " << this->log_pos << std::endl;
        std::cerr << "Flags: " << this->flags << std::endl;
    }

    /// https://dev.mysql.com/doc/internals/en/format-description-event.html
    void FormatDescriptionEvent::parseImpl(ReadBuffer & payload)
    {
        payload.readStrict(reinterpret_cast<char *>(&binlog_version), 2);
        assert(binlog_version == EVENT_VERSION_V4);
        payload.readStrict(reinterpret_cast<char *>(server_version.data()), 50);
        payload.readStrict(reinterpret_cast<char *>(&create_timestamp), 4);
        payload.readStrict(reinterpret_cast<char *>(&event_header_length), 1);
        assert(event_header_length == EVENT_HEADER_LENGTH);

        size_t len = header.event_size - (2 + 50 + 4 + 1 + EVENT_HEADER_LENGTH) - 1;
        event_type_header_length.resize(len);
        payload.readStrict(reinterpret_cast<char *>(event_type_header_length.data()), len);
        assert(event_type_header_length[WRITE_ROWS_EVENT_V2] == ROWS_HEADER_LEN_V2);
        assert(event_type_header_length[UPDATE_ROWS_EVENT_V2] == ROWS_HEADER_LEN_V2);
        assert(event_type_header_length[DELETE_ROWS_EVENT_V2] == ROWS_HEADER_LEN_V2);
    }

    void FormatDescriptionEvent::print() const
    {
        header.print();
        std::cerr << "Binlog Version: " << this->binlog_version << std::endl;
        std::cerr << "Server Version: " << this->server_version << std::endl;
        std::cerr << "Create Timestamp: " << this->create_timestamp << std::endl;
        std::cerr << "Event Header Len: " << this->event_header_length << std::endl;
    }

    /// https://dev.mysql.com/doc/internals/en/rotate-event.html
    void RotateEvent::parseImpl(ReadBuffer & payload)
    {
        payload.readStrict(reinterpret_cast<char *>(&position), 8);
        size_t len = header.event_size - EVENT_HEADER_LENGTH - 8 - CHECKSUM_CRC32_SIGNATURE_LENGTH;
        next_binlog.resize(len);
        payload.readStrict(reinterpret_cast<char *>(next_binlog.data()), len);
    }

    void RotateEvent::print() const
    {
        header.print();
        std::cerr << "Position: " << this->position << std::endl;
        std::cerr << "Next Binlog: " << this->next_binlog << std::endl;
    }

    /// https://dev.mysql.com/doc/internals/en/query-event.html
    void QueryEvent::parseImpl(ReadBuffer & payload)
    {
        payload.readStrict(reinterpret_cast<char *>(&thread_id), 4);
        payload.readStrict(reinterpret_cast<char *>(&exec_time), 4);
        payload.readStrict(reinterpret_cast<char *>(&schema_len), 1);
        payload.readStrict(reinterpret_cast<char *>(&error_code), 2);
        payload.readStrict(reinterpret_cast<char *>(&status_len), 2);

        status.resize(status_len);
        payload.readStrict(reinterpret_cast<char *>(status.data()), status_len);

        schema.resize(schema_len);
        payload.readStrict(reinterpret_cast<char *>(schema.data()), schema_len);
        payload.ignore(1);

        size_t len
            = header.event_size - EVENT_HEADER_LENGTH - 4 - 4 - 1 - 2 - 2 - status_len - schema_len - 1 - CHECKSUM_CRC32_SIGNATURE_LENGTH;
        query.resize(len);
        payload.readStrict(reinterpret_cast<char *>(query.data()), len);
    }

    void QueryEvent::print() const
    {
        header.print();
        std::cerr << "Thread ID: " << this->thread_id << std::endl;
        std::cerr << "Execution Time: " << this->exec_time << std::endl;
        std::cerr << "Schema Len: " << this->schema_len << std::endl;
        std::cerr << "Error Code: " << this->error_code << std::endl;
        std::cerr << "Status Len: " << this->status_len << std::endl;
        std::cerr << "Schema: " << this->schema << std::endl;
        std::cerr << "Query: " << this->query << std::endl;
    }

    void XIDEvent::parseImpl(ReadBuffer & payload) { payload.readStrict(reinterpret_cast<char *>(&xid), 8); }


    void XIDEvent::print() const
    {
        header.print();
        std::cerr << "XID: " << this->xid << std::endl;
    }

    void TableMapEvent::parseImpl(ReadBuffer & payload)
    {
        payload.readStrict(reinterpret_cast<char *>(&table_id), 6);
        payload.readStrict(reinterpret_cast<char *>(&flags), 2);

        payload.readStrict(reinterpret_cast<char *>(&schema_len), 1);
        schema.resize(schema_len);
        payload.readStrict(reinterpret_cast<char *>(schema.data()), schema_len);
        payload.ignore(1);

        payload.readStrict(reinterpret_cast<char *>(&table_len), 1);
        table.resize(table_len);
        payload.readStrict(reinterpret_cast<char *>(table.data()), table_len);
        payload.ignore(1);

        column_count = readLengthEncodedNumber(payload);
        for (auto i = 0U; i < column_count; i++)
        {
            UInt8 v = 0x00;
            payload.readStrict(reinterpret_cast<char *>(&v), 1);
            column_type.emplace_back(v);
        }

        String meta;
        readLengthEncodedString(meta, payload);
        parseMeta(meta);

        size_t len = (column_count + 8) / 7;
        payload.readStrict(reinterpret_cast<char *>(null_bitmap.data()), len);
    }

    void TableMapEvent::parseMeta(String meta)
    {
        auto pos = 0;
        column_meta.reserve(column_count);
        for (auto i = 0U; i < column_count; i++)
        {
            UInt16 typ = column_type[i];
            switch (typ)
            {
                case MYSQL_TYPE_DECIMAL:
                case MYSQL_TYPE_TINY:
                case MYSQL_TYPE_SHORT:
                case MYSQL_TYPE_LONG:
                case MYSQL_TYPE_NULL:
                case MYSQL_TYPE_TIMESTAMP:
                case MYSQL_TYPE_LONGLONG:
                case MYSQL_TYPE_INT24:
                case MYSQL_TYPE_DATE:
                case MYSQL_TYPE_TIME:
                case MYSQL_TYPE_DATETIME:
                case MYSQL_TYPE_YEAR:
                case MYSQL_TYPE_NEWDATE: {
                    /// No data here.
                    column_meta.emplace_back(0);
                    break;
                }

                case MYSQL_TYPE_FLOAT:
                case MYSQL_TYPE_DOUBLE:
                case MYSQL_TYPE_TIMESTAMP2:
                case MYSQL_TYPE_DATETIME2:
                case MYSQL_TYPE_TIME2:
                case MYSQL_TYPE_JSON:
                case MYSQL_TYPE_TINY_BLOB:
                case MYSQL_TYPE_MEDIUM_BLOB:
                case MYSQL_TYPE_LONG_BLOB:
                case MYSQL_TYPE_BLOB:
                case MYSQL_TYPE_GEOMETRY: {
                    column_meta.emplace_back(UInt16(meta[pos]));
                    pos += 1;
                    break;
                }
                case MYSQL_TYPE_NEWDECIMAL:
                case MYSQL_TYPE_ENUM:
                case MYSQL_TYPE_SET:
                case MYSQL_TYPE_STRING: {
                    column_meta.emplace_back((UInt16(meta[pos]) << 8) + UInt16(meta[pos + 1]));
                    pos += 2;
                    break;
                }

                case MYSQL_TYPE_VARCHAR:
                case MYSQL_TYPE_BIT:
                case MYSQL_TYPE_VAR_STRING: {
                    column_meta.emplace_back(UInt16(meta[pos]) + (UInt16(meta[pos + 1] << 8)));
                    pos += 2;
                    break;
                }
                default:
                    throw ReplicationError("ParseMetaData: Unhandled data type:" + std::to_string(typ), ErrorCodes::UNKNOWN_EXCEPTION);
            }
        }
    }

    void TableMapEvent::print() const
    {
        header.print();
        std::cerr << "Table ID: " << this->table_id << std::endl;
        std::cerr << "Flags: " << this->flags << std::endl;
        std::cerr << "Schema Len: " << this->schema_len << std::endl;
        std::cerr << "Schema: " << this->schema << std::endl;
        std::cerr << "Table Len: " << this->table_len << std::endl;
        std::cerr << "Table: " << this->table << std::endl;
        std::cerr << "Column Count: " << this->column_count << std::endl;
        for (auto i = 0U; i < column_count; i++)
        {
            std::cerr << "Column Type [" << i << "]: " << column_type[i] << ", Meta: " << column_meta[i] << std::endl;
        }
        std::cerr << "Null Bitmap: " << this->null_bitmap << std::endl;
    }

    void RowsEvent::parseImpl(ReadBuffer & payload)
    {
        payload.readStrict(reinterpret_cast<char *>(&table_id), 6);
        payload.readStrict(reinterpret_cast<char *>(&flags), 2);

        /// This extra_data_len contains the 2 bytes length.
        payload.readStrict(reinterpret_cast<char *>(&extra_data_len), 2);
        payload.ignore(extra_data_len - 2);

        number_columns = readLengthEncodedNumber(payload);
        size_t columns_bitmap_size = (number_columns + 8) / 7;
        payload.readStrict(reinterpret_cast<char *>(columns_before_bitmap.data()), columns_bitmap_size);
        if (header.type == UPDATE_ROWS_EVENT_V2)
        {
            payload.readStrict(reinterpret_cast<char *>(columns_after_bitmap.data()), columns_bitmap_size);
        }

        while (payload.available() > CHECKSUM_CRC32_SIGNATURE_LENGTH)
        {
            parseRow(payload, columns_before_bitmap);
            if (header.type == UPDATE_ROWS_EVENT_V2)
            {
                parseRow(payload, columns_after_bitmap);
            }
        }
    }

    void RowsEvent::parseRow(ReadBuffer & payload, String bitmap)
    {
        UInt32 field_type = 0;
        UInt32 field_len = 0;

        bitmap = "";
        size_t columns_null_bitmap_size = (number_columns + 8) / 7;
        String columns_null_bitmap;
        columns_null_bitmap.resize(columns_null_bitmap_size);
        payload.readStrict(reinterpret_cast<char *>(columns_null_bitmap.data()), columns_null_bitmap_size);

        Tuple row;
        for (auto i = 0U; i < number_columns; i++)
        {
            field_type = table_map->column_type[i];
            UInt16 meta = table_map->column_meta[i];

            if (field_type == MYSQL_TYPE_STRING)
            {
                if (meta >= 256)
                {
                    UInt32 byte0 = meta >> 8;
                    UInt32 byte1 = meta & 0xff;
                    if ((byte0 & 0x30) != 0x30)
                    {
                        field_len = byte1 | (((byte0 & 0x30) ^ 0x30) << 4);
                        field_type = byte0 | 0x30;
                    }
                    else
                    {
                        switch (byte0)
                        {
                            case MYSQL_TYPE_SET:
                            case MYSQL_TYPE_ENUM:
                            case MYSQL_TYPE_STRING:
                                field_type = byte0;
                                field_len = byte1;
                                break;
                            default:
                                throw ReplicationError("ParseRow: Illegal event", ErrorCodes::UNKNOWN_EXCEPTION);
                        }
                    }
                }
                else
                {
                    field_len = meta;
                }
            }

            switch (field_type)
            {
                case MYSQL_TYPE_LONG: {
                    UInt32 val = 0;
                    payload.readStrict(reinterpret_cast<char *>(&val), 4);
                    row.push_back(Field{UInt32{val}});
                    break;
                }
                case MYSQL_TYPE_VARCHAR:
                case MYSQL_TYPE_VAR_STRING: {
                    uint32_t size = meta;
                    if (size < 256)
                    {
                        uint8_t tmp1 = 0;
                        payload.readStrict(reinterpret_cast<char *>(&tmp1), 1);
                        size = tmp1;
                    }
                    else
                    {
                        uint16_t tmp2 = 0;
                        payload.readStrict(reinterpret_cast<char *>(&tmp2), 2);
                        size = tmp2;
                    }

                    String val;
                    val.resize(size);
                    payload.readStrict(reinterpret_cast<char *>(val.data()), size);
                    row.push_back(Field{String{val}});
                    break;
                }
                case MYSQL_TYPE_STRING: {
                    UInt32 size = field_len;
                    if (size < 256)
                    {
                        uint8_t tmp1 = 0;
                        payload.readStrict(reinterpret_cast<char *>(&tmp1), 1);
                        size = tmp1;
                    }
                    else
                    {
                        uint16_t tmp2 = 0;
                        payload.readStrict(reinterpret_cast<char *>(&tmp2), 2);
                        size = tmp2;
                    }
                    break;
                }
            }
        }
        rows.push_back(row);
    }

    void RowsEvent::print() const
    {
        FieldVisitorToString to_string;

        header.print();
        std::cerr << "Schema: " << this->schema << std::endl;
        std::cerr << "Table: " << this->table << std::endl;
        for (auto i = 0U; i < rows.size(); i++)
        {
            std::cerr << "Row[" << i << "]: " << applyVisitor(to_string, rows[i]) << std::endl;
        }
    }

    void DryRunEvent::parseImpl(ReadBuffer & payload)
    {
        while (payload.next())
        {
        }
    }

    void DryRunEvent::print() const
    {
        header.print();
        std::cerr << "[DryRun Event]" << std::endl;
    }

    void MySQLFlavor::readPayloadImpl(ReadBuffer & payload)
    {
        UInt16 header = static_cast<unsigned char>(*payload.position());
        switch (header)
        {
            case PACKET_EOF:
                throw ReplicationError("Master maybe lost", ErrorCodes::UNKNOWN_EXCEPTION);
            case PACKET_ERR:
                ERR_Packet err;
                err.readPayloadImpl(payload);
                throw ReplicationError(err.error_message, ErrorCodes::UNKNOWN_EXCEPTION);
        }
        // skip the header flag.
        payload.ignore(1);

        EventType event_type = static_cast<EventType>(*(payload.position() + 4));
        switch (event_type)
        {
            case FORMAT_DESCRIPTION_EVENT: {
                event = std::make_shared<FormatDescriptionEvent>();
                event->parseHeader(payload);
                event->parseEvent(payload);
                position.updateLogPos(event->header.log_pos);
                break;
            }
            case ROTATE_EVENT: {
                event = std::make_shared<RotateEvent>();
                event->parseHeader(payload);
                event->parseEvent(payload);

                auto rotate = std::dynamic_pointer_cast<RotateEvent>(event);
                position.updateLogPos(event->header.log_pos);
                position.updateLogName(rotate->next_binlog);
                break;
            }
            case QUERY_EVENT: {
                event = std::make_shared<QueryEvent>();
                event->parseHeader(payload);
                event->parseEvent(payload);
                if (event->header.event_size > QUERY_EVENT_BEGIN_LENGTH)
                    position.updateLogPos(event->header.log_pos);
                break;
            }
            case XID_EVENT: {
                event = std::make_shared<XIDEvent>();
                event->parseHeader(payload);
                event->parseEvent(payload);
                position.updateLogPos(event->header.log_pos);
                break;
            }
            case TABLE_MAP_EVENT: {
                event = std::make_shared<TableMapEvent>();
                event->parseHeader(payload);
                event->parseEvent(payload);

                table_map = std::dynamic_pointer_cast<TableMapEvent>(event);
                position.updateLogPos(event->header.log_pos);
                break;
            }
            case WRITE_ROWS_EVENT_V2: {
                event = std::make_shared<WriteRowsEvent>(table_map);
                event->parseHeader(payload);
                event->parseEvent(payload);
                break;
            }
            case DELETE_ROWS_EVENT_V2: {
                event = std::make_shared<DeleteRowsEvent>(table_map);
                event->parseHeader(payload);
                event->parseEvent(payload);
                break;
            }
            case UPDATE_ROWS_EVENT_V2: {
                event = std::make_shared<UpdateRowsEvent>(table_map);
                event->parseHeader(payload);
                event->parseEvent(payload);
                break;
            }
            default: {
                event = std::make_shared<DryRunEvent>();
                event->parseHeader(payload);
                event->parseEvent(payload);
                position.updateLogPos(event->header.log_pos);
                break;
            }
        }
        payload.tryIgnore(CHECKSUM_CRC32_SIGNATURE_LENGTH);
    }
}

}
