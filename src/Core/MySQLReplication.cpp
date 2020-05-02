#include "MySQLReplication.h"

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

    void EventHeader::dump() const
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
    void FormatDescriptionEvent::parse(ReadBuffer & payload)
    {
        payload.readStrict(reinterpret_cast<char *>(&binlog_version), 2);
        assert(binlog_version == EVENT_VERSION);
        payload.readStrict(reinterpret_cast<char *>(server_version.data()), 50);
        payload.readStrict(reinterpret_cast<char *>(&create_timestamp), 4);
        payload.readStrict(reinterpret_cast<char *>(&event_header_length), 1);
        assert(event_header_length == EVENT_HEADER_LENGTH);

        size_t len = header.event_size - (2 + 50 + 4 + 1 + EVENT_HEADER_LENGTH) - 1;
        event_type_header_length.resize(len);
        payload.readStrict(reinterpret_cast<char *>(event_type_header_length.data()), len);
        payload.readStrict(reinterpret_cast<char *>(&checksum_alg), 1);
    }

    void FormatDescriptionEvent::dump()
    {
        header.dump();
        std::cerr << "Binlog Version: " << this->binlog_version << std::endl;
        std::cerr << "Server Version: " << this->server_version << std::endl;
        std::cerr << "Create Timestamp: " << this->create_timestamp << std::endl;
        std::cerr << "Event Header Len: " << this->event_header_length << std::endl;
        std::cerr << "Binlog Checksum Alg: " << ToString(this->checksum_alg) << std::endl;
    }

    /// https://dev.mysql.com/doc/internals/en/rotate-event.html
    void RotateEvent::parse(ReadBuffer & payload)
    {
        payload.readStrict(reinterpret_cast<char *>(&position), 8);
        size_t len = header.event_size - EVENT_HEADER_LENGTH - 8;
        next_binlog.resize(len);
        payload.readStrict(reinterpret_cast<char *>(next_binlog.data()), len);
    }

    void RotateEvent::dump()
    {
        header.dump();
        std::cerr << "Position: " << this->position << std::endl;
        std::cerr << "Next Binlog: " << this->next_binlog << std::endl;
    }

    void DryRunEvent::parse(ReadBuffer & payload)
    {
        while (payload.next())
        {
        }
    }

    void DryRunEvent::dump()
    {
        header.dump();
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
                break;
            }
            case ROTATE_EVENT: {
                event = std::make_shared<RotateEvent>();
                break;
            }
            default: {
                event = std::make_shared<DryRunEvent>();
                break;
            }
        }
        event->header.parse(payload);
        event->parse(payload);
    }
}

}
