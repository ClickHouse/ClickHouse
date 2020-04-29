#include "MySQLReplication.h"

namespace DB::MySQLReplication
{
namespace ErrorCodes
{
    extern const int UNKNOWN_EXCEPTION;
    extern const int UNEXPECTED_PACKET_FROM_SERVER;
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


void EventHeader::dump()
{
    std::cerr << "=== Event Header ===" << std::endl;
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
    payload.readStrict(reinterpret_cast<char *>(server_version.data()), 50);
    payload.readStrict(reinterpret_cast<char *>(&create_timestamp), 4);
    payload.readStrict(reinterpret_cast<char *>(&event_header_length), 1);
    readNullTerminated(event_type_header_length, payload);
}

void FormatDescriptionEvent::dump()
{
    std::cerr << "=== FormatDescriptionEvent ===" << std::endl;
}

/// https://dev.mysql.com/doc/internals/en/rotate-event.html
void RotateEvent::parse(ReadBuffer & payload)
{
    payload.readStrict(reinterpret_cast<char *>(&position), 8);
    readString(next_binlog, payload);
}

void RotateEvent::dump()
{
    std::cerr << "=== RotateEvent ===" << std::endl;
    std::cerr << "Position: " << this->position << std::endl;
    std::cerr << "Next Binlog: " << this->next_binlog << std::endl;
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
    // skip the header
    payload.ignore(1);

    EventHeader event_header;
    event_header.parse(payload);
    switch (event_header.type)
    {
        case FORMAT_DESCRIPTION_EVENT: {
            event = std::make_shared<FormatDescriptionEvent>();
            event->parse(payload);
            break;
        }
        case ROTATE_EVENT: {
            event = std::make_shared<RotateEvent>();
            event->parse(payload);
            break;
        }
        default:
            throw ReplicationError("Unsupported event: " + std::to_string(event_header.type), ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER);
    }
}

}
