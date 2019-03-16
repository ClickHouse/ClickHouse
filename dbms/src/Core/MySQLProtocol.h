#pragma once

#include <IO/WriteBuffer.h>
#include <random>
#include <sstream>

/// Implementation of MySQL wire protocol

namespace DB {
namespace MySQLProtocol {

const size_t MAX_PACKET_LENGTH = (1 << 24) - 1; // 16 mb
const size_t SCRAMBLE_LENGTH = 20;
const size_t AUTH_PLUGIN_DATA_PART_1_LENGTH = 8;
const size_t MYSQL_ERRMSG_SIZE = 512;

namespace Authentication {
    const std::string Native41 = "mysql_native_password";
}

enum CharacterSet {
    UTF8 = 33
};

enum StatusFlags {
    SERVER_SESSION_STATE_CHANGED = 0x4000
};

enum Capability {
    CLIENT_CONNECT_WITH_DB = 0x00000008,
    CLIENT_PROTOCOL_41 = 0x00000200,
    CLIENT_TRANSACTIONS = 0x00002000, // TODO
    CLIENT_SESSION_TRACK = 0x00800000, // TODO
    CLIENT_SECURE_CONNECTION = 0x00008000,
    CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000,
    CLIENT_PLUGIN_AUTH = 0x00080000,
    CLIENT_DEPRECATE_EOF = 0x01000000,
};

enum Command {
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
    COM_RESET_CONNECTION = 0x1f,
    COM_DAEMON = 0x1d
};

enum ColumnType {
    MYSQL_TYPE_DECIMAL = 0x00,
    MYSQL_TYPE_TINY = 0x01,
    MYSQL_TYPE_SHORT = 0x02,
    MYSQL_TYPE_LONG = 0x03,
    MYSQL_TYPE_FLOAT = 0x04,
    MYSQL_TYPE_DOUBLE = 0x05,
    MYSQL_TYPE_NULL = 0x06,
    MYSQL_TYPE_TIMESTAMP = 0x07,
    MYSQL_TYPE_LONGLONG = 0x08,
    MYSQL_TYPE_INT24 = 0x09,
    MYSQL_TYPE_DATE = 0x0a,
    MYSQL_TYPE_TIME = 0x0b,
    MYSQL_TYPE_DATETIME = 0x0c,
    MYSQL_TYPE_YEAR = 0x0d,
    MYSQL_TYPE_VARCHAR = 0x0f,
    MYSQL_TYPE_BIT = 0x10,
    MYSQL_TYPE_NEWDECIMAL = 0xf6,
    MYSQL_TYPE_ENUM = 0xf7,
    MYSQL_TYPE_SET = 0xf8,
    MYSQL_TYPE_TINY_BLOB = 0xf9,
    MYSQL_TYPE_MEDIUM_BLOB = 0xfa,
    MYSQL_TYPE_LONG_BLOB = 0xfb,
    MYSQL_TYPE_BLOB = 0xfc,
    MYSQL_TYPE_VAR_STRING = 0xfd,
    MYSQL_TYPE_STRING = 0xfe,
    MYSQL_TYPE_GEOMETRY = 0xff
};

uint64_t readLenenc(std::istringstream &ss);

std::string writeLenenc(uint64_t x);

void writeLenencStr(std::string &payload, const std::string &s);


class ProtocolError : public DB::Exception {
public:
    using Exception::Exception;
};


/// https://dev.mysql.com/doc/internals/en/connection-phase-packets.html


class Handshake {
    int protocol_version = 0xa;
    std::string server_version;
    uint32_t connection_id;
    uint32_t capability_flags;
    uint8_t character_set;
    uint32_t status_flags;
    std::string auth_plugin_data;
public:
    explicit Handshake(uint32_t connection_id, std::string server_version)
        : protocol_version(0xa)
        , server_version(std::move(server_version))
        , connection_id(connection_id)
        , capability_flags(
            CLIENT_PROTOCOL_41
            | CLIENT_SECURE_CONNECTION
            | CLIENT_PLUGIN_AUTH
            | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
            | CLIENT_CONNECT_WITH_DB
            | CLIENT_DEPRECATE_EOF)
        , character_set(63)
        , status_flags(0) {
        auth_plugin_data.resize(SCRAMBLE_LENGTH);

        auto seed = std::chrono::system_clock::now().time_since_epoch().count();
        std::default_random_engine generator (static_cast<unsigned int>(seed));

        std::uniform_int_distribution<char> distribution(0);
        for (size_t i = 0; i < SCRAMBLE_LENGTH; i++) {
            auth_plugin_data[i] = distribution(generator);
        }
    }

    std::string getPayload() {
        std::string result;
        result.append(1, protocol_version);
        result.append(server_version);
        result.append(1, 0x0);
        result.append(reinterpret_cast<const char *>(&connection_id), 4);
        result.append(auth_plugin_data.substr(0, AUTH_PLUGIN_DATA_PART_1_LENGTH));
        result.append(1, 0x0);
        result.append(reinterpret_cast<const char *>(&capability_flags), 2);
        result.append(reinterpret_cast<const char *>(&character_set), 1);
        result.append(reinterpret_cast<const char *>(&status_flags), 2);
        result.append((reinterpret_cast<const char *>(&capability_flags)) + 2, 2);
        result.append(1, SCRAMBLE_LENGTH + 1);
        result.append(1, 0x0);
        result.append(10, 0x0);
        result.append(auth_plugin_data.substr(AUTH_PLUGIN_DATA_PART_1_LENGTH, SCRAMBLE_LENGTH - AUTH_PLUGIN_DATA_PART_1_LENGTH));
        result.append(Authentication::Native41);
        result.append(1, 0x0);
        return result;
    }
};

class HandshakeResponse {
public:
    uint32_t capability_flags;
    uint32_t max_packet_size;
    uint8_t character_set;
    std::string username;
    std::string auth_response;
    std::string database;
    std::string auth_plugin_name;

    void readPayload(const std::string & s) {
        std::istringstream ss(s);

        ss.readsome(reinterpret_cast<char *>(&capability_flags), 4);
        ss.readsome(reinterpret_cast<char *>(&max_packet_size), 4);
        ss.readsome(reinterpret_cast<char *>(&character_set), 1);
        ss.ignore(23);

        std::getline(ss, username, static_cast<char>(0x0));

        if (capability_flags & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) {
            auto len = readLenenc(ss);
            auth_response.resize(len);
            ss.read(auth_response.data(), static_cast<std::streamsize>(len));
        } else if (capability_flags & CLIENT_SECURE_CONNECTION) {
            uint8_t len;
            ss.read(reinterpret_cast<char *>(&len), 1);
            auth_response.resize(len);
            ss.read(auth_response.data(), len);
        } else {
            std::getline(ss, auth_response, static_cast<char>(0x0));
        }

        if (capability_flags & CLIENT_CONNECT_WITH_DB) {
            std::getline(ss, database, static_cast<char>(0x0));
        }

        if (capability_flags & CLIENT_PLUGIN_AUTH) {
            std::getline(ss, auth_plugin_name, static_cast<char>(0x0));
        }
    }
};

class OK_Packet {
    uint8_t header;
    uint32_t capabilities;
    uint64_t affected_rows;
    uint64_t last_insert_id;
    int16_t warnings = 0;
    uint32_t status_flags;
    std::string info;
    std::string session_state_changes;
public:
    OK_Packet(uint8_t header, uint32_t capabilities, uint64_t affected_rows, uint64_t last_insert_id, uint32_t status_flags,
              int16_t warnings, std::string session_state_changes)
        : header(header)
        , capabilities(capabilities)
        , affected_rows(affected_rows)
        , last_insert_id(last_insert_id)
        , warnings(warnings)
        , status_flags(status_flags)
        , session_state_changes(std::move(session_state_changes))
    {
    }

    std::string getPayload() {
        std::string result;
        result.append(1, header);
        result.append(writeLenenc(affected_rows));
        result.append(writeLenenc(last_insert_id));

        if (capabilities & CLIENT_PROTOCOL_41) {
            result.append(reinterpret_cast<const char *>(&status_flags), 2);
            result.append(reinterpret_cast<const char *>(&warnings), 2);
        } else if (capabilities & CLIENT_TRANSACTIONS) {
            result.append(reinterpret_cast<const char *>(&status_flags), 2);
        }

        if (capabilities & CLIENT_SESSION_TRACK) {
            result.append(writeLenenc(info.length()));
            result.append(info);
            if (status_flags & SERVER_SESSION_STATE_CHANGED) {
                result.append(writeLenenc(session_state_changes.length()));
                result.append(session_state_changes);
            }
        } else {
            result.append(info);
        }
        return result;
    }
};

class EOF_Packet {
    int warnings;
    int status_flags;
public:
    EOF_Packet(int warnings, int status_flags): warnings(warnings), status_flags(status_flags) {}

    std::string getPayload() {
        std::string result;
        result.append(1, 0xfe); // EOF header
        result.append(reinterpret_cast<const char *>(&warnings), 2);
        result.append(reinterpret_cast<const char *>(&status_flags), 2);
        return result;
    }
};

class ERR_Packet {
    int error_code;
    std::string sql_state;
    std::string error_message;
public:
    ERR_Packet(int error_code, std::string sql_state, std::string error_message)
        : error_code(error_code)
        , sql_state(std::move(sql_state))
        , error_message(std::move(error_message))
    {
    }

    std::string getPayload()
    {
        std::string result;
        result.append(1, 0xff);
        result.append(reinterpret_cast<const char *>(&error_code), 2);
        result.append("#", 1);
        result.append(sql_state.data(), sql_state.length());
        result.append(error_message.data(), std::min(error_message.length(), MYSQL_ERRMSG_SIZE));
        return result;
    }
};

class ColumnDefinition {
    std::string schema;
    std::string table;
    std::string org_table;
    std::string name;
    std::string org_name;
    size_t next_length = 0x0c;
    uint16_t character_set;
    uint32_t column_length;
    ColumnType column_type;
    uint16_t flags;
    uint8_t decimals = 0x00;
public:
    explicit ColumnDefinition(
        std::string schema,
        std::string table,
        std::string org_table,
        std::string name,
        std::string org_name,
        uint16_t character_set,
        uint32_t column_length,
        ColumnType column_type,
        uint16_t flags,
        uint8_t decimals)

        : schema(std::move(schema))
        , table(std::move(table))
        , org_table(std::move(org_table))
        , name(std::move(name))
        , org_name(std::move(org_name))
        , character_set(character_set)
        , column_length(column_length)
        , column_type(column_type)
        , flags(flags)
        , decimals(decimals)
    {
    }

    std::string getPayload() {
        std::string result;
        writeLenencStr(result, "def"); // always "def"
        writeLenencStr(result, schema);
        writeLenencStr(result, table);
        writeLenencStr(result, org_table);
        writeLenencStr(result, name);
        writeLenencStr(result, org_name);
        result.append(writeLenenc(next_length));
        result.append(reinterpret_cast<const char *>(&character_set), 2);
        result.append(reinterpret_cast<const char *>(&column_length), 4);
        result.append(reinterpret_cast<const char *>(&column_type), 1);
        result.append(reinterpret_cast<const char *>(&flags), 2);
        result.append(reinterpret_cast<const char *>(&decimals), 2);
        result.append(2, 0x0);
        return result;
    }
};

class ComFieldList {
public:
    std::string table, field_wildcard;

    void readPayload(const std::string & payload)
    {
        std::istringstream ss(payload);
        ss.ignore(1); // command byte
        std::getline(ss, table, static_cast<char>(0x0));
        field_wildcard = payload.substr(table.length() + 2); // rest of the packet
    }
};


}
}
