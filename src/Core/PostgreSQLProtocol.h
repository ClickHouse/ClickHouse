#pragma once

#include <IO/LimitReadBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Session.h>
#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/Base64.h>
#include <Common/quoteString.h>
#include <Common/StringUtils.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Poco/RegularExpression.h>
#include <Poco/Net/StreamSocket.h>
#include <Parsers/Lexer.h>
#include <Parsers/ParserPreparedStatement.h>
#include <Poco/RandomStream.h>
#include <Poco/SHA1Engine.h>
#include <Access/Credentials.h>
#include <algorithm>
#include <chrono>
#include <limits>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Interpreters/Context.h>
#include <Access/AccessControl.h>
#include <Access/User.h>
#include <fmt/core.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_PACKET_FROM_CLIENT;
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_TYPE;
    extern const int LIMIT_EXCEEDED;
}


namespace PostgreSQLProtocol
{

namespace Messaging
{

enum class FrontMessageType : Int32
{
// first message types
    CANCEL_REQUEST = 80877102,
    SSL_REQUEST = 80877103,
    GSSENC_REQUEST = 80877104,

// other front message types
    PASSWORD_MESSAGE = 'p',
    QUERY = 'Q',
    TERMINATE = 'X',
    PARSE = 'P',
    BIND = 'B',
    DESCRIBE = 'D',
    SYNC = 'S',
    FLUSH = 'H',
    CLOSE = 'C',
    EXECUTE = 'E',
    COPY_DATA = 'd',
    COPY_COMPLETION = 'c',
};

enum class MessageType : Int32
{
// common
    ERROR_RESPONSE = 0,
    CANCEL_REQUEST = 1,
    COMMAND_COMPLETE = 2,
    NOTICE_RESPONSE = 3,
    NOTIFICATION_RESPONSE = 4,
    PARAMETER_STATUS = 5,
    READY_FOR_QUERY = 6,
    SYNC = 7,
    SYNC_COMPLETE = 7,
    TERMINATE = 8,

// start up and authentication
    AUTHENTICATION_OK = 30,
    AUTHENTICATION_KERBEROS_V5 = 31,
    AUTHENTICATION_CLEARTEXT_PASSWORD = 32,
    AUTHENTICATION_MD5_PASSWORD = 33,
    AUTHENTICATION_SCM_CREDENTIAL = 34,
    AUTHENTICATION_GSS = 35,
    AUTHENTICATION_SSPI = 36,
    AUTHENTICATION_GSS_CONTINUE = 37,
    AUTHENTICATION_SASL = 38,
    AUTHENTICATION_SASL_CONTINUE = 39,
    AUTHENTICATION_SASL_FINAL = 40,
    BACKEND_KEY_DATA = 41,
    GSSENC_REQUEST = 42,
    GSS_RESPONSE = 43,
    NEGOTIATE_PROTOCOL_VERSION = 44,
    PASSWORD_MESSAGE = 45,
    SASL_INITIAL_RESPONSE = 46,
    SASL_RESPONSE = 47,
    SSL_REQUEST = 48,
    STARTUP_MESSAGE = 49,

// simple query
    DATA_ROW = 100,
    EMPTY_QUERY_RESPONSE = 101,
    ROW_DESCRIPTION = 102,
    QUERY = 103,

// extended query
    BIND = 120,
    BIND_COMPLETE = 121,
    CLOSE = 122,
    CLOSE_COMPLETE = 123,
    DESCRIBE = 124,
    EXECUTE = 125,
    FLUSH = 126,
    NODATA = 127,
    PARAMETER_DESCRIPTION = 128,
    PARSE = 129,
    PARSE_COMPLETE = 130,
    PORTAL_SUSPENDED = 131,

// copy query
    COPY_DATA = 171,
    COPY_DONE = 172,
    COPY_FAIL = 173,
    COPY_IN_RESPONSE = 174,
    COPY_OUT_RESPONSE = 175,
    COPY_BOTH_RESPONSE = 176,

// function query (deprecated by the protocol)
    FUNCTION_CALL = 190,
    FUNCTION_CALL_RESPONSE = 191,
};

/** Column 'typelem' from 'pg_type' table. NB: not all types are compatible with PostgreSQL's ones */
enum class ColumnType : Int32
{
    BOOL = 16,
    CHAR = 18,
    INT8 = 20,
    INT2 = 21,
    INT4 = 23,
    FLOAT4 = 700,
    FLOAT8 = 701,
    VARCHAR = 1043,
    DATE = 1082,
    NUMERIC = 1700,
    UUID = 2950,
};

class ColumnTypeSpec
{
public:
    ColumnType type;
    Int16 len;

    ColumnTypeSpec(ColumnType type_, Int16 len_) : type(type_), len(len_) {}
};

ColumnTypeSpec convertDataTypeToPostgresColumnTypeSpec(const DataTypePtr & data_type);

class MessageTransport
{
private:
    ReadBuffer * in;
    WriteBuffer * out;

public:
    explicit MessageTransport(WriteBuffer * out_) : in(nullptr), out(out_) {}

    MessageTransport(ReadBuffer * in_, WriteBuffer * out_): in(in_), out(out_) {}

    template<typename TMessage>
    std::unique_ptr<TMessage> receiveWithPayloadSize(Int32 payload_size)
    {
        if (payload_size < 0)
            throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT,
                            "Negative payload size {} received from client", payload_size);

        std::unique_ptr<TMessage> message = std::make_unique<TMessage>(payload_size);

        /// The message is parsed with a buffer limited to the declared payload size, so that parsing
        /// cannot read past the end of the message. Otherwise a client could declare a small message
        /// and then stream data without a terminator, making the parser consume it without a bound.
        LimitReadBuffer limited_in(*in, {.read_no_more = static_cast<size_t>(payload_size)});
        message->deserialize(limited_in);
        return message;
    }

    template<typename TMessage>
    std::unique_ptr<TMessage> receive()
    {
        std::unique_ptr<TMessage> message = std::make_unique<TMessage>();
        message->deserialize(*in);
        return message;
    }

    FrontMessageType receiveMessageType()
    {
        char type = 0;
        in->readStrict(type);
        return static_cast<FrontMessageType>(type);
    }

    template<typename TMessage>
    void send(TMessage & message, bool flush=false)
    {
        message.serialize(*out);
        if (flush)
            out->next();
    }

    template<typename TMessage>
    void send(TMessage && message, bool flush=false)
    {
        send(message, flush);
    }

    void send(char message, bool flush=false)
    {
        out->write(message);
        if (flush)
            out->next();
    }

    void dropMessage()
    {
        Int32 size = 0;
        readBinaryBigEndian(size, *in);
        if (size < 4)
            throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT,
                            "Wrong message length {} received from client, it must be at least 4", size);
        in->ignore(size - 4);
    }

    void flush()
    {
        out->next();
    }
};

/** Basic class for messages sent by client or server. */
class IMessage
{
public:
    virtual MessageType getMessageType() const = 0;

    virtual ~IMessage() = default;
};

class ISerializable
{
public:
    /** Should be overridden for sending the message */
    virtual void serialize(WriteBuffer & out) const = 0;

    /** Size of the message in bytes including message length part (4 bytes) */
    virtual Int32 size() const = 0;

    ISerializable() = default;

    ISerializable(const ISerializable &) = default;

    virtual ~ISerializable() = default;
};

class FrontMessage : public IMessage
{
public:
    /** Should be overridden for receiving the message
     * NB: This method should not read the first byte, which means the type of the message
     * (if type is provided for the message by the protocol).
     */
    virtual void deserialize(ReadBuffer & in) = 0;

protected:
    template <typename F>
    static void deserializePayload(ReadBuffer & in, std::string_view message_name, F && deserialize_payload)
    {
        Int32 size = 0;
        readBinaryBigEndian(size, in);
        if (size < 4)
            throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT,
                            "Wrong message length {} in {}, it must be at least 4", size, message_name);

        const size_t payload_size = static_cast<size_t>(size - 4);
        LimitReadBuffer payload_in(in, {.read_no_less = payload_size, .read_no_more = payload_size});
        try
        {
            deserialize_payload(payload_in);
        }
        catch (...)
        {
            /// Keep the stream aligned before the handler starts discarding messages through `Sync`.
            payload_in.ignore(payload_size - payload_in.count());
            throw;
        }

        const size_t unread_payload_bytes = payload_size - payload_in.count();
        payload_in.ignore(unread_payload_bytes);
        if (unread_payload_bytes != 0)
            throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT,
                            "Wrong message length {} in {}, it has {} unexpected trailing payload bytes",
                            size, message_name, unread_payload_bytes);
    }
};

class BackendMessage : public IMessage, public ISerializable
{};

class FirstMessage : public FrontMessage
{
public:
    Int32 payload_size;

    FirstMessage() = delete;
    explicit FirstMessage(int payload_size_) : payload_size(payload_size_) {}
};

class CancelRequest : public FirstMessage
{
public:
    Int32 process_id = 0;
    UInt32 secret_key = 0;

    explicit CancelRequest(int payload_size_) : FirstMessage(payload_size_) {}

    void deserialize(ReadBuffer & in) override
    {
        readBinaryBigEndian(process_id, in);
        readBinaryBigEndian(secret_key, in);
    }

    MessageType getMessageType() const override
    {
        return MessageType::CANCEL_REQUEST;
    }
};

class ErrorOrNoticeResponse : BackendMessage
{
public:
    enum Severity {ERROR = 0, FATAL = 1, PANIC = 2, WARNING = 3, NOTICE = 4, DEBUG = 5, INFO = 6, LOG = 7};

private:
    Severity severity;
    String sql_state;
    String message;

    String enum_to_string[8] = {"ERROR", "FATAL", "PANIC", "WARNING", "NOTICE", "DEBUG", "INFO", "LOG"};

    char isErrorOrNotice() const
    {
        switch (severity)
        {
            case ERROR:
            case FATAL:
            case PANIC:
                return 'E';
            case WARNING:
            case NOTICE:
            case DEBUG:
            case INFO:
            case LOG:
                return 'N';
        }
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "Unknown severity type {}", std::to_string(severity));
    }

public:
    ErrorOrNoticeResponse(const Severity & severity_, const String & sql_state_, const String & message_)
    : severity(severity_)
    , sql_state(sql_state_)
    , message(message_)
    {}

    void serialize(WriteBuffer & out) const override
    {
        out.write(isErrorOrNotice());
        Int32 sz = size();
        writeBinaryBigEndian(sz, out);

        out.write('S');
        writeNullTerminatedString(enum_to_string[severity], out);
        out.write('C');
        writeNullTerminatedString(sql_state, out);
        out.write('M');
        writeNullTerminatedString(message, out);

        out.write(0);
    }

    Int32 size() const override
    {
        // message length part + (1 + sizes of other fields + 1) + null byte in the end of the message
        return static_cast<Int32>(
            4 +
            (1 + enum_to_string[severity].size() + 1) +
            (1 + sql_state.size() + 1) +
            (1 + message.size() + 1) +
            1);
    }

    MessageType getMessageType() const override
    {
        if (isErrorOrNotice() == 'E')
            return MessageType::ERROR_RESPONSE;
        return MessageType::NOTICE_RESPONSE;
    }
};

class ReadyForQuery : BackendMessage
{
public:
    void serialize(WriteBuffer &out) const override
    {
        out.write('Z');
        writeBinaryBigEndian(size(), out);
        // 'I' means that we are not in a transaction block. We use it here, because ClickHouse doesn't support transactions.
        out.write('I');
    }

    Int32 size() const override
    {
        return 4 + 1;
    }

    MessageType getMessageType() const override
    {
        return MessageType::READY_FOR_QUERY;
    }
};

class Terminate : FrontMessage
{
public:
    void deserialize(ReadBuffer & in) override
    {
        in.ignore(4);
    }

    MessageType getMessageType() const override
    {
        return MessageType::TERMINATE;
    }
};

class StartupMessage : FirstMessage
{
public:
    String user;
    String database;
    // includes username, may also include database and other runtime parameters
    UnorderedMapWithMemoryTracking<String, String> parameters;

    explicit StartupMessage(Int32 payload_size_) : FirstMessage(payload_size_) {}

    void deserialize(ReadBuffer & in) override
    {
        Int32 ps = payload_size - 1;
        while (ps > 0)
        {
            String parameter_name;
            String parameter_value;
            readNullTerminated(parameter_name, in);
            readNullTerminated(parameter_value, in);
            ps -= parameter_name.size() + 1;
            ps -= parameter_value.size() + 1;

            if (parameter_name == "user")
            {
                user = parameter_value;
            }
            else if (parameter_name == "database")
            {
                database = parameter_value;
            }

            parameters.insert({std::move(parameter_name), std::move(parameter_value)});

            /// `payload_size` is the declared size of the message and never changes, so the check
            /// has to be made against the remaining size instead.
            if (ps < 0)
            {
                throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT,
                                "Size of payload is larger than one declared in the message of type {}.",
                                static_cast<UInt64>(getMessageType()));
            }
        }
        in.ignore();
    }

    MessageType getMessageType() const override
    {
        return MessageType::STARTUP_MESSAGE;
    }
};

class AuthenticationCleartextPassword : public Messaging::BackendMessage
{
public:
    void serialize(WriteBuffer & out) const override
    {
        out.write('R');
        writeBinaryBigEndian(size(), out);
        writeBinaryBigEndian(static_cast<Int32>(3), out); // specifies that a clear-text password is required (by protocol)
    }

    Int32 size() const override
    {
        // length of message + special int32
        return 4 + 4;
    }

    MessageType getMessageType() const override
    {
        return MessageType::AUTHENTICATION_CLEARTEXT_PASSWORD;
    }
};

class AuthenticationSASL : public Messaging::BackendMessage
{
public:
    static constexpr std::string_view supported_method = "SCRAM-SHA-256";

    void serialize(WriteBuffer & out) const override
    {
        out.write('R');
        writeBinaryBigEndian(size(), out);
        writeBinaryBigEndian(static_cast<Int32>(10), out);
        writeNullTerminatedString(String(supported_method), out);
        out.write(0);
    }

    Int32 size() const override
    {
        return 4 + 4 + supported_method.size() + 1 + 1;
    }

    MessageType getMessageType() const override
    {
        return MessageType::AUTHENTICATION_SASL;
    }
};

class SASLInitialResponse : public Messaging::FrontMessage
{
public:
    String auth_method;
    String sasl_mechanism;

    void deserialize(ReadBuffer & in) override
    {
        UInt8 message_type = 0;
        readBinaryBigEndian(message_type, in);
        Int32 size = 0;
        readBinaryBigEndian(size, in);
        readNullTerminated(auth_method, in);
        Int32 size_sasl_mechanism = 0;
        readBinaryBigEndian(size_sasl_mechanism, in);
        /// -1 is the protocol sentinel for "no initial response"; any other negative value is malformed.
        if (size_sasl_mechanism < -1)
            throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT,
                            "Wrong SASL mechanism length {} in SASLInitialResponse, it must not be less than -1", size_sasl_mechanism);
        if (size_sasl_mechanism > 0)
        {
            sasl_mechanism.resize(size_sasl_mechanism);
            in.readStrict(sasl_mechanism.data(), size_sasl_mechanism);
        }
    }

    MessageType getMessageType() const override
    {
        return MessageType::SASL_INITIAL_RESPONSE;
    }
};

class AuthenticationSASLContinue : public Messaging::BackendMessage
{
public:
    String data;

    explicit AuthenticationSASLContinue(const String & data_)
        : data(data_)
    {
    }

    void serialize(WriteBuffer & out) const override
    {
        out.write('R');
        writeBinaryBigEndian(size(), out);
        writeBinaryBigEndian(static_cast<Int32>(11), out);
        out.write(data.data(), data.size());
    }

    Int32 size() const override
    {
        return 4 + 4 + static_cast<Int32>(data.size());
    }

    MessageType getMessageType() const override
    {
        return MessageType::AUTHENTICATION_SASL_CONTINUE;
    }
};

class SASLResponse : public Messaging::FrontMessage
{
public:
    String sasl_mechanism;

    void deserialize(ReadBuffer & in) override
    {
        UInt8 message_type = 0;
        readBinaryBigEndian(message_type, in);
        Int32 size = 0;
        readBinaryBigEndian(size, in);
        if (size < 4)
            throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT,
                            "Wrong message length {} in SASLResponse, it must be at least 4", size);
        sasl_mechanism.resize(size - 4);
        in.readStrict(sasl_mechanism.data(), size - 4);
    }

    MessageType getMessageType() const override
    {
        return MessageType::SASL_RESPONSE;
    }
};

class AuthenticationOk : BackendMessage
{
public:
    void serialize(WriteBuffer & out) const override
    {
        out.write('R');
        writeBinaryBigEndian(size(), out);
        writeBinaryBigEndian(0, out); // specifies that the authentication was successful (by protocol)
    }

    Int32 size() const override
    {
        // length of message + special int32
        return 4 + 4;
    }

    MessageType getMessageType() const override
    {
        return MessageType::AUTHENTICATION_OK;
    }
};

class PasswordMessage : FrontMessage
{
public:
    String password;

    void deserialize(ReadBuffer & in) override
    {
        Int32 sz = 0;
        readBinaryBigEndian(sz, in);
        readNullTerminated(password, in);
    }

    MessageType getMessageType() const override
    {
        return MessageType::PASSWORD_MESSAGE;
    }
};

class ParameterStatus : BackendMessage
{
private:
    String name;
    String value;

public:
    ParameterStatus(String name_, String value_)
    : name(name_)
    , value(value_)
    {}

    void serialize(WriteBuffer & out) const override
    {
        out.write('S');
        writeBinaryBigEndian(size(), out);
        writeNullTerminatedString(name, out);
        writeNullTerminatedString(value, out);
    }

    Int32 size() const override
    {
        return static_cast<Int32>(4 + name.size() + 1 + value.size() + 1);
    }

    MessageType getMessageType() const override
    {
        return MessageType::PARAMETER_STATUS;
    }
};

class BackendKeyData : BackendMessage
{
private:
    Int32 process_id;
    UInt32 secret_key;

public:
    BackendKeyData(Int32 process_id_, UInt32 secret_key_)
    : process_id(process_id_)
    , secret_key(secret_key_)
    {}

    void serialize(WriteBuffer & out) const override
    {
        out.write('K');
        writeBinaryBigEndian(size(), out);
        writeBinaryBigEndian(process_id, out);
        writeBinaryBigEndian(secret_key, out);
    }

    Int32 size() const override
    {
        return 4 + 4 + 4;
    }

    MessageType getMessageType() const override
    {
        return MessageType::BACKEND_KEY_DATA;
    }
};

class Query : FrontMessage
{
public:
    String query;

    void deserialize(ReadBuffer & in) override
    {
        deserializePayload(in, "Query message", [this](ReadBuffer & payload_in)
        {
            readNullTerminated(query, payload_in);
        });
    }

    MessageType getMessageType() const override
    {
        return MessageType::QUERY;
    }
};

class ParseQuery : FrontMessage
{
public:
    String function_name;
    String sql_query;
    Int16 num_params{};
    /// Parameter type OIDs from `Parse`; 0 requests inference.
    VectorWithMemoryTracking<Int32> parameter_types;

    void deserialize(ReadBuffer & in) override
    {
        deserializePayload(in, "Parse message", [this](ReadBuffer & payload_in)
        {
            readNullTerminated(function_name, payload_in);
            readNullTerminated(sql_query, payload_in);
            readBinaryBigEndian(num_params, payload_in);
            if (num_params < 0)
                throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT,
                                "Wrong parameter count {} in Parse message, it must not be negative", num_params);
            parameter_types.reserve(num_params);
            Int32 oid_param = 0;
            for (int i = 0; i < num_params; ++i)
            {
                readBinaryBigEndian(oid_param, payload_in);
                parameter_types.push_back(oid_param);
            }
        });
    }

    MessageType getMessageType() const override
    {
        return MessageType::PARSE;
    }
};

class ParseQueryComplete : BackendMessage
{
public:
    ParseQueryComplete() = default;

    void serialize(WriteBuffer & out) const override
    {
        out.write('1');
        writeBinaryBigEndian(size(), out);
    }

    Int32 size() const override
    {
        return 4;
    }

    MessageType getMessageType() const override
    {
        return MessageType::PARSE_COMPLETE;
    }
};

class BindQuery : FrontMessage
{
public:
    String portal_name;
    String function_name;
    /// Raw `Bind` values; `std::nullopt` represents protocol `NULL`.
    VectorWithMemoryTracking<std::optional<String>> parameters;
    Int16 num_params{};
    /// Set after reading the full message; `attachBindQuery` rejects it.
    bool has_binary_format_param = false;

    void deserialize(ReadBuffer & in) override
    {
        deserializePayload(in, "Bind message", [this](ReadBuffer & payload_in)
        {
            readNullTerminated(portal_name, payload_in);
            readNullTerminated(function_name, payload_in);

            /// Read all format codes before rejecting binary values to preserve stream alignment.
            Int16 num_format_params = 0;
            readBinaryBigEndian(num_format_params, payload_in);
            if (num_format_params < 0)
                throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT,
                                "Wrong parameter format code count {} in Bind message, it must not be negative", num_format_params);
            Int16 format_param = 0;
            for (Int16 i = 0; i < num_format_params; ++i)
            {
                readBinaryBigEndian(format_param, payload_in);
                if (format_param != 0)
                    has_binary_format_param = true;
            }
            readBinaryBigEndian(num_params, payload_in);
            if (num_params < 0)
                throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT,
                                "Wrong parameter count {} in Bind message, it must not be negative", num_params);
            for (int i = 0; i < num_params; ++i)
            {
                Int32 sz_param = 0;
                readBinaryBigEndian(sz_param, payload_in);
                /// -1 is the protocol sentinel for a NULL parameter and no value bytes follow;
                /// any other negative value is malformed.
                if (sz_param < -1)
                    throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT,
                                    "Wrong parameter length {} in Bind message, it must not be less than -1", sz_param);
                if (sz_param == -1)
                {
                    parameters.emplace_back(std::nullopt);
                    continue;
                }
                String current_param(sz_param, 0);
                payload_in.readStrict(current_param.data(), sz_param);
                parameters.push_back(std::move(current_param));
            }

            /// Consume result format codes; this implementation always returns text.
            Int16 num_format_params_result = 0;
            readBinaryBigEndian(num_format_params_result, payload_in);
            if (num_format_params_result < 0)
                throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT,
                                "Wrong result format code count {} in Bind message, it must not be negative", num_format_params_result);
            Int16 format_param_result = 0;
            for (Int16 i = 0; i < num_format_params_result; ++i)
                readBinaryBigEndian(format_param_result, payload_in);
        });
    }

    MessageType getMessageType() const override
    {
        return MessageType::BIND;
    }
};

class BindQueryComplete : BackendMessage
{
public:
    BindQueryComplete() = default;

    void serialize(WriteBuffer & out) const override
    {
        out.write('2');
        writeBinaryBigEndian(size(), out);
    }

    Int32 size() const override
    {
        return 4;
    }

    MessageType getMessageType() const override
    {
        return MessageType::BIND_COMPLETE;
    }
};

class DescribeQuery : FrontMessage
{
public:
    char describe{};
    String function_name;

    void deserialize(ReadBuffer & in) override
    {
        deserializePayload(in, "Describe message", [this](ReadBuffer & payload_in)
        {
            payload_in.readStrict(&describe, 1);
            readNullTerminated(function_name, payload_in);
        });
    }

    MessageType getMessageType() const override
    {
        return MessageType::DESCRIBE;
    }

};

class ExecuteQuery : FrontMessage
{
public:
    String portal_name;
    Int32 max_rows{};

    void deserialize(ReadBuffer & in) override
    {
        deserializePayload(in, "Execute message", [this](ReadBuffer & payload_in)
        {
            readNullTerminated(portal_name, payload_in);
            readBinaryBigEndian(max_rows, payload_in);
        });
    }

    MessageType getMessageType() const override
    {
        return MessageType::BIND;
    }

};

class EmptyQueryResponse : public BackendMessage
{
public:
    void serialize(WriteBuffer & out) const override
    {
        out.write('I');
        writeBinaryBigEndian(size(), out);
    }

    Int32 size() const override
    {
        return 4;
    }

    MessageType getMessageType() const override
    {
        return MessageType::EMPTY_QUERY_RESPONSE;
    }
};

enum class FormatCode : Int16
{
    TEXT = 0,
    BINARY = 1,
};

class CloseQuery : FrontMessage
{
public:
    String function_name;
    /// 'S' for prepared statement, 'P' for portal
    char close_target = 0;

    void deserialize(ReadBuffer & in) override
    {
        deserializePayload(in, "Close message", [this](ReadBuffer & payload_in)
        {
            Int8 byte = 0;
            readBinaryBigEndian(byte, payload_in);
            close_target = static_cast<char>(byte);
            readNullTerminated(function_name, payload_in);
        });
    }

    MessageType getMessageType() const override
    {
        return MessageType::CLOSE;
    }
};

class CloseQueryComplete : BackendMessage
{
public:
    CloseQueryComplete() = default;

    void serialize(WriteBuffer & out) const override
    {
        /// 'C' is `CommandComplete`; `CloseComplete` is tagged with '3' per
        /// the PostgreSQL message protocol.
        out.write('3');
        writeBinaryBigEndian(size(), out);
    }

    Int32 size() const override
    {
        return 4;
    }

    MessageType getMessageType() const override
    {
        return MessageType::CLOSE_COMPLETE;
    }
};

class SyncQuery : FrontMessage
{
public:
    void deserialize(ReadBuffer & in) override
    {
        Int32 size = 0;
        readBinaryBigEndian(size, in);
        if (size != 4)
            throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT,
                            "Wrong message length {} in Sync message, it must be 4", size);
    }

    MessageType getMessageType() const override
    {
        return MessageType::SYNC;
    }
};

class FieldDescription : ISerializable
{
private:
    const String & name;
    ColumnTypeSpec type_spec;
    FormatCode format_code;

public:
    FieldDescription(const String & name_, const DataTypePtr & data_type, FormatCode format_code_ = FormatCode::TEXT)
    : name(name_)
    , type_spec(convertDataTypeToPostgresColumnTypeSpec(data_type))
    , format_code(format_code_)
    {}

    void serialize(WriteBuffer & out) const override
    {
        writeNullTerminatedString(name, out);
        writeBinaryBigEndian(static_cast<Int32>(0), out);
        writeBinaryBigEndian(static_cast<Int16>(0), out);
        writeBinaryBigEndian(static_cast<Int32>(type_spec.type), out);
        writeBinaryBigEndian(type_spec.len, out);
        writeBinaryBigEndian(static_cast<Int32>(-1), out);
        writeBinaryBigEndian(static_cast<Int16>(format_code), out);
    }

    Int32 size() const override
    {
        // size of name (C string)
        // + object ID of the table (Int32 and always zero) + attribute number of the column (Int16 and always zero)
        // + type object id (Int32) + data type size (Int16)
        // + type modifier (Int32 and always -1) + format code (Int16)
        return static_cast<Int32>((name.size() + 1) + 4 + 2 + 4 + 2 + 4 + 2);
    }
};

class RowDescription : BackendMessage
{
private:
    const VectorWithMemoryTracking<FieldDescription> & fields_descr;

public:
    explicit RowDescription(const VectorWithMemoryTracking<FieldDescription> & fields_descr_) : fields_descr(fields_descr_) {}

    void serialize(WriteBuffer & out) const override
    {
        out.write('T');
        writeBinaryBigEndian(size(), out);
        writeBinaryBigEndian(static_cast<Int16>(fields_descr.size()), out);
        for (const FieldDescription & field : fields_descr)
            field.serialize(out);
    }

    Int32 size() const override
    {
        Int32 sz = 4 + 2; // size of message + number of fields
        for (const FieldDescription & field : fields_descr)
            sz += field.size();
        return sz;
    }

    MessageType getMessageType() const override
    {
        return MessageType::ROW_DESCRIPTION;
    }
};

class StringField : public ISerializable
{
private:
    String str;
public:
    explicit StringField(String str_) : str(str_) {}

    void serialize(WriteBuffer & out) const override
    {
        writeString(str, out);
    }

    Int32 size() const override
    {
        return static_cast<Int32>(str.size());
    }
};

class NullField : public ISerializable
{
public:
    void serialize(WriteBuffer & /* out */) const override {}

    Int32 size() const override
    {
        return -1;
    }
};

class DataRow : BackendMessage
{
private:
    const VectorWithMemoryTracking<std::shared_ptr<ISerializable>> & row;

public:
    explicit DataRow(const VectorWithMemoryTracking<std::shared_ptr<ISerializable>> & row_) : row(row_) {}

    void serialize(WriteBuffer & out) const override
    {
        out.write('D');
        writeBinaryBigEndian(size(), out);
        writeBinaryBigEndian(static_cast<Int16>(row.size()), out);
        for (const std::shared_ptr<ISerializable> & field : row)
        {
            Int32 sz = field->size();
            writeBinaryBigEndian(sz, out);
            if (sz > 0)
                field->serialize(out);
        }
    }

    Int32 size() const override
    {
        Int32 sz = 4 + 2; // size of message + number of fields
        /// If values is NULL, field size is -1 and data not added.
        for (const std::shared_ptr<ISerializable> & field : row)
            sz += 4 + (field->size() > 0 ? field->size() : 0);
        return sz;
    }

    MessageType getMessageType() const override
    {
        return MessageType::DATA_ROW;
    }
};

class CopyDataQuery : FrontMessage
{
public:
    String query;

    void deserialize(ReadBuffer & in) override
    {
        Int32 sz = 0;
        readBinaryBigEndian(sz, in);
        readNullTerminated(query, in);
    }

    MessageType getMessageType() const override
    {
        return MessageType::COPY_DATA;
    }
};

class CopyInResponse : public BackendMessage
{
public:
    void serialize(WriteBuffer & out) const override
    {
        out.write('G');
        writeBinaryBigEndian(size(), out);
        writeBinaryBigEndian(static_cast<char>(0), out);
        writeBinaryBigEndian(static_cast<Int16>(0), out);
    }

    Int32 size() const override
    {
        return 4 + 1 + 2;
    }

    MessageType getMessageType() const override
    {
        return MessageType::COPY_IN_RESPONSE;
    }
};

class CopyOutResponse : public BackendMessage
{
    int num_columns;
public:
    explicit CopyOutResponse(int num_columns_ = 1)
        : num_columns(num_columns_)
    {
    }

    void serialize(WriteBuffer & out) const override
    {
        out.write('H');
        writeBinaryBigEndian(size(), out);
        writeBinaryBigEndian(static_cast<Int8>(FormatCode::TEXT), out);
        writeBinaryBigEndian(static_cast<Int16>(num_columns), out);
        for (int i = 0; i < num_columns; ++i)
            writeBinaryBigEndian(static_cast<Int16>(FormatCode::TEXT), out);
    }

    Int32 size() const override
    {
        return 4 + 1 + 2 + 2 * num_columns;
    }

    MessageType getMessageType() const override
    {
        return MessageType::COPY_OUT_RESPONSE;
    }
};

class CopyInData : FrontMessage
{
public:
    String query;

    void deserialize(ReadBuffer & in) override
    {
        Int32 sz = 0;
        readBinaryBigEndian(sz, in);
        if (sz < static_cast<Int32>(sizeof(Int32)))
            throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT,
                            "Wrong message length {} in CopyData, it must be at least 4", sz);
        query.reserve(sz - sizeof(Int32));
        for (size_t i = 0; i < sz - sizeof(Int32); ++i)
        {
            char byte = 0;
            readBinary(byte, in);
            query.push_back(byte);
        }
    }

    MessageType getMessageType() const override
    {
        return MessageType::COPY_DATA;
    }
};

class CopyDone : FrontMessage
{
public:
    void deserialize(ReadBuffer & in) override
    {
        Int32 size = 0;
        readBinaryBigEndian(size, in);
        if (size != 4)
            throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT,
                            "Wrong message length {} in CopyDone message, it must be 4", size);
    }

    MessageType getMessageType() const override
    {
        return MessageType::COPY_DONE;
    }
};

class CopyOutData : public BackendMessage
{
    VectorWithMemoryTracking<char> data;
public:
    explicit CopyOutData(VectorWithMemoryTracking<char> data_)
        : data(data_)
    {
    }

    void serialize(WriteBuffer & out) const override
    {
        writeBinaryBigEndian('d', out);
        writeBinaryBigEndian(size(), out);
        out.write(data.data(), data.size());
    }

    Int32 size() const override
    {
        return 4 + static_cast<Int32>(data.size());
    }

    MessageType getMessageType() const override
    {
        return MessageType::COPY_DATA;
    }
};

class CopyDataResponse : BackendMessage
{
public:
    void serialize(WriteBuffer & out) const override
    {
        out.write('d');
        writeBinaryBigEndian(size(), out);
    }

    Int32 size() const override
    {
        return 4;
    }

    MessageType getMessageType() const override
    {
        return MessageType::COPY_DATA;
    }
};

class CopyCompletionResponse : BackendMessage
{
public:
    void serialize(WriteBuffer & out) const override
    {
        out.write('c');
        writeBinaryBigEndian(size(), out);
    }

    Int32 size() const override
    {
        return 4;
    }

    MessageType getMessageType() const override
    {
        return MessageType::COPY_DONE;
    }
};


/**
* CommandComplete message for PostgreSQL wire protocol
* Reference: https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-COMMANDCOMPLETE
*/
class CommandComplete : BackendMessage
{
public:
    enum Command
    {
        BEGIN = 0,
        COMMIT = 1,
        INSERT = 2,
        DELETE = 3,
        UPDATE = 4,
        SELECT = 5,
        MOVE = 6,
        FETCH = 7,
        COPY = 8,
        PREPARE = 9,
        CREATE_TABLE = 10,
        CREATE_DATABASE = 11,
        DROP_TABLE = 12,
        DROP_DATABASE = 13,
        ALTER_TABLE = 14,
        TRUNCATE = 15,
        USE = 16,
        SET = 17
    };
private:
    String enum_to_string[18] =
    {
        "BEGIN", "COMMIT", "INSERT", "DELETE", "UPDATE", "SELECT", "MOVE", "FETCH", "COPY", "PREPARE",
        "CREATE TABLE", "CREATE DATABASE", "DROP TABLE", "DROP DATABASE", "ALTER TABLE",
        "TRUNCATE", "USE", "SET"
    };

    String value;

public:
    CommandComplete(Command cmd_, UInt64 rows_count_)
    {
        value = enum_to_string[cmd_];

        // Commands that include row count according to PostgreSQL protocol
        // Note: UPDATE and DELETE in ClickHouse always return 0 because ClickHouse uses
        // lightweight deletes/updates that don't track affected rows in the same way as PostgreSQL
        bool include_row_count = (cmd_ == Command::INSERT || cmd_ == Command::DELETE ||
                                  cmd_ == Command::UPDATE || cmd_ == Command::SELECT ||
                                  cmd_ == Command::MOVE || cmd_ == Command::FETCH || cmd_ == Command::COPY);

        if (include_row_count)
        {
            String add = " ";
            if (cmd_ == Command::INSERT)
                add = " 0 ";  // OID (always 0 for ClickHouse tables)
            value += add + std::to_string(rows_count_);
        }
    }

    /// Construct a CommandComplete carrying an explicit command tag verbatim.
    /// Used for driver-specific commands (e.g. `RESET ALL`, `UNLISTEN *`) that
    /// ClickHouse accepts as no-ops and for which no row count applies.
    explicit CommandComplete(String tag_)
        : value(std::move(tag_))
    {
    }

    void serialize(WriteBuffer & out) const override
    {
        out.write('C');
        writeBinaryBigEndian(size(), out);
        writeNullTerminatedString(value, out);
    }

    Int32 size() const override
    {
        return static_cast<Int32>(4 + value.size() + 1);
    }

    MessageType getMessageType() const override
    {
        return MessageType::COMMAND_COMPLETE;
    }

    // Extract and normalize prefix: skip leading spaces, collapse multiple spaces to one, convert to uppercase on the fly
    static String extractNormalizedPrefix(const String & query, size_t max_len)
    {
        String prefix;
        prefix.reserve(max_len);

        bool prev_was_space = true;

        for (size_t i = 0; i < query.size() && prefix.size() < max_len; ++i)
        {
            if (std::isspace(query[i]))
            {
                if (!prev_was_space)
                {
                    prefix.push_back(' ');
                    prev_was_space = true;
                }
            }
            else
            {
                prefix.push_back(static_cast<char>(std::toupper(query[i])));
                prev_was_space = false;
            }
        }

        return prefix;
    }

    static Command classifyQuery(const String & query)
    {
        static const VectorWithMemoryTracking<std::pair<String, Command>> query_patterns = {
            {"CREATE TEMPORARY TABLE", Command::CREATE_TABLE},
            {"CREATE TABLE", Command::CREATE_TABLE},
            {"CREATE DATABASE", Command::CREATE_DATABASE},
            {"DROP TABLE", Command::DROP_TABLE},
            {"DROP DATABASE", Command::DROP_DATABASE},
            {"ALTER TABLE", Command::ALTER_TABLE},
            {"TRUNCATE", Command::TRUNCATE},
            {"BEGIN", Command::BEGIN},
            {"COMMIT", Command::COMMIT},
            {"INSERT", Command::INSERT},
            {"DELETE", Command::DELETE},
            {"UPDATE", Command::UPDATE},
            {"SELECT", Command::SELECT},
            {"MOVE", Command::MOVE},
            {"FETCH", Command::FETCH},
            {"COPY", Command::COPY},
            {"PREPARE", Command::PREPARE},
            {"USE", Command::USE}, // ClickHouse-specific, not have in PostgreSQL
            {"SET", Command::SET},
        };

        // Calculate max pattern length from query_patterns
        static const size_t MAX_PATTERN_LEN = []()
        {
            size_t max_len = 0;
            for (const auto & [pattern, _] : query_patterns)
                max_len = std::max(pattern.size(), max_len);
            return max_len;
        }();

        String prefix = extractNormalizedPrefix(query, MAX_PATTERN_LEN);

        for (const auto & [pattern, command] : query_patterns)
        {
            if (prefix.starts_with(pattern))
                return command;
        }

        return Command::SELECT;
    }
};

}

namespace PGAuthentication
{

class AuthenticationMethod
{
protected:
    static void setPassword(
        const String & user_name,
        const String & password,
        Session & session,
        const Poco::Net::SocketAddress & address)
    {
        session.authenticate(user_name, password, address);
    }

public:
    virtual bool isSupportedForUser(const String &, Session &) const
    {
        return true;
    }

    virtual void authenticate(
        const String & user_name,
        Session & session,
        Messaging::MessageTransport & mt,
        const Poco::Net::SocketAddress & address) = 0;

    virtual AuthenticationType getType() const = 0;

    virtual ~AuthenticationMethod() = default;
};

class NoPasswordAuth : public AuthenticationMethod
{
public:
    void authenticate(
        const String & user_name,
        Session & session,
        [[maybe_unused]] Messaging::MessageTransport & mt,
        const Poco::Net::SocketAddress & address) override
    {
        setPassword(user_name, "", session, address);
    }

    AuthenticationType getType() const override
    {
        return AuthenticationType::NO_PASSWORD;
    }
};

class CleartextPasswordAuth : public AuthenticationMethod
{
public:
    void authenticate(
        const String & user_name,
        Session & session,
        Messaging::MessageTransport & mt,
        const Poco::Net::SocketAddress & address) override
    {
        mt.send(Messaging::AuthenticationCleartextPassword(), true);

        Messaging::FrontMessageType type = mt.receiveMessageType();
        if (type == Messaging::FrontMessageType::PASSWORD_MESSAGE)
        {
            std::unique_ptr<Messaging::PasswordMessage> password = mt.receive<Messaging::PasswordMessage>();
            setPassword(user_name, password->password, session, address);
        }
        else
            throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT,
                    "Client sent wrong message or closed the connection. Message byte was {}.",
                    static_cast<Int32>(type));
    }

    AuthenticationType getType() const override
    {
        return AuthenticationType::PLAINTEXT_PASSWORD;
    }
};

class ScrambleSHA256Auth : public AuthenticationMethod
{
    enum class ScramSaltKind : uint8_t
    {
        /// The user has no `scram_sha256_password` at all.
        NoScram,
        /// Live verifiers sharing a single salt, which PostgreSQL SCRAM can offer on the wire.
        Live,
        /// Only expired verifiers: the salt is still usable to run the exchange and report invalid credentials.
        ExpiredOnly,
        /// A live verifier that PostgreSQL SCRAM cannot represent: a second factor, several different salts to
        /// choose from, or another method that narrows the session and which a SCRAM client proof cannot be checked
        /// against.
        UnsupportedConfiguration,
    };

    struct ScramSalt
    {
        ScramSaltKind kind = ScramSaltKind::NoScram;
        String salt;
        /// The user has another live authentication method that the PostgreSQL protocol can offer on the wire.
        bool has_live_alternative = false;
    };

    /// A method that limits the session (`GRANTS`) or its lifetime (`VALID UNTIL`) takes part in the fail-close
    /// combination of `IAccessStorage::authenticateImpl` when it accepts the same credential.
    static bool methodNarrowsSession(const AuthenticationData & auth_method)
    {
        return auth_method.getValidUntil() != 0 || !auth_method.getGrants().structurallyEmpty();
    }

    ScramSalt getScramSalt(const String & user_name, Session & session) const
    {
        const auto & access_control = session.globalContext()->getAccessControl();
        const time_t now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());

        ScramSalt result;
        std::optional<String> expired_scram_salt;
        std::optional<String> live_scram_salt;
        bool unsupported_configuration = false;

        if (auto id = access_control.find<User>(user_name))
        {
            if (auto user = access_control.tryRead<User>(*id))
            {
                /// First pass: choose the salt that `AuthenticationSASLContinue` would send.
                for (const auto & auth_method : user->authentication_methods)
                {
                    if (auth_method.getType() != AuthenticationType::SCRAM_SHA256_PASSWORD)
                        continue;

                    const auto valid_until = auth_method.getValidUntil();
                    if (valid_until && now > valid_until)
                    {
                        if (!expired_scram_salt)
                            expired_scram_salt = auth_method.getSalt();
                        continue;
                    }

                    /// PostgreSQL SCRAM cannot represent a second factor.
                    if (auth_method.getOneTimePassword())
                    {
                        unsupported_configuration = true;
                        continue;
                    }

                    /// Several live verifiers are representable as long as they agree on the salt: the exchange sends a
                    /// single salt in `AuthenticationSASLContinue`, and the client proof derived from it is then checked
                    /// against every stored salted password of the user. Differing salts cannot be represented, because
                    /// only one of them can be sent on the wire.
                    if (live_scram_salt && *live_scram_salt != auth_method.getSalt())
                    {
                        unsupported_configuration = true;
                        continue;
                    }

                    live_scram_salt = auth_method.getSalt();
                }

                /// Second pass: look for the other authentication methods of the user.
                for (const auto & auth_method : user->authentication_methods)
                {
                    const auto type = auth_method.getType();
                    const auto valid_until = auth_method.getValidUntil();
                    const bool expired = valid_until && now > valid_until;

                    /// The only other authentication methods the PostgreSQL protocol can offer on the wire.
                    if (!expired
                        && (type == AuthenticationType::NO_PASSWORD || type == AuthenticationType::PLAINTEXT_PASSWORD))
                        result.has_live_alternative = true;

                    if (!live_scram_salt)
                        continue;

                    /// `IAccessStorage::authenticateImpl` fails close for ambiguous credentials: when the same
                    /// credential is accepted by several methods, the session is limited to the intersection of their
                    /// `GRANTS` and expires at the earliest of their `VALID UNTIL`, even when that moment has already
                    /// passed. That scan re-checks the credential against the other methods, but a SCRAM client proof
                    /// can only be checked against a `scram_sha256_password` method that uses the very salt sent in
                    /// `AuthenticationSASLContinue`: the proof is derived from the salted password and the salt is part
                    /// of the authentication message. A method that could narrow the session but cannot be matched by
                    /// the proof would silently drop out of the combination, so a password shared with such a method
                    /// would be accepted over PostgreSQL while the native protocol rejects it as expired or grants it
                    /// less. Refuse to run the exchange in that case instead of authenticating with weaker checks.
                    if (!methodNarrowsSession(auth_method))
                        continue;
                    /// Methods verified against an external system never take part in the combination.
                    if (!authenticationTypeIsVerifiedLocally(type))
                        continue;
                    if (type == AuthenticationType::SCRAM_SHA256_PASSWORD && auth_method.getSalt() == *live_scram_salt)
                        continue;

                    unsupported_configuration = true;
                }
            }
        }

        if (unsupported_configuration)
            result.kind = ScramSaltKind::UnsupportedConfiguration;
        else if (live_scram_salt)
        {
            result.kind = ScramSaltKind::Live;
            result.salt = *live_scram_salt;
        }
        else if (expired_scram_salt)
        {
            result.kind = ScramSaltKind::ExpiredOnly;
            result.salt = *expired_scram_salt;
        }

        return result;
    }

    static size_t findPatternPosition(const String & key, const String & pattern)
    {
        size_t pos = key.size();
        for (size_t i = 0; i + 1 < key.size(); ++i)
        {
            if (key.substr(i, 2) == pattern)
            {
                pos = i + 2;
                break;
            }
        }
        if (pos == key.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Client response should contain nonce");

        return pos;
    }

    static String parseResponse(const String & key, const String & pattern)
    {
        String result;
        auto pos = findPatternPosition(key, pattern);

        while (pos < key.size() && key[pos] != ',')
        {
            result.push_back(key[pos]);
            ++pos;
        }
        return result;
    }

    static String parseClientNonce(const String & key)
    {
        return parseResponse(key, "r=");
    }

    static String parseProof(const String & key)
    {
        return parseResponse(key, "p=");
    }

    static String parseUsername(const String & key)
    {
        return parseResponse(key, "n=");
    }

    static size_t findProofPosition(const String & key)
    {
        return findPatternPosition(key, "p=");
    }

public:
    bool isSupportedForUser(const String & user_name, Session & session) const override
    {
        const auto scram_salt = getScramSalt(user_name, session);

        if (scram_salt.kind == ScramSaltKind::NoScram)
            return false;
        if (scram_salt.kind == ScramSaltKind::Live)
            return true;

        /// Neither an expired verifier nor a configuration that the protocol cannot represent can lead to a successful
        /// login. Select SCRAM in these cases only if nothing else can succeed either, so that an expired verifier is
        /// reported as invalid credentials and an unsupported configuration is reported as such, instead of shadowing
        /// a method that would have worked.
        return !scram_salt.has_live_alternative;
    }

    static String generateNonce()
    {
        static constexpr size_t nonce_length = 16;

        String scramble;
        scramble.resize(nonce_length + 1, 0);
        Poco::RandomInputStream generator;

        for (size_t i = 0; i < nonce_length; ++i)
        {
            generator >> scramble[i];
            scramble[i] %= 13;
            scramble[i] += 'n';
        }

        return base64Encode(scramble);
    }

    /**
     * This function implements the client-side logic for the SCRAM-SHA-256
     * authentication protocol. It exchanges messages with the server to
     * establish a secure connection.
     *
     * The function constructs the authentication message (auth_message) by
     * concatenating the client-first-message-bare, the server-first-message,
     * and the client-final-message-without-proof.  The messages exchanged with the server are:
     * - Messaging::AuthenticationSASL: Initial SASL authentication request.
     * - Messaging::AuthenticationSASLContinue:  SASL continue message.
     * - Messaging::SASLResponse: Generic SASL response from the server.
     *
     * **SCRAM-SHA-256 Message Formats:**
     *
     *  - **Client First Message:** y,,n=<username>,r=<client_nonce>
     *    - n: Attribute for the username.
     *    - r: Attribute for the client-generated nonce.
     *
     *  - **Server First Message:** r=<client_nonce><server_nonce>,s=<salt>,i=<iterations>
     *    - r: Attribute for the combined client and server nonces.
     *    - s: Attribute for the salt.
     *    - i: Attribute for the number of iterations.
     *
     *  - **Client Final Message:** c=<channel_binding>,r=<combined_nonce>,p=<client_proof>
     *    - c: Attribute for channel binding data (often empty).
     *    - r: Attribute for the combined client and server nonces.
     *    - p: Attribute for the client's computed proof.
     *
     * The function retrieves the salt from the user's authentication methods.
     * It then computes the client proof and uses it to authenticate the session.
     */
    void authenticate(
        const String & user_name,
        Session & session,
        Messaging::MessageTransport & mt,
        const Poco::Net::SocketAddress & address) override
    {
        static constexpr int num_iterations = 4096;

        String auth_message;

        const auto scram_salt = getScramSalt(user_name, session);
        if (scram_salt.kind == ScramSaltKind::UnsupportedConfiguration || scram_salt.kind == ScramSaltKind::NoScram)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "PostgreSQL protocol does not support this `scram_sha256_password` authentication configuration");

        mt.send(Messaging::AuthenticationSASL(), true);
        auto rsp = mt.receive<Messaging::SASLInitialResponse>();

        auto server_nonce = generateNonce();
        auto client_nonce = parseClientNonce(rsp->sasl_mechanism);
        auth_message += fmt::format("n={},r={}", parseUsername(rsp->sasl_mechanism), client_nonce);
        auto nonce = client_nonce + server_nonce;

        auto sasl_continue_message = fmt::format("r={},s={},i={}", nonce, scram_salt.salt, num_iterations);
        mt.send(Messaging::AuthenticationSASLContinue(sasl_continue_message), true);
        auth_message += "," + sasl_continue_message;
        auto rsp_continue = mt.receive<Messaging::SASLResponse>();
        auto proof = parseProof(rsp_continue->sasl_mechanism);
        auto proof_position = findProofPosition(rsp_continue->sasl_mechanism);
        auth_message += "," + rsp_continue->sasl_mechanism.substr(0, proof_position - 3);

        auto credentials = ScramSHA256Credentials(user_name, proof, auth_message, num_iterations);
        session.authenticate(credentials, address);
    }

    AuthenticationType getType() const override
    {
        return AuthenticationType::SCRAM_SHA256_PASSWORD;
    }
};

class AuthenticationManager
{
private:
    LoggerPtr log = getLogger("AuthenticationManager");
    UnorderedMapWithMemoryTracking<AuthenticationType, std::shared_ptr<AuthenticationMethod>> type_to_method = {};

public:
    explicit AuthenticationManager(const VectorWithMemoryTracking<std::shared_ptr<AuthenticationMethod>> & auth_methods)
    {
        for (const std::shared_ptr<AuthenticationMethod> & method : auth_methods)
        {
            type_to_method[method->getType()] = method;
        }
    }

    void authenticate(
        const String & user_name,
        Session & session,
        Messaging::MessageTransport & mt,
        const Poco::Net::SocketAddress & address)
    {
        try
        {
            const auto user_authentication_types = session.getAuthenticationTypesOrLogInFailure(user_name);

            for (auto user_authentication_type : user_authentication_types)
            {
                if (type_to_method.contains(user_authentication_type) && type_to_method[user_authentication_type]->isSupportedForUser(user_name, session))
                {
                    type_to_method[user_authentication_type]->authenticate(user_name, session, mt, address);
                    mt.send(Messaging::AuthenticationOk(), true);
                    LOG_DEBUG(log, "Authentication for user {} was successful.", user_name);
                    return;
                }
            }
        }
        catch (const Exception & e)
        {
            const bool unsupported_authentication_configuration = e.code() == ErrorCodes::NOT_IMPLEMENTED;
            mt.send(Messaging::ErrorOrNoticeResponse(
                Messaging::ErrorOrNoticeResponse::ERROR,
                unsupported_authentication_configuration ? "0A000" : "28P01",
                unsupported_authentication_configuration ? "Authentication configuration is not supported by the PostgreSQL protocol" : "Invalid user or password"),
                true);

            throw;
        }

        mt.send(Messaging::ErrorOrNoticeResponse(Messaging::ErrorOrNoticeResponse::ERROR, "0A000", "Authentication method is not supported"),
                true);

        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "None of the authentication methods registered for the user are supported");
    }
};
}

namespace PostgresPreparedStatements
{

class PreparedStatemetsManager
{
public:
    explicit PreparedStatemetsManager(std::optional<size_t> limit_statements_)
        : limit_statements(limit_statements_)
    {
    }

    void addStatement(ASTPreparedStatement * statement)
    {
        if (limit_statements && statements.size() + 1 >= limit_statements.value())
            throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Statements limit exceeded");

        statements[statement->function_name] =
            PreparedStatement{statement->function_body, statement->parameter_types, countPlaceholders(statement->function_body)};
    }

    String getStatement(ASTExecute * execute)
    {
        auto it = statements.find(execute->function_name);
        if (it == statements.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown statement");
        /// Require one argument for each referenced placeholder.
        if (execute->arguments.size() != it->second.parameter_count)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "EXECUTE supplies {} argument(s) but the prepared statement has {} parameter(s)",
                execute->arguments.size(), it->second.parameter_count);

        /// `EXECUTE` arguments are bare literals, so spaces keep them separate from adjacent tokens.
        VectorWithMemoryTracking<String> arguments;
        arguments.reserve(execute->arguments.size());
        for (const auto & argument : execute->arguments)
            arguments.push_back(fmt::format(" {} ", argument));
        return substitute(it->second.body, arguments);
    }

    void deleteStatement(const String & function_name)
    {
        auto it = statements.find(function_name);
        if (it == statements.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown statement");

        statements.erase(it);
    }

    /// `Close` on an unknown statement is a successful no-op.
    void tryDeleteStatement(const String & function_name)
    {
        statements.erase(function_name);
    }

    void attachBindQuery(std::unique_ptr<PostgreSQLProtocol::Messaging::BindQuery> query)
    {
        /// A single bind slot can represent only the unnamed portal.
        if (!query->portal_name.empty())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Named portals are not supported in the PostgreSQL wire protocol, "
                "got portal name '{}'", query->portal_name);

        /// Binary values require type-specific decoding, which is not implemented.
        if (query->has_binary_format_param)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Binary format parameters are not supported in Bind messages, use the text format");

        /// A portal keeps the statement snapshot resolved by `Bind`.
        auto it = statements.find(query->function_name);
        if (it == statements.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown statement");

        /// OID count is not arity because `Parse` can omit inferred parameter types.
        if (query->parameters.size() != it->second.parameter_count)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Bind supplies {} parameter value(s) but the prepared statement has {} parameter(s)",
                query->parameters.size(), it->second.parameter_count);

        /// A new `Bind` replaces the unnamed portal.
        bound_statement = it->second;
        bind_query = std::move(query);
    }

    String getStatmentFromBind()
    {
        if (!bind_query || !bound_statement.has_value())
            throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Execute without prior Bind");

        const auto & parameter_types = bound_statement->parameter_types;

        /// Convert every value to a safe SQL fragment before substitution.
        VectorWithMemoryTracking<String> arguments;
        arguments.reserve(bind_query->parameters.size());
        for (size_t i = 0; i < bind_query->parameters.size(); ++i)
        {
            const auto & parameter = bind_query->parameters[i];
            if (!parameter.has_value())
            {
                arguments.emplace_back("NULL");
                continue;
            }
            /// OID 0 requests inference; unmapped declared OIDs become strings.
            const Int32 oid = i < parameter_types.size() ? parameter_types[i] : 0;
            if (auto formatted = formatTypedParameter(oid, *parameter))
                arguments.push_back(std::move(*formatted));
            else if (oid == 0)
                arguments.push_back(formatInferredParameter(*parameter));
            else
                arguments.push_back(quoteString(*parameter));
        }

        return substitute(bound_statement->body, arguments);
    }

    void resetBindQuery()
    {
        bind_query.reset();
        bound_statement.reset();
    }

private:
    struct PreparedStatement
    {
        String body;
        /// Empty for the simple `PREPARE`/`EXECUTE` path.
        VectorWithMemoryTracking<Int32> parameter_types;
        /// Highest referenced `$N`, which defines `Bind` arity.
        size_t parameter_count = 0;
    };

    /// Return the highest `$N` placeholder token in the statement.
    static size_t countPlaceholders(const String & body)
    {
        size_t max_index = 0;
        Lexer lexer(body.data(), body.data() + body.size());
        for (Token token = lexer.nextToken(); !token.isEnd(); token = lexer.nextToken())
        {
            if (token.isError())
                break;
            std::string_view text(token.begin, token.size());
            if (token.type != TokenType::BareWord || text.size() <= 1 || text[0] != '$')
                continue;
            size_t index = 0;
            for (size_t i = 1; i < text.size(); ++i)
            {
                if (text[i] < '0' || text[i] > '9')
                {
                    index = 0;
                    break;
                }
                /// An overflowing index cannot reference a supplied argument.
                if (index > (std::numeric_limits<size_t>::max() - 9) / 10)
                {
                    index = 0;
                    break;
                }
                index = index * 10 + static_cast<size_t>(text[i] - '0');
            }
            max_index = std::max(max_index, index);
        }
        return max_index;
    }

    UnorderedMapWithMemoryTracking<String, PreparedStatement> statements;
    std::optional<size_t> limit_statements;
    std::unique_ptr<PostgreSQLProtocol::Messaging::BindQuery> bind_query;
    /// Statement snapshot owned by the unnamed portal.
    std::optional<PreparedStatement> bound_statement;

    /// Match one decimal literal: `[sign] digits [. digits] [(e|E) [sign] digits]`.
    static bool isSingleNumericLiteral(const String & value)
    {
        std::string_view remaining = value;
        if (remaining.empty())
            return false;

        if (remaining[0] == '+' || remaining[0] == '-')
            remaining.remove_prefix(1);

        bool has_mantissa_digit = false;
        while (!remaining.empty() && remaining[0] >= '0' && remaining[0] <= '9')
        {
            has_mantissa_digit = true;
            remaining.remove_prefix(1);
        }
        if (!remaining.empty() && remaining[0] == '.')
        {
            remaining.remove_prefix(1);
            while (!remaining.empty() && remaining[0] >= '0' && remaining[0] <= '9')
            {
                has_mantissa_digit = true;
                remaining.remove_prefix(1);
            }
        }
        if (!has_mantissa_digit)
            return false;

        if (!remaining.empty() && (remaining[0] == 'e' || remaining[0] == 'E'))
        {
            remaining.remove_prefix(1);
            if (!remaining.empty() && (remaining[0] == '+' || remaining[0] == '-'))
                remaining.remove_prefix(1);
            bool has_exponent_digit = false;
            while (!remaining.empty() && remaining[0] >= '0' && remaining[0] <= '9')
            {
                has_exponent_digit = true;
                remaining.remove_prefix(1);
            }
            if (!has_exponent_digit)
                return false;
        }

        return remaining.empty();
    }

    /// Maximum `Decimal256` precision.
    static constexpr UInt32 DECIMAL256_MAX_PRECISION = 76;

    /// Reject unreasonable exponents before arithmetic or zero-padding.
    static constexpr Int64 MAX_ABS_EXPONENT = 1000;

    /// Convert a validated numeric literal to an exact exponent-free decimal and scale.
    /// Return `std::nullopt` if it exceeds `Decimal256`.
    static std::optional<std::pair<String, UInt32>> normalizeDecimal(const String & value)
    {
        std::string_view remaining = value;
        if (remaining.empty())
            return std::nullopt;

        bool negative = remaining[0] == '-';
        if (remaining[0] == '+' || negative)
            remaining.remove_prefix(1);

        /// Collect mantissa digits and scale.
        String digits;
        Int64 point_from_right = 0;
        bool seen_point = false;
        while (!remaining.empty() && ((remaining[0] >= '0' && remaining[0] <= '9') || remaining[0] == '.'))
        {
            if (remaining[0] == '.')
            {
                seen_point = true;
                remaining.remove_prefix(1);
                continue;
            }
            digits += remaining[0];
            if (seen_point)
                ++point_from_right;
            remaining.remove_prefix(1);
        }

        /// Shift the decimal point by the exponent.
        if (!remaining.empty() && (remaining[0] == 'e' || remaining[0] == 'E'))
        {
            remaining.remove_prefix(1);
            bool exp_negative = false;
            if (!remaining.empty() && (remaining[0] == '+' || remaining[0] == '-'))
            {
                exp_negative = (remaining[0] == '-');
                remaining.remove_prefix(1);
            }
            Int64 exp = 0;
            while (!remaining.empty() && remaining[0] >= '0' && remaining[0] <= '9')
            {
                exp = exp * 10 + (remaining[0] - '0');
                /// Bound work and prevent exponent overflow.
                if (exp > MAX_ABS_EXPONENT)
                    return std::nullopt;
                remaining.remove_prefix(1);
            }
            point_from_right += exp_negative ? exp : -exp;
        }

        /// Strip leading zeros from the integer part but keep at least one digit.
        size_t first_significant = 0;
        while (first_significant + 1 < digits.size()
               && digits[first_significant] == '0'
               && static_cast<Int64>(digits.size() - first_significant) > point_from_right)
            ++first_significant;
        digits.erase(0, first_significant);

        /// Materialize trailing zeros for a negative scale.
        while (point_from_right < 0)
        {
            digits += '0';
            ++point_from_right;
        }
        UInt32 scale = static_cast<UInt32>(point_from_right);

        /// Add a leading zero when the scale covers all digits.
        if (scale >= digits.size())
            digits.insert(0, String(scale - digits.size() + 1, '0'));

        if (scale > DECIMAL256_MAX_PRECISION || digits.size() > DECIMAL256_MAX_PRECISION)
            return std::nullopt;

        String plain;
        if (negative)
            plain += '-';
        const size_t int_len = digits.size() - scale;
        const std::string_view digits_view{digits};
        plain += digits_view.substr(0, int_len);
        if (scale > 0)
        {
            plain += '.';
            plain += digits_view.substr(int_len);
        }
        return std::make_pair(std::move(plain), scale);
    }

    /// Map OIDs whose PostgreSQL text form ClickHouse parses without conversion.
    static const char * clickhouseTypeForOID(Int32 oid)
    {
        switch (oid)
        {
            case 16:   return "Bool";           /// bool
            case 21:   return "Int16";          /// int2
            case 23:   return "Int32";          /// int4
            case 20:   return "Int64";          /// int8
            case 26:   return "UInt32";         /// oid (unsigned)
            case 700:  return "Float32";        /// float4
            case 701:  return "Float64";        /// float8
            case 1082: return "Date32";         /// date
            case 1114: return "DateTime64(6)";  /// timestamp
            /// Preserve `timestamptz` UTC semantics.
            case 1184: return "DateTime64(6, 'UTC')";  /// timestamptz
            case 2950: return "UUID";           /// uuid
            default:   return nullptr;
        }
    }

    /// Format a declared type with `accurateCast` so validation cannot wrap values.
    /// `numeric` is normalized to an exact `Decimal256`; unsupported OIDs return
    /// `std::nullopt`. Values remain quoted and escaped in every cast.
    static std::optional<String> formatTypedParameter(Int32 oid, const String & value)
    {
        if (oid == 1700) /// numeric
        {
            if (!isSingleNumericLiteral(value))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Invalid value {} for a numeric prepared-statement parameter", quoteString(value));
            auto normalized = normalizeDecimal(value);
            if (!normalized)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Value {} for a numeric prepared-statement parameter exceeds the maximum "
                                "representable Decimal precision", quoteString(value));
            return fmt::format("accurateCast({}, 'Decimal256({})')", quoteString(normalized->first), normalized->second);
        }

        if (const char * type = clickhouseTypeForOID(oid))
            /// Quote both arguments because a type name can contain quotes.
            return fmt::format("accurateCast({}, {})", quoteString(value), quoteString(type));

        return std::nullopt;
    }

    /// Match only the case-insensitive boolean keywords `true` and `false`.
    static bool isBooleanLiteral(const String & value)
    {
        auto ci_equals = [](std::string_view input, std::string_view keyword)
        {
            const auto [input_it, keyword_it] = std::ranges::mismatch(
                input, keyword, [](char character, char expected) { return character == expected || character == expected - 'a' + 'A'; });
            return input_it == input.end() && keyword_it == keyword.end();
        };
        return ci_equals(value, "true") || ci_equals(value, "false");
    }

    /// Preserve unambiguous numeric and boolean literals for OID 0 inference.
    /// Quote all other values. Spaces keep bare values separate from adjacent tokens.
    static String formatInferredParameter(const String & value)
    {
        if (isSingleNumericLiteral(value) || isBooleanLiteral(value))
            return fmt::format(" {} ", value);
        return quoteString(value);
    }

    /// Substitute placeholder tokens only; arguments must already be safe SQL fragments.
    /// The lexer excludes placeholders inside strings, identifiers, comments, and heredocs.
    static String substitute(const String & body, const VectorWithMemoryTracking<String> & arguments)
    {
        String result;
        result.reserve(body.size());
        Lexer lexer(body.data(), body.data() + body.size());
        for (Token token = lexer.nextToken(); !token.isEnd(); token = lexer.nextToken())
        {
            if (token.isError())
            {
                /// Malformed SQL: emit the rest verbatim and let the parser report it.
                result.append(token.begin, body.data() + body.size());
                break;
            }

            std::string_view text(token.begin, token.size());
            if (token.type == TokenType::BareWord && text.size() > 1 && text[0] == '$')
            {
                /// Reject non-digits and stop before the index can overflow.
                size_t index = 0;
                for (size_t i = 1; i < text.size(); ++i)
                {
                    if (text[i] < '0' || text[i] > '9' || index > arguments.size())
                    {
                        index = 0;
                        break;
                    }
                    index = index * 10 + static_cast<size_t>(text[i] - '0');
                }
                if (index >= 1 && index <= arguments.size())
                {
                    result += arguments[index - 1];
                    continue;
                }
            }
            result.append(token.begin, token.size());
        }
        return result;
    }

};

}

}
}
