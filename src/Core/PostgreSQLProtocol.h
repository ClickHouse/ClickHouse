#pragma once

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Session.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/Base64.h>
#include <Poco/RegularExpression.h>
#include <Poco/Net/StreamSocket.h>
#include <Parsers/ParserPreparedStatement.h>
#include <Poco/RandomStream.h>
#include <Poco/SHA1Engine.h>
#include <Access/Credentials.h>
#include <unordered_map>
#include <utility>

#include <Interpreters/Context_fwd.h>
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

//// Column 'typelem' from 'pg_type' table. NB: not all types are compatible with PostgreSQL's ones
enum class ColumnType : Int32
{
    CHAR = 18,
    INT8 = 20,
    INT2 = 21,
    INT4 = 23,
    FLOAT4 = 700,
    FLOAT8 = 701,
    BPCHAR = 1042,
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

ColumnTypeSpec convertTypeIndexToPostgresColumnTypeSpec(TypeIndex type_index);

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
        std::unique_ptr<TMessage> message = std::make_unique<TMessage>(payload_size);
        message->deserialize(*in);
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
        Int32 size;
        readBinaryBigEndian(size, *in);
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
    Int32 secret_key = 0;

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
    std::unordered_map<String, String> parameters;

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

            if (payload_size < 0)
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
        UInt8 message_type;
        readBinaryBigEndian(message_type, in);
        Int32 size;
        readBinaryBigEndian(size, in);
        readNullTerminated(auth_method, in);
        Int32 size_sasl_mechanism;
        readBinaryBigEndian(size_sasl_mechanism, in);
        sasl_mechanism.resize(size_sasl_mechanism);
        in.readStrict(sasl_mechanism.data(), size_sasl_mechanism);
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
        UInt8 message_type;
        readBinaryBigEndian(message_type, in);
        Int32 size;
        readBinaryBigEndian(size, in);
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
        Int32 sz;
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
    Int32 secret_key;

public:
    BackendKeyData(Int32 process_id_, Int32 secret_key_)
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
        Int32 sz;
        readBinaryBigEndian(sz, in);
        readNullTerminated(query, in);
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
    Int16 num_params;

    void deserialize(ReadBuffer & in) override
    {
        Int32 sz;
        readBinaryBigEndian(sz, in);
        readNullTerminated(function_name, in);
        readNullTerminated(sql_query, in);
        readBinaryBigEndian(num_params, in);
        Int32 oid_param;
        for (int i = 0; i < num_params; ++i)
            readBinaryBigEndian(oid_param, in);
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
    std::vector<String> parameters;
    Int16 num_params;

    void deserialize(ReadBuffer & in) override
    {
        Int32 sz;
        readBinaryBigEndian(sz, in);
        readNullTerminated(portal_name, in);
        readNullTerminated(function_name, in);

        Int16 num_format_params;
        readBinaryBigEndian(num_format_params, in);
        Int16 format_param;
        for (Int16 i = 0; i < num_format_params; ++i)
        {
            readBinaryBigEndian(format_param, in);
        }
        readBinaryBigEndian(num_params, in);
        for (int i = 0; i < num_params; ++i)
        {
            Int32 sz_param;
            readBinaryBigEndian(sz_param, in);
            String current_param(sz_param, 0);
            in.readStrict(current_param.data(), sz_param);
            parameters.push_back(current_param);
        }

        Int16 num_format_params_result;
        readBinaryBigEndian(num_format_params_result, in);
        Int16 format_param_result;
        for (Int16 i = 0; i < num_format_params_result; ++i)
            readBinaryBigEndian(format_param_result, in);
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
    char describe;
    String function_name;

    void deserialize(ReadBuffer & in) override
    {
        Int32 sz;
        readBinaryBigEndian(sz, in);
        in.readStrict(&describe, 1);
        readNullTerminated(function_name, in);
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
    Int32 max_rows;

    void deserialize(ReadBuffer & in) override
    {
        Int32 sz;
        readBinaryBigEndian(sz, in);
        readNullTerminated(portal_name, in);
        readBinaryBigEndian(max_rows, in);
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

    void deserialize(ReadBuffer & in) override
    {
        Int32 sz;
        readBinaryBigEndian(sz, in);
        Int8 byte;
        readBinaryBigEndian(byte, in);
        readNullTerminated(function_name, in);
    }

    MessageType getMessageType() const override
    {
        return MessageType::CLOSE;
    }
};

class CloseQueryComplete : BackendMessage
{
public:
    void serialize(WriteBuffer & out) const override
    {
        out.write('C');
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
        Int32 sz;
        readBinaryBigEndian(sz, in);
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
    FieldDescription(const String & name_, TypeIndex type_index, FormatCode format_code_ = FormatCode::TEXT)
    : name(name_)
    , type_spec(convertTypeIndexToPostgresColumnTypeSpec(type_index))
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
    const std::vector<FieldDescription> & fields_descr;

public:
    explicit RowDescription(const std::vector<FieldDescription> & fields_descr_) : fields_descr(fields_descr_) {}

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
    const std::vector<std::shared_ptr<ISerializable>> & row;

public:
    explicit DataRow(const std::vector<std::shared_ptr<ISerializable>> & row_) : row(row_) {}

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

class CommandComplete : BackendMessage
{
public:
    enum Command {BEGIN = 0, COMMIT = 1, INSERT = 2, DELETE = 3, UPDATE = 4, SELECT = 5, MOVE = 6, FETCH = 7, COPY = 8, EXECUTE = 9};
private:
    String enum_to_string[10] = {"BEGIN", "COMMIT", "INSERT", "DELETE", "UPDATE", "SELECT", "MOVE", "FETCH", "COPY", "EXECUTE"};

    String value;

public:
    CommandComplete(Command cmd_, Int32 rows_count_)
    {
        value = enum_to_string[cmd_];
        String add = " ";
        if (cmd_ == Command::INSERT)
            add = " 0 ";
        value += add + std::to_string(rows_count_);
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

    static Command classifyQuery(const String & query)
    {
        std::vector<String> query_types({"BEGIN", "COMMIT", "INSERT", "DELETE", "UPDATE", "SELECT", "MOVE", "FETCH", "COPY", "EXECUTE"});
        for (size_t i = 0; i != query_types.size(); ++i)
        {
            String::const_iterator iter = std::search(
                query.begin(),
                query.end(),
                query_types[i].begin(),
                query_types[i].end(),
                [](char a, char b){return std::toupper(a) == b;});

            if (iter != query.end())
                return static_cast<Command>(i);
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

        mt.send(Messaging::AuthenticationSASL(), true);
        auto rsp = mt.receive<Messaging::SASLInitialResponse>();

        auto server_nonce = generateNonce();
        auto client_nonce = parseClientNonce(rsp->sasl_mechanism);
        auth_message += fmt::format("n={},r={}", parseUsername(rsp->sasl_mechanism), client_nonce);
        auto nonce = client_nonce + server_nonce;

        String salt;
        const auto& access_control = session.globalContext()->getAccessControl();
        if (auto id = access_control.find<User>(user_name))
        {
            if (auto user = access_control.tryRead<User>(*id))
            {
                for (const auto & auth_method : user->authentication_methods)
                {
                    salt = auth_method.getSalt();
                }
            }
        }
        auto sasl_continue_message = fmt::format("r={},s={},i={}", nonce, salt, num_iterations);
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
    std::unordered_map<AuthenticationType, std::shared_ptr<AuthenticationMethod>> type_to_method = {};

public:
    explicit AuthenticationManager(const std::vector<std::shared_ptr<AuthenticationMethod>> & auth_methods)
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
                if (type_to_method.find(user_authentication_type) != type_to_method.end())
                {
                    type_to_method[user_authentication_type]->authenticate(user_name, session, mt, address);
                    mt.send(Messaging::AuthenticationOk(), true);
                    LOG_DEBUG(log, "Authentication for user {} was successful.", user_name);
                    return;
                }
            }
        }
        catch (const Exception&)
        {
            mt.send(Messaging::ErrorOrNoticeResponse(Messaging::ErrorOrNoticeResponse::ERROR, "28P01", "Invalid user or password"),
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

        statements[statement->function_name] = statement->function_body;
    }

    String getStatement(ASTExecute * execute)
    {
        return getStatement(execute->function_name, execute->arguments);
    }

    void deleteStatement(ASTDeallocate * query)
    {
        auto it = statements.find(query->function_name);
        if (it == statements.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown statement");

        statements.erase(it);
    }

    void attachBindQuery(std::unique_ptr<PostgreSQLProtocol::Messaging::BindQuery> query)
    {
        if (bind_query)
            throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Query is already binded");

        bind_query = std::move(query);
    }

    String getStatmentFromBind()
    {
        auto result = getStatement(bind_query->function_name, bind_query->parameters);

        return result;
    }

    void resetBindQuery(const String& function_name)
    {
        statements.erase(function_name);
        bind_query.reset();
    }

private:
    std::unordered_map<String, String> statements;
    std::optional<size_t> limit_statements;
    std::unique_ptr<PostgreSQLProtocol::Messaging::BindQuery> bind_query;

    String getStatement(const String & function_name, const std::vector<String> & arguments)
    {
        auto it = statements.find(function_name);
        if (it == statements.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown statement");

        auto body = it->second;
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            auto templ = "$" + std::to_string(i + 1);
            auto pos = body.find(templ);
            if (pos != std::string::npos)
            {
                body.replace(pos, templ.size(), arguments[i]);
            }
        }
        return body;
    }

};

}

}
}
