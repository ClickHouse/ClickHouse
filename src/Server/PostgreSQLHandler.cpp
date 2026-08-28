#include <memory>
#include <optional>
#include <string_view>
#include <vector>
#include <Server/PostgreSQLHandler.h>
#include <IO/CascadeWriteBuffer.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/ReadBufferFromString.h>
#include <IO/copyData.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/Lexer.h>
#include <Parsers/parseQuery.h>
#include <Poco/String.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Server/TCPServer.h>
#include <boost/algorithm/string/trim.hpp>
#include <base/scope_guard.h>
#include <pcg_random.hpp>
#include <Common/Exception.h>
#include <Common/CurrentThread.h>
#include <Common/QueryScope.h>
#include <Common/SettingSource.h>
#include <Common/SettingsChanges.h>
#include <Common/config_version.h>
#include <Common/randomSeed.h>
#include <Common/setThreadName.h>
#include <Common/StringUtils.h>
#include <Core/PostgreSQLProtocol.h>
#include <IO/WriteBufferFromString.h>
#include <Parsers/ASTCopyQuery.h>
#include <Parsers/ParserCopyQuery.h>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>

#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ParserQuery.h>
#include <fmt/format.h>
#include <Formats/FormatFactory.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/PostgreSQLArrayText.h>

#if USE_SSL
#    include <Server/CertificateReloader.h>
#    include <Poco/Net/SSLManager.h>
#    include <Poco/Net/SecureStreamSocket.h>
#    include <Poco/Net/Utility.h>
#    include <Poco/StringTokenizer.h>
#endif

namespace DB
{
namespace Setting
{
    extern const SettingsNonZeroUInt64 max_insert_block_size;
    extern const SettingsUInt64 max_insert_block_size_bytes;
    extern const SettingsUInt64 min_insert_block_size_rows;
    extern const SettingsUInt64 min_insert_block_size_bytes;
}

namespace ServerSetting
{
    extern const ServerSettingsString default_session_user;
}

namespace ErrorCodes
{
    extern const int AUTHENTICATION_FAILED;
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int OPENSSL_ERROR;
    extern const int QUERY_WAS_CANCELLED;
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
    extern const int UNSUPPORTED_METHOD;
    extern const int UNKNOWN_PACKET_FROM_CLIENT;
}

namespace
{

/// Presents the payload of a `COPY ... FROM STDIN` as one continuous stream. PostgreSQL `CopyData`
/// frame boundaries are transport-only: a client may split one logical row (or even one multi-byte
/// character) across several frames and may pack many rows into one, so the input format must parse
/// the concatenation of all frames, not each frame in isolation. A frame body is handed out in
/// whatever pieces the socket delivers it, never materialized as a whole: a client is free to
/// announce a huge frame and stall in the middle of it, which must neither make the server hold the
/// frame resident nor delay an external cancellation until the frame is complete. `CopyDone` ends
/// the stream.
class CopyInDataReadBuffer : public ReadBuffer
{
public:
    CopyInDataReadBuffer(
        PostgreSQLProtocol::Messaging::MessageTransport & transport_,
        ReadBufferFromPocoSocket & socket_in_,
        std::function<void()> check_cancelled_)
        : ReadBuffer(nullptr, 0)
        , transport(transport_)
        , socket_in(socket_in_)
        , check_cancelled(std::move(check_cancelled_))
    {
    }

private:
    /// Waits until the socket has something to read. An external cancellation (a PostgreSQL
    /// `CancelRequest` or `KILL QUERY`) only marks the query killed in the process list; nothing wakes a
    /// socket read blocked on a paused client. So poll in short slices and check for the kill in
    /// between, and the staging aborts promptly instead of sitting here until the client speaks again.
    void waitForDataOrCancel()
    {
        static constexpr size_t cancellation_check_interval_microseconds = 100'000;
        while (!socket_in.poll(cancellation_check_interval_microseconds))
            check_cancelled();
    }

    /// Makes sure the socket buffer holds at least one byte, and returns how many it holds.
    size_t waitForSomeData()
    {
        while (!socket_in.hasPendingData())
        {
            waitForDataOrCancel();
            /// `poll` said the socket is readable, so this refill does not block. A closed connection
            /// mid-copy is a client that will never finish it.
            if (socket_in.eof())
            {
                protocol_error = true;
                throw Exception(
                    ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT,
                    "Unexpected end of stream while reading the payload of COPY FROM STDIN");
            }
        }
        return socket_in.available();
    }

    bool nextImpl() override
    {
        /// End-of-stream must be sticky: `ReadBuffer::eof` may probe `next` again after the stream has
        /// ended (e.g. the parallel-parsing segmentator does), and by then the client is already waiting
        /// for `CommandComplete` and sends nothing more - reading the socket again would deadlock.
        if (received_copy_done)
            return false;

        while (true)
        {
            /// Hand out the body of the frame being received a piece at a time, exactly as the socket
            /// delivers it: the reader on the other side of this buffer (the staging store) is what
            /// bounds memory, and a client stalled in the middle of a frame is cancellable between
            /// pieces. The bytes are handed out in place - nothing else reads the socket buffer while
            /// a copy is being staged - so no copy of the frame is made either.
            if (remaining_frame_bytes > 0)
            {
                const size_t piece = std::min(waitForSomeData(), remaining_frame_bytes);
                working_buffer = Buffer(socket_in.position(), socket_in.position() + piece);
                socket_in.position() += piece;
                remaining_frame_bytes -= piece;
                return true;
            }

            /// Push out anything buffered on the write side before blocking on the client.
            transport.flush();
            waitForDataOrCancel();
            PostgreSQLProtocol::Messaging::FrontMessageType message_type = transport.receiveMessageType();
            if (message_type == PostgreSQLProtocol::Messaging::FrontMessageType::COPY_DATA)
            {
                /// The frame length is read here instead of through `receive<CopyInData>()`, which would
                /// reserve and read the whole advertised payload in one go.
                Int32 frame_size = 0;
                char frame_size_bytes[sizeof(Int32)];
                /// Once the `CopyData` tag has been consumed, a cancellation cannot
                /// safely drain the next frame until its complete length header is
                /// known. In particular, this must be false before the first byte.
                frame_header_complete = false;
                for (size_t byte = 0; byte < sizeof(frame_size_bytes); ++byte)
                {
                    waitForSomeData();
                    frame_size_bytes[byte] = *socket_in.position();
                    ++socket_in.position();
                    /// A cancel that lands in the middle of the length leaves the stream out of sync:
                    /// the drain after the cancel could not tell payload from the next message header.
                    frame_header_complete = byte + 1 == sizeof(frame_size_bytes);
                }
                ReadBufferFromMemory frame_size_in(frame_size_bytes, sizeof(frame_size_bytes));
                readBinaryBigEndian(frame_size, frame_size_in);
                if (frame_size < static_cast<Int32>(sizeof(Int32)))
                {
                    protocol_error = true;
                    throw Exception(
                        ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT,
                        "Wrong message length {} in CopyData, it must be at least 4", frame_size);
                }

                /// An empty frame is legal and does not mean end-of-stream; wait for the next message.
                remaining_frame_bytes = static_cast<size_t>(frame_size) - sizeof(Int32);
                continue;
            }
            if (message_type == PostgreSQLProtocol::Messaging::FrontMessageType::COPY_COMPLETION)
            {
                /// `CopyDone` is a fixed-size message and `deserialize` rejects any other length, but it
                /// has consumed only the length field by then, so the advertised trailing bytes stay in
                /// the stream and would be read as the next message. That desynchronization is not
                /// recoverable: flag it up front so the error tears the connection down instead of being
                /// reported as an ordinary query error, and clear the flag once the frame is consumed.
                protocol_error = true;
                transport.receive<PostgreSQLProtocol::Messaging::CopyDone>();
                protocol_error = false;
                received_copy_done = true;
                return false;
            }
            /// A `CopyFail` is a normal client-side abort of the copy (libpq sends it when its local data
            /// source errors or the copy is cancelled), not a protocol violation. Record the abort and throw
            /// to unwind the staging loop (the payload is staged in full before anything reaches the insert
            /// pipeline, so an abort leaves no partial rows behind); the COPY_FROM handler recognizes the
            /// abort via `clientAborted`, sends a clean `ErrorResponse` and returns the query as handled so
            /// the run loop follows with `ReadyForQuery`, keeping the connection alive. A plain rethrow
            /// instead would propagate out of `processQuery` to the run loop and drop the connection before
            /// `ReadyForQuery`, which a driver such as psycopg2 reports as a lost connection.
            if (message_type == PostgreSQLProtocol::Messaging::FrontMessageType::COPY_FAILURE)
            {
                auto copy_fail = transport.receive<PostgreSQLProtocol::Messaging::CopyFail>();
                received_copy_done = true;
                client_aborted = true;
                abort_reason = copy_fail->message;
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "COPY FROM STDIN aborted by the client: {}", abort_reason);
            }
            protocol_error = true;
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Received incorrect message type - expected {} or {}, got {}",
                PostgreSQLProtocol::Messaging::FrontMessageType::COPY_DATA,
                PostgreSQLProtocol::Messaging::FrontMessageType::COPY_COMPLETION,
                message_type);
        }
    }

public:
    /// Whether the client aborted the copy with a `CopyFail` message, and the reason it sent. Used by the
    /// COPY_FROM handler to turn the abort into a clean `ErrorResponse` instead of a connection teardown.
    bool clientAborted() const { return client_aborted; }
    const String & abortReason() const { return abort_reason; }

    /// How many bytes of the frame being received are still unread, and whether the stream stopped on a
    /// message boundary at all. Used to resynchronize the connection when the copy is abandoned in the
    /// middle of a frame: those bytes are payload and have to be skipped before the next message header.
    size_t pendingFrameBytes() const { return remaining_frame_bytes; }
    bool canResynchronize() const { return frame_header_complete; }
    bool protocolError() const { return protocol_error; }

private:
    PostgreSQLProtocol::Messaging::MessageTransport & transport;
    ReadBufferFromPocoSocket & socket_in;
    /// Throws when the query this copy stages for has been killed externally.
    std::function<void()> check_cancelled;
    size_t remaining_frame_bytes = 0;
    bool frame_header_complete = true;
    bool received_copy_done = false;
    bool client_aborted = false;
    bool protocol_error = false;
    String abort_reason;
};

/// Some PostgreSQL drivers issue session-management commands during connection
/// setup or teardown that have no ClickHouse equivalent, for example `RESET ALL`
/// and `UNLISTEN *` sent by the Skunk driver. Instead of failing such a command
/// with a syntax error, ClickHouse accepts it as a no-op and replies with a
/// `CommandComplete` carrying the matching PostgreSQL command tag. See issue
/// https://github.com/ClickHouse/ClickHouse/issues/12476.
///
/// Returns the command tag to report if `query` is such a no-op command, or
/// std::nullopt if the query must be executed normally. None of the recognized
/// keywords (`UNLISTEN`, `RESET`, `DISCARD`) is a valid ClickHouse statement
/// start, so there are no false positives.
///
/// This is applied only in the simple-query (`Q`) protocol path, consistent with the other
/// driver-compatibility no-ops (`BEGIN` / `COMMIT` / `SET application_name`): drivers emit these
/// session-management commands as plain, unparameterized statements, so there is no reason to run
/// them through the extended `Parse` / `Bind` / `Execute` flow, and the extended path deliberately
/// performs no such driver-specific rewriting.
/// Whether a simple-query packet consists of a single statement, that is, whether it contains no `;`
/// that separates statements. The check has to be SQL-aware: a `;` inside a string literal, a quoted
/// identifier, a dollar-quoted string or a comment is ordinary text, not a separator. In particular
/// `SET application_name TO 'jdbc;a'` is one statement, and the value of `application_name` comes
/// straight from a user-supplied JDBC/libpq connection string, so semicolons in it are legal and
/// common; a naive scan for the first `;` would reject such a handshake statement.
///
/// A packet that cannot be scanned to the end — an unterminated literal or block comment — is
/// reported as "not a single statement" as well, so that the caller falls through to normal
/// processing, where it fails with a proper parser error.
bool isSingleStatementQuery(const String & query)
{
    const size_t size = query.size();
    /// Set once a `;` is seen outside of any literal or comment: nothing but whitespace and comments
    /// may follow it, otherwise the packet holds more than one statement.
    bool statement_ended = false;

    size_t i = 0;
    while (i < size)
    {
        const char c = query[i];

        /// A line comment runs to the end of the line, a block comment is terminated by `*/` and,
        /// unlike in the SQL standard, nests in PostgreSQL.
        if (c == '-' && i + 1 < size && query[i + 1] == '-')
        {
            i += 2;
            while (i < size && query[i] != '\n')
                ++i;
            continue;
        }
        if (c == '/' && i + 1 < size && query[i + 1] == '*')
        {
            size_t depth = 1;
            i += 2;
            while (i < size && depth > 0)
            {
                if (query[i] == '/' && i + 1 < size && query[i + 1] == '*')
                {
                    ++depth;
                    i += 2;
                }
                else if (query[i] == '*' && i + 1 < size && query[i + 1] == '/')
                {
                    --depth;
                    i += 2;
                }
                else
                    ++i;
            }
            if (depth > 0)
                return false;
            continue;
        }

        /// Comments were skipped above, so any other non-whitespace character after the terminating
        /// `;` starts a second statement.
        if (statement_ended)
        {
            if (!isWhitespaceASCII(c))
                return false;
            ++i;
            continue;
        }

        if (c == '\'')
        {
            /// A string literal: `''` stands for an embedded quote. In the `E'...'` form a backslash
            /// escapes the next character as well; ClickHouse reports
            /// `standard_conforming_strings = on`, so a backslash is an escape only in that form.
            const bool backslash_escapes = i > 0 && (query[i - 1] == 'E' || query[i - 1] == 'e')
                && (i == 1 || !isWordCharASCII(query[i - 2]));
            ++i;
            bool closed = false;
            while (i < size)
            {
                if (backslash_escapes && query[i] == '\\' && i + 1 < size)
                {
                    i += 2;
                    continue;
                }
                if (query[i] == '\'')
                {
                    if (i + 1 < size && query[i + 1] == '\'')
                    {
                        i += 2;
                        continue;
                    }
                    ++i;
                    closed = true;
                    break;
                }
                ++i;
            }
            if (!closed)
                return false;
            continue;
        }

        if (c == '"')
        {
            /// A quoted identifier: `""` stands for an embedded double quote.
            ++i;
            bool closed = false;
            while (i < size)
            {
                if (query[i] == '"')
                {
                    if (i + 1 < size && query[i + 1] == '"')
                    {
                        i += 2;
                        continue;
                    }
                    ++i;
                    closed = true;
                    break;
                }
                ++i;
            }
            if (!closed)
                return false;
            continue;
        }

        if (c == '$')
        {
            /// A dollar-quoted string `$tag$ ... $tag$` (the tag may be empty). Anything else that
            /// starts with `$`, such as the `$1` parameter placeholder, is ordinary text.
            size_t tag_end = i + 1;
            while (tag_end < size && (isWordCharASCII(query[tag_end]) || query[tag_end] == '$'))
            {
                if (query[tag_end] == '$')
                    break;
                ++tag_end;
            }
            if (tag_end < size && query[tag_end] == '$')
            {
                const String delimiter = query.substr(i, tag_end - i + 1);
                const size_t closing = query.find(delimiter, tag_end + 1);
                if (closing == String::npos)
                    return false;
                i = closing + delimiter.size();
                continue;
            }
            ++i;
            continue;
        }

        if (c == ';')
        {
            statement_ended = true;
            ++i;
            continue;
        }

        ++i;
    }

    return true;
}

std::optional<String> classifyNoOpDriverCommand(const String & query)
{
    /// Only treat the packet as a no-op when it consists of a single statement. A simple-query
    /// packet may contain several `;`-separated statements; if we shortcut on the leading keyword
    /// we would acknowledge the whole packet and silently skip the rest (e.g. `RESET ALL; SELECT 1`
    /// or, worse, `RESET ALL; DROP TABLE t`).
    if (!isSingleStatementQuery(query))
        return std::nullopt;

    /// Normalize the whole statement, not a fixed-size prefix: a `SET application_name TO '...'`
    /// value is arbitrarily long (drivers put free-form client info there), and cutting it off at a
    /// byte budget would push a perfectly valid no-op past the cap and execute it as a real
    /// ClickHouse `SET`, breaking connection setup. Normalization never grows the text, and the
    /// single-statement scan above already walked the whole packet, so this stays linear.
    String prefix = PostgreSQLProtocol::Messaging::CommandComplete::extractNormalizedPrefix(query, query.size());

    /// The single-statement guard above rejected any statement after a terminating `;`, so the only
    /// `;` that can remain at the end is that terminator itself (with trailing whitespace already
    /// collapsed). Drop it so it is not mistaken for a command argument below. A `;` inside a string
    /// literal is left alone: it is not at the end, because the closing quote follows it.
    while (!prefix.empty() && (prefix.back() == ';' || prefix.back() == ' '))
        prefix.pop_back();

    /// `extractNormalizedPrefix` already uppercased the text and collapsed runs of
    /// whitespace to single spaces, so a keyword is a run of 'A'..'Z'. Take the next
    /// such run, skipping a leading space; this also stops at a `;` or `*`.
    size_t pos = 0;
    const auto take_word = [&]() -> String
    {
        while (pos < prefix.size() && prefix[pos] == ' ')
            ++pos;
        const size_t start = pos;
        while (pos < prefix.size() && prefix[pos] >= 'A' && prefix[pos] <= 'Z')
            ++pos;
        return prefix.substr(start, pos - start);
    };
    const auto has_more = [&]() -> bool
    {
        while (pos < prefix.size() && prefix[pos] == ' ')
            ++pos;
        return pos < prefix.size();
    };

    /// An argument in the normalized prefix: an identifier — a letter or `_` followed by letters,
    /// digits, `_` or `$` (the text is already uppercased). With `allow_dots`, a qualified name
    /// such as `EXTENSION.SETTING` (for `RESET`) is a single token as well.
    const auto take_identifier = [&](bool allow_dots) -> String
    {
        while (pos < prefix.size() && prefix[pos] == ' ')
            ++pos;
        const size_t start = pos;
        if (pos < prefix.size() && ((prefix[pos] >= 'A' && prefix[pos] <= 'Z') || prefix[pos] == '_'))
        {
            ++pos;
            while (pos < prefix.size()
                && ((prefix[pos] >= 'A' && prefix[pos] <= 'Z') || (prefix[pos] >= '0' && prefix[pos] <= '9')
                    || prefix[pos] == '_' || prefix[pos] == '$' || (allow_dots && prefix[pos] == '.')))
                ++pos;
        }
        return prefix.substr(start, pos - start);
    };

    /// Not `const`: the early returns below move it out, and `performance-no-automatic-move`
    /// (clang-tidy) rejects returning a `const` local because constness prevents the move.
    String command = take_word();

    /// The keyword must end at a word boundary: `RESET1FOO` must not be taken for `RESET`.
    if (pos < prefix.size() && prefix[pos] != ' ')
        return std::nullopt;

    /// Accept the connection-cleanup commands the Skunk driver actually sends: `RESET { name | ALL }`
    /// and `UNLISTEN { channel | * }`, reporting the bare keyword as the tag. `LISTEN` and `NOTIFY`
    /// are deliberately NOT accepted here: unlike `UNLISTEN` (idempotent unsubscribe-all cleanup),
    /// they are application-visible PostgreSQL pub/sub operations, and this handler never delivers a
    /// `NotificationResponse`, so acknowledging them would turn an unsupported feature into a silent
    /// false success instead of a plain error. Issue #12476 only asks for `UNLISTEN *` / `RESET ALL`.
    ///
    /// Accept exactly one argument — an identifier (for `RESET` possibly a qualified
    /// `extension.setting` name; `ALL` is itself covered as an identifier), or `*` for `UNLISTEN` —
    /// and require the statement to end right after it, so that malformed variants such as a bare
    /// `RESET`, `RESET foo bar` or `UNLISTEN * garbage` are not acknowledged as success but fall
    /// through to the normal error path. Valid forms that no driver is known to emit — quoted
    /// identifiers and multi-word variants such as `RESET SESSION AUTHORIZATION` — likewise fall
    /// through, as before this change.
    if (command == "UNLISTEN" || command == "RESET")
    {
        String arg;
        if (command == "UNLISTEN" && has_more() && prefix[pos] == '*')
        {
            arg = "*";
            ++pos;
        }
        else
        {
            arg = take_identifier(/* allow_dots = */ command == "RESET");
        }
        if (arg.empty() || has_more())
            return std::nullopt;
        return command;
    }

    /// The JDBC driver opens every connection with `SET extra_float_digits = 3` and
    /// `SET application_name = '...'`. Neither parameter has a ClickHouse counterpart, so these two -
    /// and only these two - are acknowledged as no-ops. The statement must be exactly a single
    /// well-formed `SET` of one of these parameters: anything else (another parameter, a trailing
    /// statement, a query merely containing such text as a literal) falls through to normal
    /// processing, where it is executed or fails with a proper error.
    if (command == "SET")
    {
        /// PostgreSQL allows an optional `SESSION` or `LOCAL` scope keyword.
        String name = take_identifier(/* allow_dots = */ false);
        if (name == "SESSION" || name == "LOCAL")
            name = take_identifier(/* allow_dots = */ false);
        if (name != "EXTRA_FLOAT_DIGITS" && name != "APPLICATION_NAME")
            return std::nullopt;

        /// `SET name = value` or `SET name TO value`.
        if (!has_more())
            return std::nullopt;
        if (prefix[pos] == '=')
        {
            ++pos;
        }
        else
        {
            if (take_word() != "TO")
                return std::nullopt;
        }

        /// The value: a single-quoted string literal (with `''` escapes), or a single unquoted
        /// token such as a number, `DEFAULT`, or a bare identifier.
        if (!has_more())
            return std::nullopt;
        if (prefix[pos] == '\'')
        {
            ++pos;
            while (pos < prefix.size())
            {
                if (prefix[pos] == '\'')
                {
                    if (pos + 1 < prefix.size() && prefix[pos + 1] == '\'')
                    {
                        pos += 2;
                        continue;
                    }
                    break;
                }
                ++pos;
            }
            if (pos >= prefix.size())
                return std::nullopt;
            ++pos;
        }
        else
        {
            const size_t start = pos;
            while (pos < prefix.size()
                && ((prefix[pos] >= 'A' && prefix[pos] <= 'Z') || (prefix[pos] >= '0' && prefix[pos] <= '9')
                    || prefix[pos] == '_' || prefix[pos] == '$' || prefix[pos] == '.' || prefix[pos] == '+'
                    || prefix[pos] == '-'))
                ++pos;
            if (pos == start)
                return std::nullopt;
        }
        if (has_more())
            return std::nullopt;
        return command;
    }

    if (command == "DISCARD")
    {
        /// PostgreSQL accepts only `DISCARD { ALL | PLANS | SEQUENCES | TEMP | TEMPORARY }`
        /// (with `TEMPORARY` normalized to `TEMP`). Reject a bare `DISCARD` or an unknown
        /// subcommand such as `DISCARD FOO` instead of claiming success for a command we were
        /// never asked to emulate.
        String arg = take_word();
        if (arg == "TEMPORARY")
            arg = "TEMP";
        if ((arg == "ALL" || arg == "PLANS" || arg == "SEQUENCES" || arg == "TEMP") && !has_more())
            return command + " " + arg;
        return std::nullopt;
    }

    return std::nullopt;
}

}

PostgreSQLHandler::PostgreSQLHandler(
    const Poco::Net::StreamSocket & socket_,
#if USE_SSL
    const std::string & prefix_,
#endif
    IServer & server_,
    TCPServer & tcp_server_,
    bool ssl_enabled_,
    bool secure_required_,
    Int32 connection_id_,
    std::optional<String> default_session_user_,
    VectorWithMemoryTracking<std::shared_ptr<PostgreSQLProtocol::PGAuthentication::AuthenticationMethod>> & auth_methods_,
    const ProfileEvents::Event & read_event_,
    const ProfileEvents::Event & write_event_)
    : Poco::Net::TCPServerConnection(socket_)
#if USE_SSL
    , config(server_.config())
    , prefix(prefix_)
#endif
    , server(server_)
    , tcp_server(tcp_server_)
    , ssl_enabled(ssl_enabled_)
    , secure_required(secure_required_)
    , connection_id(connection_id_)
    , default_session_user(std::move(default_session_user_))
    , read_event(read_event_)
    , write_event(write_event_)
    , authentication_manager(auth_methods_)
    , prepared_statements_manager(std::nullopt)
{
    /// The secret key belongs to the connection, not to a single statement: it is handed to the client
    /// once, in `BackendKeyData`, and the client presents it back on a separate connection to cancel
    /// whatever this connection is running. Every statement of this connection therefore runs under the
    /// query id `postgres:<connection id>:<secret key>`, which is both unguessable and the id a cancel
    /// request resolves to. Statements of one connection run one after another, so reusing the id is safe.
    pcg64_fast gen{randomSeed()};
    secret_key = std::uniform_int_distribution<Int32>(0, INT32_MAX)(gen);

    changeIO(socket());

#if USE_SSL
    params.privateKeyFile = config.getString(prefix + Poco::Net::SSLManager::CFG_PRIV_KEY_FILE, "");
    params.certificateFile = config.getString(prefix + Poco::Net::SSLManager::CFG_CERTIFICATE_FILE, params.privateKeyFile);
    if (!params.privateKeyFile.empty() && !params.certificateFile.empty())
    {
        params.caLocation = config.getString(prefix + Poco::Net::SSLManager::CFG_CA_LOCATION, "");
        if (params.caLocation.empty())
        {
            auto ctx = Poco::Net::SSLManager::instance().defaultServerContext();
            params.caLocation = ctx->getCAPaths().caLocation;
        }

        params.verificationMode = Poco::Net::SSLManager::VAL_VER_MODE;
        if (config.hasProperty(prefix + Poco::Net::SSLManager::CFG_VER_MODE))
        {
            std::string mode = config.getString(prefix + Poco::Net::SSLManager::CFG_VER_MODE);
            params.verificationMode = Poco::Net::Utility::convertVerificationMode(mode);
        }

        params.verificationDepth = config.getInt(prefix + Poco::Net::SSLManager::CFG_VER_DEPTH, Poco::Net::SSLManager::VAL_VER_DEPTH);
        params.loadDefaultCAs
            = config.getBool(prefix + Poco::Net::SSLManager::CFG_ENABLE_DEFAULT_CA, Poco::Net::SSLManager::VAL_ENABLE_DEFAULT_CA);
        params.cipherList = config.getString(prefix + Poco::Net::SSLManager::CFG_CIPHER_LIST, Poco::Net::SSLManager::VAL_CIPHER_LIST);
        params.cipherList
            = config.getString(prefix + Poco::Net::SSLManager::CFG_CYPHER_LIST, params.cipherList); // for backwards compatibility

        bool require_tlsv1 = config.getBool(prefix + Poco::Net::SSLManager::CFG_REQUIRE_TLSV1, false);
        bool require_tlsv1_1 = config.getBool(prefix + Poco::Net::SSLManager::CFG_REQUIRE_TLSV1_1, false);
        bool require_tlsv1_2 = config.getBool(prefix + Poco::Net::SSLManager::CFG_REQUIRE_TLSV1_2, false);
        if (require_tlsv1_2)
            usage = Poco::Net::Context::TLSV1_2_SERVER_USE;
        else if (require_tlsv1_1)
            usage = Poco::Net::Context::TLSV1_1_SERVER_USE;
        else if (require_tlsv1)
            usage = Poco::Net::Context::TLSV1_SERVER_USE;
        else
            usage = Poco::Net::Context::SERVER_USE;

        params.dhParamsFile = config.getString(prefix + Poco::Net::SSLManager::CFG_DH_PARAMS_FILE, "");
        params.ecdhCurve = config.getString(prefix + Poco::Net::SSLManager::CFG_ECDH_CURVE, "");

        std::string disabled_protocols_list = config.getString(prefix + Poco::Net::SSLManager::CFG_DISABLE_PROTOCOLS, "");
        Poco::StringTokenizer dp_tok(
            disabled_protocols_list, ";,", Poco::StringTokenizer::TOK_TRIM | Poco::StringTokenizer::TOK_IGNORE_EMPTY);
        disabled_protocols = 0;
        for (const auto & token : dp_tok)
        {
            if (token == "sslv2")
                disabled_protocols |= Poco::Net::Context::PROTO_SSLV2;
            else if (token == "sslv3")
                disabled_protocols |= Poco::Net::Context::PROTO_SSLV3;
            else if (token == "tlsv1")
                disabled_protocols |= Poco::Net::Context::PROTO_TLSV1;
            else if (token == "tlsv1_1")
                disabled_protocols |= Poco::Net::Context::PROTO_TLSV1_1;
            else if (token == "tlsv1_2")
                disabled_protocols |= Poco::Net::Context::PROTO_TLSV1_2;
            else if (token == "tlsv1_3")
                disabled_protocols |= Poco::Net::Context::PROTO_TLSV1_3;
        }

        extended_verification = config.getBool(prefix + Poco::Net::SSLManager::CFG_EXTENDED_VERIFICATION, false);
        prefer_server_ciphers = config.getBool(prefix + Poco::Net::SSLManager::CFG_PREFER_SERVER_CIPHERS, false);
    }
#endif
}

void PostgreSQLHandler::changeIO(Poco::Net::StreamSocket & socket)
{
    in = std::make_shared<ReadBufferFromPocoSocket>(socket, read_event);
    out = std::make_shared<AutoCanceledWriteBuffer<WriteBufferFromPocoSocket>>(socket, write_event);
    message_transport = std::make_shared<PostgreSQLProtocol::Messaging::MessageTransport>(in.get(), out.get());
}

void PostgreSQLHandler::run()
{
    DB::setThreadName(ThreadName::POSTGRES_HANDLER);

    session = std::make_unique<Session>(server.context(), ClientInfo::Interface::POSTGRESQL);
    SCOPE_EXIT({ session.reset(); });

    session->setClientConnectionId(connection_id);

    try
    {
        if (!startup())
            return;

        while (tcp_server.isOpen())
        {
            if (!is_query_in_progress)
                message_transport->send(PostgreSQLProtocol::Messaging::ReadyForQuery(), true);

            constexpr size_t connection_check_timeout = 1; // 1 second
            while (!in->poll(1000000 * connection_check_timeout))
                if (!tcp_server.isOpen())
                    return;
            PostgreSQLProtocol::Messaging::FrontMessageType message_type = message_transport->receiveMessageType();
            if (!tcp_server.isOpen())
                return;

            /// PostgreSQL requires an extended-protocol error to discard all messages through the next
            /// `Sync`. Keep the connection alive for that recovery point instead of treating an unsupported
            /// `Bind` parameter as a fatal socket error.
            if (ignore_extended_query_messages_until_sync
                && message_type != PostgreSQLProtocol::Messaging::FrontMessageType::SYNC
                && message_type != PostgreSQLProtocol::Messaging::FrontMessageType::TERMINATE)
            {
                message_transport->dropMessage();
                continue;
            }

            switch (message_type)
            {
                case PostgreSQLProtocol::Messaging::FrontMessageType::QUERY:
                    processQuery();
                    message_transport->flush();
                    break;
                case PostgreSQLProtocol::Messaging::FrontMessageType::TERMINATE:
                    LOG_DEBUG(log, "Client closed the connection");
                    return;
                case PostgreSQLProtocol::Messaging::FrontMessageType::PARSE:
                    is_query_in_progress = true;
                    processParseQuery();
                    message_transport->flush();
                    break;
                case PostgreSQLProtocol::Messaging::FrontMessageType::BIND:
                    is_query_in_progress = true;
                    processBindQuery();
                    message_transport->flush();
                    break;
                case PostgreSQLProtocol::Messaging::FrontMessageType::EXECUTE:
                    is_query_in_progress = true;
                    processExecuteQuery();
                    message_transport->flush();
                    break;
                case PostgreSQLProtocol::Messaging::FrontMessageType::SYNC:
                    is_query_in_progress = false;
                    processSyncQuery();
                    message_transport->flush();
                    break;
                case PostgreSQLProtocol::Messaging::FrontMessageType::DESCRIBE:
                    is_query_in_progress = true;
                    processDescribeQuery();
                    break;
                case PostgreSQLProtocol::Messaging::FrontMessageType::FLUSH:
                    is_query_in_progress = true;
                    message_transport->send(
                        PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse(
                            PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse::ERROR,
                            "0A000",
                            "ClickHouse doesn't support extended query mechanism"),
                        true);
                    LOG_ERROR(log, "Client tried to access via extended query protocol");
                    message_transport->dropMessage();
                    /// `Flush` is a complete extended-protocol message. Once it
                    /// has produced an error, discard the remaining messages in
                    /// the cycle through the matching `Sync`.
                    ignore_extended_query_messages_until_sync = true;
                    break;
                case PostgreSQLProtocol::Messaging::FrontMessageType::CLOSE:
                    is_query_in_progress = true;
                    processCloseQuery();
                    message_transport->flush();
                    break;
                default:
                    message_transport->send(
                        PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse(
                            PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse::ERROR,
                            "0A000",
                            "Command is not supported"),
                        true);
                    LOG_ERROR(log, "Command is not supported. Command code {:d}", static_cast<Int32>(message_type));
                    message_transport->dropMessage();
            }
        }
    }
    catch (const Poco::Exception &exc)
    {
        log->log(exc);
    }

}

bool PostgreSQLHandler::startup()
{
    Int32 payload_size = 0;
    Int32 info = 0;
    establishSecureConnection(payload_size, info);

    if (static_cast<PostgreSQLProtocol::Messaging::FrontMessageType>(info) == PostgreSQLProtocol::Messaging::FrontMessageType::CANCEL_REQUEST)
    {
        LOG_DEBUG(log, "Client issued request canceling");
        cancelRequest();
        return false;
    }

    std::unique_ptr<PostgreSQLProtocol::Messaging::StartupMessage> start_up_msg = receiveStartupMessage(payload_size);

    /// An empty user name means the default session user: the `default_session_user`
    /// server setting, possibly overridden for this listener in the `protocols` section.
    /// If the resolved name is empty too (explicitly configured to prohibit connections
    /// without a user name), authentication fails on the empty user name below.
    if (start_up_msg->user.empty())
        start_up_msg->user = default_session_user
            ? *default_session_user
            : String(server.context()->getServerSettings()[ServerSetting::default_session_user]);

    const auto & user_name = start_up_msg->user;
    if (user_name.empty())
    {
        auto exception = Exception(ErrorCodes::AUTHENTICATION_FAILED, "Got an empty user name from PostgreSQL startup message");
        session->onAuthenticationFailure(user_name, socket().peerAddress(), exception);
        message_transport->send(
            PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse(
                PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse::ERROR, "28P01", "Invalid user or password"),
            true);
        return false;
    }

    authentication_manager.authenticate(user_name, *session, *message_transport, socket().peerAddress());

    try
    {
        session->makeSessionContext();
        session->sessionContext()->setDefaultFormat("PostgreSQLWire");
        if (!start_up_msg->database.empty())
        {
            /// `database` is a real setting, so enforce its constraints on the startup-message
            /// database too, consistently with `USE`, `SET database = ...` and the HTTP
            /// `?database=...` parameter.
            SettingsChanges database_change;
            database_change.setSetting("database", start_up_msg->database);
            session->sessionContext()->checkSettingsConstraints(database_change, SettingSource::QUERY);
            session->sessionContext()->setCurrentDatabase(start_up_msg->database);
        }
    }
    catch (const Exception & exc)
    {
        message_transport->send(
            PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse(
                PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse::ERROR, "XX000", exc.message()),
            true);
        throw;
    }

    sendParameterStatusData(*start_up_msg);

    message_transport->send(
        PostgreSQLProtocol::Messaging::BackendKeyData(connection_id, secret_key), true);

    LOG_DEBUG(log, "Successfully finished Startup stage");
    return true;
}

void PostgreSQLHandler::establishSecureConnection(Int32 & payload_size, Int32 & info)
{
    bool was_secure_connection = false;
    bool was_encryption_req = true;
    readBinaryBigEndian(payload_size, *in);
    readBinaryBigEndian(info, *in);

    switch (static_cast<PostgreSQLProtocol::Messaging::FrontMessageType>(info))
    {
        case PostgreSQLProtocol::Messaging::FrontMessageType::SSL_REQUEST:
            LOG_DEBUG(log, "Client requested SSL");
            if (ssl_enabled)
            {
                was_secure_connection = true;
                makeSecureConnectionSSL();
            }
            else
                message_transport->send('N', true);
            break;
        case PostgreSQLProtocol::Messaging::FrontMessageType::GSSENC_REQUEST:
            LOG_DEBUG(log, "Client requested GSSENC");
            message_transport->send('N', true);
            break;
        default:
            was_encryption_req = false;
    }
    if (was_encryption_req)
    {
        readBinaryBigEndian(payload_size, *in);
        readBinaryBigEndian(info, *in);
    }

    if (secure_required && !was_secure_connection)
    {
        message_transport->send(
            PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse(
                PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse::ERROR, "XX000", "SSL connection required."),
            true);
        throw Exception(ErrorCodes::OPENSSL_ERROR, "SSL connection required.");
    }
}

#if USE_SSL
void PostgreSQLHandler::makeSecureConnectionSSL()
{
    message_transport->send('S', true);
    Poco::Net::Context::Ptr ctx;
    if (!params.privateKeyFile.empty() && !params.certificateFile.empty())
    {
        ctx = Poco::Net::SSLManager::instance().getCustomServerContext(prefix);
        if (!ctx)
        {
            ctx = new Poco::Net::Context(usage, params);
            ctx->disableProtocols(disabled_protocols);
            ctx->enableExtendedCertificateVerification(extended_verification);
            if (prefer_server_ciphers)
                ctx->preferServerCiphers();
            CertificateReloader::instance().tryLoad(config, ctx->sslContext(), prefix);
            ctx = Poco::Net::SSLManager::instance().setCustomServerContext(prefix, ctx);
        }
    }
    else
    {
        ctx = Poco::Net::SSLManager::instance().defaultServerContext();
    }
    ss = std::make_shared<Poco::Net::SecureStreamSocket>(Poco::Net::SecureStreamSocket::attach(socket(), ctx));
    changeIO(*ss);
}
#else
void PostgreSQLHandler::makeSecureConnectionSSL() {}
#endif

void PostgreSQLHandler::sendParameterStatusData(PostgreSQLProtocol::Messaging::StartupMessage & start_up_message)
{
    auto & parameters = start_up_message.parameters;

    if (parameters.contains("application_name"))
        message_transport->send(PostgreSQLProtocol::Messaging::ParameterStatus("application_name", parameters["application_name"]));
    if (parameters.contains("client_encoding"))
        message_transport->send(PostgreSQLProtocol::Messaging::ParameterStatus("client_encoding", parameters["client_encoding"]));
    else
        message_transport->send(PostgreSQLProtocol::Messaging::ParameterStatus("client_encoding", "UTF8"));

    message_transport->send(PostgreSQLProtocol::Messaging::ParameterStatus("server_version", VERSION_STRING));
    message_transport->send(PostgreSQLProtocol::Messaging::ParameterStatus("server_encoding", "UTF8"));
    message_transport->send(PostgreSQLProtocol::Messaging::ParameterStatus("DateStyle", "ISO"));
    /// The simple-query scanner treats a backslash in a plain string literal as an ordinary character
    /// (only the `E'...'` form escapes), which is PostgreSQL's `standard_conforming_strings = on`
    /// behavior - report it, so libpq-based clients do not double backslashes in plain literals
    /// (libpq assumes `off` when the parameter is not reported). `current_setting` reports the same.
    message_transport->send(PostgreSQLProtocol::Messaging::ParameterStatus("standard_conforming_strings", "on"));
    message_transport->send(PostgreSQLProtocol::Messaging::ParameterStatus("integer_datetimes", "on"));
    message_transport->flush();
}

String PostgreSQLHandler::queryIdFor(Int32 connection_id_, Int32 secret_key_)
{
    return fmt::format("postgres:{:d}:{:d}", connection_id_, secret_key_);
}

String PostgreSQLHandler::currentQueryId() const
{
    return queryIdFor(connection_id, secret_key);
}

void PostgreSQLHandler::cancelRequest()
{
    std::unique_ptr<PostgreSQLProtocol::Messaging::CancelRequest> msg =
        message_transport->receiveWithPayloadSize<PostgreSQLProtocol::Messaging::CancelRequest>(8);

    /// A cancel request arrives on a connection of its own which, by the protocol, never authenticates:
    /// the pair of numbers it carries is the credential, and the secret key half of it is what makes the
    /// query id of the connection being cancelled unguessable. So there is no authenticated session here
    /// to make a query context from - cancel the query through the process list directly.
    ///
    /// PostgreSQL answers a cancel request with nothing at all and closes the connection, whatever the
    /// outcome, so that a caller cannot probe for live backends. Report the outcome to the log only.
    String query_id = queryIdFor(msg->process_id, msg->secret_key);
    CancellationCode code = server.context()->getProcessList().sendCancelToPostgreSQLQuery(query_id);
    LOG_DEBUG(log, "Cancellation of query {}: {}", query_id, code == CancellationCode::CancelSent ? "sent" : "not sent");
}

inline std::unique_ptr<PostgreSQLProtocol::Messaging::StartupMessage> PostgreSQLHandler::receiveStartupMessage(int payload_size)
{
    /// The declared size is read from the wire before any authentication, and the message is read
    /// into memory in full, so it has to be bounded. PostgreSQL uses the same limit.
    static constexpr Int32 max_startup_message_size = 10000;

    std::unique_ptr<PostgreSQLProtocol::Messaging::StartupMessage> message;
    try
    {
        if (payload_size < 8 || payload_size > max_startup_message_size)
            throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT,
                "Startup message declares a size of {} bytes, while it must be between 8 and {} bytes",
                payload_size, max_startup_message_size);

        message = message_transport->receiveWithPayloadSize<PostgreSQLProtocol::Messaging::StartupMessage>(payload_size - 8);
    }
    catch (const Exception &)
    {
        message_transport->send(
            PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse(
                PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse::ERROR, "08P01", "Can't correctly handle Startup message"),
            true);
        throw;
    }

    LOG_DEBUG(log, "Successfully received Startup message");
    return message;
}

/// PostgreSQL clients qualify catalog tables and functions with the `pg_catalog`
/// schema, e.g. `pg_catalog.pg_class` or `pg_catalog.pg_table_is_visible(c.oid)`
/// (psql does so for the `\d` command). ClickHouse has no `pg_catalog` database:
/// the catalog tables are emulated with per-session temporary views
/// (see `initializeSystemTables`) and the functions are registered globally.
/// Removing the qualifier at the token level maps such queries onto them.
/// String literals are left intact - only a `pg_catalog` identifier that is not
/// itself qualified and is followed by a dot and another identifier is removed.
static String removePgCatalogQualifier(const String & query)
{
    if (!query.contains("pg_catalog"))
        return query;

    std::vector<Token> tokens;
    Lexer lexer(query.data(), query.data() + query.size());
    for (Token token = lexer.nextToken(); !token.isEnd(); token = lexer.nextToken())
        tokens.push_back(token);

    auto is_pg_catalog = [](const Token & token)
    {
        std::string_view text(token.begin, token.size());
        return (token.type == TokenType::BareWord && text == "pg_catalog")
            || (token.type == TokenType::QuotedIdentifier && text == "\"pg_catalog\"");
    };

    auto next_significant = [&](size_t i) -> std::optional<size_t>
    {
        for (size_t j = i + 1; j < tokens.size(); ++j)
            if (tokens[j].isSignificant())
                return j;
        return std::nullopt;
    };

    String result;
    result.reserve(query.size());
    std::optional<size_t> prev_emitted_significant;
    for (size_t i = 0; i < tokens.size(); ++i)
    {
        const Token & token = tokens[i];
        if (is_pg_catalog(token)
            && (!prev_emitted_significant || tokens[*prev_emitted_significant].type != TokenType::Dot))
        {
            auto dot = next_significant(i);
            if (dot && tokens[*dot].type == TokenType::Dot)
            {
                auto after_dot = next_significant(*dot);
                if (after_dot
                    && (tokens[*after_dot].type == TokenType::BareWord || tokens[*after_dot].type == TokenType::QuotedIdentifier))
                {
                    /// Skip the qualifier and the dot (and anything insignificant in between).
                    i = *dot;
                    continue;
                }
            }
        }
        result.append(token.begin, token.end);
        if (token.isSignificant())
            prev_emitted_significant = i;
    }
    return result;
}

PostgreSQLHandler::CopyQueryResult PostgreSQLHandler::processCopyQuery(const String & query)
{
    copy_protocol_error = false;
    ParserCopyQuery parser_copy;
    ASTPtr copy_query_parsed;

    try
    {
        copy_query_parsed = parseQuery(parser_copy, query, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    }
    catch (const Exception &)
    {
        copy_query_parsed.reset();
    }


    /// PostgreSQL binary `COPY` uses its own wire format (a `PGCOPY\n\377\r\n\0` header, per-tuple field
    /// counts and per-field length framing). We do not implement it, and `CopyInResponse` / `CopyOutResponse`
    /// always advertise the text format code, so emitting ClickHouse's `RowBinary` for `WITH FORMAT binary`
    /// would hand a real PostgreSQL client a payload it cannot parse. Reject it explicitly instead - the text
    /// and CSV formats cover the self-connect use case (ClickHouse reads the result with `pqxx`, which uses
    /// text `COPY`).
    ///
    /// Send an `ErrorResponse` and return (marking the query as handled) rather than throwing: this is an
    /// ordinary "your query failed" error, not a fatal connection error, so the connection must stay open
    /// for the `ReadyForQuery` that the run loop sends next. Throwing would tear the connection down right
    /// after the `ErrorResponse` and before that `ReadyForQuery`, and a driver such as `libpq`/`psycopg2`
    /// (unlike the plain `psql` REPL) that is still completing the command cycle then hits EOF and reports
    /// "server closed the connection unexpectedly" instead of surfacing this message.
    if (copy_query_parsed && copy_query_parsed->as<ASTCopyQuery>()->format == ASTCopyQuery::Formats::Binary)
    {
        message_transport->send(
            PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse(
                PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse::ERROR, "0A000",
                "PostgreSQL binary COPY format is not supported; use the text or CSV format"),
            true);
        return CopyQueryResult::ErrorHandled;
    }

    /// A `COPY` we cannot faithfully serve is rejected here rather than silently mis-served. This covers a
    /// copy endpoint other than the client stream (only `COPY ... TO STDOUT` and `COPY ... FROM STDIN` are
    /// implemented - a file path or `PROGRAM '...'` would make us drive the wrong side of the protocol), and
    /// a data-formatting option we cannot honor (a non-default `DELIMITER`, a non-default `NULL` marker, a
    /// `HEADER`, or any option we do not interpret): honoring only the format while dropping such options
    /// would stream output that does not match what the client requested. The parser records the reason in
    /// `unsupported_option`. Like the binary rejection above, this is sent as an ordinary `ErrorResponse`
    /// (not thrown) so the connection stays open for the following `ReadyForQuery`.
    if (copy_query_parsed && !copy_query_parsed->as<ASTCopyQuery>()->unsupported_option.empty())
    {
        message_transport->send(
            PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse(
                PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse::ERROR, "0A000",
                fmt::format("PostgreSQL COPY with {} is not supported",
                            copy_query_parsed->as<ASTCopyQuery>()->unsupported_option)),
            true);
        return CopyQueryResult::ErrorHandled;
    }

    /* The Postgres protocol for a copy query is different from simple queries such as SELECT.
     * In the case of a COPY FROM request, the server sends CopyInResponse - a sign of readiness to receive data from the client.
     * The client then sends CopyInData until all data has been sent.
     * After this, the server sends a CommandComplete response.
     * For more detailes see https://www.dolthub.com/blog/2024-09-17-tabular-data-imports/
     */
    if (copy_query_parsed && copy_query_parsed->as<ASTCopyQuery>()->type == ASTCopyQuery::QueryType::COPY_FROM)
    {
        auto * copy_query = copy_query_parsed->as<ASTCopyQuery>();
        auto query_context = session->makeQueryContext();
        query_context->setCurrentQueryId(currentQueryId());

        /// PostgreSQL's CSV convention is that an empty unquoted field means NULL (a quoted empty string
        /// stays an empty string), while ClickHouse's CSV default marker is `\N`. Apply the marker the
        /// client asked for - PostgreSQL's default, or an explicit `NULL '\N'` - so that nullable values
        /// are read back faithfully.
        if (copy_query->format == ASTCopyQuery::Formats::CSV)
            query_context->setSetting("format_csv_null_representation", copy_query->csv_null_marker);

        /// Prepare the emulated `pg_catalog` views before the copy, mirroring `processQuery`, so that the
        /// `COPY` path sees the same catalog surface as ordinary queries on a fresh connection.
        prepareSystemTables(query_context, query);

        QueryScope query_scope = QueryScope::create(query_context);

        String columns_to_insert;
        if (!copy_query->column_names.empty())
        {
            for (const auto & column_name : copy_query->column_names)
                columns_to_insert += fmt::format("{}, ", column_name);
            columns_to_insert.pop_back();
            columns_to_insert.pop_back();
            columns_to_insert = "(" + columns_to_insert + ")";
        }

        /// `table_name` is already rendered as valid SQL by the parser (each part of a compound
        /// `database.table` name separately backquoted), so it must not be wrapped in backquotes again.
        String table_name = copy_query->table_name;
        /// The emulated catalog relations are temporary views, which ClickHouse exposes only in the
        /// current database. Accept PostgreSQL's explicit catalog qualification at the wire boundary and
        /// resolve it to those views, so discovery and the subsequent COPY use the same relation.
        if (table_name.starts_with("pg_catalog."))
            table_name.erase(0, sizeof("pg_catalog.") - 1);
        else if (table_name.starts_with("`pg_catalog`."))
            table_name.erase(0, sizeof("`pg_catalog`.") - 1);

        auto [ast, io] = executeQuery(fmt::format("INSERT INTO {} {} FROM INFILE 'psql_copy'", table_name, columns_to_insert), query_context, {}, QueryProcessingStage::Enum::Complete);
        chassert(io.pipeline.pushing());

        String format = toString(copy_query->format);

        const Settings & settings = query_context->getSettingsRef();

        message_transport->send(PostgreSQLProtocol::Messaging::CopyInResponse(static_cast<Int32>(io.pipeline.getHeader().columns())), true);

        /// `CopyData` frame boundaries carry no meaning: a single logical row may arrive split across
        /// several frames (a client is free to chunk the payload however it likes), and one frame may
        /// hold many rows. So the whole `COPY` payload is parsed by a single input format reading the
        /// concatenation of all frames as one stream - never one parser per frame, which would drop a
        /// partial trailing row of every frame.
        ///
        /// The payload is staged in full before any of it is fed to the insert pipeline. Sinks such as
        /// `MergeTreeSink` commit parts while the insert streams (`consume` / `finishDelayedChunk`), so
        /// feeding the pipeline as frames arrive would let a client abort (`CopyFail`) after the first
        /// flushed block leave partial rows visible even though the `COPY` reports failure - and a client
        /// retry would then duplicate them. Staging moves the commit boundary after a successful
        /// `CopyDone`: an aborted copy never touches the insert pipeline at all.
        ///
        /// The staging store is bounded in memory: the first part of the payload is kept in a
        /// `MemoryWriteBuffer` (allocated under the query scope, so the query's memory limits account
        /// for it), and anything beyond that spills to a temporary file on disk, so a large `COPY`
        /// stages in O(1) memory instead of holding the whole payload resident.
        static constexpr size_t max_staged_bytes_in_memory = 1024 * 1024;

        /// The insert query this `COPY` stages for is already registered in the process list (under the
        /// connection's `postgres:<id>:<key>` query id), so an external `CancelRequest` or `KILL QUERY`
        /// targets it; the staging loop below observes the kill through this check while it waits for
        /// the client.
        auto process_list_element = query_context->getProcessListElement();
        CopyInDataReadBuffer copy_in_stream(
            *message_transport, *in,
            [process_list_element]
            {
                if (process_list_element && process_list_element->isKilled())
                    throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "COPY FROM STDIN was cancelled");
            });
        CascadeWriteBuffer::WriteBufferPtrs staging_buffers;
        staging_buffers.emplace_back(std::make_shared<MemoryWriteBuffer>(max_staged_bytes_in_memory));
        CascadeWriteBuffer::WriteBufferConstructors staging_buffers_lazy;
        staging_buffers_lazy.emplace_back(
            [tmp_data = query_context->getTempDataOnDisk()](const WriteBufferPtr &) -> WriteBufferPtr
            {
                return std::make_unique<TemporaryDataBuffer>(tmp_data);
            });
        CascadeWriteBuffer staged_out(std::move(staging_buffers), std::move(staging_buffers_lazy));
        try
        {
            copyData(copy_in_stream, staged_out);
            staged_out.finalize();
        }
        catch (...)
        {
            staged_out.cancel();

            if (copy_in_stream.protocolError())
            {
                copy_protocol_error = true;
                throw;
            }

            /// A failed write to the client cancels `out` (see `WriteBuffer::next`); nothing can be
            /// sent on a canceled buffer, not even an error response - tear the connection down.
            if (out->isCanceled())
                throw;

            /// An external cancel (`CancelRequest` / `KILL QUERY`) aborts the copy the way PostgreSQL
            /// does: release the insert query promptly (nothing has been pushed to its pipeline, so the
            /// target table is untouched), report `57014 query_canceled` to the client, and keep
            /// consuming the copy-subprotocol frames the client is still entitled to send until it
            /// terminates the copy, so the connection stays usable afterwards.
            if (getCurrentExceptionCode() == ErrorCodes::QUERY_WAS_CANCELLED)
            {
                io.onException(/*log_as_error=*/ false);
                /// `onException` stops the pipeline but keeps the process list entry; drop it explicitly
                /// so the killed query leaves `system.processes` before the drain below blocks on the
                /// client (a `KILL QUERY ... SYNC` must not wait for the client to end the copy).
                io.process_list_entries.clear();
                message_transport->send(
                    PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse(
                        PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse::ERROR, "57014",
                        "canceling COPY FROM STDIN due to user request"),
                    true);
                /// The cancel can land in the middle of a `CopyData` frame, even in the middle of its
                /// length field if the client split the header. In the first case the rest of the frame is
                /// payload and is skipped before the drain looks for the next message; in the second there
                /// is no way to tell payload from a header any more, so the connection cannot be reused -
                /// let the error propagate and close it, as PostgreSQL does on a desynchronized stream.
                if (!copy_in_stream.canResynchronize())
                    throw;
                discardRemainingCopyInFrames(copy_in_stream.pendingFrameBytes());
                return CopyQueryResult::ErrorHandled;
            }

            /// A client-initiated abort (`CopyFail`) is an ordinary "your copy failed" error, not a fatal
            /// connection error: reply with an `ErrorResponse` and report the query as handled so the run
            /// loop follows with `ReadyForQuery` and the connection stays usable (mirroring the binary and
            /// unsupported-option rejections above). Nothing has been pushed to the insert pipeline, so the
            /// target table is untouched. Any other error is a genuine failure - rethrow it.
            if (copy_in_stream.clientAborted())
            {
                message_transport->send(
                    PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse(
                        PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse::ERROR, "57014",
                        fmt::format("COPY FROM STDIN aborted by the client: {}", copy_in_stream.abortReason())),
                    true);
                return CopyQueryResult::ErrorHandled;
            }
            throw;
        }

        /// The staged payload has to be read twice - once to parse it in full, once to insert it (see
        /// below) - so keep it in a re-readable form. A spilled part lives in a temporary file that can
        /// be reopened for every pass, while `MemoryWriteBuffer`'s read buffer is one-shot and consumes
        /// the buffer, so materialize the in-memory part (bounded by `max_staged_bytes_in_memory`) into a
        /// string of our own.
        String staged_in_memory;
        TemporaryDataBuffer * staged_spilled = nullptr;
        /// The result buffers own the spilled temporary file, so hold on to them: `staged_spilled` below
        /// points into this vector and is read from twice.
        auto staged_result_buffers = staged_out.getResultBuffers();
        for (auto & staged_buf : staged_result_buffers)
        {
            if (auto * spilled = dynamic_cast<TemporaryDataBuffer *>(staged_buf.get()))
            {
                staged_spilled = spilled;
            }
            else if (auto * readable = dynamic_cast<IReadableWriteBuffer *>(staged_buf.get()))
            {
                if (auto reread_buf = readable->tryGetReadBuffer())
                {
                    WriteBufferFromString in_memory_out(staged_in_memory);
                    copyData(*reread_buf, in_memory_out);
                    in_memory_out.finalize();
                }
            }
        }

        /// Reassemble the staged payload as one read stream: the in-memory part, then the spilled part.
        const auto make_staged_stream = [&]
        {
            auto stream = std::make_unique<ConcatReadBuffer>();
            if (!staged_in_memory.empty())
                stream->appendBuffer(std::make_unique<ReadBufferFromString>(staged_in_memory));
            if (staged_spilled)
            {
                if (auto reread_buf = staged_spilled->read())
                    stream->appendBuffer(std::move(reread_buf));
            }
            return stream;
        };

        /// `Array(...)` columns arrive in the PostgreSQL array-literal spelling (`{...}`) - that is what a
        /// PostgreSQL client sends, and what `COPY ... TO STDOUT` emits on the way out - while the text
        /// input formats only understand ClickHouse's `[...]`. Read those fields as `String` and translate
        /// them afterwards, mirroring the pre-rendering the `COPY TO` path does, so that an array column
        /// round-trips through `COPY`.
        const Block insert_header = io.pipeline.getHeader();
        Block parse_header;
        std::vector<size_t> array_positions;
        for (size_t col = 0; col < insert_header.columns(); ++col)
        {
            const auto & src = insert_header.getByPosition(col);
            if (isArray(src.type))
            {
                array_positions.push_back(col);
                auto str_type = std::make_shared<DataTypeString>();
                parse_header.insert({str_type->createColumn(), str_type, src.name});
            }
            else
                parse_header.insert({src.type->createColumn(), src.type, src.name});
        }

        FormatSettings array_settings;
        array_settings.bool_true_representation = "t";
        array_settings.bool_false_representation = "f";

        /// Translate the array columns of a parsed chunk from the PostgreSQL literal back into the target
        /// `Array(...)` type. A literal that is malformed, or whose element does not fit the target type,
        /// throws - during the validation pass below this happens before the table is touched.
        const auto convert_arrays = [&](Chunk chunk)
        {
            if (array_positions.empty())
                return chunk;

            const size_t num_rows = chunk.getNumRows();
            auto columns = chunk.detachColumns();
            for (const size_t col : array_positions)
            {
                const auto & type = insert_header.getByPosition(col).type;
                const auto & literals = assert_cast<const ColumnString &>(*columns[col]);
                auto array_column = type->createColumn();
                array_column->reserve(num_rows);
                for (size_t row = 0; row < num_rows; ++row)
                    readPostgreSQLArrayText(*array_column, *type, std::string_view(literals.getDataAt(row)), array_settings);
                columns[col] = std::move(array_column);
            }
            return Chunk(std::move(columns), num_rows);
        };

        const auto make_input_format = [&](ReadBuffer & stream)
        {
            return FormatFactory::instance().getInput(
                format,
                stream,
                parse_header,
                query_context,
                settings[Setting::max_insert_block_size],
                std::nullopt,
                nullptr,
                nullptr,
                false,
                CompressionMethod::None,
                false,
                settings[Setting::max_insert_block_size_bytes],
                settings[Setting::min_insert_block_size_rows],
                settings[Setting::min_insert_block_size_bytes]);
        };

        /// Parse the whole staged payload before a single row reaches the insert pipeline. A malformed
        /// row - or a value that does not fit its target column - can appear anywhere in the payload,
        /// and the sink commits parts as the data streams through it, which `cancel` cannot roll back.
        /// Detecting such an error only on the way in would leave the rows before it visible, while the
        /// client is told the `COPY` failed: PostgreSQL clients treat `COPY FROM STDIN` as all-or-nothing
        /// and would duplicate that prefix when they retry. Parsing everything up front costs one extra
        /// pass over the staged bytes and keeps the durable commit boundary after the whole payload has
        /// been accepted.
        ///
        /// This covers the parsing and the conversion to the target columns, that is, everything decided
        /// by the payload itself. An error raised deeper in the insert pipeline - a materialized view, a
        /// storage error - is still not rolled back, because a ClickHouse `INSERT` is not transactional;
        /// such a `COPY` behaves exactly like a plain `INSERT` of the same data.
        try
        {
            auto validation_stream = make_staged_stream();
            auto validation_format = make_input_format(*validation_stream);
            while (true)
            {
                auto chunk = validation_format->generate();
                if (chunk.empty())
                    break;
                /// Translating the arrays is part of the validation: a malformed array literal must be
                /// caught here, while the table is still untouched.
                convert_arrays(std::move(chunk));
            }
        }
        catch (...)
        {
            /// The payload has been received in full, so the copy sub-protocol is complete and the
            /// connection is in a clean state: report the failure the way PostgreSQL does - an
            /// `ErrorResponse` for a handled query, after which the run loop sends `ReadyForQuery` - and
            /// keep the connection usable. The target table has not been touched.
            tryLogCurrentException(log, "Failed to parse the payload of COPY FROM STDIN");
            message_transport->send(
                PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse(
                    PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse::ERROR, "22P04",
                    fmt::format("COPY FROM STDIN failed: {}", getCurrentExceptionMessage(/* with_stacktrace = */ false))),
                true);
            return CopyQueryResult::ErrorHandled;
        }

        /// The executor is created only after the payload is staged and parsed in full: creating it
        /// earlier would leave an unfinished pushing executor behind on the return paths above.
        auto insert_stream = make_staged_stream();
        auto executor = std::make_unique<PushingPipelineExecutor>(io.pipeline);
        auto format_ptr = make_input_format(*insert_stream);

        executor->start();
        Int32 rows_count = 0;
        try
        {
            while (true)
            {
                auto chunk = format_ptr->generate();
                if (chunk.empty())
                    break;

                rows_count += static_cast<Int32>(chunk.getNumRows());
                executor->push(convert_arrays(std::move(chunk)));
            }
            executor->finish();
        }
        catch (...)
        {
            executor->cancel();
            throw;
        }

        /// PostgreSQL reports the number of rows the copy inserted in the command tag ("COPY n"), which
        /// clients show and scripts check.
        auto command = PostgreSQLProtocol::Messaging::CommandComplete::Command::COPY;
        message_transport->send(PostgreSQLProtocol::Messaging::CommandComplete(command, rows_count), true);
        return CopyQueryResult::Success;
    }

    /* In the case of a COPY TO request, the server calculates the number of columns and then sends it to the client in CopyOutResponse.
     * After this, the server sends the data in a CopyOutData message, and when the data runs out, it sends a CopyCompletionResponse.
     * For more detailes see https://www.dolthub.com/blog/2024-09-17-tabular-data-imports/
     */
    if (copy_query_parsed && copy_query_parsed->as<ASTCopyQuery>()->type == ASTCopyQuery::QueryType::COPY_TO)
    {
        auto * copy_query = copy_query_parsed->as<ASTCopyQuery>();
        auto query_context = session->makeQueryContext();
        query_context->setCurrentQueryId(currentQueryId());

        /// PostgreSQL's CSV convention is that an empty unquoted field means NULL (a quoted empty string is
        /// written as `""`), while ClickHouse's CSV default marker is `\N`. Apply the marker the client
        /// asked for - PostgreSQL's default, or an explicit `NULL '\N'` - so that nullable values are
        /// streamed in the form the client expects.
        if (copy_query->format == ASTCopyQuery::Formats::CSV)
            query_context->setSetting("format_csv_null_representation", copy_query->csv_null_marker);

        /// Lazily prepare the emulated `pg_catalog` views before running the copied query, exactly as
        /// `processQuery` does for ordinary queries. Otherwise `COPY (SELECT * FROM pg_namespace) TO STDOUT`
        /// on a fresh connection would fail with `UNKNOWN_TABLE` while a plain `SELECT * FROM pg_namespace`
        /// succeeds.
        prepareSystemTables(query_context, query);

        QueryScope query_scope = QueryScope::create(query_context);

        String columns_to_select = "*";
        if (!copy_query->column_names.empty())
        {
            columns_to_select.clear();
            for (const auto & column_name : copy_query->column_names)
                columns_to_select += fmt::format("{}, ", column_name);
            columns_to_select.pop_back();
            columns_to_select.pop_back();
        }

        /// `COPY (query) TO STDOUT` streams the result of an arbitrary query (this is how libpq/pqxx read
        /// result sets); `COPY table TO STDOUT` streams a whole table.
        String table_name = copy_query->table_name;
        /// See the corresponding COPY FROM path above.
        if (table_name.starts_with("pg_catalog."))
            table_name.erase(0, sizeof("pg_catalog.") - 1);
        else if (table_name.starts_with("`pg_catalog`."))
            table_name.erase(0, sizeof("`pg_catalog`.") - 1);

        auto select_query = copy_query->subquery.empty()
            ? fmt::format("SELECT {} FROM {};", columns_to_select, table_name)
            : copy_query->subquery;
        auto [ast, io] = executeQuery(select_query, query_context, {}, QueryProcessingStage::Enum::Complete);
        chassert(io.pipeline.pulling());

        /// `Array(...)` columns must be streamed in PostgreSQL array-literal form (`{...}`) rather than
        /// ClickHouse's `[...]`, because libpq/pqxx (and ClickHouse's own `postgresql(...)` source, which
        /// reads the result back with `pqxx::array_parser`) expect the PostgreSQL spelling. The text COPY
        /// output formats (TSV/CSV) have no notion of that, so array columns are pre-rendered here into a
        /// `String` column holding the PostgreSQL literal; the header used for the output format carries
        /// `String` for those columns accordingly. Elements use the `t`/`f` boolean spelling like the rest
        /// of the PostgreSQL wire path. (The binary format is rejected earlier, so this path is text-only.)
        const Block source_header = io.pipeline.getHeader();
        std::vector<UInt8> is_array_column(source_header.columns(), 0);
        Block output_header;
        for (size_t col = 0; col < source_header.columns(); ++col)
        {
            const auto & src = source_header.getByPosition(col);
            if (isArray(src.type))
            {
                is_array_column[col] = 1;
                auto str_type = std::make_shared<DataTypeString>();
                output_header.insert({str_type->createColumn(), str_type, src.name});
            }
            else
                output_header.insert({src.type->createColumn(), src.type, src.name});
        }

        FormatSettings array_settings;
        array_settings.bool_true_representation = "t";
        array_settings.bool_false_representation = "f";

        message_transport->send(PostgreSQLProtocol::Messaging::CopyOutResponse(static_cast<Int32>(source_header.columns())));
        VectorWithMemoryTracking<char> result_buf;
        WriteBufferFromVectorImpl<decltype(result_buf)> output_buffer(result_buf);
        auto format_ptr = FormatFactory::instance().getOutputFormat(toString(copy_query->format), output_buffer, output_header, query_context);
        auto executor = std::make_unique<PullingPipelineExecutor>(io.pipeline);
        Block block;
        Int32 rows_count = 0;
        try
        {
            while (executor->pull(block))
            {
                /// PostgreSQL's COPY protocol expects one CopyData message per row, and libpq/pqxx rely on
                /// this (they do not re-split a message into rows). Serialize each row on its own instead of
                /// formatting the whole block and splitting the result on '\n': the latter only works for the
                /// text/TSV path (where newlines inside values are escaped) and corrupts formats where a single
                /// row is not one physical line - e.g. a quoted CSV field containing a newline, or the binary
                /// format, which is not newline-delimited at all and would otherwise emit nothing.
                Block materialized = materializeBlock(block);
                for (size_t row = 0, num_rows = materialized.rows(); row < num_rows; ++row)
                {
                    output_buffer.restart(DBMS_DEFAULT_BUFFER_SIZE); // This will recreate the moved-out vector.

                    Columns row_columns;
                    row_columns.reserve(materialized.columns());
                    for (size_t col = 0; col < materialized.columns(); ++col)
                    {
                        const auto & elem = materialized.getByPosition(col);
                        if (is_array_column[col])
                        {
                            auto str_column = ColumnString::create();
                            WriteBufferFromOwnString literal;
                            writePostgreSQLArrayText(*elem.column, *elem.type, row, literal, array_settings);
                            str_column->insertData(literal.str().data(), literal.str().size());
                            row_columns.push_back(std::move(str_column));
                        }
                        else
                            row_columns.push_back(elem.column->cut(row, 1));
                    }

                    format_ptr->write(output_header.cloneWithColumns(row_columns));
                    format_ptr->flush();
                    output_buffer.finalize();

                    message_transport->send(PostgreSQLProtocol::Messaging::CopyOutData(result_buf));
                }
                rows_count += static_cast<Int32>(materialized.rows());
            }
            /// The buffer is finalized after each row above, but a result with no rows at all (for example a
            /// catalog probe for a table that does not exist) never enters the loop, and a `WriteBuffer` must
            /// not reach its destructor neither finalized nor canceled. `finalize` is idempotent, so this is
            /// a no-op when the last row already finalized the buffer.
            output_buffer.finalize();
        }
        catch (...)
        {
            output_buffer.cancel();

            /// A failed write to the client cancels `out` (see `WriteBuffer::next`); nothing can be
            /// sent on a canceled buffer, not even an error response - tear the connection down.
            if (out->isCanceled())
                throw;

            /// PostgreSQL reports a backend-detected error during copy-out with an `ErrorResponse` and
            /// reverts to normal processing - the connection stays usable. Every `CopyData` frame above
            /// is sent whole, so the stream is at a message boundary; libpq surfaces the error to the
            /// client once the run loop follows with `ReadyForQuery` (closing the connection instead
            /// would make libpq report a lost connection and swallow the error text).
            tryLogCurrentException(log, "Failed to stream the result of COPY TO STDOUT");
            message_transport->send(
                PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse(
                    PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse::ERROR, "2F000",
                    "Query execution failed.\n" + getCurrentExceptionMessage(/* with_stacktrace = */ false)),
                true);
            return CopyQueryResult::ErrorHandled;
        }
        /// A COPY TO STDOUT must be terminated by CopyDone, then CommandComplete ("COPY n"), then
        /// ReadyForQuery (sent by the caller). libpq/pqxx report an error if CommandComplete is missing.
        message_transport->send(PostgreSQLProtocol::Messaging::CopyCompletionResponse());
        message_transport->send(
            PostgreSQLProtocol::Messaging::CommandComplete(
                PostgreSQLProtocol::Messaging::CommandComplete::Command::COPY, rows_count),
            true);
        return CopyQueryResult::Success;
    }

    return CopyQueryResult::NotCopy;
}

void PostgreSQLHandler::discardRemainingCopyInFrames(size_t pending_frame_bytes)
{
    /// Mirrors what PostgreSQL does after reporting an error mid `COPY ... FROM STDIN`: the client may
    /// keep sending `CopyData` frames until it learns of the error, and the backend must consume and
    /// discard them; the copy sub-protocol ends when the client sends `CopyDone` or `CopyFail`, and only
    /// then may `ReadyForQuery` follow. Anything else at this point is a protocol violation.

    /// The copy may have been abandoned in the middle of a frame body (an external cancel does not wait
    /// for a frame boundary), and the rest of that body is payload: skip it, or the next message header
    /// would be looked for inside it.
    while (pending_frame_bytes > 0)
    {
        while (!in->poll(1000000))
            if (!tcp_server.isOpen())
                return;
        /// `poll` said the socket is readable, so this refill does not block; skipping only what has
        /// arrived keeps the loop responsive to the server shutting down.
        if (in->eof())
            return;
        const size_t skipped = std::min(in->available(), pending_frame_bytes);
        in->position() += skipped;
        pending_frame_bytes -= skipped;
    }

    while (true)
    {
        while (!in->poll(1000000))
            if (!tcp_server.isOpen())
                return;
        PostgreSQLProtocol::Messaging::FrontMessageType message_type = message_transport->receiveMessageType();
        switch (message_type)
        {
            case PostgreSQLProtocol::Messaging::FrontMessageType::COPY_DATA:
                message_transport->dropMessage();
                break;
            case PostgreSQLProtocol::Messaging::FrontMessageType::COPY_COMPLETION:
            case PostgreSQLProtocol::Messaging::FrontMessageType::COPY_FAILURE:
                message_transport->dropMessage();
                return;
            default:
                throw Exception(
                    ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT,
                    "Received message of type {} while discarding the payload of an aborted COPY FROM STDIN",
                    static_cast<Int32>(message_type));
        }
    }
}

namespace
{

/// Splits a PostgreSQL simple-query message into its statements on top-level semicolons. The scan is
/// SQL-aware through `skipOpaqueSQLToken`: a semicolon inside a string literal, a quoted identifier,
/// a comment or a dollar-quoted string does not split. The statements are not parsed - unlike
/// `splitMultipartQuery`, this accepts the statements only the PostgreSQL handler understands
/// (`PREPARE`, `EXECUTE`, `COPY`, transaction control, driver no-ops), which the ClickHouse parser
/// would reject. Fragments of whitespace and comments only (a trailing semicolon, `;;`, a trailing
/// `-- comment`) are dropped.
std::vector<String> splitPostgreSQLStatements(const String & text)
{
    std::vector<String> statements;
    const size_t size = text.size();
    size_t start = 0;
    size_t i = 0;

    const auto flush = [&](size_t end)
    {
        String fragment = text.substr(start, end - start);
        start = end + 1;
        /// A fragment of whitespace and comments only (a trailing semicolon, `;;`, a trailing
        /// `-- comment`) is not a statement.
        size_t k = 0;
        while (k < fragment.size())
        {
            const char c = fragment[k];
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v')
            {
                ++k;
                continue;
            }
            const bool is_comment = (c == '-' && k + 1 < fragment.size() && fragment[k + 1] == '-')
                || (c == '/' && k + 1 < fragment.size() && fragment[k + 1] == '*');
            if (!is_comment)
            {
                statements.push_back(std::move(fragment));
                return;
            }
            k = PostgreSQLProtocol::PostgresPreparedStatements::skipOpaqueSQLToken(fragment, k);
        }
    };

    while (i < size)
    {
        if (const size_t opaque_end = PostgreSQLProtocol::PostgresPreparedStatements::skipOpaqueSQLToken(text, i); opaque_end != i)
        {
            i = opaque_end;
            continue;
        }
        if (text[i] == ';')
            flush(i);
        ++i;
    }
    flush(size);

    return statements;
}

}

void PostgreSQLHandler::processQuery()
{
    /// A malformed or truncated frontend frame leaves the transport stream desynchronized. Read the
    /// complete frame before entering the recoverable query-error path, so deserialization errors close
    /// the connection instead of emitting `ReadyForQuery` on a corrupt stream.
    std::unique_ptr<PostgreSQLProtocol::Messaging::Query> query =
        message_transport->receive<PostgreSQLProtocol::Messaging::Query>();

    /// Output position before the currently executing statement. If a statement
    /// fails when nothing has been sent for it yet, the session can be kept alive.
    size_t out_bytes_before_statement = out->count();
    try
    {
        if (isEmptyQuery(query->query))
        {
            message_transport->send(PostgreSQLProtocol::Messaging::EmptyQueryResponse());
            return;
        }

        /// PostgreSQL clients qualify catalog objects with the `pg_catalog` schema; the emulated
        /// catalog lives in per-session temporary views instead, so the qualifier is stripped here.
        String query_text = removePgCatalogQualifier(query->query);

        /// The message is split into statements without parsing them: a PostgreSQL simple-query
        /// message may mix ClickHouse SQL with statements only this handler understands (`BEGIN`,
        /// `PREPARE`, `EXECUTE`, `COPY`, driver no-ops), which `splitMultipartQuery` - built on the
        /// ClickHouse parser - would reject as a syntax error before the per-statement dispatch
        /// below could see them.
        std::vector<String> queries = splitPostgreSQLStatements(query_text);

        for (auto & sql_query : queries)
        {
            /// PostgreSQL simple-query messages may contain several statements. Apply the same
            /// PostgreSQL-specific dispatch to each fragment as to a one-statement message.
            if (isTransactionControlQuery(sql_query))
            {
                message_transport->send(
                    PostgreSQLProtocol::Messaging::CommandComplete(
                        PostgreSQLProtocol::Messaging::CommandComplete::classifyQuery(sql_query), 0),
                    true);
                continue;
            }

            /// Accept driver-specific session-management commands (e.g. `RESET ALL`, `UNLISTEN *`, the
            /// JDBC handshake's `SET application_name` / `SET extra_float_digits`) as no-ops instead of
            /// failing them with a syntax error.
            if (auto noop_command_tag = classifyNoOpDriverCommand(sql_query))
            {
                message_transport->send(PostgreSQLProtocol::Messaging::CommandComplete(std::move(*noop_command_tag)), true);
                continue;
            }

            if (processPrepareStatement(sql_query))
                continue;

            if (processDeallocate(sql_query))
                continue;

            const auto copy_result = processCopyQuery(sql_query);
            if (copy_result == CopyQueryResult::Success)
                continue;
            if (copy_result == CopyQueryResult::ErrorHandled)
                break;

            auto query_context = session->makeQueryContext();
            query_context->setCurrentQueryId(currentQueryId());

            if (processExecute(sql_query, query_context))
                continue;

            /// Refresh the emulated catalog against each actual statement rather than the outer message text:
            /// a semicolon-separated `CREATE TABLE t ...; SELECT oid FROM pg_class ...` must see `t` in the
            /// catalog when the second statement runs (a single refresh before the split would happen before
            /// `t` exists). This mirrors the extended-protocol path, which refreshes on the bound statement.
            prepareSystemTables(query_context, sql_query);

            QueryScope query_scope = QueryScope::create(query_context);

            PostgreSQLProtocol::Messaging::CommandComplete::Command command =
                PostgreSQLProtocol::Messaging::CommandComplete::classifyQuery(sql_query);

            out_bytes_before_statement = out->count();
            UInt64 affected_rows = executeQueryWithTracking(std::move(sql_query), query_context, command);

            message_transport->send(PostgreSQLProtocol::Messaging::CommandComplete(command, static_cast<Int32>(affected_rows)), true);
        }

    }
    catch (const Exception & e)
    {
        if (copy_protocol_error)
            throw;
        /// A failed write to the client cancels `out` (see `WriteBuffer::next`), and nothing can be
        /// sent on a canceled buffer - not even the error response. Writing to it anyway would be a
        /// logical error, so tear the connection down.
        if (out->isCanceled())
            throw;
        bool nothing_sent_for_failed_statement = out->count() == out_bytes_before_statement;
        message_transport->send(
            PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse(
            PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse::ERROR, "2F000", "Query execution failed.\n" + e.displayText()),
            true);
        /// A failed query does not terminate the session in PostgreSQL: the server
        /// sends `ErrorResponse` and returns to the `ReadyForQuery` state. This is
        /// only safe while nothing has been sent for the failed statement -
        /// otherwise the output stream may be cut in the middle of a message and
        /// continuing would desynchronize the protocol framing, so in that case
        /// tear the connection down.
        if (nothing_sent_for_failed_statement)
            return;
        throw;
    }
}

UInt64 PostgreSQLHandler::executeQueryWithTracking(
    String && sql_query,
    ContextMutablePtr query_context,
    PostgreSQLProtocol::Messaging::CommandComplete::Command command)
{
    /// Track affected rows using a progress callback (similar to the MySQL handler). The callback must
    /// own the counters: `processQuery` reuses one query context for every statement of a multipart
    /// query, so a callback left installed on the context outlives this frame and may be invoked (as the
    /// chained previous callback) by a later statement running on a pipeline thread. Stack-local counters
    /// captured by reference would then be a use-after-return. The previous callback is restored on exit
    /// for the same reason - otherwise every statement would chain another dead lambda onto the context.
    struct Counters
    {
        std::atomic<UInt64> result_rows {0};  /// For SELECT
        std::atomic<UInt64> written_rows {0}; /// For INSERT
    };
    auto counters = std::make_shared<Counters>();
    auto prev_callback = query_context->getProgressCallback();
    query_context->setProgressCallback([prev_callback, counters](const Progress & progress)
    {
        if (prev_callback)
            prev_callback(progress);
        counters->result_rows += progress.result_rows;
        counters->written_rows += progress.written_rows;
    });
    SCOPE_EXIT({ query_context->setProgressCallback(prev_callback); });

    auto set_result_details = [](const QueryResultDetails & details)
    {
        if (details.format && *details.format != "PostgreSQLWire")
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "PostgreSQL protocol does not support custom output formats");
    };

    // Execute query with PostgreSQLWire output format.
    auto read_buf = std::make_unique<ReadBufferFromOwnString>(std::move(sql_query));
    executeQuery(std::move(read_buf), *out, query_context, set_result_details);

    // Determine affected rows based on command type
    return (command == PostgreSQLProtocol::Messaging::CommandComplete::Command::INSERT)
        ? counters->written_rows.load()
        : counters->result_rows.load();
}

bool PostgreSQLHandler::processPrepareStatement(const String & query)
{
    auto parser = ParserPrepare();
    ASTPtr prepare;
    try
    {
        prepare = parseQuery(parser, query, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    }
    catch (const Exception &)
    {
        return false;
    }

    auto * prepared_statement = prepare->as<ASTPreparedStatement>();
    ParserQuery statement_parser(prepared_statement->function_body.data() + prepared_statement->function_body.size());
    parseQuery(statement_parser, prepared_statement->function_body, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

    prepared_statements_manager.addStatement(prepared_statement);

    PostgreSQLProtocol::Messaging::CommandComplete::Command command =
        PostgreSQLProtocol::Messaging::CommandComplete::classifyQuery(query);
    message_transport->send(PostgreSQLProtocol::Messaging::CommandComplete(command, 0), true);
    return true;
}

bool PostgreSQLHandler::processExecute(const String & query, ContextMutablePtr query_context)
{
    auto parser = ParserExecute();
    ASTPtr prepare;
    try
    {
        prepare = parseQuery(parser, query, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    }
    catch (const Exception &)
    {
        return false;
    }

    auto result_query = prepared_statements_manager.getStatement(prepare->as<ASTExecute>());

    /// Refresh the emulated catalog against the resolved statement, not the outer `EXECUTE s` text:
    /// the prepared SQL is what may actually read `pg_*` catalog objects.
    prepareSystemTables(query_context, result_query);

    PostgreSQLProtocol::Messaging::CommandComplete::Command command =
        PostgreSQLProtocol::Messaging::CommandComplete::classifyQuery(result_query);

    QueryScope query_scope = QueryScope::create(query_context);

    UInt64 affected_rows = executeQueryWithTracking(std::move(result_query), query_context, command);

    message_transport->send(PostgreSQLProtocol::Messaging::CommandComplete(command, static_cast<Int32>(affected_rows)), true);

    return true;
}

bool PostgreSQLHandler::processDeallocate(const String & query)
{
    auto parser = ParserDeallocate();
    ASTPtr deallocate;
    try
    {
        deallocate = parseQuery(parser, query, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    }
    catch (const Exception &)
    {
        return false;
    }

    prepared_statements_manager.deleteStatement(deallocate->as<ASTDeallocate>()->function_name);

    PostgreSQLProtocol::Messaging::CommandComplete::Command command =
        PostgreSQLProtocol::Messaging::CommandComplete::classifyQuery(query);
    message_transport->send(PostgreSQLProtocol::Messaging::CommandComplete(command, 0), true);
    return true;
}

void PostgreSQLHandler::processParseQuery()
{
    std::unique_ptr<PostgreSQLProtocol::Messaging::ParseQuery> query =
        message_transport->receive<PostgreSQLProtocol::Messaging::ParseQuery>();

    try
    {
        /// Extended-protocol `COPY` needs the dedicated CopyIn/CopyOut message exchange. It is currently
        /// implemented only for the simple-query path, so reject it during `Parse` rather than executing it
        /// later through the generic wire output path and desynchronizing the frontend/backend stream.
        ParserCopyQuery copy_parser;
        try
        {
            parseQuery(copy_parser, query->sql_query, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "COPY is not supported in extended PostgreSQL protocol queries");
        }
        catch (const Exception & e)
        {
            if (e.code() == ErrorCodes::NOT_IMPLEMENTED)
                throw;
        }

        /// PostgreSQL rejects an invalid statement at `Parse` time. Validate it
        /// before registering its name so an invalid parse cannot poison a named
        /// prepared statement slot.
        ParserQuery parser(query->sql_query.data() + query->sql_query.size());
        parseQuery(parser, query->sql_query, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

        auto statement = make_intrusive<ASTPreparedStatement>();
        statement->function_name = query->function_name;
        statement->function_body = removePgCatalogQualifier(query->sql_query);
        prepared_statements_manager.addStatement(statement.get());
        message_transport->send(PostgreSQLProtocol::Messaging::ParseQueryComplete(), true);
    }
    catch (const Exception & e)
    {
        /// A failed write to the client cancels `out` (see `WriteBuffer::next`); nothing can be sent
        /// on a canceled buffer, not even the error response - tear the connection down.
        if (out->isCanceled())
            throw;
        message_transport->send(
            PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse(
                PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse::ERROR, "2F000", "Query execution failed.\n" + e.displayText()),
            true);
        ignore_extended_query_messages_until_sync = true;
    }
}

void PostgreSQLHandler::processBindQuery()
{
    std::unique_ptr<PostgreSQLProtocol::Messaging::BindQuery> query =
        message_transport->receive<PostgreSQLProtocol::Messaging::BindQuery>();

    try
    {
        prepared_statements_manager.attachBindQuery(std::move(query));
        message_transport->send(PostgreSQLProtocol::Messaging::BindQueryComplete(), true);
    }
    catch (const Exception & e)
    {
        /// A failed write to the client cancels `out` (see `WriteBuffer::next`); nothing can be sent
        /// on a canceled buffer, not even the error response - tear the connection down.
        if (out->isCanceled())
            throw;
        message_transport->send(
            PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse(
                PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse::ERROR, "2F000", "Query execution failed.\n" + e.displayText()),
            true);
        ignore_extended_query_messages_until_sync = true;
    }
}

void PostgreSQLHandler::processDescribeQuery()
{
    /// The message is received outside any recoverable path: a malformed or truncated frame
    /// desynchronizes the stream and must close the session.
    std::unique_ptr<PostgreSQLProtocol::Messaging::DescribeQuery> query =
        message_transport->receive<PostgreSQLProtocol::Messaging::DescribeQuery>();

    /// PostgreSQL answers `Describe` with `ParameterDescription` and `RowDescription`. ClickHouse
    /// cannot derive the result header of a statement without executing it, so `Describe` is
    /// accepted silently and the `RowDescription` is sent when `Execute` produces the result -
    /// clients driving the extended protocol (psycopg, Npgsql, the JDBC driver) accept it there.
    /// Failing `Describe` instead would fail every prepared statement those clients issue.
}

void PostgreSQLHandler::processExecuteQuery()
{
    std::unique_ptr<PostgreSQLProtocol::Messaging::ExecuteQuery> query =
        message_transport->receive<PostgreSQLProtocol::Messaging::ExecuteQuery>();

    /// Output position before the statement, mirroring `processQuery`: keeping
    /// the session alive after a failure is only safe while nothing has been
    /// sent for the failed statement.
    size_t out_bytes_before_statement = out->count();
    try
    {
        /// Only the unnamed portal is supported; the corresponding rejection
        /// for `Bind` lives in `PreparedStatemetsManager::attachBindQuery`.
        if (!query->portal_name.empty())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Execute on a named portal is not supported in the PostgreSQL wire protocol, "
                "got portal name '{}'", query->portal_name);

        if (query->max_rows != 0)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Bounded Execute is not supported in the PostgreSQL wire protocol");

        auto query_context = session->makeQueryContext();
        query_context->setCurrentQueryId(currentQueryId());

        auto sql_query = prepared_statements_manager.getStatmentFromBind();
        prepareSystemTables(query_context, sql_query);

        QueryScope query_scope = QueryScope::create(query_context);

        PostgreSQLProtocol::Messaging::CommandComplete::Command command =
            PostgreSQLProtocol::Messaging::CommandComplete::classifyQuery(sql_query);

        UInt64 affected_rows = executeQueryWithTracking(std::move(sql_query), query_context, command);

        message_transport->send(PostgreSQLProtocol::Messaging::CommandComplete(command, static_cast<Int32>(affected_rows)), true);
    }
    catch (const Exception & e)
    {
        /// A failed write to the client cancels `out` (see `WriteBuffer::next`); nothing can be sent
        /// on a canceled buffer, not even the error response - tear the connection down.
        if (out->isCanceled())
            throw;
        bool nothing_sent_for_failed_statement = out->count() == out_bytes_before_statement;
        message_transport->send(
            PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse(
                PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse::ERROR, "2F000", "Query execution failed.\n" + e.displayText()),
            true);
        /// Recovering to `Sync` is only safe while nothing has been sent for the
        /// failed statement - otherwise the output stream may be cut in the
        /// middle of a message and continuing would desynchronize the protocol
        /// framing, so in that case tear the connection down (as `processQuery`
        /// does for the simple-query protocol).
        if (nothing_sent_for_failed_statement)
        {
            ignore_extended_query_messages_until_sync = true;
            return;
        }
        throw;
    }
}

void PostgreSQLHandler::processCloseQuery()
{
    std::unique_ptr<PostgreSQLProtocol::Messaging::CloseQuery> query =
        message_transport->receive<PostgreSQLProtocol::Messaging::CloseQuery>();

    try
    {
        /// 'S' means close a prepared statement, 'P' means close a portal.
        /// Closing a portal must not deallocate the prepared statement,
        /// otherwise a later Bind/Execute on the same statement would fail.
        if (query->close_target == 'S')
        {
            /// A portal has already captured the statement text at `Bind` time,
            /// so closing its source statement does not invalidate the portal.
            /// Per the PostgreSQL wire protocol, `Close` on a non-existent
            /// prepared statement is not an error — it is a silent no-op that
            /// still responds with `CloseComplete`. Using the throwing
            /// `deleteStatement` here would propagate `BAD_ARGUMENTS` out of
            /// the surrounding `try` block, send an `ErrorResponse`, and drop
            /// the connection on a stray `Close`.
            prepared_statements_manager.tryDeleteStatement(query->function_name);
        }
        else if (query->close_target == 'P')
        {
            /// Only the unnamed portal is supported; rejecting named portals
            /// keeps the behaviour consistent with `Bind` and `Execute`.
            if (!query->function_name.empty())
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Close on a named portal is not supported in the PostgreSQL wire protocol, "
                    "got portal name '{}'", query->function_name);
            prepared_statements_manager.resetBindQuery();
        }
        else
        {
            /// Per the PostgreSQL protocol only 'S' (prepared statement) and 'P'
            /// (portal) are valid `Close` targets; any other byte indicates a
            /// malformed packet and must not be silently acknowledged.
            throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT,
                "Unexpected `Close` target byte {} in the PostgreSQL wire protocol, "
                "expected 'S' (prepared statement) or 'P' (portal)",
                static_cast<int>(static_cast<unsigned char>(query->close_target)));
        }

        /// Acknowledge the `Close` request. Clients that strictly track the
        /// extended-protocol state machine wait for `CloseComplete` before
        /// proceeding.
        message_transport->send(PostgreSQLProtocol::Messaging::CloseQueryComplete(), true);
    }
    catch (const Exception & e)
    {
        /// A failed write to the client cancels `out` (see `WriteBuffer::next`); nothing can be sent
        /// on a canceled buffer, not even the error response - tear the connection down.
        if (out->isCanceled())
            throw;
        message_transport->send(
            PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse(
                PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse::ERROR, "2F000", "Query execution failed.\n" + e.displayText()),
            true);
        ignore_extended_query_messages_until_sync = true;
    }
}

void PostgreSQLHandler::processSyncQuery()
{
    /// The message is received outside any recoverable path: `Sync` is fixed-size, and a frame with a
    /// different length leaves its trailing bytes in the stream, so the connection must close without
    /// an `ErrorResponse` - the stream is already desynchronized.
    std::unique_ptr<PostgreSQLProtocol::Messaging::SyncQuery> query =
        message_transport->receive<PostgreSQLProtocol::Messaging::SyncQuery>();

    /// Per PostgreSQL protocol, `Sync` ends the current extended-query cycle
    /// and destroys the unnamed portal. We only support the unnamed portal
    /// (see `attachBindQuery`), so resetting the single bind slot is
    /// equivalent — the next Parse/Bind/Execute pair starts from a clean state.
    prepared_statements_manager.resetBindQuery();
    ignore_extended_query_messages_until_sync = false;
}

bool PostgreSQLHandler::isEmptyQuery(const String & query)
{
    if (query.empty())
        return true;
    /// golang driver pgx sends ";"
    if (query == ";")
        return true;

    Poco::RegularExpression regex(R"(\A\s*\z)");
    return regex.match(query);
}

bool PostgreSQLHandler::isTransactionControlQuery(const String & query)
{
    String normalized = query;
    /// Trim surrounding whitespace and a single trailing semicolon.
    boost::trim(normalized);
    if (!normalized.empty() && normalized.back() == ';')
    {
        normalized.pop_back();
        boost::trim(normalized);
    }
    Poco::toUpperInPlace(normalized);

    /// A transaction-control statement is a single statement. If an internal semicolon remains after the
    /// trailing one has been trimmed, this is a multi-statement simple query such as `BEGIN READ ONLY; SELECT 1`.
    /// Treating it as ignorable transaction-control would silently drop the trailing statement, so let it fall
    /// through to normal processing instead.
    if (normalized.contains(';'))
        return false;

    /// Validate the statement against the exact wrapper spellings PostgreSQL defines for plain
    /// transaction control, which are the only ones that are safe to acknowledge as no-op success.
    /// Anything else - `ROLLBACK TO [SAVEPOINT] s`, `COMMIT PREPARED 'gid'`, `ROLLBACK PREPARED 'gid'`,
    /// `SAVEPOINT`/`RELEASE` and other unsupported commands - must fall through to normal processing,
    /// where it fails with a proper error instead of a false `CommandComplete`.
    /// The mode list of BEGIN/START TRANSACTION may be separated by commas as well as whitespace,
    /// so treat commas as token separators too.
    std::vector<std::string_view> tokens;
    for (size_t pos = 0; pos < normalized.size();)
    {
        const size_t start = normalized.find_first_not_of(" \t\r\n,", pos);
        if (start == String::npos)
            break;
        size_t end = normalized.find_first_of(" \t\r\n,", start);
        if (end == String::npos)
            end = normalized.size();
        tokens.emplace_back(std::string_view(normalized).substr(start, end - start));
        pos = end;
    }

    if (tokens.empty())
        return false;

    size_t i = 0;
    const auto next_is = [&](std::string_view expected) { return i < tokens.size() && tokens[i] == expected; };

    if (next_is("BEGIN") || next_is("START"))
    {
        const bool is_start = tokens[i] == "START";
        ++i;
        if (is_start)
        {
            /// `START` is only transaction control as `START TRANSACTION`.
            if (!next_is("TRANSACTION"))
                return false;
            ++i;
        }
        else if (next_is("WORK") || next_is("TRANSACTION"))
        {
            ++i;
        }

        /// Optional transaction modes: ISOLATION LEVEL ..., READ ONLY / READ WRITE, [NOT] DEFERRABLE.
        while (i < tokens.size())
        {
            if (next_is("ISOLATION"))
            {
                ++i;
                if (!next_is("LEVEL"))
                    return false;
                ++i;
                if (next_is("SERIALIZABLE"))
                {
                    ++i;
                }
                else if (next_is("REPEATABLE"))
                {
                    ++i;
                    if (!next_is("READ"))
                        return false;
                    ++i;
                }
                else if (next_is("READ"))
                {
                    ++i;
                    if (!next_is("COMMITTED") && !next_is("UNCOMMITTED"))
                        return false;
                    ++i;
                }
                else
                    return false;
            }
            else if (next_is("READ"))
            {
                ++i;
                if (!next_is("ONLY") && !next_is("WRITE"))
                    return false;
                ++i;
            }
            else if (next_is("NOT"))
            {
                ++i;
                if (!next_is("DEFERRABLE"))
                    return false;
                ++i;
            }
            else if (next_is("DEFERRABLE"))
            {
                ++i;
            }
            else
                return false;
        }
        return true;
    }

    if (next_is("COMMIT") || next_is("END") || next_is("ROLLBACK") || next_is("ABORT"))
    {
        ++i;
        if (next_is("WORK") || next_is("TRANSACTION"))
            ++i;
        /// Optional `AND [NO] CHAIN`. Chaining just starts a new no-op transaction, so it is safe to accept.
        if (next_is("AND"))
        {
            ++i;
            if (next_is("NO"))
                ++i;
            if (!next_is("CHAIN"))
                return false;
            ++i;
        }
        return i == tokens.size();
    }

    return false;
}

Int32 PostgreSQLHandler::parseNumberColumns(const std::vector<char> & output)
{
    Int32 result = 0;
    for (const auto elem : output)
    {
        if (elem == '\n')
            return result;
        if (elem == '\t')
            result++;
    }
    return result;
}

void PostgreSQLHandler::initializeSystemTables(ContextMutablePtr query_context)
{
    /// Create an internal context from the global context (which has full access, bypassing grant checks)
    /// but sharing the same session context, so that temporary views created here
    /// are visible in subsequent user queries.
    auto internal_context = Context::createCopy(server.context());
    internal_context->makeQueryContext();
    internal_context->setCurrentQueryId(fmt::format("postgres-init:{:d}", connection_id));
    internal_context->setSessionContext(query_context->getSessionContext());

    String out_str;
    auto out_buffer = WriteBufferFromString(out_str);

    auto execute_query = [&](const String & query)
    {
        QueryScope query_scope = QueryScope::create(internal_context);
        ReadBufferFromString read_buf(query);
        executeQuery(read_buf, out_buffer, internal_context, {}, QueryFlags{ .internal = true });
    };

    /// Every type OID this handler can hand out must resolve here: the OIDs emitted from `pg_attribute`
    /// below (including the built-in catalog rows, which use `oid` columns of type 26), the range and
    /// multirange type OIDs of `pg_range` (`rngtypid` / `rngmultitypid`), and the enum type OID of
    /// `pg_enum` (`enumtypid`), because the standard introspection path of a PostgreSQL client is a join
    /// against `pg_type.oid`, and a missing row silently drops the column or type from the result.
    /// An array type carries the OID of its element type in `typelem` (`typcategory` = 'A'), as in
    /// PostgreSQL. `typreceive` is zero for every type because the server does not implement binary
    /// input decoding for bound parameters or `COPY`; advertising a receiver would cause clients to
    /// select an unsupported input path.
    execute_query(R"(CREATE TEMPORARY VIEW IF NOT EXISTS pg_type SQL SECURITY INVOKER AS
SELECT * FROM VALUES(
    'oid UInt32, typnamespace UInt32, typname String, typrelid UInt32, typnotnull UInt8, typtype String, typreceive UInt32, typelem UInt32, typbasetype UInt32, typcategory String',
    (16,   11, 'bool',        0, 0, 'b', 0, 0, 0, 'B'),
    (17,   11, 'bytea',       0, 0, 'b', 0, 0, 0, 'U'),
    (18,   11, 'char',        0, 0, 'b', 0, 0, 0, 'S'),
    (19,   11, 'name',        0, 0, 'b', 0, 0, 0, 'S'),
    (20,   11, 'int8',        0, 0, 'b', 0, 0, 0, 'N'),
    (21,   11, 'int2',        0, 0, 'b', 0, 0, 0, 'N'),
    (23,   11, 'int4',        0, 0, 'b', 0, 0, 0, 'N'),
    (25,   11, 'text',        0, 0, 'b', 0, 0, 0, 'S'),
    (26,   11, 'oid',         0, 0, 'b', 0, 0, 0, 'N'),
    (700,  11, 'float4',      0, 0, 'b', 0, 0, 0, 'N'),
    (701,  11, 'float8',      0, 0, 'b', 0, 0, 0, 'N'),
    (1043, 11, 'varchar',     0, 0, 'b', 0, 0, 0, 'S'),
    (1082, 11, 'date',        0, 0, 'b', 0, 0, 0, 'D'),
    (1114, 11, 'timestamp',   0, 0, 'b', 0, 0, 0, 'D'),
    (1184, 11, 'timestamptz', 0, 0, 'b', 0, 0, 0, 'D'),
    (1700, 11, 'numeric',     0, 0, 'b', 0, 0, 0, 'N'),
    (2950, 11, 'uuid',        0, 0, 'b', 0, 0, 0, 'U'),
    (1000, 11, '_bool',       0, 0, 'b', 0, 16,   0, 'A'),
    (1005, 11, '_int2',       0, 0, 'b', 0, 21,   0, 'A'),
    (1007, 11, '_int4',       0, 0, 'b', 0, 23,   0, 'A'),
    (1009, 11, '_text',       0, 0, 'b', 0, 25,   0, 'A'),
    (1016, 11, '_int8',       0, 0, 'b', 0, 20,   0, 'A'),
    (1021, 11, '_float4',     0, 0, 'b', 0, 700,  0, 'A'),
    (1022, 11, '_float8',     0, 0, 'b', 0, 701,  0, 'A'),
    (1182, 11, '_date',       0, 0, 'b', 0, 1082, 0, 'A'),
    (1231, 11, '_numeric',    0, 0, 'b', 0, 1700, 0, 'A'),
    (2951, 11, '_uuid',       0, 0, 'b', 0, 2950, 0, 'A'),
    (3904, 11, 'int4range',      0, 0, 'r', 0, 0, 0, 'R'),
    (3906, 11, 'numrange',       0, 0, 'r', 0, 0, 0, 'R'),
    (3910, 11, 'tsrange',        0, 0, 'r', 0, 0, 0, 'R'),
    (3912, 11, 'tstzrange',      0, 0, 'r', 0, 0, 0, 'R'),
    (3914, 11, 'daterange',      0, 0, 'r', 0, 0, 0, 'R'),
    (3926, 11, 'int8range',      0, 0, 'r', 0, 0, 0, 'R'),
    (3905, 11, 'int4multirange', 0, 0, 'm', 0, 0, 0, 'R'),
    (3907, 11, 'nummultirange',  0, 0, 'm', 0, 0, 0, 'R'),
    (3911, 11, 'tsmultirange',   0, 0, 'm', 0, 0, 0, 'R'),
    (3913, 11, 'tstzmultirange', 0, 0, 'm', 0, 0, 0, 'R'),
    (3915, 11, 'datemultirange', 0, 0, 'm', 0, 0, 0, 'R'),
    (3927, 11, 'int8multirange', 0, 0, 'm', 0, 0, 0, 'R'),
    (40000, 11, 'mood',          0, 0, 'e', 0, 0, 0, 'E')
))");

    /// `pg_namespace`, `pg_class` and `pg_attribute` combine a fixed set of built-in catalog rows (used by
    /// client type introspection) with rows derived from ClickHouse's own `system.databases`, `system.tables`
    /// and `system.columns`, so that databases, tables and columns of this server are visible through the
    /// PostgreSQL catalog. Both namespace (database) and relation (table) OIDs are joined between catalog
    /// views - `fetchPostgreSQLTableStructure` resolves a schema-qualified table through
    /// `pg_namespace.oid` -> `pg_class.relnamespace` and then pulls its columns by `pg_attribute.attrelid` -
    /// so a hash collision would merge two databases or two tables and corrupt schema inference for both. A
    /// truncated hash is not collision-free enough at realistic catalog sizes, and a per-query `row_number`
    /// rank would renumber existing objects whenever an earlier-sorting one is created or dropped, breaking
    /// clients that cache an OID or resolve it in one catalog query and follow it in another. PostgreSQL
    /// OIDs are identifiers, not ranks, so they must stay stable at least for the lifetime of the session.
    ///
    /// Therefore the OIDs live in per-session state tables (`pg_namespace_oids_data` per database,
    /// `pg_class_oids_data` per table) that are append-only: `refreshCatalogOids` assigns a fresh OID above
    /// the current maximum to every object not seen before and never renumbers or removes existing entries,
    /// so an OID observed once keeps referring to the same object for the whole connection. The state is
    /// keyed by a rename-stable identity: the object's `uuid` when it has one (`RENAME` preserves the UUID
    /// in `Atomic` databases, which is the default), with the name as a fallback for objects without a UUID
    /// (there a rename is indistinguishable from drop+create, so the renamed object gets a fresh OID). The
    /// `pg_namespace_oids` / `pg_class_oids` views join that state with the live `system.databases` /
    /// `system.tables` on that identity, so a renamed object keeps its OID and appears under its new name,
    /// while a dropped object disappears from the catalog with its OID staying reserved (and a recreated
    /// one keeps its old OID, which is harmless - stability is what matters). OID ranges are offset
    /// (namespaces above 1e9, relations above 2e9) to avoid colliding with the small built-in OIDs.
    /// The name-fallback identities hex-encode the names: hex output cannot contain the `:` separator, so
    /// `(database, name)` pairs like `('a', 'b.c')` and `('a.b', 'c')` cannot collide, and neither fallback
    /// can collide with a `uuid:` identity.
    execute_query(R"(CREATE TEMPORARY TABLE IF NOT EXISTS pg_class_oids_data
(
    identity String,
    oid UInt32
) ENGINE = Memory)");
    execute_query(R"(CREATE TEMPORARY TABLE IF NOT EXISTS pg_namespace_oids_data
(
    identity String,
    oid UInt32
) ENGINE = Memory)");
    execute_query(R"(CREATE TEMPORARY VIEW IF NOT EXISTS pg_class_oids SQL SECURITY INVOKER AS
SELECT
    tables.database AS database,
    tables.name AS name,
    tables.engine AS engine,
    oids.oid AS oid
FROM
(
    SELECT
        database,
        name,
        engine,
        if (uuid != toUUID('00000000-0000-0000-0000-000000000000'),
            concat('uuid:', toString(uuid)),
            concat('name:', hex(database), ':', hex(name))) AS identity
    FROM system.tables
) AS tables
INNER JOIN pg_class_oids_data AS oids ON tables.identity = oids.identity)");
    execute_query(R"(CREATE TEMPORARY VIEW IF NOT EXISTS pg_namespace_oids SQL SECURITY INVOKER AS
SELECT
    databases.name AS name,
    oids.oid AS oid
FROM
(
    SELECT
        name,
        if (uuid != toUUID('00000000-0000-0000-0000-000000000000'),
            concat('uuid:', toString(uuid)),
            concat('name:', hex(name))) AS identity
    FROM system.databases
) AS databases
INNER JOIN pg_namespace_oids_data AS oids ON databases.identity = oids.identity)");
    /// Every ClickHouse database is exposed as a schema under its own name, including one named `public`.
    /// The fixed names below are PostgreSQL-reserved (`pg_*`) or system-owned (`information_schema`), so
    /// they cannot shadow user data; when a ClickHouse database carries one of these names (the built-in
    /// `information_schema` always does), the fixed row takes precedence over a generated one, so a
    /// schema name always denotes a single OID.
    execute_query(R"(CREATE TEMPORARY VIEW IF NOT EXISTS pg_namespace SQL SECURITY INVOKER AS
SELECT oid, nspname FROM VALUES(
    'oid UInt32, nspname String',
    (11,    'pg_catalog'),
    (132,   'information_schema'),
    (11519, 'pg_toast'),
    (99,    'pg_temp_1'),
    (100,   'pg_toast_temp_1')
)
UNION ALL
SELECT oid, name AS nspname
FROM pg_namespace_oids
WHERE name NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1'))");
    /// One row per exposed relation, shared by `pg_class` and `pg_attribute` so their `oid` /
    /// `attrelid` values always agree. Every relation appears exactly once, under the namespace of the
    /// database that owns it: PostgreSQL clients treat `pg_class.oid` as the unique relation identifier
    /// (e.g. `JOIN pg_attribute ON attrelid = pg_class.oid`), and, more importantly, a name must denote
    /// the same relation in the catalog and in the data path. There is deliberately no synthetic `public`
    /// schema aliasing the connected database: the data statements this handler executes (`SELECT ... FROM
    /// t`, `INSERT INTO t`) resolve an unqualified name in the connected database and a qualified
    /// `public.t` in a ClickHouse database named `public`, so an alias would let schema discovery and the
    /// `COPY` that follows it target two different tables. A client that does not qualify a table with a
    /// schema resolves it through `current_schema()` instead, which this server reports as the connected
    /// database - exactly what its unqualified data statements use.
    ///
    /// `relnamespace` is taken from `pg_namespace` itself, not from the raw `pg_namespace_oids`: for a
    /// database whose name is covered by a fixed `pg_namespace` row - notably the built-in
    /// `information_schema` - the generated OID is hidden from clients, so relations must reference the
    /// fixed OID or a `pg_namespace.oid -> pg_class.relnamespace` resolution would come up empty.
    execute_query(R"(CREATE TEMPORARY VIEW IF NOT EXISTS pg_class_entries SQL SECURITY INVOKER AS
SELECT
    oids.database AS database,
    oids.name AS name,
    oids.engine AS engine,
    oids.oid AS oid,
    ns.oid AS relnamespace
FROM pg_class_oids AS oids
INNER JOIN pg_namespace AS ns ON oids.database = ns.nspname)");

    /// Every catalog view this handler emulates has a named `pg_class` row with its real PostgreSQL OID
    /// under the `pg_catalog` namespace, so that the catalog surface is closed under its own discovery
    /// rules: the standard `pg_class.relname -> pg_class.oid -> pg_attribute.attrelid` resolution (used by
    /// `fetchPostgreSQLTableStructure`, that is, by a self-connected `postgresql(...)`) finds every one of
    /// them by name, and `pg_attribute` below carries the built-in column rows for each. The remaining
    /// anonymous rows keep the historic (oid, relkind) samples that clients probe for one relation of every
    /// `relkind`; the OIDs 2615 (`pg_namespace`) and 1255 (`pg_proc`), which used to carry the 'i' and 'f'
    /// samples, now denote the real catalogs, so those samples moved to OIDs that do not collide with any
    /// emulated catalog (2662 is `pg_class_oid_index`, a real index, and 9999 is unassigned). The generated
    /// branch excludes a user table that would collide with a named row (a ClickHouse database literally
    /// named `pg_catalog` holding a table `pg_type`), so a (namespace, name) pair still denotes one
    /// relation. The anonymous rows stay in the `pg_catalog` namespace so that the query behind psql's
    /// `\d`, which excludes that namespace, does not list them as nameless relations.
    execute_query(R"(CREATE TEMPORARY VIEW IF NOT EXISTS pg_class SQL SECURITY INVOKER AS
SELECT oid, relname, relnamespace, relowner, relam, relkind FROM VALUES(
    'oid UInt32, relname String, relnamespace UInt32, relowner UInt32, relam UInt32, relkind String',
    (1247, 'pg_type', 11, 10, 2, 'r'),
    (1249, 'pg_attribute', 11, 10, 2, 'r'),
    (1255, 'pg_proc', 11, 10, 2, 'r'),
    (1259, 'pg_class', 11, 10, 2, 'r'),
    (2615, 'pg_namespace', 11, 10, 2, 'r'),
    (3501, 'pg_enum', 11, 10, 2, 'r'),
    (3541, 'pg_range', 11, 10, 2, 'r'),
    (2662, '', 11, 10, 2, 'i'),
    (3079, '', 11, 10, 0, 'v'),
    (1260, '', 11, 10, 2, 'c'),
    (9999, '', 11, 10, 2, 'f'),
    (3476, '', 11, 10, 0, 'm'),
    (3074, '', 11, 10, 2, 'S')
)
UNION ALL
SELECT
    oid,
    name AS relname,
    relnamespace,
    toUInt32(10) AS relowner,
    toUInt32(if(endsWith(engine, 'View'), 0, 2)) AS relam,
    multiIf(engine = 'MaterializedView', 'm', endsWith(engine, 'View'), 'v', 'r') AS relkind
FROM pg_class_entries
WHERE NOT (relnamespace = 11 AND name IN (
    'pg_type', 'pg_attribute', 'pg_proc', 'pg_class', 'pg_namespace', 'pg_enum', 'pg_range')))");

    /// Table access methods. Newer psql versions join `pg_am` in the query behind the `\d` command.
    /// ClickHouse table engines have no PostgreSQL equivalent, so everything that is not a view is
    /// presented as the default `heap` access method, matching `relam` in `pg_class` above.
    execute_query(R"(CREATE TEMPORARY VIEW IF NOT EXISTS pg_am AS
SELECT * FROM VALUES(
    'oid UInt32, amname String, amtype String',
    (2, 'heap', 't')
))");

    /// Besides the historic `*in` / `*out` rows, `pg_proc` must resolve every receive-function OID that
    /// `pg_type.typreceive` advertises above: a client that probes for binary I/O support by joining
    /// `pg_type.typreceive` to `pg_proc.oid` (rather than only comparing `typreceive` against zero) would
    /// otherwise drop every emulated type. The names are the ones `pg_catalog` proper uses, and the OIDs are
    /// the synthetic ones chosen for `typreceive`, so the join is closed in both directions - every
    /// advertised receive function has a row here, and none of these rows is an orphan.
    execute_query(R"(CREATE TEMPORARY VIEW IF NOT EXISTS pg_proc SQL SECURITY INVOKER AS
SELECT * FROM VALUES(
    'oid UInt32, proname String',
    (241, 'int8recv'),
    (242, 'int4recv'),
    (243, 'int2recv'),
    (244, 'namerecv'),
    (245, 'charrecv'),
    (246, 'boolrecv'),
    (247, 'textrecv'),
    (248, 'bytearecv'),
    (249, 'varcharrecv'),
    (250, 'float4recv'),
    (251, 'float8recv'),
    (252, 'date_recv'),
    (253, 'timestamp_recv'),
    (254, 'oidrecv'),
    (255, 'timestamptz_recv'),
    (256, 'numeric_recv'),
    (257, 'uuid_recv'),
    (260, 'array_recv'),
    (261, 'range_recv'),
    (262, 'multirange_recv'),
    (263, 'enum_recv'),
    (1247, 'boolin'),
    (1248, 'boolout'),
    (1249, 'byteain'),
    (1250, 'byteaout'),
    (1251, 'charin'),
    (1252, 'charout'),
    (1255, 'namein'),
    (1256, 'nameout'),
    (1259, 'int2in'),
    (1260, 'int2out'),
    (1261, 'int4in'),
    (1262, 'int4out'),
    (1265, 'textin'),
    (1266, 'textout'),
    (1286, 'float4in'),
    (1287, 'float4out'),
    (1288, 'float8in'),
    (1289, 'float8out'),
    (1344, 'date_in'),
    (1345, 'date_out'),
    (2022, 'varcharin'),
    (2023, 'varcharout'),
    (1115, 'timestamp_in'),
    (1116, 'timestamp_out')
))");

    execute_query(R"(CREATE TEMPORARY VIEW IF NOT EXISTS pg_range SQL SECURITY INVOKER AS
SELECT * FROM VALUES(
    'rngtypid UInt32, rngsubtype UInt32, rngmultitypid UInt32',
    (3904, 23,   3905),
    (3906, 1700, 3907),
    (3910, 1114, 3911),
    (3912, 1184, 3913),
    (3914, 1082, 3915),
    (3926, 20,   3927)
))");

    /// The built-in rows describe each emulated catalog view with exactly the columns, order and types the
    /// view itself emits (its `SELECT` list), so that a self-connected `postgresql(...)` read of a catalog
    /// round-trips: schema discovery infers the very header the data stream then carries. In particular
    /// `pg_type` starts with its `oid` column, and every `oid`-typed column is declared with type OID 26
    /// (`convertPostgreSQLDataType` maps `oid` to `UInt32`). Two columns deliberately deviate from
    /// PostgreSQL proper, because the emulated view renders them differently: `pg_attribute.attnotnull` is
    /// declared `text` (the view emits the strings 't'/'f', which do not parse as a boolean), and
    /// `pg_attribute.attnum` / `attndims` / `atttypmod` are `int4` (the view emits plain integers).
    execute_query(R"(CREATE TEMPORARY VIEW IF NOT EXISTS pg_attribute SQL SECURITY INVOKER AS
SELECT atttypid, attrelid, attname, attnum, attisdropped, atttypmod, attnotnull, attndims, attgenerated, 't' AS attelemnotnull FROM VALUES(
    'atttypid UInt32, attrelid UInt32, attname String, attnum Int32, attisdropped UInt8, atttypmod Int32, attnotnull String, attndims Int32, attgenerated String',
    (26, 1247, 'oid',          1,  0, -1, 't', 0, ''),
    (26, 1247, 'typnamespace', 2,  0, -1, 't', 0, ''),
    (19, 1247, 'typname',      3,  0, -1, 't', 0, ''),
    (26, 1247, 'typrelid',     4,  0, -1, 't', 0, ''),
    (16, 1247, 'typnotnull',   5,  0, -1, 't', 0, ''),
    (25, 1247, 'typtype',      6,  0, -1, 't', 0, ''),
    (26, 1247, 'typreceive',   7,  0, -1, 't', 0, ''),
    (26, 1247, 'typelem',      8,  0, -1, 't', 0, ''),
    (26, 1247, 'typbasetype',  9,  0, -1, 't', 0, ''),
    (18, 1247, 'typcategory',  10, 0, -1, 't', 0, ''),
    (26, 1249, 'atttypid',     1, 0, -1, 't', 0, ''),
    (26, 1249, 'attrelid',     2, 0, -1, 't', 0, ''),
    (19, 1249, 'attname',      3, 0, -1, 't', 0, ''),
    (23, 1249, 'attnum',       4, 0, -1, 't', 0, ''),
    (16, 1249, 'attisdropped', 5, 0, -1, 't', 0, ''),
    (23, 1249, 'atttypmod',    6, 0, -1, 't', 0, ''),
    (25, 1249, 'attnotnull',   7, 0, -1, 't', 0, ''),
    (23, 1249, 'attndims',     8, 0, -1, 't', 0, ''),
    (18, 1249, 'attgenerated', 9, 0, -1, 't', 0, ''),
    (25, 1249, 'attelemnotnull', 10, 0, -1, 't', 0, ''),
    (26, 1255, 'oid',           1, 0, -1, 't', 0, ''),
    (19, 1255, 'proname',       2, 0, -1, 't', 0, ''),
    (26, 1259, 'oid',           1, 0, -1, 't', 0, ''),
    (19, 1259, 'relname',       2, 0, -1, 't', 0, ''),
    (26, 1259, 'relnamespace',  3, 0, -1, 't', 0, ''),
    (26, 1259, 'relowner',      4, 0, -1, 't', 0, ''),
    (26, 1259, 'relam',         5, 0, -1, 't', 0, ''),
    (18, 1259, 'relkind',       6, 0, -1, 't', 0, ''),
    (26, 2615, 'oid',           1, 0, -1, 't', 0, ''),
    (19, 2615, 'nspname',       2, 0, -1, 't', 0, ''),
    (26, 3501, 'oid',           1, 0, -1, 't', 0, ''),
    (26, 3501, 'enumtypid',     2, 0, -1, 't', 0, ''),
    (701, 3501, 'enumsortorder', 3, 0, -1, 't', 0, ''),
    (19, 3501, 'enumlabel',     4, 0, -1, 't', 0, ''),
    (26, 3541, 'rngtypid',      1, 0, -1, 't', 0, ''),
    (26, 3541, 'rngsubtype',    2, 0, -1, 't', 0, ''),
    (26, 3541, 'rngmultitypid', 3, 0, -1, 't', 0, '')
)
UNION ALL
SELECT
    /// A non-array column is advertised with the OID of its scalar type; an array column is advertised with
    /// the OID of the corresponding PostgreSQL array type of its element (the innermost non-array type), and
    /// the array dimensions are reported in `attndims` below, exactly as PostgreSQL does. PostgreSQL has
    /// neither unsigned nor >64-bit integers, so the integer types that do not fit into a signed 64-bit
    /// `bigint` (`UInt64` and the 128/256-bit types) are advertised as `numeric` and carry a precision in
    /// `atttypmod` (see below) large enough to hold every value; the counterpart mapping in
    /// `convertPostgreSQLDataType` turns such a `numeric(p, 0)` back into a Decimal (or `Int256` for a
    /// precision above the Decimal256 range) that preserves the range. Only `UInt32`/`Int64`, which fit into
    /// `bigint`, keep OID 20.
    ///
    /// `DateTime`/`DateTime64` deliberately take the text fallback (the trailing 25/1009 default) instead of
    /// `timestamp`: PostgreSQL's `timestamp without time zone` cannot carry the time zone the wall-clock text
    /// is rendered in. That is obvious for a type with an explicit zone (`DateTime('UTC')`), but a `DateTime`
    /// without one is no safer - its text is rendered in the *source* server's default time zone, while a
    /// reader reconstructing a plain `DateTime`/`DateTime64(p)` reinterprets it in its *own* default zone,
    /// silently shifting every stored epoch whenever the two zones differ. As text the value round-trips
    /// losslessly as `String`. The direct `RowDescription` path in `PostgreSQLProtocol.cpp` matches this.
    multiIf(cols.ndims > 0,
                multiIf(cols.base IN ('Bool', 'Boolean'), 1000,
                        cols.base IN ('Int8', 'UInt8', 'Int16'), 1005,
                        cols.base IN ('UInt16', 'Int32'), 1007,
                        cols.base IN ('UInt32', 'Int64'), 1016,
                        cols.base IN ('UInt64', 'Int128', 'UInt128', 'Int256', 'UInt256',
                                      'Decimal', 'Decimal32', 'Decimal64', 'Decimal128', 'Decimal256'), 1231,
                        cols.base = 'Float32', 1021,
                        cols.base = 'Float64', 1022,
                        cols.base = 'UUID', 2951,
                        cols.base IN ('Date', 'Date32'), 1182,
                        cols.base IN ('String', 'FixedString'), 1009,
                        1009),
            cols.base IN ('Bool', 'Boolean'), 16,
            cols.base IN ('Int8', 'UInt8', 'Int16'), 21,
            cols.base IN ('UInt16', 'Int32'), 23,
            cols.base IN ('UInt32', 'Int64'), 20,
            cols.base IN ('UInt64', 'Int128', 'UInt128', 'Int256', 'UInt256',
                          'Decimal', 'Decimal32', 'Decimal64', 'Decimal128', 'Decimal256'), 1700,
            cols.base = 'Float32', 700,
            cols.base = 'Float64', 701,
            cols.base = 'UUID', 2950,
            cols.base IN ('Date', 'Date32'), 1082,
            cols.base IN ('String', 'FixedString'), 25,
            25) AS atttypid,
    oids.oid AS attrelid,
    cols.name AS attname,
    toInt32(cols.position) AS attnum,
    0 AS attisdropped,
    /// For the types advertised as `numeric`, encode precision and scale the way PostgreSQL does -
    /// `((precision << 16) | scale) + 4` - so that `format_type` renders `numeric(p, s)` and schema
    /// inference recovers the exact type. For an array column the modifier applies to the element type, as in
    /// PostgreSQL. `Decimal` uses its own precision/scale; the wide integer types use a scale of 0 and a
    /// precision that spans their whole value range (the 256-bit types share `numeric(78, 0)`, which holds
    /// every 256-bit integer). `convertPostgreSQLDataType` maps such a `numeric(p > 76, 0)` back to `Int256`;
    /// PostgreSQL `numeric` is signed, so a self-connected `UInt256` above the `Int256` maximum is rejected
    /// on the reading side (fail-closed) rather than recovered.
    /// Everything else uses -1 ("no modifier").
    multiIf(cols.base IN ('Decimal', 'Decimal32', 'Decimal64', 'Decimal128', 'Decimal256')
                AND cols.decimal_precision IS NOT NULL AND cols.decimal_scale IS NOT NULL,
                toInt32(assumeNotNull(cols.decimal_precision) * 65536 + assumeNotNull(cols.decimal_scale) + 4),
            cols.base = 'UInt64', toInt32(20 * 65536 + 4),
            cols.base IN ('Int128', 'UInt128'), toInt32(39 * 65536 + 4),
            cols.base IN ('Int256', 'UInt256'), toInt32(78 * 65536 + 4),
            -1) AS atttypmod,
    /// `attnotnull` describes nullability of the column value, never that of an array element.
    /// Nullable array elements are represented by PostgreSQL array syntax and do not make the
    /// `Array` column itself nullable.
    if (startsWith(cols.type, 'Nullable(') OR startsWith(cols.type, 'LowCardinality(Nullable('), 'f', 't') AS attnotnull,
    cols.ndims AS attndims,
    /// ClickHouse has no generated columns, so `attgenerated` keeps its PostgreSQL meaning and is always
    /// empty. Array-element nullability, which PostgreSQL has no catalog field for at all, is carried by
    /// the extra `attelemnotnull` column below: overloading `attgenerated` would make every
    /// `Array(Nullable(...))` column look generated to an ordinary catalog client.
    '' AS attgenerated,
    /// Emulation-only column, mirroring `attnotnull` but for the elements of an array column. The
    /// self-connect schema reader detects its presence through `pg_attribute`'s own description of itself
    /// and falls back to the PostgreSQL-native behaviour against a real server, which cannot report this.
    if (cols.ndims > 0 AND position(cols.wrappers, 'Nullable(') > 0, 'f', 't') AS attelemnotnull
FROM (
    SELECT
        database, table, name, position, type,
        /// `system.columns.numeric_precision` / `numeric_scale` are only populated for top-level numeric
        /// columns; for a `Decimal` element wrapped in `Array(...)` (or `Nullable(...)`) they are NULL, so
        /// fall back to parsing the precision and scale out of the type name. Only the leading wrappers
        /// are skipped - the same prefix as in `base` below - so a `Decimal` buried in a `Map`/`Tuple`
        /// argument list is not picked up (such columns never reach the `Decimal` branch anyway).
        coalesce(numeric_precision,
                 toUInt64OrNull(extract(type, '^(?:Nullable\(|LowCardinality\(|Array\()*Decimal\(([0-9]+), [0-9]+\)'))) AS decimal_precision,
        coalesce(numeric_scale,
                 toUInt64OrNull(extract(type, '^(?:Nullable\(|LowCardinality\(|Array\()*Decimal\([0-9]+, ([0-9]+)\)'))) AS decimal_scale,
        extract(type, '^(?:Nullable\(|LowCardinality\(|Array\()*([A-Za-z0-9]+)') AS base,
        /// The leading chain of `Nullable(` / `LowCardinality(` / `Array(` wrappers, reused below to count
        /// array dimensions and to detect whether the value (an array element, or the scalar itself) is
        /// nullable. A `Nullable(` can only appear in this chain right before the innermost scalar, so its
        /// presence marks the element as nullable even for `Array(Nullable(T))`.
        extract(type, '^((?:Nullable\(|LowCardinality\(|Array\()*)') AS wrappers,
        /// Count only the leading `Array(` wrappers. An `Array(` nested inside a `Map`/`Tuple` argument
        /// list must not make the column look like a top-level array: such columns are exposed as text.
        toInt32(countSubstrings(wrappers, 'Array(')) AS ndims
    FROM system.columns
    /// The data path streams a table with `SELECT * FROM <table>` (see `processCopyQuery`), which omits
    /// `MATERIALIZED` / `ALIAS` / `EPHEMERAL` columns by default. Advertise exactly that column set here, so
    /// the emulated catalog and the `COPY` payload agree - otherwise schema inference sees more columns than
    /// the stream carries and row decoding goes out of sync.
    WHERE default_kind NOT IN ('MATERIALIZED', 'ALIAS', 'EPHEMERAL')
) AS cols
INNER JOIN pg_class_entries AS oids ON cols.database = oids.database AND cols.table = oids.name)");

    execute_query(R"(CREATE TEMPORARY VIEW IF NOT EXISTS pg_enum SQL SECURITY INVOKER AS
SELECT * FROM VALUES(
    'oid UInt32, enumtypid UInt32, enumsortorder Float64, enumlabel String',
    (50000, 40000, 1.0, 'sad'),
    (50001, 40000, 2.0, 'ok'),
    (50002, 40000, 3.0, 'happy')
))");
}

void PostgreSQLHandler::prepareSystemTables(ContextMutablePtr query_context, const String & query)
{
    if (should_init_system_tables)
    {
        initializeSystemTables(query_context);
        should_init_system_tables = false;
    }

    /// Assign stable OIDs to databases and tables that appeared since the last refresh, but only before a
    /// statement that may actually read the emulated catalog (every catalog object's name starts with
    /// `pg_`). Unquoted PostgreSQL identifiers are case-insensitive, so the check is too - `PG_CLASS` reads
    /// the same catalog. Plain data statements skip the `system.tables` scan. The check may fire spuriously
    /// (e.g. a user table named `pg_something`), which merely costs a refresh.
    if (Poco::toLower(query).contains("pg_"))
        refreshCatalogOids(query_context);
}

void PostgreSQLHandler::refreshCatalogOids(ContextMutablePtr query_context)
{
    auto internal_context = Context::createCopy(server.context());
    internal_context->makeQueryContext();
    internal_context->setCurrentQueryId(fmt::format("postgres-oids:{:d}", connection_id));
    internal_context->setSessionContext(query_context->getSessionContext());

    String out_str;
    auto out_buffer = WriteBufferFromString(out_str);

    auto execute_query = [&](const String & query)
    {
        QueryScope query_scope = QueryScope::create(internal_context);
        ReadBufferFromString read_buf(query);
        executeQuery(read_buf, out_buffer, internal_context, {}, QueryFlags{ .internal = true });
    };

    /// Append-only: an object not seen before gets a fresh OID above the current maximum (or above the range
    /// offset when the state is empty, where `max(oid)` is 0), ordered by name only to make the very first
    /// assignment deterministic. Existing entries are never renumbered or removed, and the state is keyed by
    /// the rename-stable identity (UUID when available), so the OID a client observed keeps referring to the
    /// same object for the lifetime of the session, across renames (see `initializeSystemTables`). Temporary
    /// tables (including the emulated catalog itself) live in the nameless database, which never joins a
    /// namespace, so they are not given OIDs.
    execute_query(R"(INSERT INTO pg_namespace_oids_data (identity, oid)
SELECT
    identity,
    toUInt32(greatest((SELECT max(oid) FROM pg_namespace_oids_data), 1000000000) + row_number() OVER (ORDER BY name)) AS oid
FROM
(
    SELECT
        name,
        if (uuid != toUUID('00000000-0000-0000-0000-000000000000'),
            concat('uuid:', toString(uuid)),
            concat('name:', hex(name))) AS identity
    FROM system.databases
)
WHERE identity NOT IN (SELECT identity FROM pg_namespace_oids_data))");
    execute_query(R"(INSERT INTO pg_class_oids_data (identity, oid)
SELECT
    identity,
    toUInt32(greatest((SELECT max(oid) FROM pg_class_oids_data), 2000000000) + row_number() OVER (ORDER BY database, name)) AS oid
FROM
(
    SELECT
        database,
        name,
        if (uuid != toUUID('00000000-0000-0000-0000-000000000000'),
            concat('uuid:', toString(uuid)),
            concat('name:', hex(database), ':', hex(name))) AS identity
    FROM system.tables
    WHERE NOT is_temporary
)
WHERE identity NOT IN (SELECT identity FROM pg_class_oids_data))");
}

}
