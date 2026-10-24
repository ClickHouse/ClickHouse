#include <cstdint>
#include <Core/Defines.h>
#include <Core/Settings.h>
#include <Core/Mongo/Document.h>
#include <Core/Mongo/MongoProtocol.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Poco/Net/SocketAddress.h>
#include <Common/QueryScope.h>
#include <Common/randomSeed.h>

namespace DB::Setting
{
extern const SettingsUInt64 max_parser_backtracks;
extern const SettingsUInt64 max_parser_depth;
}

namespace DB::ErrorCodes
{
extern const int AUTHENTICATION_FAILED;
extern const int BAD_ARGUMENTS;
extern const int LIMIT_EXCEEDED;
}

namespace DB::MongoProtocol
{

Header::Header(const Header & other)
{
    message_length = other.message_length;
    request_id = other.request_id;
    response_to = other.response_to;
    operation_code = other.operation_code;
}

Header & Header::operator=(const Header & right)
{
    if (this == &right)
        return *this;

    message_length = right.message_length;
    request_id = right.request_id;
    response_to = right.response_to;
    operation_code = right.operation_code;
    return *this;
}

void Header::deserialize(ReadBuffer & in)
{
    readBinaryLittleEndian(message_length, in);
    readBinaryLittleEndian(request_id, in);
    readBinaryLittleEndian(response_to, in);
    readBinaryLittleEndian(operation_code, in);
}

void Header::serialize(WriteBuffer & out) const
{
    writeBinaryLittleEndian(message_length, out);
    writeBinaryLittleEndian(request_id, out);
    writeBinaryLittleEndian(response_to, out);
    writeBinaryLittleEndian(operation_code, out);
}

Int32 Header::size() const
{
    return SIZE;
}

String MessageTransport::receivePayload(const Header & header)
{
    /// `message_length` comes from the wire before anything is authenticated, so it is
    /// validated before it is used for an allocation.
    if (header.message_length < static_cast<UInt32>(Header::SIZE) || header.message_length > MAX_MESSAGE_SIZE)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Invalid Mongo message length {}, expected between {} and {}",
            header.message_length,
            Header::SIZE,
            MAX_MESSAGE_SIZE);

    String payload;
    payload.resize(header.message_length - Header::SIZE);
    in->readStrict(payload.data(), payload.size());
    return payload;
}

QueryExecutor::QueryExecutor(
    std::unique_ptr<Session> & session_, const Poco::Net::SocketAddress & address_, const String & default_session_user_)
    : session(session_), address(address_), default_session_user(default_session_user_), gen(randomSeed()), dis(0, INT32_MAX)
{
}

namespace
{

/** The output of `execute` is collected into a string and the reply is built out of it, so the
  * whole text is held in memory at once. A reply is one BSON document and can be no larger than
  * `MAX_BSON_OBJECT_SIZE`, so a result whose text is already several times that size cannot become
  * a reply that is sent, and it is refused while it is written instead of after it is whole. The
  * bound is a multiple of the reply size rather than equal to it because the JSON text of a value
  * and its BSON encoding differ in both directions - an escaped string takes more room as text, a
  * date less - and it bounds the memory the endpoint spends rather than what a reply may hold: the
  * exact size of the reply is checked by the encoder, on the document it is going to send. For
  * `executeStreaming` the same constant bounds one row rather than the whole result, which the
  * consumer of the rows bounds itself, exactly, while they arrive.
  */
constexpr size_t MAX_QUERY_OUTPUT_SIZE = 4 * size_t(MAX_BSON_OBJECT_SIZE);

class BoundedStringWriteBuffer : public WriteBuffer
{
public:
    explicit BoundedStringWriteBuffer(size_t max_bytes_)
        : WriteBuffer(nullptr, 0), max_bytes(max_bytes_), chunk(DBMS_DEFAULT_BUFFER_SIZE)
    {
        set(chunk.data(), chunk.size());
    }

    String & str()
    {
        finalize();
        return result;
    }

private:
    void nextImpl() override
    {
        if (!offset())
            return;

        if (result.size() + offset() > max_bytes)
            throw Exception(
                ErrorCodes::LIMIT_EXCEEDED,
                "The result is larger than the largest reply that can be sent ({} bytes). "
                "Ask for less at a time, with a filter, a projection, 'limit' and 'skip'",
                MAX_BSON_OBJECT_SIZE);

        result.append(working_buffer.begin(), offset());
        set(chunk.data(), chunk.size());
    }

    const size_t max_bytes;
    std::vector<char> chunk;
    String result;
};

/** Splits the output of a row-by-row format (`JSONEachRow` and its kin, where every row is one
  * line and a newline inside a value is escaped) into lines and hands each to a callback while
  * the query is still running. The callback sees a row as soon as it is written, so it can refuse
  * an oversized result before the rest of it is materialized: the exception it throws cancels the
  * query. One line is bounded the way the whole output of `execute` is - a single row whose text
  * is larger than that cannot become a reply that fits `MAX_BSON_OBJECT_SIZE`.
  */
class RowSplittingWriteBuffer : public WriteBuffer
{
public:
    RowSplittingWriteBuffer(size_t max_row_bytes_, const std::function<void(std::string_view)> & on_row_)
        : WriteBuffer(nullptr, 0), max_row_bytes(max_row_bytes_), on_row(on_row_), chunk(DBMS_DEFAULT_BUFFER_SIZE)
    {
        set(chunk.data(), chunk.size());
    }

    /// Hands the trailing bytes after the last newline, if any, to the callback. The row formats
    /// end every row with a newline, so ordinarily there are none.
    void flushPending()
    {
        finalize();
        if (!pending.empty())
        {
            on_row(pending);
            pending.clear();
        }
    }

private:
    void nextImpl() override
    {
        if (!offset())
            return;

        std::string_view written(working_buffer.begin(), offset());
        while (true)
        {
            auto newline = written.find('\n');
            if (newline == std::string_view::npos)
                break;

            if (pending.empty())
                on_row(written.substr(0, newline));
            else
            {
                pending.append(written.substr(0, newline));
                on_row(pending);
                pending.clear();
            }
            written.remove_prefix(newline + 1);
        }

        if (pending.size() + written.size() > max_row_bytes)
            throw Exception(
                ErrorCodes::LIMIT_EXCEEDED,
                "The result is larger than the largest reply that can be sent ({} bytes). "
                "Ask for less at a time, with a filter, a projection, 'limit' and 'skip'",
                MAX_BSON_OBJECT_SIZE);
        pending.append(written);

        set(chunk.data(), chunk.size());
    }

    const size_t max_row_bytes;
    const std::function<void(std::string_view)> & on_row;
    std::vector<char> chunk;
    String pending;
};

}

static void applyReplyOutputSettings(ContextMutablePtr & query_context)
{
    /// The JSON output of a query is parsed back into the BSON reply, so its shape must
    /// not depend on the output settings of the authenticated user's profile: with
    /// `output_format_json_quote_64bit_integers` an `Int64` column would come back as a BSON
    /// string rather than a number.
    query_context->setSetting("output_format_json_quote_64bit_integers", false);
    query_context->setSetting("output_format_json_quote_64bit_floats", false);
    /// Decimals, on the contrary, are quoted: an unquoted decimal is parsed back as a double,
    /// which cannot hold all of its digits. From the string the reply encoder builds a BSON
    /// decimal128 (see `appendTypedValue`), so the value round-trips exactly. The trailing
    /// zeros are kept so that the reply carries the full scale of the column: BSON decimals
    /// distinguish `1.5` from `1.5000000000`, and the column stores the latter.
    query_context->setSetting("output_format_json_quote_decimals", true);
    query_context->setSetting("output_format_decimal_trailing_zeros", true);
    /// Denormals are quoted rather than replaced by JSON `null`: BSON doubles can hold `NaN`
    /// and the infinities, and the reply encoder rebuilds the values from the strings `"nan"`,
    /// `"-nan"`, `"inf"` and `"-inf"` (see `appendTypedValue`).
    query_context->setSetting("output_format_json_quote_denormals", true);
    query_context->setSetting("output_format_json_named_tuples_as_objects", true);
    query_context->setSetting("output_format_json_array_of_rows", false);
    /// A `Map` is decoded as a BSON document only from a JSON object, and a member of a named
    /// tuple that is `NULL` is still a field of the document, so neither representation may be
    /// changed by the profile into an array of pairs or a missing member.
    query_context->setSetting("output_format_json_map_as_array_of_tuples", false);
    query_context->setSetting("output_format_json_skip_null_value_in_named_tuples", false);
    /// The reply carries the stored values, not a rendering of them: a precision of the profile
    /// would round a float (`1.25` to `1.3`), and the trimming of a `FixedString` would drop the
    /// zero bytes that pad it.
    query_context->setSetting("output_format_float_precision", UInt64(0));
    query_context->setSetting("output_format_trim_fixed_string", false);

    /// A reply with cursor id `0` tells the client that it has the whole result, and there is no
    /// way to tell it that the result was cut short. A limit of the profile with the `break` mode
    /// would stop the query after a prefix of the result without an error, so the limits are kept,
    /// but overflowing them is an error. The same holds for the limits of the intermediate steps:
    /// a `DISTINCT`, a `GROUP BY` or a sort that stops at its limit in the `break` (or `any`) mode
    /// answers with a part of the result as if it were all of it.
    query_context->setSetting("result_overflow_mode", String("throw"));
    query_context->setSetting("read_overflow_mode", String("throw"));
    query_context->setSetting("read_overflow_mode_leaf", String("throw"));
    query_context->setSetting("timeout_overflow_mode", String("throw"));
    query_context->setSetting("timeout_overflow_mode_leaf", String("throw"));
    query_context->setSetting("distinct_overflow_mode", String("throw"));
    query_context->setSetting("group_by_overflow_mode", String("throw"));
    query_context->setSetting("sort_overflow_mode", String("throw"));
    query_context->setSetting("set_overflow_mode", String("throw"));
    query_context->setSetting("join_overflow_mode", String("throw"));
    query_context->setSetting("transfer_overflow_mode", String("throw"));

    /// The dates of the result are parsed back into BSON dates, so they must be formatted the
    /// way the parsing expects rather than the way the user's profile asks.
    query_context->setSetting("date_time_output_format", String("simple"));

    /// Everything the handlers run through this executor is ClickHouse SQL that they wrote
    /// themselves - the Mongo request has already been translated by then. A profile that turns
    /// the Mongo dialect on for the user would otherwise have those statements reparsed as Mongo
    /// syntax, so the dialect is pinned rather than inherited.
    query_context->setSetting("dialect", String("clickhouse"));

    /// The handlers decode the answers of their own statements - `EXISTS TABLE`, the probes of the
    /// shape of a table - by their text, and ask for the format they decode with a `FORMAT`
    /// clause, or rely on the default one where they do not. A profile with `default_format`, or
    /// with `format` and `output_format` that override even an explicit `FORMAT` clause, would
    /// change that text under them, so the default is pinned and the overrides are cleared.
    query_context->setDefaultFormat("TabSeparated");
    query_context->setSetting("format", String(""));
    query_context->setSetting("output_format", String(""));
    query_context->setSetting("input_format", String(""));
}

/// A write of this endpoint is acknowledged when it has been written, which is the contract every
/// write command of it answers with. The settings of the authenticated user's profile must not
/// weaken it into an acknowledgement of a write that is still on its way.
static void applyAcknowledgedWriteSettings(ContextMutablePtr & query_context)
{
    /// An asynchronous insert returns before the row is in the table, so a read that follows the
    /// successful reply could miss it and a failure of the write would be reported to nobody.
    query_context->setSetting("async_insert", false);
    query_context->setSetting("wait_for_async_insert", true);
    /// An `update` and a `delete` are mutations, and a mutation that is only enqueued would let
    /// the client read the old rows after the write was acknowledged. Waiting on this server is
    /// what a write of one table can promise.
    query_context->setSetting("mutations_sync", Field(UInt64(1)));
    query_context->setSetting("lightweight_deletes_sync", Field(UInt64(1)));
    /// An insert into a `Distributed` table is otherwise only queued for the remote shards, and is
    /// not even affected by `async_insert`: the row would not be readable yet when the write is
    /// acknowledged, and a failure to send it would be reported to nobody.
    query_context->setSetting("distributed_foreground_insert", true);
    /// A row that fails to parse would be skipped, and a shard that is unavailable would be left
    /// out, while the write is still acknowledged as complete.
    query_context->setSetting("input_format_allow_errors_num", Field(UInt64(0)));
    query_context->setSetting("input_format_allow_errors_ratio", Field(Float64(0)));
    query_context->setSetting("skip_unavailable_shards", false);
}

ContextMutablePtr QueryExecutor::makeQueryContext()
{
    auto query_context = session->makeQueryContext();
    query_context->setCurrentQueryId(fmt::format("mongo:{:d}", dis(gen)));
    applyReplyOutputSettings(query_context);
    applyAcknowledgedWriteSettings(query_context);
    return query_context;
}

String QueryExecutor::execute(const String & query)
{
    auto query_context = makeQueryContext();

    /// A reply is one BSON document and cannot be larger than the limit advertised to clients.
    /// Limit the query result before the format materializes it into `out`; the reply encoder
    /// checks the exact BSON size afterwards, including its envelope.
    query_context->setSetting("max_result_bytes", Field(UInt64(MAX_BSON_OBJECT_SIZE)));

    auto query_scope = QueryScope::create(query_context);
    ReadBufferFromString read_buf(query);

    BoundedStringWriteBuffer out(MAX_QUERY_OUTPUT_SIZE);
    executeQuery(read_buf, out, query_context, {});

    return out.str();
}

void QueryExecutor::executeStreaming(const String & query, const std::function<void(std::string_view)> & on_row)
{
    /// No `max_result_bytes` here: that setting bounds the in-memory size of the result columns,
    /// which relates to the size of the reply only loosely in either direction, and the caller
    /// bounds the reply itself, exactly, row by row as the rows arrive.
    auto query_context = makeQueryContext();

    auto query_scope = QueryScope::create(query_context);
    ReadBufferFromString read_buf(query);

    RowSplittingWriteBuffer out(MAX_QUERY_OUTPUT_SIZE, on_row);
    executeQuery(read_buf, out, query_context, {});
    out.flushPending();
}

void QueryExecutor::authenticate(const String & username, const String & password)
{
    /// If the resolved name is empty too (explicitly configured to prohibit connections without
    /// a user name), the login is refused and recorded as a failure, like on the other endpoints.
    const String & user_name = username.empty() ? default_session_user : username;

    /** `Session::makeSessionContext` installs the session context before it counts the session
      * against `max_sessions_for_user`, so when the limit is reached the session is left
      * authenticated, with a context, but untracked - and without the settings the authentication
      * server sent, which are applied after that. A command that followed would run in it. So any
      * failure here closes the connection once the error is answered, the way the other stateful
      * protocols end a connection whose login failed.
      */
    close_connection = true;

    if (user_name.empty())
    {
        auto exception = Exception(ErrorCodes::AUTHENTICATION_FAILED, "Got an empty user name in the 'PLAIN' authentication payload");
        session->onAuthenticationFailure(user_name, address, exception);
        throw exception; /// NOLINT
    }

    session->authenticate(user_name, password, address);
    /// A connection of this endpoint is stateful, so it takes a session context of its own, like
    /// the other stateful protocols do after a login: it is what counts the connection against
    /// `max_sessions_for_user` and what applies the settings the authentication server sent.
    /// Without it every command would build its query context straight from the global one.
    session->makeSessionContext();

    close_connection = false;
}

ParserLimits QueryExecutor::getParserLimits() const
{
    const auto & settings = session->sessionOrGlobalContext()->getSettingsRef();
    return {.max_parser_depth = settings[Setting::max_parser_depth], .max_parser_backtracks = settings[Setting::max_parser_backtracks]};
}

String QueryExecutor::getAuthenticatedUserName() const
{
    /// `Session::authenticate` records the user it authenticated in the client info, and until
    /// then the name is empty - which is what an unauthenticated connection has to report.
    return session->getClientInfo().current_user;
}

}
