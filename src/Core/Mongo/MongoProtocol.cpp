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

QueryExecutor::QueryExecutor(std::unique_ptr<Session> & session_, const Poco::Net::SocketAddress & address_)
    : session(session_), address(address_), gen(randomSeed()), dis(0, INT32_MAX)
{
}

namespace
{

/** The output of a query is collected into a string and the reply is built out of it, so the whole
  * text is held in memory at once. A cursor reply is one BSON document and can be no larger than
  * `MAX_BSON_OBJECT_SIZE`, so a result whose text is already several times that size cannot become
  * a reply that is sent, and it is refused while it is written instead of after it is whole. The
  * JSON text of a value is longer than its BSON encoding - a date, a number and an escaped string
  * all take more room as text - so the bound is a multiple of the reply size rather than equal to
  * it, and it bounds the memory the endpoint spends rather than what a reply may hold: the exact
  * size of the reply is checked by the cursor encoder, on the document it is going to send.
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

}

String QueryExecutor::execute(const String & query)
{
    auto query_context = session->makeQueryContext();
    auto secret_key = dis(gen);

    query_context->setCurrentQueryId(fmt::format("mongo:{:d}", secret_key));

    /// The output of a `FORMAT JSON` query is parsed back into the BSON reply, so its shape must
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

    /// A cursor reply is one BSON document and cannot be larger than the limit advertised to
    /// clients. Limit the query result before `FORMAT JSON` materializes it into `out`; the
    /// cursor encoder checks the exact BSON size afterwards, including its envelope.
    query_context->setSetting("max_result_bytes", Field(UInt64(MAX_BSON_OBJECT_SIZE)));
    query_context->setSetting("result_overflow_mode", String("throw"));

    /// The dates of the result are parsed back into BSON dates, so they must be formatted the
    /// way the parsing expects rather than the way the user's profile asks.
    query_context->setSetting("date_time_output_format", String("simple"));

    /// Everything the handlers run through this executor is ClickHouse SQL that they wrote
    /// themselves - the Mongo request has already been translated by then. A profile that turns
    /// the Mongo dialect on for the user would otherwise have those statements reparsed as Mongo
    /// syntax, so the dialect is pinned rather than inherited.
    query_context->setSetting("dialect", String("clickhouse"));

    auto query_scope = QueryScope::create(query_context);
    ReadBufferFromString read_buf(query);

    BoundedStringWriteBuffer out(MAX_QUERY_OUTPUT_SIZE);
    executeQuery(read_buf, out, query_context, {});

    return out.str();
}

void QueryExecutor::authenticate(const String & username, const String & password)
{
    session->authenticate(username, password, address);
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
