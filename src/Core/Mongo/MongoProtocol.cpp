#include <cstdint>
#include <Core/Mongo/Document.h>
#include <Core/Mongo/MongoProtocol.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Poco/Net/SocketAddress.h>
#include <Common/QueryScope.h>
#include <Common/randomSeed.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LIMIT_EXCEEDED;
}

namespace DB::MongoProtocol
{

namespace
{

/// Collects the output of a query into a string and fails the query as soon as the output
/// grows past `max_size`, so that a result too large to be sent is never held whole in memory.
class BoundedStringWriteBufferImpl : public BufferWithOwnMemory<WriteBuffer>
{
public:
    explicit BoundedStringWriteBufferImpl(size_t max_size_)
        : max_size(max_size_)
    {
    }

    String & str()
    {
        finalize();
        return result;
    }

private:
    void nextImpl() override
    {
        if (result.size() + offset() > max_size)
            throw Exception(
                ErrorCodes::LIMIT_EXCEEDED,
                "The result is larger than the largest reply that can be sent: its text exceeds {} bytes. "
                "Ask for less at a time, with a filter, a projection, 'limit' and 'skip'",
                max_size);
        result.append(working_buffer.begin(), offset());
    }

    const size_t max_size;
    String result;
};

using BoundedStringWriteBuffer = AutoCanceledWriteBuffer<BoundedStringWriteBufferImpl>;

}

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

String QueryExecutor::execute(const String & query, const SettingsChanges & extra_settings)
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

    /// The dates of the result are parsed back into BSON dates, so they must be formatted the
    /// way the parsing expects rather than the way the user's profile asks.
    query_context->setSetting("date_time_output_format", String("simple"));

    for (const auto & change : extra_settings)
        query_context->setSetting(change.name, change.value);

    auto query_scope = QueryScope::create(query_context);
    ReadBufferFromString read_buf(query);

    /// Every output is turned into a reply, which is one BSON document of at most
    /// `MAX_BSON_OBJECT_SIZE` bytes. The exact bound is checked while the reply is built, but
    /// that happens after the query has finished, so the text of the output is bounded here
    /// as well, while the query runs (see `MAX_QUERY_OUTPUT_SIZE`).
    BoundedStringWriteBuffer out(MAX_QUERY_OUTPUT_SIZE);
    executeQuery(read_buf, out, query_context, {});

    return std::move(out.str());
}

ContextPtr QueryExecutor::getSessionContext() const
{
    return session->sessionOrGlobalContext();
}

void QueryExecutor::authenticate(const String & username, const String & password)
{
    session->authenticate(username, password, address);
}

String QueryExecutor::getAuthenticatedUserName() const
{
    /// `Session::authenticate` records the user it authenticated in the client info, and until
    /// then the name is empty - which is what an unauthenticated connection has to report.
    return session->getClientInfo().current_user;
}

}
