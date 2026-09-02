#include <Access/HTTPUserDirectoryResponseParser.h>

#include <Common/Exception.h>
#include <Common/quoteString.h>

#include <Poco/Dynamic/Var.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/String.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int AUTHENTICATION_FAILED;
}

/// Converts a JSON scalar into a `Field` that preserves its JSON type: strings stay strings,
/// integers stay integers, booleans stay booleans. Whether the name is an allowed setting and
/// how a built-in setting interprets the value is decided by the storage, not here.
Field HTTPUserDirectoryResponseParser::jsonScalarToField(const Poco::Dynamic::Var & value, const String & name)
{
    try
    {
        /// `isInteger` is also true for booleans, so booleans are classified first.
        if (value.isBoolean())
            return Field(value.convert<bool>());
        if (value.isString())
            return Field(value.convert<String>());
        if (value.isInteger())
        {
            if (value.isSigned())
            {
                Int64 signed_value = value.convert<Int64>();
                if (signed_value < 0)
                    return Field(signed_value);
                return Field(static_cast<UInt64>(signed_value));
            }
            return Field(value.convert<UInt64>());
        }
        if (value.isNumeric())
            return Field(value.convert<Float64>());
    }
    catch (const Poco::Exception &)
    {
        throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
            "The value of setting {} in the HTTP authentication server response is out of range", backQuote(name));
    }
    throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
        "The value of setting {} in the HTTP authentication server response must be a string, number or boolean", backQuote(name));
}

String HTTPUserDirectoryResponseParser::readBoundedBody(std::istream * body_stream)
{
    /// Bounded read: a compromised or broken helper must not force unbounded allocations.
    static constexpr size_t max_response_body_size = 1 * 1024 * 1024;
    String body;
    if (!body_stream)
        return body;

    char buffer[8192];
    while (body_stream->good())
    {
        body_stream->read(buffer, sizeof(buffer));
        body.append(buffer, static_cast<size_t>(body_stream->gcount()));
        if (body.size() > max_response_body_size)
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
                "HTTP authentication server response body exceeds {} bytes", max_response_body_size);
    }
    return body;
}

HTTPUserDirectoryResponseParser::Result
HTTPUserDirectoryResponseParser::parse(const Poco::Net::HTTPResponse & response, std::istream * body_stream) const
{
    const auto status = response.getStatus();

    if (status == Poco::Net::HTTPResponse::HTTP_NOT_FOUND)
    {
        /// A 404 is the routine "not my user" answer, so it is worth keeping the connection
        /// reusable: the connection pool only keeps a connection whose response body was read
        /// to the end. The body itself carries no information for us.
        readBoundedBody(body_stream);
        Result result;
        result.status = Result::Status::UserNotFound;
        return result;
    }

    if (status != Poco::Net::HTTPResponse::HTTP_OK)
        throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
            "HTTP authentication server responded with status {}", static_cast<int>(status));

    Result result;
    result.status = Result::Status::Ok;

    String body = readBoundedBody(body_stream);

    /// An empty or whitespace-only body is equivalent to an empty JSON object.
    if (Poco::trim(body).empty())
        return result;

    Poco::JSON::Object::Ptr object;
    try
    {
        Poco::JSON::Parser parser;
        object = parser.parse(body).extract<Poco::JSON::Object::Ptr>();
    }
    catch (...)
    {
        /// Poco JSON exceptions derive from Poco::Exception, which would trigger retries
        /// in HTTPAuthClient; convert to a DB exception so a malformed body fails immediately.
        throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
            "HTTP authentication server returned a malformed response body");
    }

    if (!object)
        throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
            "HTTP authentication server response is not a JSON object");

    if (object->has("settings"))
    {
        Poco::JSON::Object::Ptr settings_object = object->getObject("settings");
        if (!settings_object)
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
                "The 'settings' field of the HTTP authentication server response is not a JSON object");

        for (const auto & [name, value] : *settings_object)
        {
            if (name.empty())
                throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
                    "Empty setting name in the HTTP authentication server response");
            result.settings.emplace_back(name, jsonScalarToField(value, name));
        }
    }

    if (object->has("roles"))
    {
        Poco::JSON::Array::Ptr roles_array = object->getArray("roles");
        if (!roles_array)
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
                "The 'roles' field of the HTTP authentication server response is not an array");

        for (const auto & element : *roles_array)
        {
            if (!element.isString())
                throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
                    "The 'roles' field of the HTTP authentication server response must contain only strings");
            result.role_names.push_back(element.toString());
        }
    }

    if (object->has("valid_until"))
    {
        const auto value = object->get("valid_until");
        /// `Poco::Dynamic::Var::isInteger` reports `true` for a boolean value as well
        /// (`std::numeric_limits<bool>::is_integer` is `true`), so `isBoolean` must be
        /// rejected explicitly.
        if (!value.isInteger() || value.isBoolean())
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
                "The 'valid_until' field of the HTTP authentication server response must be an integer Unix timestamp");
        Int64 valid_until = 0;
        try
        {
            valid_until = value.convert<Int64>();
        }
        catch (...)
        {
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
                "The 'valid_until' field of the HTTP authentication server response is out of range");
        }
        if (valid_until < 0)
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
                "The 'valid_until' field of the HTTP authentication server response must not be negative");
        result.valid_until = static_cast<time_t>(valid_until);
    }

    return result;
}

}
