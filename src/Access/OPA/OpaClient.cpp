#include <Access/OPA/OpaClient.h>

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <IO/HTTPCommon.h>
#include <base/defines.h>
#include <base/sleep.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/StreamCopier.h>


namespace ProfileEvents
{
    extern const Event OpaRequests;
    extern const Event OpaRequestFailures;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
}

OpaClient::OpaClient(OpaConfigurationPtr configuration_)
    : configuration(std::move(configuration_))
{
}

Poco::JSON::Object::Ptr OpaClient::parseResponseObject(const Poco::URI & uri, const String & response_body)
{
    Poco::Dynamic::Var parsed;
    try
    {
        Poco::JSON::Parser parser;
        parsed = parser.parse(response_body);
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(
            ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER,
            "Cannot parse the response of OPA at {} as JSON: {}. Response: {}",
            uri.toString(),
            e.displayText(),
            response_body);
    }

    if (parsed.type() != typeid(Poco::JSON::Object::Ptr))
    {
        throw Exception(
            ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER,
            "Expected a JSON object in the response of OPA at {}, got: {}",
            uri.toString(),
            response_body);
    }

    return parsed.extract<Poco::JSON::Object::Ptr>();
}

String OpaClient::send(const Poco::URI & uri, const String & body) const
{
    if (configuration->log_requests)
        LOG_DEBUG(getLogger("OpaClient"), "Request to {}: {}", uri.toString(), body);

    auto milliseconds_to_wait = configuration->retry_initial_backoff_ms;

    for (size_t attempt = 0; attempt < configuration->max_tries; ++attempt)
    {
        const bool last_attempt = attempt + 1 >= configuration->max_tries;

        try
        {
            auto session = makeHTTPSession(HTTPConnectionGroupType::HTTP, uri, configuration->timeouts);

            Poco::Net::HTTPRequest request{
                Poco::Net::HTTPRequest::HTTP_POST, uri.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1};
            request.setContentType("application/json");
            request.setContentLength(static_cast<std::streamsize>(body.size()));

            if (!configuration->token.empty())
                request.set("Authorization", "Bearer " + configuration->token);

            auto & request_stream = session->sendRequest(request);
            request_stream << body;

            Poco::Net::HTTPResponse response;
            auto & response_stream = session->receiveResponse(response);

            String response_body;
            Poco::StreamCopier::copyToString(response_stream, response_body);

            if (configuration->log_responses)
                LOG_DEBUG(getLogger("OpaClient"), "Response from {} with status {}: {}", uri.toString(), static_cast<int>(response.getStatus()), response_body);

            /// A status other than 200 means the request never reached a policy, or the server
            /// rejected it. Retrying cannot change that, so report it as it is.
            if (response.getStatus() != Poco::Net::HTTPResponse::HTTP_OK)
            {
                throw Exception(
                    ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER,
                    "OPA at {} answered with status {}: {}",
                    uri.toString(),
                    static_cast<int>(response.getStatus()),
                    response_body);
            }

            return response_body;
        }
        catch (const Poco::Exception &)
        {
            /// Only a transport failure is retried. A decision that arrived and was understood is
            /// final, and a `DB::Exception` raised above is not a `Poco::Exception`, so it passes
            /// through untouched.
            if (last_attempt)
                throw;

            sleepForMilliseconds(milliseconds_to_wait);
            milliseconds_to_wait = std::min(milliseconds_to_wait * 2, configuration->retry_max_backoff_ms);
        }
    }

    UNREACHABLE();
}

bool OpaClient::parseDecision(const Poco::URI & uri, const String & response_body)
{
    const auto object = parseResponseObject(uri, response_body);

    /// OPA omits `result` when the queried document is undefined, which in practice almost always
    /// means the endpoint path does not match the policy's package and rule. Saying so is far more
    /// useful than denying every query and leaving the operator to guess why.
    if (!object->has("result"))
    {
        throw Exception(
            ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER,
            "The response of OPA at {} has no 'result' field, which means the queried document is "
            "undefined. Check that the endpoint path matches the package and rule name of the policy. Response: {}",
            uri.toString(),
            response_body);
    }

    const auto result = object->get("result");

    if (result.type() == typeid(bool))
        return result.extract<bool>();

    /// A policy may answer with the whole decision document instead of a bare boolean.
    if (result.type() == typeid(Poco::JSON::Object::Ptr))
    {
        const auto result_object = result.extract<Poco::JSON::Object::Ptr>();
        if (result_object->has("allow"))
        {
            const auto allow = result_object->get("allow");
            if (allow.type() == typeid(bool))
                return allow.extract<bool>();
        }
    }

    throw Exception(
        ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER,
        "Expected the 'result' field in the response of OPA at {} to be a boolean, or an object with a "
        "boolean 'allow' field. Response: {}",
        uri.toString(),
        response_body);
}

Strings OpaClient::parseRowFilters(const Poco::URI & uri, const String & response_body)
{
    const auto object = parseResponseObject(uri, response_body);

    /// Unlike a decision, the absence of an answer here is itself an answer: a policy that defines no
    /// filter for a table leaves `result` undefined, and almost every table has no filter.
    if (!object->has("result"))
        return {};

    const auto result = object->get("result");
    if (result.isEmpty())
        return {};

    if (result.type() != typeid(Poco::JSON::Array::Ptr))
    {
        throw Exception(
            ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER,
            "Expected the 'result' field in the response of OPA at {} to be an array of objects with an "
            "'expression' field. Response: {}",
            uri.toString(),
            response_body);
    }

    const auto array = result.extract<Poco::JSON::Array::Ptr>();

    Strings expressions;
    expressions.reserve(array->size());

    for (size_t i = 0; i < array->size(); ++i)
    {
        const auto entry = array->get(static_cast<unsigned int>(i));
        if (entry.type() != typeid(Poco::JSON::Object::Ptr))
        {
            throw Exception(
                ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER,
                "Expected element {} of the 'result' array in the response of OPA at {} to be an object. Response: {}",
                i,
                uri.toString(),
                response_body);
        }

        const auto entry_object = entry.extract<Poco::JSON::Object::Ptr>();

        String expression = entry_object->optValue<String>("expression", "");

        /// An entry without an expression cannot be applied. Skipping it would show more rows than the
        /// policy intended, so it is reported instead.
        if (expression.empty())
        {
            throw Exception(
                ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER,
                "Element {} of the 'result' array in the response of OPA at {} has no 'expression' field. Response: {}",
                i,
                uri.toString(),
                response_body);
        }

        expressions.push_back(std::move(expression));
    }

    return expressions;
}

std::vector<OpaColumnMask> OpaClient::parseColumnMasks(const Poco::URI & uri, const String & response_body)
{
    const auto object = parseResponseObject(uri, response_body);

    /// A table with no masked column leaves `result` undefined, which is the common case.
    if (!object->has("result"))
        return {};

    const auto result = object->get("result");
    if (result.isEmpty())
        return {};

    if (result.type() != typeid(Poco::JSON::Array::Ptr))
    {
        throw Exception(
            ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER,
            "Expected the 'result' field in the response of OPA at {} to be an array of objects with "
            "'column' and 'expression' fields. Response: {}",
            uri.toString(),
            response_body);
    }

    const auto array = result.extract<Poco::JSON::Array::Ptr>();

    std::vector<OpaColumnMask> masks;
    masks.reserve(array->size());

    for (size_t i = 0; i < array->size(); ++i)
    {
        const auto entry = array->get(static_cast<unsigned int>(i));
        if (entry.type() != typeid(Poco::JSON::Object::Ptr))
        {
            throw Exception(
                ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER,
                "Expected element {} of the 'result' array in the response of OPA at {} to be an object. Response: {}",
                i,
                uri.toString(),
                response_body);
        }

        const auto entry_object = entry.extract<Poco::JSON::Object::Ptr>();

        OpaColumnMask mask;
        mask.column = entry_object->optValue<String>("column", "");
        mask.expression = entry_object->optValue<String>("expression", "");

        /// A mask that does not say which column it applies to, or has nothing to apply, cannot be
        /// used. Ignoring it would show the real value, which is the opposite of what was intended.
        if (mask.column.empty() || mask.expression.empty())
        {
            throw Exception(
                ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER,
                "Element {} of the 'result' array in the response of OPA at {} needs both a 'column' and "
                "an 'expression' field. Response: {}",
                i,
                uri.toString(),
                response_body);
        }

        masks.push_back(std::move(mask));
    }

    return masks;
}

bool OpaClient::isAllowed(const OpaRequest & request, const OpaRequestContext & request_context) const
{
    const String body = request.serialize(request_context);

    /// Counted around the whole exchange, so that a request which fails to produce a usable decision
    /// is visible as a failure rather than only as a denied query.
    ProfileEvents::increment(ProfileEvents::OpaRequests);
    try
    {
        return parseDecision(configuration->uri, send(configuration->uri, body));
    }
    catch (...)
    {
        ProfileEvents::increment(ProfileEvents::OpaRequestFailures);
        throw;
    }
}

Strings OpaClient::getRowFilters(const OpaRequest & request, const OpaRequestContext & request_context) const
{
    chassert(configuration->row_filters_uri.has_value());
    const Poco::URI & uri = *configuration->row_filters_uri;

    const String body = request.serialize(request_context);

    ProfileEvents::increment(ProfileEvents::OpaRequests);
    try
    {
        return parseRowFilters(uri, send(uri, body));
    }
    catch (...)
    {
        ProfileEvents::increment(ProfileEvents::OpaRequestFailures);
        throw;
    }
}

std::vector<OpaColumnMask> OpaClient::getColumnMasks(const OpaRequest & request, const OpaRequestContext & request_context) const
{
    chassert(configuration->column_masking_uri.has_value());
    const Poco::URI & uri = *configuration->column_masking_uri;

    const String body = request.serialize(request_context);

    ProfileEvents::increment(ProfileEvents::OpaRequests);
    try
    {
        return parseColumnMasks(uri, send(uri, body));
    }
    catch (...)
    {
        ProfileEvents::increment(ProfileEvents::OpaRequestFailures);
        throw;
    }
}

}
