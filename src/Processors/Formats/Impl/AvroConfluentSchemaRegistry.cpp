#include <Processors/Formats/Impl/AvroConfluentSchemaRegistry.h>

#if USE_AVRO

#include <Common/CacheBase.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>

#include <Formats/FormatSettings.h>

#include <IO/HTTPCommon.h>

#include <Compiler.hh>
#include <ValidSchema.hh>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/Net/HTTPCredentials.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/StreamCopier.h>
#include <Poco/URI.h>


namespace CurrentMetrics
{
    extern const Metric AvroSchemaCacheBytes;
    extern const Metric AvroSchemaCacheCells;
    extern const Metric AvroSchemaRegistryCacheBytes;
    extern const Metric AvroSchemaRegistryCacheCells;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
    extern const int INCOMPATIBLE_SCHEMA;
}

ConfluentSchemaRegistry::ConfluentSchemaRegistry(const std::string & base_url_, size_t schema_cache_max_size)
    : base_url(base_url_)
    , schema_cache(CurrentMetrics::AvroSchemaCacheBytes, CurrentMetrics::AvroSchemaCacheCells, schema_cache_max_size)
    , register_cache(CurrentMetrics::AvroSchemaCacheBytes, CurrentMetrics::AvroSchemaCacheCells, schema_cache_max_size)
{
    if (base_url.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty Schema Registry URL");
}

namespace
{

ConnectionTimeouts buildTimeouts(const FormatSettings::AvroSchemaRegistryTimeouts & timeouts)
{
    return ConnectionTimeouts()
        .withConnectionTimeout(timeouts.connection_timeout)
        .withSendTimeout(timeouts.send_timeout)
        .withReceiveTimeout(timeouts.receive_timeout);
}

}

avro::ValidSchema ConfluentSchemaRegistry::getSchema(uint32_t id, const FormatSettings::AvroSchemaRegistryTimeouts & timeouts)
{
    auto [schema, loaded] = schema_cache.getOrSet(
        id,
        [this, id, &timeouts]() { return std::make_shared<avro::ValidSchema>(fetchSchema(id, timeouts)); });
    return *schema;
}

void ConfluentSchemaRegistry::applyAuth(const Poco::URI & url, Poco::Net::HTTPRequest & request) const
{
    if (!url.getUserInfo().empty())
    {
        Poco::Net::HTTPCredentials http_credentials;
        Poco::Net::HTTPBasicCredentials http_basic_credentials;

        http_credentials.fromUserInfo(url.getUserInfo());

        std::string decoded_username;
        Poco::URI::decode(http_credentials.getUsername(), decoded_username);
        http_basic_credentials.setUsername(decoded_username);

        if (!http_credentials.getPassword().empty())
        {
            std::string decoded_password;
            Poco::URI::decode(http_credentials.getPassword(), decoded_password);
            http_basic_credentials.setPassword(decoded_password);
        }

        http_basic_credentials.authenticate(request);
    }
}

avro::ValidSchema ConfluentSchemaRegistry::fetchSchema(uint32_t id, const FormatSettings::AvroSchemaRegistryTimeouts & timeouts)
{
    try
    {
        try
        {
            Poco::URI url(base_url, base_url.getPath() + "/schemas/ids/" + std::to_string(id));
            LOG_TRACE(getLogger("ConfluentSchemaRegistry"), "Fetching schema id = {} from url {}", id, url.toString());

            auto connection_timeouts = buildTimeouts(timeouts);

            Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_GET, url.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1);
            if (url.getPort())
                request.setHost(url.getHost(), url.getPort());
            else
                request.setHost(url.getHost());

            applyAuth(url, request);

            auto session = makeHTTPSession(HTTPConnectionGroupType::HTTP, url, connection_timeouts);
            session->sendRequest(request);

            Poco::Net::HTTPResponse response;
            std::istream * response_body = receiveResponse(*session, request, response, false);

            Poco::JSON::Parser parser;
            auto json_body = parser.parse(*response_body).extract<Poco::JSON::Object::Ptr>();

            auto schema = json_body->getValue<std::string>("schema");
            LOG_TRACE(getLogger("ConfluentSchemaRegistry"), "Successfully fetched schema id = {}\n{}", id, schema);
            return avro::compileJsonSchemaFromString(schema);
        }
        catch (const Exception &)
        {
            throw;
        }
        catch (const Poco::Exception & e)
        {
            throw Exception(Exception::CreateFromPocoTag{}, e);
        }
        catch (const avro::Exception & e)
        {
            throw Exception::createDeprecated(e.what(), ErrorCodes::INCORRECT_DATA);
        }
    }
    catch (Exception & e)
    {
        e.addMessage("while fetching schema id = " + std::to_string(id));
        throw;
    }
}

uint32_t ConfluentSchemaRegistry::registerSchema(const std::string & subject, const avro::ValidSchema & schema, const FormatSettings::AvroSchemaRegistryTimeouts & timeouts)
{
    std::string schema_json = schema.toJson(false);

    /// Cache key is a compact fingerprint: `subject + '\0' + 8-byte SipHash64`.
    /// Storing the full `schema_json` on the value side keeps the key small
    /// while letting us verify equality on hit, defending against hash
    /// collisions.
    std::string cache_key;
    cache_key.reserve(subject.size() + 1 + sizeof(UInt64));
    cache_key.append(subject);
    cache_key.push_back('\0');
    const UInt64 schema_hash = sipHash64(schema_json);
    cache_key.append(reinterpret_cast<const char *>(&schema_hash), sizeof(schema_hash));

    auto do_register = [&]() -> uint32_t
    {
        try
        {
            try
            {
                /// Percent-encode the subject for use as a path segment.
                std::string encoded_subject;
                Poco::URI::encode(subject, "?#/;+@&=", encoded_subject);

                /// Build the request path directly to preserve percent-encoding.
                /// Poco::URI normalizes paths by decoding %2F -> /, which breaks
                /// subjects containing slashes.
                std::string request_path = base_url.getPath() + "/subjects/" + encoded_subject + "/versions";
                LOG_TRACE(getLogger("ConfluentSchemaRegistry"), "Registering schema under subject '{}' at path {}", subject, request_path);

                auto connection_timeouts = buildTimeouts(timeouts);

                Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_POST, request_path, Poco::Net::HTTPRequest::HTTP_1_1);
                request.setContentType("application/vnd.schemaregistry.v1+json");
                if (base_url.getPort())
                    request.setHost(base_url.getHost(), base_url.getPort());
                else
                    request.setHost(base_url.getHost());

                applyAuth(base_url, request);

                /// Build request body: {"schema": "<schema_json>"}
                Poco::JSON::Object body;
                body.set("schema", schema_json);
                std::ostringstream body_stream; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
                body.stringify(body_stream);
                std::string body_str = body_stream.str();
                request.setContentLength(body_str.size());

                auto session = makeHTTPSession(HTTPConnectionGroupType::HTTP, base_url, connection_timeouts);
                std::ostream & os = session->sendRequest(request);
                os << body_str;

                Poco::Net::HTTPResponse response;
                std::istream & response_stream = session->receiveResponse(response);

                if (response.getStatus() == Poco::Net::HTTPResponse::HTTP_CONFLICT)
                {
                    std::string response_body;
                    Poco::StreamCopier::copyToString(response_stream, response_body);

                    /// Confluent Schema Registry returns a JSON object with an "error_code" and a human-readable "message".
                    std::string registry_message = response_body;
                    LOG_TRACE(getLogger("ConfluentSchemaRegistry"), "Received HTTP 409 Conflict from Schema Registry for subject '{}': {}", subject, registry_message);
                    try
                    {
                        Poco::JSON::Parser error_parser;
                        auto error_json = error_parser.parse(response_body).extract<Poco::JSON::Object::Ptr>();
                        if (error_json && error_json->has("message"))
                            registry_message = error_json->getValue<std::string>("message");
                    }
                    catch (const Poco::Exception &) // NOLINT(bugprone-empty-catch)
                    {
                        /// Fall back to the raw body if it is not JSON.
                    }

                    throw Exception(
                        ErrorCodes::INCOMPATIBLE_SCHEMA,
                        "Schema Registry returned HTTP 409 Conflict for subject '{}': {}. This typically means the schema is "
                        "incompatible with an existing version under the subject's compatibility level. One way to resolve "
                        "it is to set the compatibility level to NONE for this subject on the Confluent Schema Registry.",
                        subject, registry_message);
                }

                assertResponseIsOk(request.getURI(), response, response_stream, false);

                Poco::JSON::Parser parser;
                auto json_body = parser.parse(response_stream).extract<Poco::JSON::Object::Ptr>();
                uint32_t schema_id = json_body->getValue<uint32_t>("id");
                LOG_TRACE(getLogger("ConfluentSchemaRegistry"), "Successfully registered schema under subject '{}', id = {}", subject, schema_id);
                return schema_id;
            }
            catch (const Exception &)
            {
                throw;
            }
            catch (const Poco::Exception & e)
            {
                throw Exception(Exception::CreateFromPocoTag{}, e);
            }
            catch (const avro::Exception & e)
            {
                throw Exception::createDeprecated(e.what(), ErrorCodes::INCORRECT_DATA);
            }
        }
        catch (Exception & e)
        {
            e.addMessage("while registering schema under subject '" + subject + "'");
            throw;
        }
    };

    /// `getOrSet` serializes concurrent misses on the same key, so only one
    /// thread issues the POST and the rest wait for the cached result.
    auto [cached, _] = register_cache.getOrSet(
        cache_key,
        [&]() { return std::make_shared<CachedSchemaRegistration>(CachedSchemaRegistration{schema_json, do_register()}); });

    if (cached->schema_json == schema_json)
        return cached->schema_id;

    /// Hash collision: a different schema mapped to the same key. Register
    /// the current schema and overwrite the cache entry.
    uint32_t schema_id = do_register();
    register_cache.set(cache_key, std::make_shared<CachedSchemaRegistration>(CachedSchemaRegistration{std::move(schema_json), schema_id}));
    return schema_id;
}

#define SCHEMA_REGISTRY_CACHE_MAX_SIZE 1000

/// Cache of Schema Registry URL -> ConfluentSchemaRegistry
static CacheBase<std::string, ConfluentSchemaRegistry>
    schema_registry_cache(CurrentMetrics::AvroSchemaRegistryCacheBytes, CurrentMetrics::AvroSchemaRegistryCacheCells, SCHEMA_REGISTRY_CACHE_MAX_SIZE);

std::shared_ptr<ConfluentSchemaRegistry> getConfluentSchemaRegistry(const FormatSettings & format_settings)
{
    const auto & base_url = format_settings.avro.schema_registry_url;
    auto [schema_registry, loaded] = schema_registry_cache.getOrSet(
        base_url,
        [base_url]()
        {
            return std::make_shared<ConfluentSchemaRegistry>(base_url);
        });
    return schema_registry;
}

void clearConfluentSchemaRegistryCache()
{
    schema_registry_cache.clear();
}

}

#endif
