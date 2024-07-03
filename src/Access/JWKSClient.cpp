#include "Access/JWKSClient.h"

#include <sstream>
#include <string>
#include <utility>

#include "Common/logger_useful.h"
#include <Common/Exception.h>
#include "IO/ReadBufferFromString.h"
#include <jwt-cpp/traits/kazuho-picojson/defaults.h>
#include <picojson/picojson.h>
#include "Poco/StreamCopier.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

bool check_claims(const picojson::value &claims, const picojson::value &payload, const String &path);
bool check_claims(const picojson::value::object &claims, const picojson::value::object &payload, const String &path)
{
    for (const auto &it : claims)
    {
        const auto &payload_it = payload.find(it.first);
        if (payload_it == payload.end())
        {
            LOG_TRACE(getLogger("JWKSAuthentication"), "payload does not contain key '{}.{}'", path, it.first);
            return false;
        }
        if (!check_claims(it.second, payload_it->second, path + "." + it.first))
        {
            return false;
        }
    }
    return true;
}

bool check_claims(const picojson::value::array &claims, const picojson::value::array &payload, const String &path)
{
    if (claims.size() > payload.size())
    {
        LOG_TRACE(getLogger("JWKSAuthentication"), "payload to small for claims key '{}'", path);
        return false;
    }
    for (size_t claims_i = 0; claims_i < claims.size(); ++claims_i)
    {
        bool found = false;
        const auto &claims_val = claims.at(claims_i);
        for (const auto &payload_val : payload)
        {
            if (!check_claims(claims_val, payload_val, path + "[" + std::to_string(claims_i) + "]"))
                continue;
            found = true;
        }
        if (!found)
        {
            LOG_TRACE(getLogger("JWKSAuthentication"), "payload does not contain an object matching claims key '{}[{}]'", path, claims_i);
            return false;
        }
    }
    return true;
}

bool check_claims(const picojson::value &claims, const picojson::value &payload, const String &path)
{
    if (claims.is<picojson::array>())
    {
        if (!payload.is<picojson::array>())
        {
            LOG_TRACE(getLogger("JWKSAuthentication"), "payload does not match key type 'array' in claims '{}'", path);
            return false;
        }
        return check_claims(claims.get<picojson::array>(), payload.get<picojson::array>(), path);
    }
    if (claims.is<picojson::object>())
    {
        if (!payload.is<picojson::object>())
        {
            LOG_TRACE(getLogger("JWKSAuthentication"), "payload does not match key type 'object' in claims '{}'", path);
            return false;
        }
        return check_claims(claims.get<picojson::object>(), payload.get<picojson::object>(), path);
    }
    if (claims.is<bool>())
    {
        if (!payload.is<bool>())
        {
            LOG_TRACE(getLogger("JWKSAuthentication"), "payload does not match key type 'bool' in claims '{}'", path);
            return false;
        }
        if (claims.get<bool>() != payload.get<bool>())
        {
            LOG_TRACE(getLogger("JWKSAuthentication"), "payload does not match the value in the '{}' assertions. Expected '{}' but given '{}'", path, claims.get<bool>(), payload.get<bool>());
            return false;
        }
        return true;
    }
    if (claims.is<double>())
    {
        if (!payload.is<double>())
        {
            LOG_TRACE(getLogger("JWKSAuthentication"), "payload does not match key type 'double' in claims '{}'", path);
            return false;
        }
        if (claims.get<double>() != payload.get<double>())
        {
            LOG_TRACE(getLogger("JWKSAuthentication"), "payload does not match the value in the '{}' assertions. Expected '{}' but given '{}'", path, claims.get<double>(), payload.get<double>());
            return false;
        }
        return true;
    }
    if (claims.is<std::string>())
    {
        if (!payload.is<std::string>())
        {
            LOG_TRACE(getLogger("JWKSAuthentication"), "payload does not match key type 'std::string' in claims '{}'", path);
            return false;
        }
        if (claims.get<std::string>() != payload.get<std::string>())
        {
            LOG_TRACE(getLogger("JWKSAuthentication"), "payload does not match the value in the '{}' assertions. Expected '{}' but given '{}'", path, claims.get<std::string>(), payload.get<std::string>());
            return false;
        }
        return true;
    }
    #ifdef PICOJSON_USE_INT64
    if (claims.is<int64_t>())
    {
        if (!payload.is<int64_t>())
        {
            LOG_TRACE(getLogger("JWKSAuthentication"), "payload does not match key type 'int64_t' in claims '{}'", path);
            return false;
        }
        if (claims.get<int64_t>() != payload.get<int64_t>())
        {
            LOG_TRACE(getLogger("JWKSAuthentication"), "payload does not match the value in claims '{}'. Expected '{}' but given '{}'", path, claims.get<int64_t>(), payload.get<int64_t>());
            return false;
        }
        return true;
    }
    #endif
    LOG_ERROR(getLogger("JWKSAuthentication"), "'{}' claims do not match any known type", path);
    return false;
}

bool check_claims(const String &claims, const picojson::value::object &payload)
{
    if (claims.empty())
        return true;
    picojson::value json;
    auto errors = picojson::parse(json, claims);
    if (!errors.empty())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Bad JWT claims: {}", errors);
    if (!json.is<picojson::object>())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bad JWT claims: is not an object");
    return check_claims(json.get<picojson::value::object>(), payload, "");
}
}

JWKSClient::JWKSClient(const JWKSAuthClientParams & params_)
    : HTTPAuthClient<JWKSResponseParser>(params_)
    , m_refresh_ms(params_.refresh_ms)
{
}

JWKSClient::~JWKSClient() = default;

bool JWKSClient::verify(const String &claims, const String &token)
{
    auto decoded_jwt = jwt::decode(token);
    auto jwk = getJWKS().get_jwk(decoded_jwt.get_key_id());

    auto issuer = decoded_jwt.get_issuer();
    auto x5c = jwk.get_x5c_key_value();
    auto subject = decoded_jwt.get_subject();

    try
    {
        if (!x5c.empty() && !issuer.empty())
        {
            LOG_TRACE(getLogger("JWKSAuthentication"), "Verifying {} with 'x5c' key", subject);

            auto verifier =
                jwt::verify()
                    .allow_algorithm(jwt::algorithm::rs256(jwt::helper::convert_base64_der_to_pem(x5c), "", "", ""))
                    .leeway(60UL); // value in seconds, add some to compensate timeout

            verifier.verify(decoded_jwt);
            return check_claims(claims, decoded_jwt.get_payload_json());
        }

        LOG_TRACE(getLogger("JWKSAuthentication"), "Verifying {} with RSA components", subject);
        const auto modulus = jwk.get_jwk_claim("n").as_string();
        const auto exponent = jwk.get_jwk_claim("e").as_string();
        auto verifier = jwt::verify()
                            .allow_algorithm(jwt::algorithm::rs256(
                                jwt::helper::create_public_key_from_rsa_components(modulus, exponent)))
                            .leeway(60UL); // value in seconds, add some to compensate timeout

        verifier.verify(decoded_jwt);
        return check_claims(claims, decoded_jwt.get_payload_json());
    }
    catch (...)
    {
        tryLogCurrentException(getLogger("JWKSAuthentication"), "Failed to validate jwt");
    }
    return false;
}

jwt::jwks<jwt::traits::kazuho_picojson> JWKSClient::getJWKS()
{
    {
        std::shared_lock lock(m_update_mutex);
        auto now = std::chrono::high_resolution_clock::now();
        auto diff =  std::chrono::duration<double, std::milli>(now - m_last_request_send).count();
        if (diff < m_refresh_ms)
        {
            jwt::jwks<jwt::traits::kazuho_picojson> result(m_jwks);
            return result;
        }
    }
    std::unique_lock lock(m_update_mutex);
    auto now = std::chrono::high_resolution_clock::now();
    auto diff =  std::chrono::duration<double, std::milli>(now - m_last_request_send).count();
    if (diff < m_refresh_ms)
    {
        jwt::jwks<jwt::traits::kazuho_picojson> result(m_jwks);
        return result;
    }
    Poco::Net::HTTPRequest request{Poco::Net::HTTPRequest::HTTP_GET, this->getURI().getPathAndQuery()};
    auto result = authenticateRequest(request);
    m_jwks = std::move(result.keys);
    if (result.is_ok)
    {
        m_last_request_send = std::chrono::high_resolution_clock::now();
    }
    jwt::jwks<jwt::traits::kazuho_picojson> results(m_jwks);
    return results;
}

JWKSResponseParser::Result
JWKSResponseParser::parse(const Poco::Net::HTTPResponse & response, std::istream * body_stream) const
{
    Result result;

    if (response.getStatus() != Poco::Net::HTTPResponse::HTTPStatus::HTTP_OK)
        return result;
    result.is_ok = true;

    if (!body_stream)
        return result;

    try
    {
        String response_data;
        Poco::StreamCopier::copyToString(*body_stream, response_data);
        auto keys = jwt::parse_jwks(response_data);
        result.keys = std::move(keys);
    }
    catch (...)
    {
        LOG_INFO(getLogger("JWKSAuthentication"), "Failed to parse jwks from authentication response. Skip it.");
    }
    return result;
}

}
