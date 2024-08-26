#include "JWTVerifier.h"

#include <exception>
#include <fstream>
#include <map>
#include <utility>

#include <absl/strings/match.h>
#include <jwt-cpp/jwt.h>
#include <jwt-cpp/traits/kazuho-picojson/traits.h>
#include <picojson/picojson.h>
#include "Poco/StreamCopier.h"
#include <Poco/String.h>

#include "Common/Base64.h"
#include "Common/Exception.h"
#include "Common/logger_useful.h"
#include <Common/SettingsChanges.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int JWT_ERROR;
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
            LOG_TRACE(getLogger("JWTAuthentication"), "Key '{}.{}' not found in JWT payload", path, it.first);
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
        LOG_TRACE(getLogger("JWTAuthentication"), "JWT payload too small for claims key '{}'", path);
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
            LOG_TRACE(getLogger("JWTAuthentication"), "JWT payload does not contain an object matching claims key '{}[{}]'", path, claims_i);
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
            LOG_TRACE(getLogger("JWTAuthentication"), "JWT payload does not match key type 'array' in claims '{}'", path);
            return false;
        }
        return check_claims(claims.get<picojson::array>(), payload.get<picojson::array>(), path);
    }
    if (claims.is<picojson::object>())
    {
        if (!payload.is<picojson::object>())
        {
            LOG_TRACE(getLogger("JWTAuthentication"), "JWT payload does not match key type 'object' in claims '{}'", path);
            return false;
        }
        return check_claims(claims.get<picojson::object>(), payload.get<picojson::object>(), path);
    }
    if (claims.is<bool>())
    {
        if (!payload.is<bool>())
        {
            LOG_TRACE(getLogger("JWTAuthentication"), "JWT payload does not match key type 'bool' in claims '{}'", path);
            return false;
        }
        if (claims.get<bool>() != payload.get<bool>())
        {
            LOG_TRACE(getLogger("JWTAuthentication"), "JWT payload does not match the value in the '{}' assertions. Expected '{}' but given '{}'", path, claims.get<bool>(), payload.get<bool>());
            return false;
        }
        return true;
    }
    if (claims.is<double>())
    {
        if (!payload.is<double>())
        {
            LOG_TRACE(getLogger("JWTAuthentication"), "JWT payload does not match key type 'double' in claims '{}'", path);
            return false;
        }
        if (claims.get<double>() != payload.get<double>())
        {
            LOG_TRACE(getLogger("JWTAuthentication"), "JWT payload does not match the value in the '{}' assertions. Expected '{}' but given '{}'", path, claims.get<double>(), payload.get<double>());
            return false;
        }
        return true;
    }
    if (claims.is<std::string>())
    {
        if (!payload.is<std::string>())
        {
            LOG_TRACE(getLogger("JWTAuthentication"), "JWT payload does not match key type 'std::string' in claims '{}'", path);
            return false;
        }
        if (claims.get<std::string>() != payload.get<std::string>())
        {
            LOG_TRACE(getLogger("JWTAuthentication"), "JWT payload does not match the value in the '{}' assertions. Expected '{}' but given '{}'", path, claims.get<std::string>(), payload.get<std::string>());
            return false;
        }
        return true;
    }
    #ifdef PICOJSON_USE_INT64
    if (claims.is<int64_t>())
    {
        if (!payload.is<int64_t>())
        {
            LOG_TRACE(getLogger("JWTAuthentication"), "JWT payload does not match key type 'int64_t' in claims '{}'", path);
            return false;
        }
        if (claims.get<int64_t>() != payload.get<int64_t>())
        {
            LOG_TRACE(getLogger("JWTAuthentication"), "JWT payload does not match the value in claims '{}'. Expected '{}' but given '{}'", path, claims.get<int64_t>(), payload.get<int64_t>());
            return false;
        }
        return true;
    }
    #endif
    LOG_ERROR(getLogger("JWTAuthentication"), "JWT claim '{}' does not match any known type", path);
    return false;
}

bool check_claims(const String &claims, const picojson::value::object &payload)
{
    if (claims.empty())
        return true;
    picojson::value json;
    auto errors = picojson::parse(json, claims);
    if (!errors.empty())
        throw Exception(ErrorCodes::JWT_ERROR, "Bad JWT claims: {}", errors);
    if (!json.is<picojson::object>())
        throw Exception(ErrorCodes::JWT_ERROR, "Bad JWT claims: is not an object");
    return check_claims(json.get<picojson::value::object>(), payload, "");
}

std::map<String, Field> stringify_params(const picojson::value &params, const String &path);

std::map<String, Field> stringify_params(const picojson::value::array &params, const String &path)
{
    std::map<String, Field> result;
    for (size_t i = 0; i < params.size(); ++i)
    {
        const auto tmp_result = stringify_params(params.at(i), path + "[" + std::to_string(i) + "]");
        result.insert(tmp_result.begin(), tmp_result.end());
    }
    return result;
}

std::map<String, Field> stringify_params(const picojson::value::object &params, const String &path)
{
    auto add_path = String(path);
    if (!add_path.empty())
        add_path = add_path + ".";
    std::map<String, Field> result;
    for (const auto &it : params)
    {
        const auto tmp_result = stringify_params(it.second, add_path + it.first);
        result.insert(tmp_result.begin(), tmp_result.end());
    }
    return result;
}

std::map<String, Field> stringify_params(const picojson::value &params, const String &path)
{
    std::map<String, Field> result;
    if (params.is<picojson::array>())
        return stringify_params(params.get<picojson::array>(), path);
    if (params.is<picojson::object>())
        return stringify_params(params.get<picojson::object>(), path);
    if (params.is<bool>())
    {
        result[path] = Field(params.get<bool>());
        return result;
    }
    if (params.is<std::string>())
    {
        result[path] = Field(params.get<std::string>());
        return result;
    }
    if (params.is<double>())
    {
        result[path] = Field(params.get<double>());
        return result;
    }
    #ifdef PICOJSON_USE_INT64
    if (params.is<int64_t>())
    {
        result[path] = Field(params.get<int64_t>());
        return result;
    }
    #endif
    return result;
}
}

void IJWTVerifier::init(const JWTVerifierParams &_params)
{
    params = _params;
}

bool IJWTVerifier::verify(const String &claims, const String &token, SettingsChanges & settings) const
{
//    try
//    {
        auto decoded_jwt = jwt::decode(token);
        if (!verify_impl(decoded_jwt))
            return false;
        if (!check_claims(claims, decoded_jwt.get_payload_json()))
            return false;
        if (params.settings_key.empty())
            return true;
        const auto &payload_obj = decoded_jwt.get_payload_json();
        const auto &payload_settings = payload_obj.at(params.settings_key);
        const auto string_settings = stringify_params(payload_settings, "");
        for (const auto &it : string_settings)
            settings.insertSetting(it.first, it.second);
        return true;
//    }
//    catch (const std::exception &ex)
//    {
//        throw Exception(ErrorCodes::JWT_ERROR, "{}: Failed to validate JWT with exception {}", name, ex.what());
//    }
}

void SimpleJWTVerifierParams::validate() const
{
    auto lower_algo = Poco::toLower(algo);
    if (lower_algo == "none")
        return;
    if (algo == "ps256"   ||
        algo == "ps384"   ||
        algo == "ps512"   ||
        algo == "ed25519" ||
        algo == "ed448"   ||
        algo == "rs256"   ||
        algo == "rs384"   ||
        algo == "rs512"   ||
        algo == "es256"   ||
        algo == "es256k"  ||
        algo == "es384"   ||
        algo == "es512"   )
    {
        if (!public_key.empty())
            return;
        throw Exception(ErrorCodes::JWT_ERROR, "`public_key` parameter required for {}", algo);
    }
    if (algo == "hs256"   ||
        algo == "hs384"   ||
        algo == "hs512"   )
    {
        if (!single_key.empty())
            return;
        throw DB::Exception(ErrorCodes::JWT_ERROR, "`single_key` parameter required for {}", algo);
    }
    throw DB::Exception(ErrorCodes::JWT_ERROR, "Unknown algorithm {}", algo);
}

SimpleJWTVerifier::SimpleJWTVerifier(const String & _name)
    : IJWTVerifier(_name)
    , verifier(jwt::verify())
{}

void SimpleJWTVerifier::init(const SimpleJWTVerifierParams & _params)
{
    auto algo = Poco::toLower(_params.algo);

    IJWTVerifier::init(_params);
    verifier = jwt::verify();
    if (algo == "none")
        verifier = verifier.allow_algorithm(jwt::algorithm::none());
    else if (algo == "ps256")
        verifier = verifier.allow_algorithm(jwt::algorithm::ps256(_params.public_key, _params.private_key, _params.private_key_password, _params.private_key_password));
    else if (algo == "ps384")
        verifier = verifier.allow_algorithm(jwt::algorithm::ps384(_params.public_key, _params.private_key, _params.private_key_password, _params.private_key_password));
    else if (algo == "ps512")
        verifier = verifier.allow_algorithm(jwt::algorithm::ps512(_params.public_key, _params.private_key, _params.private_key_password, _params.private_key_password));
    else if (algo == "ed25519")
        verifier = verifier.allow_algorithm(jwt::algorithm::ed25519(_params.public_key, _params.private_key, _params.private_key_password, _params.private_key_password));
    else if (algo == "ed448")
        verifier = verifier.allow_algorithm(jwt::algorithm::ed448(_params.public_key, _params.private_key, _params.private_key_password, _params.private_key_password));
    else if (algo == "rs256")
        verifier = verifier.allow_algorithm(jwt::algorithm::rs256(_params.public_key, _params.private_key, _params.private_key_password, _params.private_key_password));
    else if (algo == "rs384")
        verifier = verifier.allow_algorithm(jwt::algorithm::rs384(_params.public_key, _params.private_key, _params.private_key_password, _params.private_key_password));
    else if (algo == "rs512")
        verifier = verifier.allow_algorithm(jwt::algorithm::rs512(_params.public_key, _params.private_key, _params.private_key_password, _params.private_key_password));
    else if (algo == "es256")
        verifier = verifier.allow_algorithm(jwt::algorithm::es256(_params.public_key, _params.private_key, _params.private_key_password, _params.private_key_password));
    else if (algo == "es256k")
        verifier = verifier.allow_algorithm(jwt::algorithm::es256k(_params.public_key, _params.private_key, _params.private_key_password, _params.private_key_password));
    else if (algo == "es384")
        verifier = verifier.allow_algorithm(jwt::algorithm::es384(_params.public_key, _params.private_key, _params.private_key_password, _params.private_key_password));
    else if (algo == "es512")
        verifier = verifier.allow_algorithm(jwt::algorithm::es512(_params.public_key, _params.private_key, _params.private_key_password, _params.private_key_password));
    else if (algo.starts_with("hs"))
    {
        auto key = _params.single_key;
        if (_params.single_key_in_base64)
            key = base64Decode(key);
        if (algo == "hs256")
            verifier = verifier.allow_algorithm(jwt::algorithm::hs256(key));
        else if (algo == "hs384")
            verifier = verifier.allow_algorithm(jwt::algorithm::hs384(key));
        else if (algo == "hs512")
            verifier = verifier.allow_algorithm(jwt::algorithm::hs512(key));
        else
            throw Exception(ErrorCodes::JWT_ERROR, "Unknown algorithm {}", _params.algo);
    }
    else
        throw Exception(ErrorCodes::JWT_ERROR, "Unknown algorithm {}", _params.algo);
}

bool SimpleJWTVerifier::verify_impl(const jwt::decoded_jwt<jwt::traits::kazuho_picojson> &token) const
{
    verifier.verify(token);
    return true;
}

JWKSVerifier::JWKSVerifier(const String & _name, std::shared_ptr<IJWKSProvider> _provider)
    : IJWTVerifier(_name)
    , provider(_provider)
{}

bool JWKSVerifier::verify_impl(const jwt::decoded_jwt<jwt::traits::kazuho_picojson> &token) const
{
    auto jwk = provider->getJWKS().get_jwk(token.get_key_id());
    auto subject = token.get_subject();
    auto algo = Poco::toLower(token.get_algorithm());
    auto verifier = jwt::verify();
    String public_key;

    try
    {
        auto issuer = token.get_issuer();
        auto x5c = jwk.get_x5c_key_value();

        if (!x5c.empty() && !issuer.empty())
        {
            LOG_TRACE(getLogger("JWTAuthentication"), "{}: Verifying {} with 'x5c' key", name, subject);
            public_key = jwt::helper::convert_base64_der_to_pem(x5c);
        }
    }
    catch (const jwt::error::claim_not_present_exception &)
    {
        /// issuer or x5c was not specified, simply do not verify against them
    }
    catch (const std::bad_cast &)
    {
        throw Exception(ErrorCodes::JWT_ERROR, "Invalid claim value type: must be string");
    }

    if (public_key.empty())
    {
        LOG_TRACE(getLogger("JWTAuthentication"), "{}: `issuer` or `x5c` not present, verifying {} with RSA components", name, subject);
        const auto modulus = jwk.get_jwk_claim("n").as_string();
        const auto exponent = jwk.get_jwk_claim("e").as_string();
        public_key = jwt::helper::create_public_key_from_rsa_components(modulus, exponent);
    }

    if (algo == "rs256")
        verifier = verifier.allow_algorithm(jwt::algorithm::rs256(public_key, "", "", ""));
    else if (algo == "rs384")
        verifier = verifier.allow_algorithm(jwt::algorithm::rs384(public_key, "", "", ""));
    else if (algo == "rs512")
        verifier = verifier.allow_algorithm(jwt::algorithm::rs512(public_key, "", "", ""));
    else
        throw Exception(ErrorCodes::JWT_ERROR, "Unknown algorithm {}", algo);
    verifier = verifier.leeway(60UL); // value in seconds, add some to compensate timeout
    verifier.verify(token);
    return true;
}

JWKSClient::JWKSClient(const JWKSAuthClientParams & params_)
    : HTTPAuthClient<JWKSResponseParser>(params_)
    , m_refresh_ms(params_.refresh_ms)
{
}

JWKSClient::~JWKSClient() = default;

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

void StaticJWKSParams::validate() const
{
    if (static_jwks.empty() && static_jwks_file.empty())
        throw Exception(ErrorCodes::JWT_ERROR, "`static_jwks` or `static_jwks_file` keys must be present in configuration");
    if (!static_jwks.empty() && !static_jwks_file.empty())
        throw Exception(ErrorCodes::JWT_ERROR, "`static_jwks` and `static_jwks_file` keys cannot both be present in configuration");
}

void StaticJWKS::init(const StaticJWKSParams& params)
{
    params.validate();
    String content = String(params.static_jwks);
    if (!params.static_jwks_file.empty())
    {
        std::ifstream ifs(params.static_jwks_file);
        content = String((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
    }
    auto keys = jwt::parse_jwks(content);
    jwks = std::move(keys);
}

}
