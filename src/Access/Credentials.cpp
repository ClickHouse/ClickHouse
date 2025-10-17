#include <Access/Credentials.h>
#include <Common/Exception.h>
#include <Poco/Net/HTTPRequest.h>
#include <Common/Crypto/X509Certificate.h>
#include <jwt-cpp/jwt.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

struct JWTClaims
{
    String issuer;
    String subject;
};

jwt::algorithm::rsa chooseRSA(const String & alg, const String & public_key, const String & private_key = {})
{
    if (alg == "RS256")
        return jwt::algorithm::rs256(public_key, private_key, {}, {});
    else if (alg == "RS384")
        return jwt::algorithm::rs384(public_key, private_key, {}, {});
    else if (alg == "RS512")
        return jwt::algorithm::rs512(public_key, private_key, {}, {});

    throw std::runtime_error("unknown RSA alg: '" + alg + "'");
}

String jwtGetUser(const String & jwt_str)
{
    auto decoded_jwt = jwt::decode(jwt_str);
    return decoded_jwt.get_subject();
}

bool jwtVerify(const String & jwt_str, const String & jwks_str)
{
    auto decoded_jwt = jwt::decode(jwt_str);
    auto jwks = jwt::parse_jwks(jwks_str);
    auto jwk = jwks.get_jwk(decoded_jwt.get_key_id());

    if (!decoded_jwt.has_issuer())
        throw std::runtime_error("no issuer in JWT");
    if (!decoded_jwt.has_subject())
        throw std::runtime_error("no subject in JWT");

    const auto issuer = decoded_jwt.get_issuer();

    if (jwk.get_key_type() == "RSA")
    {
        String public_key_pem;
        if (jwk.has_x5c())
        {
            public_key_pem = jwt::helper::convert_base64_der_to_pem(jwk.get_x5c_key_value());
        }
        else if (jwk.has_jwk_claim("n") && jwk.has_jwk_claim("e"))
        {
            const auto modulus = jwk.get_jwk_claim("n").as_string();
            const auto exponent = jwk.get_jwk_claim("e").as_string();
            public_key_pem = jwt::helper::create_public_key_from_rsa_components(modulus, exponent);
        }
        else
            throw std::runtime_error("no 'x5c' and missing 'n', 'e' claims in JWK for RSA");

        auto verifier = jwt::verify().allow_algorithm(chooseRSA(decoded_jwt.get_algorithm(), public_key_pem)).with_issuer(issuer);
        verifier.verify(decoded_jwt);
    }
    else
        throw std::runtime_error("not supported algorithm '" + decoded_jwt.get_algorithm() + "'. Only RSA is supported yet.");

    return true;
}

}


Credentials::Credentials(const String & user_name_)
    : user_name(user_name_)
{
}

const String & Credentials::getUserName() const
{
    if (!isReady())
        throwNotReady();
    return user_name;
}

bool Credentials::isReady() const
{
    return is_ready;
}

void Credentials::throwNotReady()
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Credentials are not ready");
}

AlwaysAllowCredentials::AlwaysAllowCredentials()
{
    is_ready = true;
}

AlwaysAllowCredentials::AlwaysAllowCredentials(const String & user_name_)
    : Credentials(user_name_)
{
    is_ready = true;
}

void AlwaysAllowCredentials::setUserName(const String & user_name_)
{
    user_name = user_name_;
}

#if USE_SSL
SSLCertificateCredentials::SSLCertificateCredentials(const String & user_name_, X509Certificate::Subjects && subjects_)
    : Credentials(user_name_)
    , certificate_subjects(subjects_)
{
    is_ready = true;
}

const X509Certificate::Subjects & SSLCertificateCredentials::getSSLCertificateSubjects() const
{
    if (!isReady())
        throwNotReady();
    return certificate_subjects;
}
#endif

BasicCredentials::BasicCredentials()
{
    is_ready = true;
}

BasicCredentials::BasicCredentials(const String & user_name_)
    : Credentials(user_name_)
{
    is_ready = true;
}

BasicCredentials::BasicCredentials(const String & user_name_, const String & password_)
    : Credentials(user_name_)
    , password(password_)
{
    is_ready = true;
}

void BasicCredentials::setUserName(const String & user_name_)
{
    user_name = user_name_;
}

void BasicCredentials::setPassword(const String & password_)
{
    password = password_;
}

const String & BasicCredentials::getPassword() const
{
    if (!isReady())
        throwNotReady();
    return password;
}

JWTCredentials::JWTCredentials(const String & token_)
    : token(token_)
{
    user_name = jwtGetUser(token);
    is_ready = true; // FIXME
}

JWTCredentials::JWTCredentials(const String & token_, const String & jwks_)
    : token(token_)
    , jwks(jwks_)
{
    user_name = jwtGetUser(token);
    is_ready = true; // FIXME
}

bool JWTCredentials::verify()
{
    if (token.empty())
        return false;

    if (jwks.empty())
        return false;

    return jwtVerify(token, jwks);
}

}
