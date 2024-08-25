#include <Common/JSONWebKey.h>

#include <fmt/format.h>
#include <Poco/Crypto/RSAKey.h>
#include <Common/Base64.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

std::string JSONWebKey::toString() const
{
    if (e.empty() || n.empty() || kty.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "JSONWebKey: e, n, kty must be set");

    return fmt::format(R"({{"e":"{}","kty":"{}","n":"{}"}})", e, kty, n);
}

JSONWebKey JSONWebKey::fromRSAKey(const Poco::Crypto::RSAKey & key)
{
    auto e = key.encryptionExponent();
    auto n = key.modulus();

    auto e_enc = base64Encode({e.begin(), e.end()}, /*url_encoding*/ true, /*no_padding*/ true);
    auto n_enc = base64Encode({n.begin(), n.end()}, /*url_encoding*/ true, /*no_padding*/ true);

    return JSONWebKey{
        .e = e_enc,
        .n = n_enc,
        .kty = "RSA",
    };
}

}
