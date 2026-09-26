#pragma once

#include "config.h"
#include <string>

#if USE_SSL

namespace DB
{

class KeyPair;

/// Represents a JSON Web Key (JWK) for RSA keys.
/// See RFC 7517.
struct JSONWebKey
{
    /// The public exponent of the RSA key.
    std::string e;
    /// The modulus of the RSA key.
    std::string n;
    /// Key type, e.g., "RSA".
    std::string kty;

    std::string toString() const;
    static JSONWebKey fromRSAKey(const KeyPair &);
};

}
#endif
