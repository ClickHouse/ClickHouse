#pragma once

#include <Poco/Crypto/RSAKey.h>

namespace DB
{

struct JSONWebKey
{
    std::string e;
    std::string n;
    std::string kty;

    std::string toString() const;
    static JSONWebKey fromRSAKey(const Poco::Crypto::RSAKey &);
};

}
