#pragma once

#include "config.h"
#include <string>

#if USE_SSL

namespace DB
{

class KeyPair;

struct JSONWebKey
{
    std::string e;
    std::string n;
    std::string kty;

    std::string toString() const;
    static JSONWebKey fromRSAKey(const KeyPair &);
};

}
#endif
