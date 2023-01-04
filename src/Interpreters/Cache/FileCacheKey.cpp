#include "FileCacheKey.h"

#include <Common/hex.h>
#include <Common/SipHash.h>


namespace DB
{

FileCacheKey::FileCacheKey(const std::string & path)
    : key(sipHash128(path.data(), path.size()))
    , key_prefix(toString().substr(0, 3))
{
}

FileCacheKey::FileCacheKey(const UInt128 & key_)
    : key(key_)
    , key_prefix(toString().substr(0, 3))
{
}

std::string FileCacheKey::toString() const
{
    return getHexUIntLowercase(key);
}

}
