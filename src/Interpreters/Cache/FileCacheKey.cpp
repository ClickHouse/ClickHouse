#include "FileCacheKey.h"

#include <base/hex.h>
#include <Common/SipHash.h>
#include <Core/UUID.h>


namespace DB
{

FileCacheKey::FileCacheKey(const std::string & path)
    : key(sipHash128(path.data(), path.size()))
{
}

FileCacheKey::FileCacheKey(const UInt128 & key_)
    : key(key_)
{
}

std::string FileCacheKey::toString() const
{
    return getHexUIntLowercase(key);
}

FileCacheKey FileCacheKey::random()
{
    return FileCacheKey(UUIDHelpers::generateV4().toUnderType());
}

}
