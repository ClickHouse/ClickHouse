#include "FileCacheKey.h"

#include <base/hex.h>
#include <Common/SipHash.h>
#include <Core/UUID.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
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

FileCacheKey FileCacheKey::fromPath(const std::string & path)
{
    return FileCacheKey(sipHash128(path.data(), path.size()));
}

FileCacheKey FileCacheKey::fromKey(const UInt128 & key)
{
    return FileCacheKey(key);
}

FileCacheKey FileCacheKey::fromKeyString(const std::string & key_str)
{
    if (key_str.size() != 32)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid cache key hex: {}", key_str);
    return FileCacheKey(unhexUInt<UInt128>(key_str.data()));
}

}
