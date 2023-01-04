#pragma once
#include <Core/Types.h>

namespace DB
{

struct FileCacheKey
{
    /// Hash of the path.
    UInt128 key;
    /// Prefix of the path.
    std::string key_prefix;

    std::string toString() const;

    explicit FileCacheKey(const std::string & path);

    explicit FileCacheKey(const UInt128 & path);

    bool operator==(const FileCacheKey & other) const { return key == other.key; }
};

using FileCacheKeyAndOffset = std::pair<FileCacheKey, size_t>;
struct FileCacheKeyAndOffsetHash
{
    std::size_t operator()(const FileCacheKeyAndOffset & key) const
    {
        return std::hash<UInt128>()(key.first.key) ^ std::hash<UInt64>()(key.second);
    }
};

}

namespace std
{
template <>
struct hash<DB::FileCacheKey>
{
    std::size_t operator()(const DB::FileCacheKey & k) const { return hash<UInt128>()(k.key); }
};

}
