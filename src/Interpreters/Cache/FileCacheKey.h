#pragma once
#include <Core/Types.h>

namespace DB
{

struct FileCacheKey
{
    using KeyHash = UInt128;
    KeyHash key;

    std::string toString() const;

    FileCacheKey() = default;

    explicit FileCacheKey(const std::string & path);

    explicit FileCacheKey(const UInt128 & key_);

    static FileCacheKey random();

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
