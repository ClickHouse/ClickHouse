#pragma once
#include <Core/Types.h>
#include <fmt/format.h>

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

template <>
struct fmt::formatter<DB::FileCacheKey> : fmt::formatter<std::string>
{
    template <typename FormatCtx>
    auto format(const DB::FileCacheKey & key, FormatCtx & ctx) const
    {
        return fmt::formatter<std::string>::format(key.toString(), ctx);
    }
};
