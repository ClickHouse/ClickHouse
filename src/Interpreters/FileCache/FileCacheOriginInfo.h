#pragma once
#include <Core/UUID.h>
#include <Interpreters/FileCache/FileSegmentKeyType.h>
#include <sys/types.h>

#include <functional>
#include <optional>

namespace DB
{

struct FileCacheOriginInfo
{
    using UserID = std::string;
    using Weight = UInt64;
    using SegmentKeyType = FileSegmentKeyType;

    UserID user_id;
    std::optional<Weight> weight = std::nullopt;
    SegmentKeyType segment_type = SegmentKeyType::General;

    FileCacheOriginInfo() = default;

    explicit FileCacheOriginInfo(const UserID & user_id_)
        : user_id(user_id_)
    {
    }

    FileCacheOriginInfo(const UserID & user_id_, const Weight & weight_, SegmentKeyType segment_type_ = SegmentKeyType::General)
        : user_id(user_id_)
        , weight(weight_)
        , segment_type(segment_type_)
    {
    }

    bool operator==(const FileCacheOriginInfo & other) const { return user_id == other.user_id; }
};

/// Identity of a shared origin in the dedup pool. Origins which agree on all of these fields are
/// interchangeable, so a single immutable instance is shared by every key with the same
/// OriginPoolKey. See CacheMetadata::getOrCreateSharedOrigin.
struct OriginPoolKey
{
    FileCacheOriginInfo::UserID user_id;
    std::optional<FileCacheOriginInfo::Weight> weight;
    FileCacheOriginInfo::SegmentKeyType segment_type;

    bool operator==(const OriginPoolKey & other) const = default;
};

}

template <>
struct std::hash<DB::OriginPoolKey>
{
    /// Distinct origins are very few and dominated by the user id, so hashing it alone gives
    /// a good enough spread; equality (which compares all fields) keeps lookups correct.
    size_t operator()(const DB::OriginPoolKey & key) const noexcept
    {
        return std::hash<DB::FileCacheOriginInfo::UserID>{}(key.user_id);
    }
};
