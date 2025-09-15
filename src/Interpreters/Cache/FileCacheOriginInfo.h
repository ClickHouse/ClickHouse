#pragma once
#include <Core/UUID.h>
#include <Interpreters/Cache/FileSegmentKeyType.h>
#include <sys/types.h>

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

}
