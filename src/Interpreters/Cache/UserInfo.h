#pragma once
#include <Core/UUID.h>

namespace DB
{

struct FileCacheUserInfo
{
    using UserID = std::string;
    using Weight = UInt64;

    UserID user_id;
    std::optional<Weight> weight = std::nullopt;

    FileCacheUserInfo() = default;

    explicit FileCacheUserInfo(const UserID & user_id_) : user_id(user_id_) {}

    FileCacheUserInfo(const UserID & user_id_, const Weight & weight_) : user_id(user_id_), weight(weight_) {}

    bool operator ==(const FileCacheUserInfo & other) const { return user_id == other.user_id; }
};

}
