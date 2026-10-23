#pragma once

#include <base/types.h>
#include <Common/thread_local_rng.h>
#include <Common/Priority.h>
#include <Poco/Net/SocketAddress.h>

#include <string>


namespace zkutil
{

struct ShuffleHost
{
    enum AvailabilityZoneInfo
    {
        SAME = 0,
        UNKNOWN = 1,
        OTHER = 2,
    };

    std::string host;
    bool secure = false;
    UInt8 original_index = 0;
    AvailabilityZoneInfo az_info = UNKNOWN;
    Priority priority;
    UInt64 random = 0;

    /// We should resolve it each time without caching
    mutable std::optional<Poco::Net::SocketAddress> address;

    void randomize()
    {
        random = thread_local_rng();
    }

    static bool compare(const ShuffleHost & lhs, const ShuffleHost & rhs)
    {
        return std::forward_as_tuple(lhs.az_info, lhs.priority, lhs.random)
            < std::forward_as_tuple(rhs.az_info, rhs.priority, rhs.random);
    }
};

using ShuffleHosts = std::vector<ShuffleHost>;

}
