#pragma once

#include <Core/DistributedCacheProtocol.h>
#include <Core/Types.h>
#include <IO/DistributedCacheLogMode.h>
#include <IO/DistributedCachePoolBehaviourOnLimit.h>

namespace DB
{

struct DistributedCacheSettings
{
    bool throw_on_error = false;
    bool bypass_connection_pool = false;

    size_t wait_connection_from_pool_milliseconds = 100;
    size_t connect_max_tries = 100;
    size_t read_alignment = 0;
    size_t max_unacked_inflight_packets = ::DistributedCache::MAX_UNACKED_INFLIGHT_PACKETS;
    size_t data_packet_ack_window = ::DistributedCache::ACK_DATA_PACKET_WINDOW;

    DistributedCachePoolBehaviourOnLimit pool_behaviour_on_limit = DistributedCachePoolBehaviourOnLimit::ALLOCATE_NEW_BYPASSING_POOL;
    size_t receive_response_wait_milliseconds = 10000;
    size_t receive_timeout_milliseconds = 1000;

    DistributedCacheLogMode log_mode = DistributedCacheLogMode::LOG_ON_ERROR;

    bool operator ==(const DistributedCacheSettings &) const = default;

    void validate() const;
};

}
