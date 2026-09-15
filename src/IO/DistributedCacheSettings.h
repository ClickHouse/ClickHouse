#pragma once

#include <Core/DistributedCacheDefines.h>
#include <Core/Types.h>
#include <IO/DistributedCacheLogMode.h>
#include <IO/DistributedCachePoolBehaviourOnLimit.h>

namespace DB
{
struct Settings;

struct DistributedCacheSettings
{
    bool throw_on_error = false;
    bool bypass_connection_pool = false;

    size_t wait_connection_from_pool_milliseconds = 100;
    size_t connect_max_tries = ::DistributedCache::DEFAULT_CONNECT_MAX_TRIES;
    size_t connect_backoff_min_ms = ::DistributedCache::DEFAULT_CONNECT_BACKOFF_MIN_MS;
    size_t connect_backoff_max_ms = ::DistributedCache::DEFAULT_CONNECT_BACKOFF_MAX_MS;
    size_t read_request_max_tries = ::DistributedCache::DEFAULT_READ_REQUEST_MAX_TRIES;
    size_t alignment = 0;
    size_t max_unacked_inflight_packets = ::DistributedCache::MAX_UNACKED_INFLIGHT_PACKETS;
    size_t data_packet_ack_window = ::DistributedCache::ACK_DATA_PACKET_WINDOW;
    size_t credentials_refresh_period_seconds = ::DistributedCache::DEFAULT_CREDENTIALS_REFRESH_PERIOD_SECONDS;
    size_t write_through_cache_buffer_size = 0;
    bool read_if_exists_otherwise_bypass = false;

    DistributedCachePoolBehaviourOnLimit pool_behaviour_on_limit = DistributedCachePoolBehaviourOnLimit::ALLOCATE_NEW_BYPASSING_POOL;
    size_t receive_response_wait_milliseconds = 10000;
    size_t receive_timeout_milliseconds = 1000;
    bool discard_connection_if_unread_data = true;
    bool read_only_from_current_az = true;

    size_t min_bytes_for_seek = DBMS_DEFAULT_BUFFER_SIZE;

    DistributedCacheLogMode log_mode = DistributedCacheLogMode::LOG_ON_ERROR;

    bool operator ==(const DistributedCacheSettings &) const = default;

    void load(const Settings & settings);

    void validate() const;
};

}
