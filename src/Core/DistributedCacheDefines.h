#pragma once
#include <Core/Types.h>

namespace DistributedCache
{

static constexpr auto SERVER_CONFIG_PREFIX = "distributed_cache_server";
static constexpr auto CLIENT_CONFIG_PREFIX = "distributed_cache_client";
static constexpr auto REGISTERED_SERVERS_PATH = "registry";
static constexpr auto OFFSET_ALIGNMENT_PATH = "offset_alignment";
static constexpr auto DEFAULT_ZOOKEEPER_PATH = "/distributed_cache/";
static constexpr auto MAX_VIRTUAL_NODES = 100;
static constexpr auto DEFAULT_OFFSET_ALIGNMENT = 16 * 1024 * 1024;
static constexpr auto DEFAULT_MAX_PACKET_SIZE = DB::DBMS_DEFAULT_BUFFER_SIZE;
static constexpr auto MAX_UNACKED_INFLIGHT_PACKETS = 10;
static constexpr auto ACK_DATA_PACKET_WINDOW = 5;
static constexpr auto DEFAULT_CONNECTION_POOL_SIZE = 15000;
static constexpr auto DEFAULT_CONNECTION_TTL_SEC = 200;

}
