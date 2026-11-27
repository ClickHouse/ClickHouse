#pragma once
#include <Core/Defines.h>

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
static constexpr auto DEFAULT_CONNECT_MAX_TRIES = 5;
static constexpr auto DEFAULT_READ_REQUEST_MAX_TRIES = 10;
static constexpr auto DEFAULT_CREDENTIALS_REFRESH_PERIOD_SECONDS = 5;
static constexpr auto DEFAULT_CONNECT_BACKOFF_MIN_MS = 0;
static constexpr auto DEFAULT_CONNECT_BACKOFF_MAX_MS = 50;

static constexpr auto INITIAL_PROTOCOL_VERSION = 0;
static constexpr auto PROTOCOL_VERSION_WITH_QUERY_ID = 1;
static constexpr auto PROTOCOL_VERSION_WITH_MAX_INFLIGHT_PACKETS = 2;
static constexpr auto PROTOCOL_VERSION_WITH_GCS_TOKEN = 3;
static constexpr auto PROTOCOL_VERSION_WITH_AZURE_AUTH = 4;
static constexpr auto PROTOCOL_VERSION_WITH_TEMPORATY_DATA = 5;
static constexpr auto PROTOCOL_VERSION_WITH_READ_RANGE_ID = 6;
static constexpr auto PROTOCOL_VERSION_WITH_CREDENTIALS_REFRESH = 7;
static constexpr auto PROTOCOL_VERSION_WITH_CACHE_STATS_DATA_PACKET = 8;
static constexpr auto PROTOCOL_VERSION_WITH_STORAGE_INFO_IN_CACHE_KEY = 9;
static constexpr auto PROTOCOL_VERSION_WITH_THROTTLING_SETTINGS = 10;

static constexpr UInt32 CURRENT_PROTOCOL_VERSION = PROTOCOL_VERSION_WITH_THROTTLING_SETTINGS;

}
