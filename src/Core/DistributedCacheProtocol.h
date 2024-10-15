#pragma once


#include <base/types.h>
#include <Core/Defines.h>

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

static constexpr auto INITIAL_PROTOCOL_VERSION = 0;
static constexpr auto PROTOCOL_VERSION_WITH_QUERY_ID = 1;
static constexpr auto PROTOCOL_VERSION_WITH_MAX_INFLIGHT_PACKETS = 2;
static constexpr auto PROTOCOL_VERSION_WITH_GCS_TOKEN = 3;
static constexpr UInt32 PROTOCOL_VERSION_WITH_AZURE_AUTH = 4;
static constexpr UInt32 PROTOCOL_VERSION_WITH_TEMPORATY_DATA = 5;

static constexpr UInt32 CURRENT_PROTOCOL_VERSION = PROTOCOL_VERSION_WITH_TEMPORATY_DATA;

namespace Protocol
{

static constexpr auto MIN_VERSION_WITH_QUERY_ID_IN_REQUEST = 1;

/**
 * Distributed cache protocol.
 *
 * Read request:
 *   Step1: (Client) calculate aligned_offset = aligned(file_offset) - alignment to file_offset.
 *                   The alignment is equal to `offset_alignment`
 *                   (stored on zookeeper for shared access from server and client),
 *                   which allows to guarantee if the client needs offset x,
 *                   then it will go to the server which contains a covering
 *                   file segment for this offset.
 *   Step2: (Client) calculate hash(x, remote_path, aligned_file_offset) -> h,
 *   Step3: (Client) find distributed cache server: hash_ring(h) -> s
 *   Step4: (Client) connect to s:
 *     Client: `Hello` packet (protocol_version, request_type)
 *     Server: `Hello` packet (mutual_protocol_version)
 *   Step5: send general info:
 *     Client: `ReadInfo` packet (object storage connection info, remote paths, start offset, end offset)
 *   Step6:
 *     Server: `ReadRange` packet (includes read range), and send the data.
 *     Client: `Ok` packet
 *     in case of error (Client): `EndRequest` packet.
 *   Step7:
 *     Client: do Step1 from current file offset and get aligned_offset'.
 *             If aligned_offset' == aligned_offset, do Step6 again.
 *             else: go to Step2
 *
 * Write request:
 *   Step1: (Client) calculate hash(x, remote_path, file_offset) -> h,
 *   Step2: (Client) find distributed cache server: hash_ring(h) -> s
 *   Step3: (Client) connect to s:
 *     Client: `Hello` packet (protocol_version, request_type)
 *     Server: `Hello` packet (mutual_protocol_version)
 *   Step4: send general info:
 *     Client: `WriteInfo` packet (object storage connection info, remote_path, write range)
 *   Step5: write one file_segment's range
 *     Client: `WriteRange` packet (file_segment_start_offset), then process the write.
 *     Server: `Ok` (after each `Data` packet)
 *              or `Stop` packet (on error).
 *   Step6:
 *     if eof: do Step8
 *     else: do Step7
 *   Step7:
 *     do step1: h' = hash(x, remote_path, file_offset'), where file_offset' - start of the next file segment
 *     do step2: s' = hash_ring(h')
 *     if s' == s: do Step5
 *     else: do Step8 and go to Step3
 *   Step8:
 *     Client: `EndRequest` packet
 *     Server: `Ok` packet
 */

enum RequestType
{
    Min = 0,
    Read = 1, /// read-through cache
    Write = 2, /// write-through cache
    Remove = 3, /// drop cache
    Show = 4, /// get current cache state
    CurrentMetrics = 5, /// get CurrentMetrics
    ProfileEvents = 6, /// get ProfileEvents
    Max = 8,
};

namespace Client
{
    enum Enum
    {
        Min = 0,

        /// A hello packet for handshake between client and server.
        Hello = 1,
        /// A packet to start a new request: Read, Write, Remove, Show, etc
        StartRequest = 2,
        /// A packet to identify that the request is finished.
        /// E.g. for read request we no longer need receiving data (even if requested read range is not finished);
        /// for write request no data will no longer be sent.
        EndRequest = 3,
        /// A request to continue already started request but with a new information.
        /// E.g. for read request - a new read range is needed;
        /// for write request - a new write range will be sent.
        ContinueRequest = 4,
        /// Acknowledgement of `data_packet_ack_window` processed `DataPacket` packets.
        AckRequest = 5,

        Max = 6,
    };
}

namespace Server
{
    enum Enum
    {
        Min = 0,

        /// A hello packet for handshake between client and server.
        Hello = 1,
        /// Identifies that a request was successfully executed.
        Ok = 2,
        /// Identifies a packet containing an exception message happened on server's size.
        Error = 3,
        /// Identifies a packet for a Read request.
        ReadResult = 4,
        /// Identifies a packet for incremental ProfileEvents during Read or Write request.
        ProfileCounters = 5,
        /// Identifies a packet for a Show request.
        ShowResult = 6,
        /// Identifies a packet for a ProfileEvents request.
        ProfileEvents = 7,
        /// Identifies a packet for a Metrics request.
        Metrics = 8,
        /// Identifies that this server cannot receive any more data for Write request
        /// (cache is full or errors during insertion).
        Stop = 9,

        Max = 11
    };
}

}
}
