#pragma once

#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

namespace DB
{

/// Chunked-ZSTD Keeper snapshot format — constants and header helpers.
///
/// Binary layout:
///
///   [Header — uncompressed, preallocated placeholder, backpatched after chunks]
///     magic[4]         = "CKFS"   (0x43 0x4B 0x46 0x53)  != ZSTD magic, != LZ4 header
///     uint8_t  version = 8
///     uint64_t chunk_count                              (native byte order)
///     per chunk (chunk_count times):
///       uint8_t  chunk_type                            // METADATA=0, NODES=1, SESSIONS=2
///       uint64_t compressed_offset                     // absolute byte offset from buffer start
///       uint64_t compressed_size                       // byte length of this ZSTD frame
///       uint64_t node_count                            // number of nodes (NODES chunks only; 0 for others)
///
///   [Chunk 0  METADATA]  version, snapshot_meta, zxid+digest (V5+), session_id_counter, ACL map
///   [Chunk 1..K NODES ]  each: node_count × V7-encoded (path + node)   [NO in-body node_count prefix]
///   [Chunk K+1 SESSIONS] sessions (sorted) + optional cluster config (same as legacy tail)
///
/// header_size = 4 + 1 + 8 + chunk_count * (1 + 8 + 8 + 8) = 13 + 25 * chunk_count

/// Magic bytes that identify chunked snapshots ("CKFS").
static constexpr std::string_view KEEPER_CHUNKED_SNAPSHOT_MAGIC{"CKFS", 4};

/// Version tag stored in byte 4 of the header.
static constexpr uint8_t KEEPER_CHUNKED_SNAPSHOT_VERSION = 8;

/// Minimum valid chunk_count: METADATA + at least one NODES chunk + SESSIONS.
/// The writer always serializes the root "/" node, so there is always >= 1 NODES chunk.
static constexpr uint64_t KEEPER_CHUNKED_SNAPSHOT_MIN_CHUNK_COUNT = 3;

/// Type of a chunk in the chunked snapshot format.
enum class SnapshotChunkType : uint8_t
{
    METADATA = 0, ///< snapshot_meta, zxid+digest, session_id_counter, ACL map
    NODES    = 1, ///< node_count V7-encoded nodes (node_count is in the header descriptor, not the body)
    SESSIONS = 2, ///< sessions (sorted) + optional cluster config
};

/// One entry in the chunked snapshot header's chunk table.
struct SnapshotChunkDescriptor
{
    SnapshotChunkType type = SnapshotChunkType::METADATA;
    uint64_t    compressed_offset = 0; ///< absolute byte offset from the start of the buffer/file
    uint64_t    compressed_size = 0;   ///< byte length of this independently-compressed ZSTD frame
    uint64_t    node_count = 0;        ///< number of nodes in this chunk (NODES chunks only; 0 for others)
};

/// Compute the header byte size for a given number of chunks.
///   header_size = 13 + 25 * chunk_count
constexpr size_t chunkedSnapshotHeaderSize(uint64_t chunk_count) noexcept
{
    return 13 + 25 * static_cast<size_t>(chunk_count);
}

/// Write a chunked snapshot header into `buf`.
/// `buf` must point to at least chunkedSnapshotHeaderSize(chunks.size()) writable bytes.
/// The chunk descriptors must already contain valid type/offset/size values.
/// Values are stored in native byte order (no byte-swapping is applied).
void packChunkedSnapshotHeader(std::span<const SnapshotChunkDescriptor> chunks, char * buf) noexcept;

/// Parse a chunked snapshot header from `buf` and return the chunk descriptors.
/// Throws Exception(CORRUPTED_DATA)     on structural violations (bad magic, wrong size, bad
///                                       offsets, wrong chunk ordering, overlapping chunks,
///                                       chunk_count < 3).
/// Throws Exception(UNKNOWN_FORMAT_VERSION) if the version byte is not 8.
///
/// `buf_size` is the total number of bytes in the buffer (header + all chunk data).
/// The function validates all chunk descriptors against `buf_size` before returning.
std::vector<SnapshotChunkDescriptor> parseAndValidateChunkedSnapshotHeader(const char * buf, size_t buf_size);

} // namespace DB
