#pragma once

#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

namespace DB
{

/// V8 chunked-ZSTD Keeper snapshot format — constants and header helpers.
///
/// Binary layout:
///
///   [Header — uncompressed, preallocated placeholder, backpatched after frames]
///     magic[4]         = "CKFS"   (0x43 0x4B 0x46 0x53)  != ZSTD magic, != LZ4 header
///     uint8_t  version = 8
///     uint64_t chunk_count                              (native byte order)
///     per chunk (chunk_count times):
///       uint8_t  chunk_type                            // METADATA=0, NODES=1, SESSIONS=2
///       uint64_t compressed_offset                     // absolute byte offset from buffer start
///       uint64_t compressed_size                       // byte length of this ZSTD frame
///
///   [Frame 0  METADATA]  version, snapshot_meta, zxid+digest (V5+), session_id_counter, ACL map
///   [Frame 1..K NODES ]  each: uint64_t node_count, then node_count × V7-encoded (path + node)
///   [Frame K+1 SESSIONS] sessions (sorted) + optional cluster config (same as legacy tail)
///
/// header_size = 4 + 1 + 8 + chunk_count * (1 + 8 + 8) = 13 + 17 * chunk_count

/// Magic bytes that identify V8 snapshots ("CKFS").
static constexpr std::string_view KEEPER_V8_MAGIC{"CKFS", 4};

/// Version tag stored in byte 4 of the header.
static constexpr uint8_t KEEPER_V8_VERSION = 8;

/// Minimum valid chunk_count: METADATA + at least one NODES frame + SESSIONS.
/// The writer always serializes the root "/" node, so there is always ≥ 1 NODES frame.
static constexpr uint64_t KEEPER_V8_MIN_CHUNK_COUNT = 3;

/// Type of a V8 chunk/frame.
enum class V8ChunkType : uint8_t
{
    METADATA = 0, ///< snapshot_meta, zxid+digest, session_id_counter, ACL map
    NODES    = 1, ///< uint64_t node_count + node_count V7-encoded nodes
    SESSIONS = 2, ///< sessions (sorted) + optional cluster config
};

/// One entry in the V8 header's chunk table.
struct V8FrameDescriptor
{
    V8ChunkType type;
    uint64_t    compressed_offset; ///< absolute byte offset from the start of the buffer/file
    uint64_t    compressed_size;   ///< byte length of this independently-compressed ZSTD frame
};

/// Compute the header byte size for a given number of chunks.
///   header_size = 13 + 17 * chunk_count
constexpr size_t v8HeaderSize(uint64_t chunk_count) noexcept
{
    return 13 + 17 * static_cast<size_t>(chunk_count);
}

/// Write a V8 header into `buf`.
/// `buf` must point to at least v8HeaderSize(frames.size()) writable bytes.
/// The frame descriptors must already contain valid type/offset/size values.
/// Values are stored in native byte order (no byte-swapping is applied).
/// Homogeneous-architecture cluster assumption: all Keeper nodes must share the same
/// CPU architecture, so the binary format is read back with matching native byte order.
void packV8Header(std::span<const V8FrameDescriptor> frames, char * buf) noexcept;

/// Parse a V8 header from `buf` and return the frame descriptors.
/// Throws Exception(CORRUPTED_DATA)     on structural violations (bad magic, wrong size, bad
///                                       offsets, wrong chunk ordering, overlapping frames,
///                                       chunk_count < 3).
/// Throws Exception(UNKNOWN_FORMAT_VERSION) if the version byte is not 8.
///
/// `buf_size` is the total number of bytes in the buffer (header + all frame data).
/// The function validates all frame descriptors against `buf_size` before returning.
std::vector<V8FrameDescriptor> parseAndValidateV8Header(const char * buf, size_t buf_size);

/// Static assertions for layout correctness.
static_assert(v8HeaderSize(0) == 13,  "v8HeaderSize(0) must equal 13 (4+1+8)");
static_assert(v8HeaderSize(1) == 30,  "v8HeaderSize(1) must equal 30 (13+17)");
static_assert(v8HeaderSize(3) == 64,  "v8HeaderSize(3) must equal 64 (13+51)");

} // namespace DB
