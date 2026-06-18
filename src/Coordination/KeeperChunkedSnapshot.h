#pragma once

#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

namespace DB
{

/// Chunked-ZSTD Keeper snapshot format — constants and front-header/footer helpers.
///
/// Binary layout (append-only, no seek/backpatch):
///
///   [FRONT HEADER (13 bytes): magic "CKFS"(4) + version(1) + chunk_count(8)]  @ offset 0
///   [Chunk 0  METADATA                    ]  (independent ZSTD frame, inner version byte 8)  @ offset 13
///   [Chunk 1..K NODES                     ]  (independent ZSTD frames)
///   [Chunk K+1 SESSIONS                   ]  (independent ZSTD frame)
///   [FOOTER : chunk_count × descriptor(25)]  (the index; at absolute offset = buf_size - footer_size)
///
/// Descriptor (25 bytes, native byte order):
///   uint8_t  chunk_type                    // METADATA=0, NODES=1, SESSIONS=2
///   uint64_t compressed_offset             // absolute byte offset from buffer start (>= 13)
///   uint64_t compressed_size               // byte length of this ZSTD frame
///   uint64_t node_count                    // number of nodes (NODES chunks only; 0 for others)
///
/// FRONT HEADER layout (fixed 13 bytes; written first, before any chunk):
///   char     magic[4]       (= "CKFS")
///   uint8_t  version        (= 8)
///   uint64_t chunk_count
///
/// Since chunk_count is in the front header, footer_offset is derived:
///   footer_size   = 25 * chunk_count
///   footer_offset = buf_size - footer_size
///
/// Size identity (for the append-only writer producing contiguous output):
///   buf_size = 13 + Σ(chunk sizes) + footer_size
///
/// The parser enforces that the last chunk ends exactly at footer_offset (no trailing gap).
/// Internal gaps between chunks are tolerated (monotonic non-overlap is the invariant).

/// Magic bytes that identify chunked snapshots ("CKFS").
static constexpr std::string_view KEEPER_CHUNKED_SNAPSHOT_MAGIC{"CKFS", 4};

/// Version tag stored in the front header.
static constexpr uint8_t KEEPER_CHUNKED_SNAPSHOT_VERSION = 8;

/// Minimum valid chunk_count: METADATA + at least one NODES chunk + SESSIONS.
/// The writer always serializes the root "/" node, so there is always >= 1 NODES chunk.
static constexpr uint64_t KEEPER_CHUNKED_SNAPSHOT_MIN_CHUNK_COUNT = 3;

/// Size in bytes of one chunk descriptor: type[1] + offset[8] + size[8] + node_count[8].
static constexpr size_t KEEPER_CHUNKED_SNAPSHOT_DESCRIPTOR_SIZE = 25;

/// Type of a chunk in the chunked snapshot format.
enum class SnapshotChunkType : uint8_t
{
    METADATA = 0, ///< snapshot_meta, zxid+digest, session_id_counter, ACL map
    NODES    = 1, ///< node_count V7-encoded nodes (node_count is in the footer descriptor, not the body)
    SESSIONS = 2, ///< sessions (sorted) + optional cluster config
};

/// One entry in the chunked snapshot footer's chunk descriptor table.
struct SnapshotChunkDescriptor
{
    SnapshotChunkType type = SnapshotChunkType::METADATA;
    uint64_t    compressed_offset = 0; ///< absolute byte offset from the start of the buffer/file
    uint64_t    compressed_size = 0;   ///< byte length of this independently-compressed ZSTD frame
    uint64_t    node_count = 0;        ///< number of nodes in this chunk (NODES chunks only; 0 for others)
};

/// Fixed front-header size: magic[4] + version[1] + chunk_count[8] = 13 bytes.
constexpr size_t chunkedSnapshotHeaderSize() noexcept { return 4 + 1 + 8; }

/// Footer (descriptor table) byte size for a given chunk count.
constexpr size_t chunkedSnapshotFooterSize(uint64_t chunk_count) noexcept
{
    return KEEPER_CHUNKED_SNAPSHOT_DESCRIPTOR_SIZE * static_cast<size_t>(chunk_count);
}

/// Serialize the fixed 13-byte FRONT HEADER (magic + version + chunk_count) into `buf`.
/// `buf` must point to at least chunkedSnapshotHeaderSize() writable bytes. Native byte order.
void packChunkedSnapshotHeader(uint64_t chunk_count, char * buf) noexcept;

/// Serialize the FOOTER (descriptor table) into `buf`.
/// `buf` must point to at least chunkedSnapshotFooterSize(chunks.size()) writable bytes. Native byte order.
void packChunkedSnapshotFooter(std::span<const SnapshotChunkDescriptor> chunks, char * buf) noexcept;

/// Read & validate the FRONT HEADER (magic/version/chunk_count) and the FOOTER (descriptor table),
/// returning the chunk descriptors. Throws Exception(CORRUPTED_DATA) on structural violations
/// (too small, bad magic, chunk_count < 3, footer doesn't fit, chunk overlaps header/footer, bad
/// ordering, overlap, or a trailing gap before the footer) and Exception(UNKNOWN_FORMAT_VERSION) if the
/// header version byte is not 8. Delegates structural checks to the shared tryParseChunkedSnapshot core.
std::vector<SnapshotChunkDescriptor> parseAndValidateChunkedSnapshot(const char * buf, size_t buf_size);

/// Non-throwing format detection. Returns true iff `buf` is a STRUCTURALLY VALID chunked snapshot:
/// CKFS magic, chunk_count >= MIN, a footer that fits, AND a fully valid descriptor table (first chunk
/// METADATA, last SESSIONS, middle NODES, monotonic non-overlapping offsets within [13, footer_offset),
/// and the last chunk ending exactly at footer_offset). It deliberately does NOT check the version byte
/// — version validation belongs to parseAndValidateChunkedSnapshot (→ UNKNOWN_FORMAT_VERSION), so a
/// future chunked version with a valid footer shape still routes to the parser. Full validation (not
/// just magic+count) is required because a legacy non-ZSTD snapshot is a CompressedWriteBuffer stream
/// whose leading bytes are a CityHash128 checksum (NOT a fixed magic) and can coincidentally start with
/// "CKFS"; only validating the whole descriptor table makes mis-routing a valid legacy snapshot
/// effectively impossible.
bool looksLikeChunkedSnapshot(const char * buf, size_t buf_size) noexcept;

}
