#pragma once

#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

namespace DB
{

class SeekableReadBuffer;
class WriteBuffer;

/// Append-only chunked ZSTD Keeper snapshot format.
///
/// Layout: FRONT HEADER (13 B) | Chunk 0 METADATA | Chunk 1..K NODES | FOOTER (25*K B)
///
/// Front header: magic "CKFS"[4] + version[1] + chunk_count[8]
/// Descriptor  : chunk_type[1] + compressed_offset[8] + compressed_size[8] + node_count[8]
/// Footer is placed at buf_size - 25*chunk_count; the last chunk ends exactly there.

static constexpr std::string_view KEEPER_CHUNKED_SNAPSHOT_MAGIC{"CKFS", 4};
static constexpr uint8_t KEEPER_CHUNKED_SNAPSHOT_VERSION = 8;
static constexpr uint64_t KEEPER_CHUNKED_SNAPSHOT_MIN_CHUNK_COUNT = 2; ///< METADATA + ≥1 NODES
static constexpr size_t KEEPER_CHUNKED_SNAPSHOT_DESCRIPTOR_SIZE = 25;

enum class SnapshotChunkType : uint8_t
{
    METADATA = 0, ///< zxid/digest, session counter, ACL map, sessions, cluster config
    NODES = 1,
};

struct SnapshotChunkDescriptor
{
    SnapshotChunkType type = SnapshotChunkType::METADATA;
    uint64_t compressed_offset = 0;
    uint64_t compressed_size = 0;
    uint64_t node_count = 0; ///< number of nodes (NODES chunks only)
};

/// magic[4] + version[1] + chunk_count[8]
constexpr size_t chunkedSnapshotHeaderSize() noexcept
{
    return 4 + 1 + 8;
}
constexpr size_t chunkedSnapshotFooterSize(uint64_t chunk_count) noexcept
{
    return KEEPER_CHUNKED_SNAPSHOT_DESCRIPTOR_SIZE * static_cast<size_t>(chunk_count);
}

void packChunkedSnapshotHeader(uint64_t chunk_count, WriteBuffer & out);
void packChunkedSnapshotFooter(std::span<const SnapshotChunkDescriptor> chunks, WriteBuffer & out);

/// Validates the front header and footer, returns the chunk descriptors.
/// Throws CORRUPTED_DATA or UNKNOWN_FORMAT_VERSION on structural violations.
std::vector<SnapshotChunkDescriptor> parseAndValidateChunkedSnapshot(SeekableReadBuffer & in);

/// Non-throwing format probe. Returns true if the buffer is a structurally valid chunked snapshot.
/// Does not check the version byte — version validation is left to parseAndValidateChunkedSnapshot.
/// Full descriptor-table validation (not just magic) is required because a legacy snapshot's leading
/// CityHash128 bytes can coincidentally match "CKFS".
bool isChunkedSnapshot(SeekableReadBuffer & in) noexcept;

}
