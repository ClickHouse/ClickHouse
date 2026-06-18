#include <Coordination/KeeperChunkedSnapshot.h>

#include <Common/Exception.h>

#include <cstring>
#include <fmt/format.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int UNKNOWN_FORMAT_VERSION;
}

void packChunkedSnapshotHeader(std::span<const SnapshotChunkDescriptor> chunks, char * buf) noexcept
{
    // magic[4]
    memcpy(buf, KEEPER_CHUNKED_SNAPSHOT_MAGIC.data(), 4);
    buf += 4;

    // version (1 byte)
    *reinterpret_cast<uint8_t *>(buf) = KEEPER_CHUNKED_SNAPSHOT_VERSION;
    buf += 1;

    // chunk_count (8 bytes, native byte order)
    uint64_t count = static_cast<uint64_t>(chunks.size());
    memcpy(buf, &count, 8);
    buf += 8;

    // Chunk descriptors: each is 17 bytes (1 + 8 + 8)
    for (const auto & cd : chunks)
    {
        *reinterpret_cast<uint8_t *>(buf) = static_cast<uint8_t>(cd.type);
        buf += 1;
        memcpy(buf, &cd.compressed_offset, 8);
        buf += 8;
        memcpy(buf, &cd.compressed_size, 8);
        buf += 8;
    }
}

std::vector<SnapshotChunkDescriptor> parseAndValidateChunkedSnapshotHeader(const char * buf, size_t buf_size)
{
    // Minimum header size check (magic + version + chunk_count = 4+1+8 = 13 bytes).
    if (buf_size < 13)
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "Chunked snapshot header too small: got {} bytes, need at least 13",
            buf_size);

    // Magic check.
    if (memcmp(buf, KEEPER_CHUNKED_SNAPSHOT_MAGIC.data(), 4) != 0)
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "Chunked snapshot has wrong magic bytes");

    // Version check.
    uint8_t version = 0;
    memcpy(&version, buf + 4, 1);
    if (version != KEEPER_CHUNKED_SNAPSHOT_VERSION)
        throw Exception(
            ErrorCodes::UNKNOWN_FORMAT_VERSION,
            "Chunked snapshot has unexpected version {}, expected {}",
            static_cast<unsigned>(version),
            static_cast<unsigned>(KEEPER_CHUNKED_SNAPSHOT_VERSION));

    // chunk_count.
    uint64_t chunk_count = 0;
    memcpy(&chunk_count, buf + 5, 8);

    // Bounds check: chunk_count * 17 descriptors must fit in the remaining bytes.
    // Use division form to avoid overflow: chunk_count <= (buf_size - 13) / 17.
    if (chunk_count > (buf_size - 13) / 17)
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "Chunked snapshot chunk_count {} implies header size {} that exceeds buffer size {}",
            chunk_count,
            chunkedSnapshotHeaderSize(chunk_count),
            buf_size);

    // Minimum chunk count: METADATA + at least one NODES chunk + SESSIONS.
    if (chunk_count < KEEPER_CHUNKED_SNAPSHOT_MIN_CHUNK_COUNT)
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "Chunked snapshot has chunk_count {} < minimum {} "
            "(snapshot must have at least one METADATA chunk, one NODES chunk, and one SESSIONS chunk)",
            chunk_count,
            KEEPER_CHUNKED_SNAPSHOT_MIN_CHUNK_COUNT);

    const size_t header_size = chunkedSnapshotHeaderSize(chunk_count);
    const char * p = buf + 13; // points to the first chunk descriptor

    std::vector<SnapshotChunkDescriptor> chunks;
    chunks.reserve(static_cast<size_t>(chunk_count));

    uint64_t prev_end = 0; // tracks offset + size of the previous chunk for non-overlap check

    for (uint64_t i = 0; i < chunk_count; ++i)
    {
        SnapshotChunkDescriptor cd;

        uint8_t type_raw = 0;
        memcpy(&type_raw, p, 1);
        p += 1;
        memcpy(&cd.compressed_offset, p, 8);
        p += 8;
        memcpy(&cd.compressed_size, p, 8);
        p += 8;

        // Validate chunk type.
        if (type_raw > static_cast<uint8_t>(SnapshotChunkType::SESSIONS))
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Chunked snapshot chunk {} has unknown chunk_type {}",
                i, static_cast<unsigned>(type_raw));
        cd.type = static_cast<SnapshotChunkType>(type_raw);

        // Validate chunk ordering (must be exactly METADATA first, SESSIONS last, NODES in between).
        if (i == 0 && cd.type != SnapshotChunkType::METADATA)
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Chunked snapshot first chunk must be METADATA, got {}",
                static_cast<unsigned>(type_raw));
        if (i == chunk_count - 1 && cd.type != SnapshotChunkType::SESSIONS)
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Chunked snapshot last chunk must be SESSIONS, got {}",
                static_cast<unsigned>(type_raw));
        if (i > 0 && i < chunk_count - 1 && cd.type != SnapshotChunkType::NODES)
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Chunked snapshot middle chunk {} must be NODES, got {}",
                i, static_cast<unsigned>(type_raw));

        // Validate chunk offset: must be at or after the header.
        if (cd.compressed_offset < header_size)
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Chunked snapshot chunk {} has offset {} that overlaps the header (size {})",
                i, cd.compressed_offset, header_size);

        // Validate chunk bounds: [offset, offset+size) must lie within [0, buf_size).
        if (cd.compressed_offset > buf_size || cd.compressed_size > buf_size - cd.compressed_offset)
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Chunked snapshot chunk {} [{}, {}+{}) extends beyond buffer size {}",
                i, cd.compressed_offset, cd.compressed_offset, cd.compressed_size, buf_size);

        // Non-overlap: each chunk must start at or after the end of the previous one.
        if (i > 0 && cd.compressed_offset < prev_end)
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Chunked snapshot chunk {} at offset {} overlaps previous chunk ending at {}",
                i, cd.compressed_offset, prev_end);

        prev_end = cd.compressed_offset + cd.compressed_size;
        chunks.push_back(cd);
    }

    return chunks;
}

} // namespace DB
