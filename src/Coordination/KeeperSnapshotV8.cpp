#include <Coordination/KeeperSnapshotV8.h>

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

void packV8Header(std::span<const V8FrameDescriptor> frames, char * buf) noexcept
{
    // magic[4]
    memcpy(buf, KEEPER_V8_MAGIC.data(), 4);
    buf += 4;

    // version (1 byte)
    *reinterpret_cast<uint8_t *>(buf) = KEEPER_V8_VERSION;
    buf += 1;

    // chunk_count (8 bytes, native byte order)
    uint64_t count = static_cast<uint64_t>(frames.size());
    memcpy(buf, &count, 8);
    buf += 8;

    // Frame descriptors: each is 17 bytes (1 + 8 + 8)
    for (const auto & f : frames)
    {
        *reinterpret_cast<uint8_t *>(buf) = static_cast<uint8_t>(f.type);
        buf += 1;
        memcpy(buf, &f.compressed_offset, 8);
        buf += 8;
        memcpy(buf, &f.compressed_size, 8);
        buf += 8;
    }
}

std::vector<V8FrameDescriptor> parseAndValidateV8Header(const char * buf, size_t buf_size)
{
    // Minimum header size check (magic + version + chunk_count = 4+1+8 = 13 bytes).
    if (buf_size < 13)
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "V8 snapshot header too small: got {} bytes, need at least 13",
            buf_size);

    // Magic check.
    if (memcmp(buf, KEEPER_V8_MAGIC.data(), 4) != 0)
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "V8 snapshot has wrong magic bytes");

    // Version check.
    uint8_t version = 0;
    memcpy(&version, buf + 4, 1);
    if (version != KEEPER_V8_VERSION)
        throw Exception(
            ErrorCodes::UNKNOWN_FORMAT_VERSION,
            "V8 snapshot has unexpected version {}, expected {}",
            static_cast<unsigned>(version),
            static_cast<unsigned>(KEEPER_V8_VERSION));

    // chunk_count.
    uint64_t chunk_count = 0;
    memcpy(&chunk_count, buf + 5, 8);

    // Bounds check: chunk_count * 17 descriptors must fit in the remaining bytes.
    // Use division form to avoid overflow: chunk_count <= (buf_size - 13) / 17.
    if (chunk_count > (buf_size - 13) / 17)
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "V8 snapshot chunk_count {} implies header size {} that exceeds buffer size {}",
            chunk_count,
            v8HeaderSize(chunk_count),
            buf_size);

    // Minimum chunk count: METADATA + at least one NODES + SESSIONS.
    if (chunk_count < KEEPER_V8_MIN_CHUNK_COUNT)
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "V8 snapshot has chunk_count {} < minimum {} "
            "(snapshot must have at least one METADATA, one NODES, and one SESSIONS frame)",
            chunk_count,
            KEEPER_V8_MIN_CHUNK_COUNT);

    const size_t header_size = v8HeaderSize(chunk_count);
    const char * p = buf + 13; // points to the first frame descriptor

    std::vector<V8FrameDescriptor> frames;
    frames.reserve(static_cast<size_t>(chunk_count));

    uint64_t prev_end = 0; // tracks offset + size of the previous frame for non-overlap check

    for (uint64_t i = 0; i < chunk_count; ++i)
    {
        V8FrameDescriptor f;

        uint8_t type_raw = 0;
        memcpy(&type_raw, p, 1);
        p += 1;
        memcpy(&f.compressed_offset, p, 8);
        p += 8;
        memcpy(&f.compressed_size, p, 8);
        p += 8;

        // Validate chunk type.
        if (type_raw > static_cast<uint8_t>(V8ChunkType::SESSIONS))
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "V8 snapshot chunk {} has unknown chunk_type {}",
                i, static_cast<unsigned>(type_raw));
        f.type = static_cast<V8ChunkType>(type_raw);

        // Validate frame ordering (must be exactly METADATA first, SESSIONS last, NODES in between).
        if (i == 0 && f.type != V8ChunkType::METADATA)
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "V8 snapshot first chunk must be METADATA, got {}",
                static_cast<unsigned>(type_raw));
        if (i == chunk_count - 1 && f.type != V8ChunkType::SESSIONS)
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "V8 snapshot last chunk must be SESSIONS, got {}",
                static_cast<unsigned>(type_raw));
        if (i > 0 && i < chunk_count - 1 && f.type != V8ChunkType::NODES)
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "V8 snapshot middle chunk {} must be NODES, got {}",
                i, static_cast<unsigned>(type_raw));

        // Validate frame offset: must be at or after the header.
        if (f.compressed_offset < header_size)
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "V8 snapshot chunk {} has offset {} that overlaps the header (size {})",
                i, f.compressed_offset, header_size);

        // Validate frame bounds: [offset, offset+size) must lie within [0, buf_size).
        if (f.compressed_offset > buf_size || f.compressed_size > buf_size - f.compressed_offset)
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "V8 snapshot chunk {} [{}, {}+{}) extends beyond buffer size {}",
                i, f.compressed_offset, f.compressed_offset, f.compressed_size, buf_size);

        // Non-overlap: each frame must start at or after the end of the previous one.
        if (i > 0 && f.compressed_offset < prev_end)
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "V8 snapshot chunk {} at offset {} overlaps previous frame ending at {}",
                i, f.compressed_offset, prev_end);

        prev_end = f.compressed_offset + f.compressed_size;
        frames.push_back(f);
    }

    return frames;
}

} // namespace DB
