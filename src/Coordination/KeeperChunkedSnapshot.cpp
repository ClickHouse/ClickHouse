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

void packChunkedSnapshotHeader(uint64_t chunk_count, char * buf) noexcept
{
    char * p = buf;
    memcpy(p, KEEPER_CHUNKED_SNAPSHOT_MAGIC.data(), 4);                 p += 4;
    *reinterpret_cast<uint8_t *>(p) = KEEPER_CHUNKED_SNAPSHOT_VERSION;  p += 1;
    memcpy(p, &chunk_count, 8);
}

void packChunkedSnapshotFooter(std::span<const SnapshotChunkDescriptor> chunks, char * buf) noexcept
{
    char * p = buf;
    for (const auto & cd : chunks)
    {
        *reinterpret_cast<uint8_t *>(p) = static_cast<uint8_t>(cd.type); p += 1;
        memcpy(p, &cd.compressed_offset, 8); p += 8;
        memcpy(p, &cd.compressed_size, 8);   p += 8;
        memcpy(p, &cd.node_count, 8);        p += 8;
    }
}

namespace
{

enum class ChunkedParseStatus
{
    Ok = 0,
    TooSmall,            ///< buf_size < header
    BadMagic,            ///< first 4 bytes != CKFS
    BadVersion,          ///< version byte != 8 (only reported when check_version)
    BadChunkCount,       ///< chunk_count < MIN
    FooterDoesNotFit,    ///< chunk_count * 25 > buf_size - header
    BadDescriptorType,   ///< type > SESSIONS
    BadChunkOrder,       ///< first != METADATA / last != SESSIONS / middle != NODES
    ChunkOutOfBounds,    ///< extends into footer, or starts before header / overlaps previous chunk
    TrailingGap,         ///< last chunk does not end exactly at footer_offset (F2)
};

/// Structural validator shared by detection and parsing.
/// `out` is OPTIONAL: pass nullptr to validate only (the detection path — allocation-free, so the
/// noexcept predicate is honest); pass a vector to also materialize the descriptors (the parser path).
/// NOT marked noexcept: when `out != nullptr`, reserve()/push_back() may throw std::bad_alloc, which
/// must propagate as a normal exception, not std::terminate. With `out == nullptr` the body performs
/// only memcpy/comparisons/arithmetic and never allocates or throws.
/// `check_version` gates the version byte: detection passes false (version-independent routing → future
/// versions still reach the parser, which reports the precise version error); the parser passes true.
ChunkedParseStatus tryParseChunkedSnapshot(
    const char * buf, size_t buf_size, bool check_version, std::vector<SnapshotChunkDescriptor> * out)
{
    const size_t header_size = chunkedSnapshotHeaderSize(); // 13
    if (buf_size < header_size)
        return ChunkedParseStatus::TooSmall;
    if (memcmp(buf, KEEPER_CHUNKED_SNAPSHOT_MAGIC.data(), 4) != 0)
        return ChunkedParseStatus::BadMagic;
    if (check_version && static_cast<uint8_t>(buf[4]) != KEEPER_CHUNKED_SNAPSHOT_VERSION)
        return ChunkedParseStatus::BadVersion;

    uint64_t chunk_count = 0;
    memcpy(&chunk_count, buf + 5, 8);
    if (chunk_count < KEEPER_CHUNKED_SNAPSHOT_MIN_CHUNK_COUNT)
        return ChunkedParseStatus::BadChunkCount;

    const size_t bytes_after_header = buf_size - header_size;
    if (chunk_count > bytes_after_header / KEEPER_CHUNKED_SNAPSHOT_DESCRIPTOR_SIZE) // overflow-safe
        return ChunkedParseStatus::FooterDoesNotFit;

    const size_t footer_size = chunkedSnapshotFooterSize(chunk_count);
    const uint64_t footer_offset = static_cast<uint64_t>(buf_size) - footer_size;
    // fit check guarantees footer_offset >= header_size and footer_offset < buf_size (footer_size >= 75 > 0)

    const char * cursor = buf + footer_offset;
    if (out)
    {
        out->clear();
        out->reserve(static_cast<size_t>(chunk_count)); // bounded: chunk_count <= (buf_size-13)/25; may throw → parser path only
    }
    uint64_t prev_end = header_size;               // first chunk must start at/after the front header

    for (uint64_t i = 0; i < chunk_count; ++i)
    {
        SnapshotChunkDescriptor d;
        uint8_t type_raw = 0;
        memcpy(&type_raw, cursor, 1);            cursor += 1;
        memcpy(&d.compressed_offset, cursor, 8); cursor += 8;
        memcpy(&d.compressed_size, cursor, 8);   cursor += 8;
        memcpy(&d.node_count, cursor, 8);        cursor += 8;

        if (type_raw > static_cast<uint8_t>(SnapshotChunkType::SESSIONS))
            return ChunkedParseStatus::BadDescriptorType;
        d.type = static_cast<SnapshotChunkType>(type_raw);
        if (i == 0 && d.type != SnapshotChunkType::METADATA)                       return ChunkedParseStatus::BadChunkOrder;
        if (i == chunk_count - 1 && d.type != SnapshotChunkType::SESSIONS)         return ChunkedParseStatus::BadChunkOrder;
        if (i > 0 && i < chunk_count - 1 && d.type != SnapshotChunkType::NODES)    return ChunkedParseStatus::BadChunkOrder;

        // Upper bound (overflow-safe) + lower bound / non-overlap (prev_end starts at header_size).
        if (d.compressed_offset > footer_offset || d.compressed_size > footer_offset - d.compressed_offset)
            return ChunkedParseStatus::ChunkOutOfBounds;
        if (d.compressed_offset < prev_end)
            return ChunkedParseStatus::ChunkOutOfBounds;

        prev_end = d.compressed_offset + d.compressed_size;
        if (out)
            out->push_back(d); // may throw → parser path only
    }

    // F2: with no trailer, the derived footer must be tied to the chunk region. The writer always
    // emits the footer immediately after the last chunk, so the last chunk must end EXACTLY at
    // footer_offset. Internal gaps between chunks are still tolerated (offset >= prev_end above);
    // only a trailing gap before the footer is rejected.
    if (prev_end != footer_offset)
        return ChunkedParseStatus::TrailingGap;

    return ChunkedParseStatus::Ok;
}

} // anonymous namespace

bool looksLikeChunkedSnapshot(const char * buf, size_t buf_size) noexcept
{
    return tryParseChunkedSnapshot(buf, buf_size, /*check_version=*/false, /*out=*/nullptr)
        == ChunkedParseStatus::Ok;
}

std::vector<SnapshotChunkDescriptor> parseAndValidateChunkedSnapshot(const char * buf, size_t buf_size)
{
    std::vector<SnapshotChunkDescriptor> chunks;
    // out=&chunks materializes descriptors; the core is not noexcept, so a std::bad_alloc here
    // propagates as a normal exception (never std::terminate). check_version=true gates the version.
    const ChunkedParseStatus st = tryParseChunkedSnapshot(buf, buf_size, /*check_version=*/true, &chunks);
    switch (st)
    {
        case ChunkedParseStatus::Ok:
            return chunks;
        case ChunkedParseStatus::BadVersion:
            throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION,
                "Chunked snapshot has unexpected version {}, expected {}",
                static_cast<unsigned>(static_cast<uint8_t>(buf[4])),
                static_cast<unsigned>(KEEPER_CHUNKED_SNAPSHOT_VERSION));
        case ChunkedParseStatus::TooSmall:
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "Chunked snapshot too small for front header: got {} bytes, need at least {}",
                buf_size, chunkedSnapshotHeaderSize());
        case ChunkedParseStatus::BadMagic:
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Chunked snapshot front header has wrong magic bytes");
        case ChunkedParseStatus::BadChunkCount:
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "Chunked snapshot chunk_count below the minimum of {} (need METADATA + >=1 NODES + SESSIONS)",
                KEEPER_CHUNKED_SNAPSHOT_MIN_CHUNK_COUNT);
        case ChunkedParseStatus::FooterDoesNotFit:
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "Chunked snapshot descriptor footer does not fit in the bytes after the {}-byte front header",
                chunkedSnapshotHeaderSize());
        case ChunkedParseStatus::BadDescriptorType:
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Chunked snapshot footer has an unknown chunk_type");
        case ChunkedParseStatus::BadChunkOrder:
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "Chunked snapshot chunk ordering invalid (expected METADATA, NODES..., SESSIONS)");
        case ChunkedParseStatus::ChunkOutOfBounds:
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "Chunked snapshot chunk offset/size out of bounds (overlaps the front header, a previous chunk, or the footer)");
        case ChunkedParseStatus::TrailingGap:
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "Chunked snapshot last chunk does not end exactly where the footer begins (trailing gap / wrong footer provenance)");
    }
    UNREACHABLE();
}

}
