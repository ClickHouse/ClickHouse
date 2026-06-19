#include <Coordination/KeeperChunkedSnapshot.h>

#include <Common/Exception.h>
#include <Common/LoggingFormatStringHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/WriteHelpers.h>

#include <cstring>
#include <expected>

namespace DB
{

namespace ErrorCodes
{
extern const int CORRUPTED_DATA;
extern const int UNKNOWN_FORMAT_VERSION;
}

void packChunkedSnapshotHeader(uint64_t chunk_count, WriteBuffer & out)
{
    writeString(KEEPER_CHUNKED_SNAPSHOT_MAGIC, out);
    writeBinary(KEEPER_CHUNKED_SNAPSHOT_VERSION, out);
    writeBinary(chunk_count, out);
}

void packChunkedSnapshotFooter(std::span<const SnapshotChunkDescriptor> chunks, WriteBuffer & out)
{
    for (const auto & cd : chunks)
    {
        writeBinary(static_cast<uint8_t>(cd.type), out);
        writeBinary(cd.compressed_offset, out);
        writeBinary(cd.compressed_size, out);
        writeBinary(cd.node_count, out);
    }
}

namespace
{

struct ParseError
{
    PreformattedMessage message;
    int error_code;
};

template <typename... Args>
[[nodiscard]] std::unexpected<ParseError> makeParseError(int code, FormatStringHelper<Args...> fmt, Args &&... args)
{
    return std::unexpected(ParseError{PreformattedMessage::create(std::move(fmt), std::forward<Args>(args)...), code});
}

using ParseResult = std::expected<std::vector<SnapshotChunkDescriptor>, ParseError>;

ParseResult parseChunkedSnapshotImpl(SeekableReadBuffer & in, bool check_version)
{
    const size_t header_size = chunkedSnapshotHeaderSize();

    char magic[4];
    in.readStrict(magic, 4);
    if (memcmp(magic, KEEPER_CHUNKED_SNAPSHOT_MAGIC.data(), 4) != 0)
        return makeParseError(ErrorCodes::CORRUPTED_DATA, "Chunked snapshot front header has wrong magic bytes");

    uint8_t version = 0;
    readBinary(version, in);
    if (check_version && version != KEEPER_CHUNKED_SNAPSHOT_VERSION)
        return makeParseError(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Chunked snapshot has unexpected version {}, expected {}", version, KEEPER_CHUNKED_SNAPSHOT_VERSION);

    uint64_t chunk_count = 0;
    readBinary(chunk_count, in);
    if (chunk_count < KEEPER_CHUNKED_SNAPSHOT_MIN_CHUNK_COUNT)
        return makeParseError(ErrorCodes::CORRUPTED_DATA, "Chunked snapshot chunk_count below the minimum of {} (need METADATA + >=1 NODES)", KEEPER_CHUNKED_SNAPSHOT_MIN_CHUNK_COUNT);

    const size_t footer_size = chunkedSnapshotFooterSize(chunk_count);
    const uint64_t footer_offset = static_cast<uint64_t>(in.seek(-static_cast<off_t>(footer_size), SEEK_END));

    std::vector<SnapshotChunkDescriptor> chunks;
    chunks.reserve(static_cast<size_t>(chunk_count));
    uint64_t prev_end = header_size;

    for (uint64_t i = 0; i < chunk_count; ++i)
    {
        SnapshotChunkDescriptor d;
        uint8_t type_raw = 0;
        readBinary(type_raw, in);
        readBinary(d.compressed_offset, in);
        readBinary(d.compressed_size, in);
        readBinary(d.node_count, in);

        if (type_raw > static_cast<uint8_t>(SnapshotChunkType::NODES))
            return makeParseError(ErrorCodes::CORRUPTED_DATA, "Chunked snapshot footer has an unknown chunk_type");
        d.type = static_cast<SnapshotChunkType>(type_raw);

        if (i == 0 && d.type != SnapshotChunkType::METADATA)
            return makeParseError(ErrorCodes::CORRUPTED_DATA, "Chunked snapshot chunk ordering invalid (expected METADATA, then NODES...)");
        if (i > 0 && d.type != SnapshotChunkType::NODES)
            return makeParseError(ErrorCodes::CORRUPTED_DATA, "Chunked snapshot chunk ordering invalid (expected METADATA, then NODES...)");

        if (d.compressed_offset > footer_offset || d.compressed_size > footer_offset - d.compressed_offset)
            return makeParseError(ErrorCodes::CORRUPTED_DATA, "Chunked snapshot chunk offset/size out of bounds (overlaps the front header, a previous chunk, or the footer)");
        if (d.compressed_offset < prev_end)
            return makeParseError(ErrorCodes::CORRUPTED_DATA, "Chunked snapshot chunk offset/size out of bounds (overlaps the front header, a previous chunk, or the footer)");

        prev_end = d.compressed_offset + d.compressed_size;
        chunks.push_back(d);
    }

    if (prev_end != footer_offset)
        return makeParseError(ErrorCodes::CORRUPTED_DATA, "Chunked snapshot last chunk does not end exactly where the footer begins (trailing gap / wrong footer provenance)");

    return chunks;
}

} // anonymous namespace

bool isChunkedSnapshot(SeekableReadBuffer & in) noexcept
{
    try
    {
        return parseChunkedSnapshotImpl(in, /*check_version=*/false).has_value();
    }
    catch (...) // Ok: any IO or structural error means the buffer is not a chunked snapshot
    {
        return false;
    }
}

std::vector<SnapshotChunkDescriptor> parseAndValidateChunkedSnapshot(SeekableReadBuffer & in)
{
    auto result = parseChunkedSnapshotImpl(in, /*check_version=*/true);
    if (!result)
        throw Exception(std::move(result.error().message), result.error().error_code);
    return std::move(*result);
}

}
