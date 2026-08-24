#pragma once

#include <IO/ReadBuffer.h>
#include <Processors/Formats/Impl/PuffinBlockInputFormat.h>
#include <base/types.h>

#include <limits>
#include <string_view>
#include <vector>
#include <optional>

namespace DB
{

/// Absolute cap on on-disk deletion-vector-v1 blob length before reading bytes into memory.
/// Aligns with Iceberg DeleteLoader's 2 GiB content-size check.
constexpr size_t PUFFIN_DV_MAX_BLOB_SIZE = 2ULL * 1024 * 1024 * 1024;
/// Absolute cap on materialized deleted positions (~800 MiB of UInt64s at this limit).
constexpr UInt64 PUFFIN_DV_MAX_MATERIALIZED_POSITIONS = 100'000'000;

/// Leading / footer-open magic is always 4 bytes (`PFA1`).
constexpr size_t PUFFIN_MAGIC_SIZE = 4;
constexpr size_t PUFFIN_FOOTER_TRAILER_SIZE = 12;
/// Absolute cap on footer payload size (uncompressed JSON bytes, or declared LZ4 contentSize).
constexpr size_t PUFFIN_FOOTER_MAX_PAYLOAD_SIZE = 16 * 1024 * 1024;

/// Non-seekable SQL path must buffer the whole file to reach the trailer. Cap total buffered size
/// so a crafted pipe cannot allocate unbounded memory before footer-length validation.
/// Sized for one max DV blob + max footer (header magic + blob + footer magic + payload + trailer).
constexpr size_t PUFFIN_NON_SEEKABLE_MAX_BUFFERED_SIZE = PUFFIN_MAGIC_SIZE + PUFFIN_DV_MAX_BLOB_SIZE
    + PUFFIN_MAGIC_SIZE + PUFFIN_FOOTER_MAX_PAYLOAD_SIZE + PUFFIN_FOOTER_TRAILER_SIZE;

/// Iceberg reserved `_pos` field id (`std::numeric_limits<Int32>::max() - 2`). Spark / Iceberg
/// writers put this singleton in deletion-vector-v1 puffin `fields` for file-scoped DVs.
constexpr Int32 ICEBERG_ROW_POSITION_FIELD_ID = std::numeric_limits<Int32>::max() - 2;

/// File-scoped DVs may use `fields=[]` or `fields=[ICEBERG_ROW_POSITION_FIELD_ID]`.
/// Any other list is treated as unsupported column-scoped deletion vectors.
void validateDeletionVectorV1Fields(const std::vector<Int32> & fields, size_t blob_index);

/// Validate that [offset, offset + length) fits within file_size.
void validatePuffinBlobBounds(Int64 offset, Int64 length, size_t file_size, std::string_view context = "Puffin deletion vector");

/// Iceberg deletion-vector-v1 envelope magic (`0xD1D33964`), shared with SQL `Puffin` decode.
inline constexpr UInt8 DELETION_VECTOR_MAGIC[4] = {0xD1, 0xD3, 0x39, 0x64};

/// Fail closed before envelope peek / full allocate. Shared by SQL `Puffin` and Iceberg loaders.
/// Order: cardinality ceiling, then length bounds (`length < 0`, absolute blob cap, min envelope).
void checkDeletionVectorBlobReadLimits(Int64 length, std::optional<UInt64> expected_cardinality);

/// Validate deletion-vector-v1 envelope (combined_length + magic) against declared blob `length`.
/// `header` must point at the first 8 bytes of the blob. Throws on mismatch before a full allocate.
void validateDeletionVectorEnvelope(const UInt8 * header, Int64 length);

/// Deserialize a deletion-vector-v1 blob (magic + CRC wrapper + roaring bitmap payload).
std::vector<UInt64> deserializeDeletionVectorV1Blob(std::string_view blob_bytes, std::optional<UInt64> expected_cardinality = std::nullopt);

/// Read a deletion-vector-v1 blob from a Puffin file at the given offset and length.
std::vector<UInt64> readDeletionVectorFromPuffin(ReadBuffer & file, Int64 offset, Int64 length, std::optional<UInt64> expected_cardinality = std::nullopt);

/// Append bytes from `buf` into `out` until EOF. Throws if `out` would exceed `max_buffered_size`.
void appendReadBufferWithAbsoluteSizeLimit(ReadBuffer & buf, std::vector<UInt8> & out, size_t max_buffered_size);

/// Compatibility alias for Iceberg / tests: same as `readPuffinFooterFromSeekable`.
inline std::vector<PuffinBlob> readPuffinFooterBlobsFromSeekable(SeekableReadBuffer & seekable, size_t file_size)
{
    return readPuffinFooterFromSeekable(seekable, file_size);
}

/// Find the unique footer blob at (`content_offset`, `content_size_in_bytes`) and bind it as a
/// deletion-vector-v1 for `expected_referenced_data_file` with `expected_cardinality`.
/// Throws if the slice is missing, ambiguous, or does not match the expected DV identity.
const PuffinBlob & bindDeletionVectorBlob(
    const std::vector<PuffinBlob> & blobs,
    Int64 content_offset,
    Int64 content_size_in_bytes,
    std::string_view expected_referenced_data_file,
    UInt64 expected_cardinality);

}
