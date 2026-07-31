#pragma once

#include <IO/ReadBuffer.h>
#include <base/types.h>

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

/// Validate that [offset, offset + length) fits within file_size.
void validatePuffinBlobBounds(Int64 offset, Int64 length, size_t file_size, std::string_view context = "Puffin deletion vector");

/// Validate deletion-vector-v1 envelope (combined_length + magic) against declared blob `length`.
/// `header` must point at the first 8 bytes of the blob. Throws on mismatch before a full allocate.
void validateDeletionVectorEnvelope(const UInt8 * header, Int64 length);

/// Deserialize a deletion-vector-v1 blob (magic + CRC wrapper + roaring bitmap payload).
std::vector<UInt64> deserializeDeletionVectorV1Blob(std::string_view blob_bytes, std::optional<UInt64> expected_cardinality = std::nullopt);

/// Read a deletion-vector-v1 blob from a Puffin file at the given offset and length.
std::vector<UInt64> readDeletionVectorFromPuffin(ReadBuffer & file, Int64 offset, Int64 length, std::optional<UInt64> expected_cardinality = std::nullopt);

}
