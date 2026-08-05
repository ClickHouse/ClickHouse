#pragma once

#include <IO/ReadBuffer.h>
#include <IO/SeekableReadBuffer.h>
#include <Storages/ObjectStorage/DataLakes/PuffinDeletionVectorReader.h>
#include <base/types.h>

#include <map>
#include <string_view>
#include <vector>

namespace DB
{

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

struct PuffinBlob
{
    String type;
    Int64 snapshot_id = 0;
    Int64 sequence_number = 0;
    std::vector<Int32> fields;
    Int64 offset = 0;
    Int64 length = 0;
    String compression_codec;
    std::map<String, String> properties;
};

/// Append bytes from `buf` into `out` until EOF. Throws if `out` would exceed `max_buffered_size`.
void appendReadBufferWithAbsoluteSizeLimit(ReadBuffer & buf, std::vector<UInt8> & out, size_t max_buffered_size);

/// Parse the Puffin footer of a seekable file and return blob metadata.
std::vector<PuffinBlob> readPuffinFooterBlobsFromSeekable(SeekableReadBuffer & seekable, size_t file_size);

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
