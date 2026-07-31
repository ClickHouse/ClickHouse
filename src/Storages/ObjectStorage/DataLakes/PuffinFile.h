#pragma once

#include <IO/SeekableReadBuffer.h>
#include <base/types.h>

#include <map>
#include <string_view>
#include <vector>

namespace DB
{

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
