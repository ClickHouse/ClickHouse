#pragma once

#include "config.h"

#if USE_AVRO

#include <base/types.h>

#include <Poco/JSON/Array.h>

#include <map>
#include <optional>
#include <vector>

namespace DB
{

/// The `apache-datasketches-theta-v1` blob of one field: a serialized compact theta sketch and its estimate.
struct PuffinThetaSketchBlob
{
    Int32 field_id;
    String sketch_bytes;
    UInt64 ndv;
};

/// A Puffin file (https://iceberg.apache.org/puffin-spec/) of theta sketch blobs, as an Iceberg table statistics file.
struct PuffinThetaSketchFile
{
    String bytes;
    size_t footer_size;
    /// The `blob-metadata` of the file's entry in the table metadata `statistics`.
    Poco::JSON::Array::Ptr blob_metadata;
};

/// Uncompressed blobs, each with its estimate as the `ndv` property.
PuffinThetaSketchFile writePuffinThetaSketches(const std::vector<PuffinThetaSketchBlob> & blobs, Int64 snapshot_id, Int64 sequence_number);

/// The serialized theta sketches of a Puffin file by field id, decompressed (zstd or lz4), or nothing if `puffin_bytes`
/// is not a Puffin file with an uncompressed footer. Blobs of other types, of several fields or of another codec are
/// skipped.
std::optional<std::map<Int32, String>> readPuffinThetaSketches(const String & puffin_bytes);

}

#endif
