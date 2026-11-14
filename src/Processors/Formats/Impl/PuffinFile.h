#pragma once

#include <Processors/Formats/Impl/PuffinBlockInputFormat.h>

#include <optional>
#include <string_view>

/// Apache Iceberg Puffin files (https://iceberg.apache.org/puffin-spec/), independent of the blob type.

namespace DB
{

class WriteBuffer;

constexpr const char * PUFFIN_THETA_SKETCH_BLOB_TYPE = "apache-datasketches-theta-v1";

/// Bounds a crafted compressed blob; a theta sketch of the default size is tens of KiB.
constexpr size_t PUFFIN_BLOB_MAX_DECOMPRESSED_SIZE = 64 * 1024 * 1024;

/// Reads a Puffin file: the footer on construction, blobs on demand. Buffers the whole file if `in` cannot seek.
class PuffinReader
{
public:
    explicit PuffinReader(ReadBuffer & in_);

    const VectorWithMemoryTracking<PuffinBlob> & getBlobs() const { return footer.blobs; }

    /// The decompressed bytes of `blob`, or nothing for a codec other than `zstd` and `lz4`.
    std::optional<String> readBlob(const PuffinBlob & blob) const;

private:
    ReadBuffer & in;
    PuffinFooter footer;
};

/// Writes a Puffin file with an uncompressed footer to `out`, from its start.
class PuffinWriter
{
public:
    explicit PuffinWriter(WriteBuffer & out_);

    /// Appends `bytes` as given: already compressed if `blob.compression_codec` is set.
    void addBlob(PuffinBlob blob, std::string_view bytes);

    /// Writes the footer. Returns its size with both magics and the trailer: Iceberg's `file-footer-size-in-bytes`.
    size_t finalize();

private:
    WriteBuffer & out;
    VectorWithMemoryTracking<PuffinBlob> blobs;
};

}
