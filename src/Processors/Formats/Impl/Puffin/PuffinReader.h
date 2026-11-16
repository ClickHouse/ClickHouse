#pragma once

#include <Processors/Formats/Impl/Puffin/PuffinBlockInputFormat.h>

#include <optional>

namespace DB
{

constexpr int PUFFIN_BLOB_MAX_ZSTD_WINDOW_LOG = 26;
constexpr size_t PUFFIN_BLOB_MAX_SIZE = 1ULL << PUFFIN_BLOB_MAX_ZSTD_WINDOW_LOG;

class PuffinReader
{
public:
    explicit PuffinReader(ReadBuffer & in_);

    [[nodiscard]] const VectorWithMemoryTracking<PuffinBlob> & getBlobs() const { return footer.blobs; }

    [[nodiscard]] std::optional<String> readBlob(const PuffinBlob & blob) const;

private:
    ReadBuffer & in;
    PuffinFooter footer;
};

}
