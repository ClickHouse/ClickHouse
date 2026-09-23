#pragma once

#include <Storages/MergeTree/UniqueKey/UniqueKeyProbe.h>

#include <Core/Names.h>

#include <cstddef>

namespace DB
{

class Block;

/// Simple single-threaded MergeTree probe. Encodes the key batch once, then
/// walks the partition snapshot newest-first, stopping at the first live hit
/// per key. Correct but not performance-tuned — no sharding, no worker threads,
/// no per-part `MultiGet`, no early-termination across parts. This is the
/// baseline implementation downstream components integrate against; a parallel
/// implementation can be added later behind the same `IUniqueKeyProbe` contract
/// and selected via `makeUniqueKeyProbe`.
class UniqueKeyProbeSimple final : public IUniqueKeyProbe
{
public:
    UniqueKeyProbeSimple(
        ProbeTargetsSupplier supplier_,
        Names unique_key_column_names_,
        size_t max_encoded_size_);

    std::vector<ProbeResult> probeBatch(const Block & keys, const String & partition_id) override;

private:
    ProbeTargetsSupplier supplier;
    Names unique_key_column_names;
    size_t max_encoded_size;
};

}
