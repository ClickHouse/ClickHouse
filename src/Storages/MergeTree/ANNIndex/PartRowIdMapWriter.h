#pragma once

#include <Storages/MergeTree/ANNIndex/PartRowId.h>

#include <vector>

namespace DB
{

class IANNGroupStorage;
struct WriteSettings;

/// Build-side writer for `id_map.bin`.
/// Lifecycle: used only during index construction; discarded after `writeTo`.
///
/// The caller is expected to append records in increasing `internal_id` order; there is no
/// runtime check for this — the contract is enforced by the builder.
class PartRowIdMapWriter
{
public:
    /// Append a record. The index in the internal vector becomes the `internal_id` used by
    /// the graph/quantiser that is built alongside.
    void append(const PartRowId & row);

    size_t size() const { return records.size(); }
    bool empty() const { return records.empty(); }

    /// Write header + body + footer (with xxh64 checksum over body) to `id_map.bin`
    /// inside the given group storage.
    void writeTo(IANNGroupStorage & storage, const WriteSettings & settings) const;

private:
    std::vector<PartRowId> records;
};

}
