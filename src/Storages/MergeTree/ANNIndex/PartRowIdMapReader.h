#pragma once

#include <Storages/MergeTree/ANNIndex/PartRowId.h>

#include <vector>

namespace DB
{

class IANNGroupStorage;
struct ReadSettings;

/// Query-side reader for `id_map.bin` using a **read-all** strategy.
///
/// Lifecycle: constructed once per group on load, then immutable for the lifetime of that
/// group. Concurrent `lookup` is safe — no synchronisation needed.
///
/// `lookup` is an unchecked `vector` subscript, intended for the search hot path.
class PartRowIdMapReader
{
public:
    /// Load the whole file into memory. Validates magic / version / record_size / checksum.
    /// Throws `CORRUPTED_DATA` on any mismatch.
    void loadFrom(IANNGroupStorage & storage, const ReadSettings & settings);

    /// O(1) unchecked lookup. Caller must ensure `internal_id < size`.
    PartRowId lookup(UInt32 internal_id) const
    {
        return records[internal_id];
    }

    size_t size() const { return records.size(); }
    bool empty() const { return records.empty(); }

private:
    std::vector<PartRowId> records;
};

}
