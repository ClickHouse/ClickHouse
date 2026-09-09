#pragma once

#include <base/defines.h>
#include <base/types.h>

#include <mutex>
#include <string>
#include <unordered_map>

namespace DB
{

/// The number of files referencing each blob of a plain-rewritable disk, for the blobs shared by hard links.
///
/// Only the blobs with more than one link are stored. Every blob known to the metadata is referenced by at least one file,
/// so a blob absent from the map has exactly one link. This keeps the structure small: it is empty until hard links are created,
/// and MergeTree shares blobs only between a part and its mutated successor, until the old part is removed.
class BlobLinkCounts
{
public:
    uint32_t get(const std::string & blob_key) const;

    /// Adds the deltas accumulated by a transaction.
    void apply(const std::unordered_map<std::string, int64_t> & deltas);

    /// Replaces the contents with the counts calculated from the loaded layout. Counts of 1 are dropped.
    void replace(const std::unordered_map<std::string, uint32_t> & new_counts);

private:
    mutable std::mutex mutex;
    std::unordered_map<std::string, uint32_t> counts TSA_GUARDED_BY(mutex);
};

}
