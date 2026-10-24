#pragma once

#include <Core/Names.h>
#include <Storages/MergeTree/MergeTreeDataPartTTLInfo.h>

#include <ctime>
#include <map>
#include <memory>

namespace DB
{

class IMergeTreeDataPart;
using MergeTreeDataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
struct FutureMergedMutatedPart;
struct StorageInMemoryMetadata;

/// The skip indexes whose `CLEAR INDEX` rule has expired for every row covered by `ttl_infos`,
/// mapped to the earliest such rule's TTL.
std::map<String, time_t> getIndexesWithExpiredClearTTL(
    const StorageInMemoryMetadata & metadata, const MergeTreeDataPartTTLInfos & ttl_infos, time_t current_time);

struct ExpiredIndexFiles
{
    NameSet files;
    bool packed_archive_dirty = false;
};

/// The files of the part's expired skip indexes, and whether any of them is in `skp_idx.packed`.
ExpiredIndexFiles getExpiredIndexFiles(
    const IMergeTreeDataPart & part, const std::shared_ptr<const StorageInMemoryMetadata> & metadata_snapshot, time_t current_time);

/// Return whether the part has a checksummed or packed file of the skip index. Does no storage IO
/// unless the part has `skp_idx.packed`.
bool partHasSkipIndexFiles(const IMergeTreeDataPart & part, const String & index_name, const StorageInMemoryMetadata & metadata);

/// Whether a `TTLClearIndex` merge can hardlink the part's files on this replica rather than copy them.
bool canHardlinkFilesForIndexClear(const MergeTreeDataPartPtr & part);

/// Disk space a `TTLClearIndex` merge of the part writes on this replica. A merge that copies the part's files
/// writes the whole part. Otherwise the estimate is the size of `skp_idx.packed`, which the merge may rewrite. The other files it writes are small.
UInt64 estimateDiskSpaceForIndexClear(const MergeTreeDataPartPtr & part);

/// Whether a planned `TTLClearIndex` merge has the shape its result relies on. That shape is one source
/// part, no patch parts, and the source's data version, format and UUID. The planned part's values come
/// from the replication log entry, so a replica whose source part differs fails the check.
bool futurePartMatchesSourcePart(const FutureMergedMutatedPart & future_part);

}
