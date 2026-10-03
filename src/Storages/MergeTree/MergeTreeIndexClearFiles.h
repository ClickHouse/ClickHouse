#pragma once

#include <Core/Names.h>
#include <Storages/MergeTree/MergeTreeDataPartTTLInfo.h>
#include <Storages/MergeTree/MergeTreeIndices.h>

#include <ctime>
#include <memory>
#include <set>

namespace DB
{

class DataPartStorageOnDiskBase;
class IDataPartStorage;
class IMergeTreeDataPart;
struct MergeTreeDataPartChecksums;
struct MergeTreeSettings;
struct StorageInMemoryMetadata;

struct SkipIndexClearFiles
{
    NameSet files;
    bool packed_archive_dirty = false;
    bool has_existing_files = false;
};

bool isIndexExpiredByTTL(
    const std::shared_ptr<const StorageInMemoryMetadata> & metadata_snapshot,
    const MergeTreeDataPartTTLInfos & ttl_infos,
    const String & index_name,
    time_t current_time,
    bool ttl_merges_allowed);

std::set<MergeTreeIndexPtr> getIndexesExpiredByClearTTL(
    const std::shared_ptr<const StorageInMemoryMetadata> & metadata_snapshot,
    const MergeTreeSettings & settings,
    const MergeTreeDataPartTTLInfos & ttl_infos,
    time_t current_time,
    bool ttl_merges_allowed);

bool markExpiredIndexClearTTLsFinished(
    const std::shared_ptr<const StorageInMemoryMetadata> & metadata_snapshot,
    MergeTreeDataPartTTLInfos & ttl_infos,
    time_t current_time);

SkipIndexClearFiles getClearIndexFilesToClear(
    const std::shared_ptr<const IMergeTreeDataPart> & part,
    const std::shared_ptr<const StorageInMemoryMetadata> & metadata_snapshot,
    time_t current_time,
    bool ttl_merges_allowed);

/// Return the logical, legacy, and resolved filenames that may belong to these skip indexes.
NameSet getSkipIndexSubstreamFileNames(
    const std::set<MergeTreeIndexPtr> & indexes,
    const String & mrk_extension,
    const MergeTreeDataPartChecksums & checksums,
    const IDataPartStorage * storage = nullptr);

/// Resolve files for a clear, drop, or recalculation and report whether they exist separately
/// or in `skp_idx.packed`.
SkipIndexClearFiles collectSkipIndexClearFiles(
    const std::set<MergeTreeIndexPtr> & indexes,
    const String & mrk_extension,
    const MergeTreeDataPartChecksums & checksums,
    const IDataPartStorage & storage);

/// Return whether the part contains a checksummed or packed skip-index file, including
/// standalone files omitted from checksums by older mutations.
bool partHasSkipIndexFiles(const IMergeTreeDataPart & part, const MergeTreeIndexPtr & index);

/// Return whether standalone storage contains a declared data or mark file for the index.
bool skipIndexHasStandaloneFiles(
    const IMergeTreeIndex & index,
    const IDataPartStorage & storage,
    const String & mrk_extension);

/// Return whether the packed skip-index archive contains a data or mark file for the index.
bool skipIndexHasFilesInPackedArchive(
    const IMergeTreeIndex & index,
    const DataPartStorageOnDiskBase * storage,
    const String & mrk_extension);

/// `DROP INDEX` mutations may run after the index disappears from metadata. Return matching
/// skip-index filenames present in `skp_idx.packed`.
NameSet getDroppedSkipIndexArchiveFileNames(
    const NameSet & dropped_index_names,
    const std::set<MergeTreeIndexPtr> & surviving_indexes,
    bool escape_index_filenames,
    const String & mrk_extension,
    const IMergeTreeDataPart & part,
    const DataPartStorageOnDiskBase & storage);

}
