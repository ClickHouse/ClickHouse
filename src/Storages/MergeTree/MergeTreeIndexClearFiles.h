#pragma once

#include <Core/Names.h>
#include <Storages/MergeTree/MergeTreeDataPartTTLInfo.h>
#include <Storages/MergeTree/MergeTreeIndices.h>

#include <ctime>
#include <functional>
#include <memory>
#include <optional>
#include <set>

namespace DB
{

class DataPartStorageOnDiskBase;
class IDataPartStorage;
class IMergeTreeDataPart;
struct MergeTreeDataPartChecksums;
struct MergeTreeSettings;
struct ReadSettings;
struct StorageInMemoryMetadata;
struct WriteSettings;

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

/// Return whether the part still contains any file of the skip index. This uses checksums and
/// the cached packed archive listing without calling `existsFile`.
bool partHasSkipIndexFiles(const IMergeTreeDataPart & part, const MergeTreeIndexPtr & index);

/// Return whether the packed skip-index archive contains a data or mark file for the index.
bool skipIndexHasFilesInPackedArchive(
    const IMergeTreeIndex & index,
    const DataPartStorageOnDiskBase * storage,
    const String & mrk_extension);

/// `DROP INDEX` mutations may run after the index disappears from metadata. Return matching
/// skip-index filenames present in `skp_idx.packed`.
NameSet getDroppedSkipIndexArchiveFileNames(
    const NameSet & dropped_index_names,
    bool escape_index_filenames,
    const String & mrk_extension,
    const DataPartStorageOnDiskBase & storage);

struct PartFileCopyOptions
{
    const NameSet * files_to_skip = nullptr;
    const NameSet * files_to_copy = nullptr;
    bool copy_instead_of_hardlinks = false;
    bool fail_on_temporary_projection_directories = false;
    bool fail_on_projection_subdirectories = false;
    std::function<void()> cancellation_callback;
};

/// Return false if copyPartFilesWithSkip would reject the source part before copying anything.
bool canCopyPartFilesWithSkip(
    const IDataPartStorage & source_storage,
    const PartFileCopyOptions & options);

/// Copy or hardlink source part files into destination according to the skip/include sets.
/// Projection directories are copied recursively. Returned names are the source files that were
/// hardlinked, using projection-prefixed names for projection files to match mutation tracking.
std::optional<NameSet> copyPartFilesWithSkip(
    const IDataPartStorage & source_storage,
    IDataPartStorage & destination_storage,
    const PartFileCopyOptions & options,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings);

}
