#pragma once

#include <Core/Names.h>
#include <Storages/MergeTree/MergeTreeIndices.h>

#include <set>
#include <optional>

namespace DB
{

class DataPartStorageOnDiskBase;
class IDataPartStorage;
struct MergeTreeDataPartChecksums;

struct SkipIndexClearFiles
{
    NameSet files;
    bool packed_archive_dirty = false;
    bool has_existing_files = false;
};

/// Return every concrete filename that may belong to these skip indexes in a part:
/// logical substream names, mark files, legacy data extensions, and hashed / storage-overlay names.
NameSet getSkipIndexSubstreamFileNames(
    const std::set<MergeTreeIndexPtr> & indexes,
    const String & mrk_extension,
    const MergeTreeDataPartChecksums & checksums,
    const IDataPartStorage * storage = nullptr);

/// Resolve files for a clear/drop/recalculate operation and report whether any of them exist as
/// standalone files or virtual members of `skp_idx.packed`.
SkipIndexClearFiles collectSkipIndexClearFiles(
    const std::set<MergeTreeIndexPtr> & indexes,
    const String & mrk_extension,
    const MergeTreeDataPartChecksums & checksums,
    const IDataPartStorage & storage);

/// True iff any data or mark substream of the index lives in the source part's packed skip-index archive.
bool skipIndexHasFilesInPackedArchive(
    const IMergeTreeIndex & index,
    const DataPartStorageOnDiskBase * storage,
    const String & mrk_extension);

/// DROP INDEX mutations may run after the dropped index disappeared from metadata. Probe the
/// format-independent skip-index filename candidates used by existing index types and return exact
/// virtual names that are present in `skp_idx.packed`.
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
    const PartFileCopyOptions & options);

}
