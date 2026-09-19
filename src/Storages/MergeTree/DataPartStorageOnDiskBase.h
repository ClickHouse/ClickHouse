#pragma once
#include <IO/PackedFilesReader.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Disks/IDisk.h>
#include <Disks/IVolume.h>
#include <memory>
#include <mutex>
#include <string>

namespace DB
{

class IVolume;
using VolumePtr = std::shared_ptr<IVolume>;
class PackedFilesWriter;

class DataPartStorageOnDiskBase : public IDataPartStorage
{
public:
    DataPartStorageOnDiskBase(
        VolumePtr volume_,
        std::string root_path_,
        std::string part_dir_);

    Projections getProjections() const override;
    void setProjections(Projections projections_) override;
    using IDataPartStorage::detectProjections; /// keep the no-arg overload visible under the override
    Projections detectProjections(const ProjectionScan & scan) const override;

    bool hasProjection(const std::string & dir_name) const override;
    Projection getProjection(const std::string & dir_name) const override;
    std::optional<Projection> tryGetProjection(const std::string & dir_name) const override;
    Projection projectionPlacement(const std::string & dir_name, ProjectionStorageFormat format) const override;

    std::pair<std::string, std::string> getProjectionRootAndDir(const std::string & dir_name, ProjectionStorageFormat format) const override;
    bool existsProjectionDir(const std::string & dir_name, ProjectionStorageFormat format) const override;

    void removeProjection(const Projection & projection) override;
    Projection renameProjection(const Projection & projection, const std::string & new_dir_name, bool fsync) override;
    void syncProjectionStoragePath(const Projection & projection, IDataPartStorage & projection_storage) const override;
    void removeProjectionResidue(const Projection & placement) override;

    void setZeroCopyReplicationEnabled(bool value) override { zero_copy_replication_enabled = value; }
    void setFlatProjectionStorageInUse(bool value) override { flat_projection_storage_in_use = value; }

    std::string getFullPath() const override;
    std::string getRelativePath() const override;
    std::string getPartDirectory() const override;
    std::string getFullRootPath() const override;
    std::string getParentDirectory() const override;

    Poco::Timestamp getLastModified() const override;
    UInt64 calculateTotalSizeOnDisk() const override;

    /// Returns path to place detached part in or nullopt if we don't need to detach part (if it already exists and has the same content)
    std::optional<String> getRelativePathForPrefix(LoggerPtr log, const String & prefix, bool detached, bool broken) const override;

    /// Returns true if detached part already exists and has the same content (compares checksums.txt and the list of files)
    bool looksLikeBrokenDetachedPartHasTheSameContent(const String & detached_part_path, std::optional<String> & original_checksums_content,
                                                      std::optional<Strings> & original_files_list) const;

    void setRelativePath(const std::string & path) override;

    std::string getDiskName() const override;
    std::string getDiskType() const override;
    bool isStoredOnRemoteDisk() const override;
    std::optional<String> getCacheName() const override;
    bool supportZeroCopyReplication() const override;
    bool supportParallelWrite() const override;
    bool isBroken() const override;
    bool isReadonly() const override;
    std::string getDiskPath() const override;
    ReservationPtr reserve(UInt64 bytes) const override;
    ReservationPtr tryReserve(UInt64 bytes) const override;
    DiskPtr getDisk() const;

    /// File reads resolve the per-part skp_idx.packed overlay first: a "skp_idx_..." name that is a
    /// member of the archive is served from it, anything else (incl. all non-index files) falls
    /// through to the storage's native access (existsFileImpl etc.). The overlay lives here, in the
    /// base that owns the packed-index format, and is `final` so no storage can bypass it: a storage
    /// only fills in native access via the *Impl hooks. Storage-agnostic -- a storage that keeps no
    /// standalone skp_idx.packed (e.g. packed part storage) simply has no reader, so the *Impl path
    /// serves the substreams instead. Keeps the archive out of the read pipeline and the callers.
    bool existsFile(const std::string & name) const final;
    size_t getFileSize(const std::string & file_name) const final;
    void prepareRead(
        const std::string & name,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        ReadPipeline & pipeline) const final;
    std::unique_ptr<ReadBufferFromFileBase> readFileIfExists(
        const std::string & name,
        const ReadSettings & settings,
        std::optional<size_t> read_hint) const final;

    /// True iff @name resolves to a virtual file inside this part's skp_idx.packed archive (and
    /// not a standalone file on disk). Lets external callers distinguish per-file substreams
    /// from packed ones without touching the archive reader directly.
    bool isFileInPackedSkipIndicesArchive(const std::string & name) const;

    /// True iff this part has a packed skip-index archive (skp_idx.packed). Cheaper than calling
    /// isFileInPackedSkipIndicesArchive with a sentinel and clearer at call sites that just need
    /// to know whether the archive must be rebuilt/excluded from hardlinks.
    bool hasSkipIndicesPackedArchive() const;

    /// Copy the named virtual files from this storage's skp_idx.packed archive into @target so
    /// the writer can ship a complete archive after also writing fresh recalc'd entries. Used by
    /// mutations to preserve surviving in-archive indices that aren't being recomputed when the
    /// source archive cannot be hardlinked (because the writer is about to write into the same
    /// file name in the new part). Names not present in the archive are skipped silently; this
    /// matches the contract of dropped_skip_index_archive_file_names where probing for absent extensions is
    /// expected.
    void copyPackedSkipIndicesFilesInto(
        const NameSet & file_names,
        PackedFilesWriter & target,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings) const;

    /// Rewrite this storage's skp_idx.packed into a fresh archive on @new_storage, dropping any
    /// virtual file whose name is in @dropped_skip_index_archive_file_names (exact match). Callers must
    /// pre-resolve the full in-archive substream filenames; passing only an index-name prefix
    /// would over-match when two indices share a prefix (e.g. "a" and "a.b" with
    /// escape_index_filenames=0). If every entry would be dropped, no archive is written and the
    /// corresponding checksum entry is removed instead. Used by MutateSomePartColumnsTask::prepare
    /// when DROP INDEX targets an in-archive index and there's no writer pipeline to rebuild
    /// from data.
    void filterPackedSkipIndicesArchiveTo(
        const NameSet & dropped_skip_index_archive_file_names,
        IDataPartStorage & new_storage,
        const WriteSettings & write_settings,
        const ReadSettings & read_settings,
        MergeTreeDataPartChecksums & checksums,
        bool sync) const;

    ReplicatedFilesDescription getReplicatedFilesDescription(const NameSet & file_names) const override;
    ReplicatedFilesDescription getReplicatedFilesDescriptionForRemoteDisk(const NameSet & file_names) const override;

    void backup(
        const MergeTreeDataPartChecksums & checksums,
        const NameSet & files_without_checksums,
        const String & path_in_backup,
        const BackupSettings & backup_settings,
        bool make_temporary_hard_links,
        BackupEntries & backup_entries,
        TemporaryFilesOnDisks * temp_dirs,
        bool is_projection_part,
        bool allow_backup_broken_projection) const override;

    MutableDataPartStoragePtr freeze(
        const std::string & to,
        const std::string & dir_path,
        const DiskPtr & dst_disk,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        std::function<void(const DiskPtr &)> save_metadata_callback,
        const ClonePartParams & params) const override;

    MutableDataPartStoragePtr clonePart(
        const std::string & to,
        const std::string & dir_path,
        const DiskPtr & dst_disk,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        LoggerPtr log,
        const std::function<void()> & cancellation_hook
        ) const override;

    void rename(
        std::string new_root_path,
        std::string new_part_dir,
        LoggerPtr log,
        bool remove_new_dir_if_exists,
        bool fsync_part_dir) override;

    void remove(
        CanRemoveCallback && can_remove_callback,
        const MergeTreeDataPartChecksums & checksums,
        std::list<ProjectionChecksums> projections,
        bool is_temp,
        LoggerPtr log) override;

    void changeRootPath(const std::string & from_root, const std::string & to_root) override;
    void createDirectories() override;

    std::unique_ptr<WriteBufferFromFileBase> writeTransactionFile(const String & txn_file_name, WriteMode mode) const override;

    void removeRecursive() override;
    void removeSharedRecursive(bool keep_in_remote_fs) override;

    SyncGuardPtr getDirectorySyncGuard() const override;
    bool hasActiveTransaction() const override;

    bool isCaseInsensitive() const override;

protected:

    DataPartStorageOnDiskBase(
        VolumePtr volume_,
        std::string root_path_,
        std::string part_dir_,
        DiskTransactionPtr transaction_);

    virtual MutableDataPartStoragePtr create(
        VolumePtr volume_,
        std::string root_path_,
        std::string part_dir_,
        bool initialize_) const = 0;

    /// Incremental owned-set updates for createProjection/renameProjection/removeProjection; require an already-seeded set.
    void addProjection(Projection projection_);
    void dropProjection(const std::string & dir_name);

    /// Descriptor for a READ handle: the owned one if present, else probed from disk across both layouts (a not-owned name is a
    /// broken/missing projection or a manifest desync). Nested placeholder when absent in both layouts.
    Projection resolveProjectionForRead(const std::string & dir_name) const;

    /// Shared tail of freeze/clonePart: the copy owns exactly what the source owned (temps are
    /// transient and not copied), plus the zero-copy flag; entries are re-parented to dest_storage.
    void seedFrozenCopy(IDataPartStorage & dest_storage) const;

    /// A dir at a freeze destination sibling placement is residue of a failed operation on a
    /// same-named part; remote blobs are kept only when zero-copy replication may share them.
    void removeStaleProjectionSiblingAtDestination(
        const DiskPtr & dst_disk, const std::string & proj_dst, const DiskTransactionPtr & external_transaction) const;

    /// Destination paths of the copied FLAT projection siblings of a freeze/clone into `<to>/<dir_path>`
    /// (one per owned non-temp FLAT projection). Used to make the copied siblings durable together with
    /// the part dir (see fsyncFrozenCloneTree).
    Strings frozenFlatSiblingClonePaths(const std::string & to, const std::string & dir_path) const;

    /// Repoint at a moved location; unlike setRelativePath, content is guaranteed unchanged, so caches stay.
    void setPathKeepingCaches(std::string new_root_path, std::string new_part_dir);

    /// Lazily load the per-part skp_idx.packed archive (if any), reading it as a standalone disk
    /// file. Subsequent calls return the cached reader, or nullptr when there is no such file --
    /// including on storages that don't keep skp_idx.packed standalone (e.g. packed part storage,
    /// where index substreams live in data.packed and are served by the *Impl hooks instead). Used
    /// by the file-read overlay above and by the public archive helpers.
    ///
    /// Returns a shared owning handle, not a raw pointer: callers dereference the reader after the
    /// internal mutex is released, while a concurrent resetReader/seed can replace or drop the
    /// cached reader. Holding a shared_ptr for the duration of use keeps the object alive and
    /// avoids a use-after-free on the cached archive index.
    ///
    /// Virtual so packed part storage can route the probe through its outer data.packed reader:
    /// there skp_idx.packed is a virtual member of data.packed rather than a standalone disk file,
    /// so the disk-probe default always misses.
    virtual std::shared_ptr<const PackedFilesReader> getSkipIndicesPackedReader() const;

    /// Cheap pre-filtered lookup for the file-read overlay: returns the archive reader only when
    /// @name is a "skp_idx_..." substream that the archive actually contains, else nullptr. The
    /// prefix gate keeps unrelated files (checksums.txt, count.txt, columns.txt, ...) from loading
    /// or probing skp_idx.packed at all -- avoiding extra metadata I/O on remote/Keeper disks and
    /// keeping a bad/future-version archive from blocking reads of unrelated files.
    ///
    /// Virtual so packed part storage can disable the base file-read overlay by returning nullptr:
    /// its skp_idx.packed lives inside data.packed, so the overlay's standalone-archive read
    /// composition is invalid there. Packed serves index substreams through its *Impl hooks instead.
    virtual std::shared_ptr<const PackedFilesReader> getArchiveReaderForFile(const std::string & name) const;

    /// Copy a single archive member into @target, reading it through this storage's readFile
    /// overlay. Shared by copyPackedSkipIndicesFilesInto and filterPackedSkipIndicesArchiveTo.
    void copyArchiveEntryTo(
        const PackedFilesReader & source_archive,
        const String & file_name,
        PackedFilesWriter & target,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings) const;

public:
    /// Pre-populate the cached PackedFilesReader from an in-memory index produced by the
    /// writer's PackedFilesWriter::finalize. This lets the overlay (existsFile / getFileSize)
    /// answer queries about packed substreams BEFORE the archive file is fully committed on
    /// disk, which matters on object-storage disks where the file isn't visible until the
    /// underlying multipart upload finishes (the writer only calls preFinalize at fillChecksums
    /// time; the actual finalize happens later). After the file is committed, on-disk reads
    /// would work too, but the in-memory index is always cheaper and equally authoritative.
    void seedSkipIndicesPackedReader(const PackedFilesIO::Index & index) const;

    /// Seed this storage's PackedFilesReader from `source`'s archive index. Used when the
    /// unchanged skp_idx.packed is hardlinked from `source` into this (new) part: the hardlink
    /// shares the source's bytes, so the source's index applies verbatim. No-op if `source` has
    /// no packed archive. See the seed rationale above.
    void seedSkipIndicesPackedReaderFrom(const IDataPartStorage & source) const;
protected:

    VolumePtr volume;
    std::string root_path;
    std::string part_dir;
    DiskTransactionPtr transaction;
    bool has_shared_transaction = false;

    /// Zero-copy replication policy; default true keeps residue blobs (fail-safe until seeded).
    bool zero_copy_replication_enabled = true;

    /// Whether the table uses the FLAT projection layout; default true is fail-safe (an unset storage still scans for siblings). Gates the
    /// flat-sibling parts-root scans so the default legacy_nested table pays no per-part listing. See setFlatProjectionStorageInUse.
    bool flat_projection_storage_in_use = true;

    /// The owned projection set. ready=false: never seeded (reads throw); ready=true: authoritative (absent key = no projection). Paths are
    /// derived, so only setRelativePath drops it.
    mutable std::mutex projections_mutex;
    mutable bool owned_projections_ready TSA_GUARDED_BY(projections_mutex) = false;
    mutable Projections owned_projections TSA_GUARDED_BY(projections_mutex);

    /// Cached probe state for skp_idx.packed. probed=false means we haven't checked the disk yet;
    /// probed=true with reader=null means we checked and the archive isn't present.
    mutable std::mutex skip_indices_packed_mutex;
    mutable bool skip_indices_packed_probed TSA_GUARDED_BY(skip_indices_packed_mutex) = false;
    mutable std::shared_ptr<const PackedFilesReader> skip_indices_packed_reader TSA_GUARDED_BY(skip_indices_packed_mutex);

    template <typename Op>
    void executeWriteOperation(Op && op)
    {
        if (transaction)
            op(*transaction);
        else
            op(*volume->getDisk());
    }

private:
    void clearDirectory(
        const std::string & dir,
        const CanRemoveDescription & can_remove_description,
        const MergeTreeDataPartChecksums & checksums,
        bool is_temp,
        LoggerPtr log);

    /// For names of expected data part files returns the actual names
    /// of files in filesystem to which data of these files is written.
    /// Actual file name may be the same as expected
    /// or be the name of the file with packed data.
    virtual NameSet getActualFileNamesOnDisk(const NameSet & file_names) const = 0;

    /// Native file access for the concrete storage (disk files for Full storage; data.packed
    /// members for Packed storage), without the skp_idx.packed overlay. The `final`
    /// existsFile/getFileSize/prepareRead/readFileIfExists add the overlay and delegate here.
    virtual bool existsFileImpl(const std::string & name) const = 0;
    virtual size_t getFileSizeImpl(const std::string & file_name) const = 0;
    virtual void prepareReadImpl(
        const std::string & name,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        ReadPipeline & pipeline) const = 0;
    virtual std::unique_ptr<ReadBufferFromFileBase> readFileIfExistsImpl(
        const std::string & name,
        const ReadSettings & settings,
        std::optional<size_t> read_hint) const = 0;

    /// Returns the destination path for the part directory while copying a detached part.
    String getPartDirForPrefix(const String & prefix, bool detached, int try_no) const;
};

/// Make a freeze/clone hardlink directory tree durable: fsync `clone_dir_path` and every
/// subdirectory below it (children first), then fsync every ancestor directory from its
/// immediate parent up to and including the disk root. fsync(dir) persists the entries inside
/// dir, not dir's own entry in its parent, so the parent chain must be synced too. Shared by
/// both freeze overrides (Full and Packed). A no-op on remote/object disks
/// (getDirectorySyncGuard returns nullptr there). `sibling_dir_paths` are the copied FLAT
/// projection sibling directories (`<part>.<name>.proj`); they live beside `clone_dir_path`,
/// so their subtrees must be synced explicitly (the shared ancestor chain is walked once).
void fsyncFrozenCloneTree(IDisk & disk, const std::string & clone_dir_path, const Strings & sibling_dir_paths = {});

}
