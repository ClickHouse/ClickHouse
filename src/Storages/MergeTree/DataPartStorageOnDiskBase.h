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
class DataPartStorageOnDiskBase;

/// Manifest component of an on-disk part storage. The structural home for the future per-part
/// manifest access protocol; no manifest exists yet.
class DataPartManifestStorageOnDisk final : public IDataPartManifestStorage
{
public:
    explicit DataPartManifestStorageOnDisk(const DataPartStorageOnDiskBase & part_storage_) : part_storage(part_storage_) {}

    bool hasManifest() const override { return false; }

private:
    /// The part storage this manifest belongs to; unused until the manifest protocol lands.
    [[maybe_unused]] const DataPartStorageOnDiskBase & part_storage;
};

/// Skip-index component of an on-disk part storage. Owns the per-part skp_idx.packed archive:
/// the lazily-loaded reader over it, and the copy/filter/seed operations mutations and writers use.
/// This default implementation reads skp_idx.packed as a standalone disk file (full part storage);
/// packed part storage subclasses it to route through the outer data.packed archive.
class DataPartIndexStorageOnDisk : public IDataPartIndexStorage
{
public:
    explicit DataPartIndexStorageOnDisk(const DataPartStorageOnDiskBase & part_storage_) : part_storage(part_storage_) {}

    bool isFileInPackedSkipIndicesArchive(const std::string & name) const override;
    bool hasSkipIndicesPackedArchive() const override;

    void copyPackedSkipIndicesFilesInto(
        const NameSet & file_names,
        PackedFilesWriter & target,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings) const override;

    void filterPackedSkipIndicesArchiveTo(
        const NameSet & dropped_skip_index_archive_file_names,
        IDataPartStorage & new_storage,
        const WriteSettings & write_settings,
        const ReadSettings & read_settings,
        MergeTreeDataPartChecksums & checksums,
        bool sync) const override;

    void seedSkipIndicesPackedReaderFrom(const IDataPartStorage & source) const override;

    /// Lazily load the archive reader (if any). Subsequent calls return the cached reader, or
    /// nullptr when there is no archive.
    ///
    /// Returns a shared owning handle, not a raw pointer: callers dereference the reader after the
    /// internal mutex is released, while a concurrent seed can replace the cached reader. Holding a
    /// shared_ptr for the duration of use keeps the object alive and avoids a use-after-free on the
    /// cached archive index.
    virtual std::shared_ptr<const PackedFilesReader> getSkipIndicesPackedReader() const;

    /// Cheap pre-filtered lookup for the part storage's file-read overlay: returns the archive
    /// reader only when @name is a "skp_idx_..." substream that the archive actually contains, else
    /// nullptr. The prefix gate keeps unrelated files (checksums.txt, count.txt, columns.txt, ...)
    /// from loading or probing skp_idx.packed at all -- avoiding extra metadata I/O on remote/Keeper
    /// disks and keeping a bad/future-version archive from blocking reads of unrelated files.
    ///
    /// Packed part storage disables the overlay by returning nullptr: its skp_idx.packed lives
    /// inside data.packed, so the standalone-archive read composition is invalid there and the
    /// storage's *Impl hooks serve the substreams instead.
    virtual std::shared_ptr<const PackedFilesReader> getArchiveReaderForFile(const std::string & name) const;

    /// Pre-populate the cached reader from an in-memory index produced by the writer's
    /// PackedFilesWriter::finalize. This lets the part storage's overlay (existsFile / getFileSize)
    /// answer queries about packed substreams BEFORE the archive file is fully committed on disk,
    /// which matters on object-storage disks where the file isn't visible until the underlying
    /// multipart upload finishes. After the file is committed, on-disk reads would work too, but the
    /// in-memory index is always cheaper and equally authoritative.
    void seedSkipIndicesPackedReader(const PackedFilesIO::Index & index) const;

    /// Drop the cached reader and probe state. For when the part storage is repointed at an
    /// unrelated directory (setRelativePath) whose archive has a different index. Safe for
    /// concurrent readers because the reader is shared-owned: a reader still using it keeps it
    /// alive until done.
    void resetSkipIndicesPackedReader() const;

    /// Clear a cached *miss* (keep a successfully-loaded reader). For when the part directory is
    /// renamed or re-rooted: a loaded reader stays valid (its archive index is path-independent and
    /// reads resolve the archive's current location), but a probe that ran while the rename was in
    /// progress could have looked at the old location and found no file.
    void clearStaleSkipIndicesPackedMiss() const;

    ~DataPartIndexStorageOnDisk() override = default;

protected:
    /// Copy a single archive member into @target, reading it through the part storage's readFile
    /// overlay. Shared by copyPackedSkipIndicesFilesInto and filterPackedSkipIndicesArchiveTo.
    void copyArchiveEntryTo(
        const PackedFilesReader & source_archive,
        const String & file_name,
        PackedFilesWriter & target,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings) const;

    const DataPartStorageOnDiskBase & part_storage;

    /// Cached probe state for skp_idx.packed. probed=false means we haven't checked the disk yet;
    /// probed=true with reader=null means we checked and the archive isn't present.
    mutable std::mutex skip_indices_packed_mutex;
    mutable bool skip_indices_packed_probed TSA_GUARDED_BY(skip_indices_packed_mutex) = false;
    mutable std::shared_ptr<const PackedFilesReader> skip_indices_packed_reader TSA_GUARDED_BY(skip_indices_packed_mutex);
};

/// Statistics component of an on-disk part storage. Owns access to the packed statistics archive
/// ("statistics.packed"); the per-column wide layout reads through the part storage generically.
class DataPartStatisticsStorageOnDisk final : public IDataPartStatisticsStorage
{
public:
    explicit DataPartStatisticsStorageOnDisk(const DataPartStorageOnDiskBase & part_storage_) : part_storage(part_storage_) {}

    const PackedFilesReader * getPackedStatisticsReader(const ReadSettings & read_settings) const override;

    std::unique_ptr<ReadBufferFromFileBase> readPackedStatisticsFile(
        const String & file_name, const ReadSettings & read_settings, size_t file_size) const override;

private:
    const DataPartStorageOnDiskBase & part_storage;

    /// The reader holds only the archive index; reads resolve the archive's current location, so a
    /// part that has since been renamed or moved still reads from the right place.
    mutable std::mutex packed_statistics_mutex;
    mutable std::unique_ptr<PackedFilesReader> packed_statistics_reader TSA_GUARDED_BY(packed_statistics_mutex);
};

class DataPartStorageOnDiskBase : public virtual IDataPartStorage
{
public:
    DataPartStorageOnDiskBase(VolumePtr volume_, std::string root_path_, std::string part_dir_);

    /// Component storages of this part (see IDataPartStorage): separate objects owned by this
    /// storage, always present on on-disk storages. Covariant so on-disk callers get the concrete
    /// component types.
    DataPartManifestStorageOnDisk * getManifestStorage() override { return manifest_storage.get(); }
    const DataPartManifestStorageOnDisk * getManifestStorage() const override { return manifest_storage.get(); }
    DataPartIndexStorageOnDisk * getIndexStorage() override { return index_storage.get(); }
    const DataPartIndexStorageOnDisk * getIndexStorage() const override { return index_storage.get(); }
    DataPartStatisticsStorageOnDisk * getStatisticsStorage() override { return statistics_storage.get(); }
    const DataPartStatisticsStorageOnDisk * getStatisticsStorage() const override { return statistics_storage.get(); }

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
    /// through to the storage's native access (existsFileImpl etc.). The overlay composes here (the
    /// part storage owns file reads) but the archive knowledge lives in the index component; the
    /// methods are `final` so no storage can bypass the composition: a storage only fills in native
    /// access via the *Impl hooks.
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
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        std::function<void(const DiskPtr &)> save_metadata_callback,
        const ClonePartParams & params) const override;

    MutableDataPartStoragePtr freezeRemote(
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

    DataPartStorageOnDiskBase(VolumePtr volume_, std::string root_path_, std::string part_dir_, DiskTransactionPtr transaction_);
    virtual MutableDataPartStoragePtr create(VolumePtr volume_, std::string root_path_, std::string part_dir_, bool initialize_) const = 0;

    VolumePtr volume;
    std::string root_path;
    std::string part_dir;
    DiskTransactionPtr transaction;
    bool has_shared_transaction = false;

    /// The component storages of this part. Always present; the concrete storage classes replace
    /// the index component with their own flavor in their constructors (packed part storage keeps
    /// skp_idx.packed inside data.packed rather than as a standalone file).
    std::unique_ptr<DataPartManifestStorageOnDisk> manifest_storage;
    std::unique_ptr<DataPartIndexStorageOnDisk> index_storage;
    std::unique_ptr<DataPartStatisticsStorageOnDisk> statistics_storage;

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
/// (getDirectorySyncGuard returns nullptr there).
void fsyncFrozenCloneTree(IDisk & disk, const std::string & clone_dir_path);

}
