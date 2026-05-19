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
    DataPartStorageOnDiskBase(VolumePtr volume_, std::string root_path_, std::string part_dir_);

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
    /// matches the contract of dropped_archive_file_names where probing for absent extensions is
    /// expected.
    void copyPackedSkipIndicesFilesInto(
        const NameSet & file_names,
        PackedFilesWriter & target,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings) const;

    /// Rewrite this storage's skp_idx.packed into a fresh archive on @new_storage, dropping any
    /// virtual file whose name is in @dropped_archive_file_names (exact match). Callers must
    /// pre-resolve the full in-archive substream filenames; passing only an index-name prefix
    /// would over-match when two indices share a prefix (e.g. "a" and "a.b" with
    /// escape_index_filenames=0). If every entry would be dropped, no archive is written and the
    /// corresponding checksum entry is removed instead. Used by MutateSomePartColumnsTask::prepare
    /// when DROP INDEX targets an in-archive index and there's no writer pipeline to rebuild
    /// from data.
    void filterPackedSkipIndicesArchiveTo(
        const NameSet & dropped_archive_file_names,
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

    /// Lazily load the per-part skp_idx.packed archive (if any). Subsequent calls return the
    /// cached reader (or nullptr if no archive exists). Used internally by the readFile /
    /// existsFile / getFileSize overlays in the subclasses, and by the public helpers above
    /// that operate on the archive.
    const PackedFilesReader * getSkipIndicesPackedReader() const;

    VolumePtr volume;
    std::string root_path;
    std::string part_dir;
    DiskTransactionPtr transaction;
    bool has_shared_transaction = false;

    /// Cached probe state for skp_idx.packed. probed=false means we haven't checked the disk yet;
    /// probed=true with reader=null means we checked and the archive isn't present.
    mutable std::mutex skip_indices_packed_mutex;
    mutable bool skip_indices_packed_probed TSA_GUARDED_BY(skip_indices_packed_mutex) = false;
    mutable std::unique_ptr<PackedFilesReader> skip_indices_packed_reader TSA_GUARDED_BY(skip_indices_packed_mutex);

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

    /// Returns the destination path for the part directory while copying a detached part.
    String getPartDirForPrefix(const String & prefix, bool detached, int try_no) const;
};

}
