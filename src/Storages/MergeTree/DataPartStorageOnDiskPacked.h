#pragma once

#include <Storages/MergeTree/MergeTreeDataPartType.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <IO/PackedFilesWriter.h>
#include <IO/PackedFilesReader.h>
#include <IO/ReadPipeline.h>

namespace DB
{

/// A storage for data part that stores all files of part in one archive
/// (except projections and several files listed below).
class DataPartStorageOnDiskPacked final : public DataPartStorageOnDiskBase
{
public:
    static constexpr auto DATA_FILE_EXTENSION = PackedFilesIO::ARCHIVE_EXTENSION;
    inline static const String DATA_FILE_NAME = String("data") + DATA_FILE_EXTENSION;

    inline static const std::unordered_set<String> files_written_separately =
    {
        "delete-on-destroy.txt",
        "txn_version.txt",
    };

    DataPartStorageOnDiskPacked(
        VolumePtr volume_,
        std::string root_path_,
        std::string part_dir_,
        const ReadSettings & read_settings_,
        bool initialize_ = true);

    MergeTreeDataPartStorageType getType() const override { return MergeTreeDataPartStorageType::Packed; }

    MutableDataPartStoragePtr getProjection(const std::string & name, bool use_parent_transaction = true) override; // NOLINT
    MutableDataPartStoragePtr getProjectionNoInitialize(const std::string & name, bool use_parent_transaction = true) override; // NOLINT
    DataPartStoragePtr getProjection(const std::string & name) const override;

    bool exists() const override;
    bool existsDirectory(const std::string & file_name) const override;

    DataPartStorageIteratorPtr iterate() const override;
    Poco::Timestamp getFileLastModified(const String &) const override;
    PackedFilesIO::FileOffset getFileOffsetAndSize(const std::string & file_name) const;
    std::optional<UInt64> getPackedFileUncompressedSize(const std::string & file_name) const override;
    String getActualFileNameOnDisk(const String & file_name) const;
    UInt32 getRefCount(const std::string & file_name) const override;
    std::vector<std::string> getRemotePaths(const std::string & file_name) const override;
    String getUniqueId() const override;

    void rename(
        std::string new_root_path,
        std::string new_part_dir,
        LoggerPtr log,
        bool remove_new_dir_if_exists,
        bool fsync_part_dir) override;

    void createProjection(const std::string & name) override;

    void changeRootPath(const std::string & from_root, const std::string & to_root) override;

    std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const String & name,
        size_t buf_size,
        WriteMode mode,
        const WriteSettings & settings) override;

    void createFile(const String & name) override;
    void moveFile(const String & from_name, const String & to_name) override;
    void replaceFile(const String & from_name, const String & to_name) override;

    void removeFile(const String & name) override;
    void removeFileIfExists(const String & name) override;

    void removeRecursive() override;
    void removeSharedRecursive(bool keep_in_remote_fs) override;

    void createHardLinkFrom(const IDataPartStorage & source, const std::string & from, const std::string & to) override;
    void copyFileFrom(const IDataPartStorage & source, const std::string & from, const std::string & to) override;

    void beginTransaction() override;
    void commitTransaction() override;

    void setPreferredFileOrder(const Strings & file_names) override { preferred_file_order = file_names; }

    /// Finalizes writer and writes buffered data into transaction.
    void precommitTransaction() override;

#if CLICKHOUSE_CLOUD
    TransactionCommitOutcomeVariant tryCommitTransaction(const TransactionCommitOptionsVariant & options) override;
    void undoTransaction() override;
#endif

    bool cloneCopiesWholeArchive(const ClonePartParams & params) const override;

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

#if CLICKHOUSE_CLOUD
    void serializeAuxiliaryInfo(WriteBuffer &) const override;
    void deserializeAuxiliaryInfo(ReadBuffer &) override;
#endif

private:
    /// Reader is initialized from index of archive if file
    /// with archive exists or after finalization of writer.
    std::optional<PackedFilesReader> reader;

    /// Writer is initialized after creating a disk transaction.
    std::optional<PackedFilesWriter> writer;

    bool is_precommitted = false;

    Strings preferred_file_order;

    String getRelativeDataPath() const;
    bool isWrittenSeparately(const String & file_name) const;

    /// True if any of the given files lives inside data.packed (rather than being written separately),
    /// which forces the whole archive to be copied rather than hardlinked during a clone/freeze.
    bool anyArchivedFileRequestedForCopy(const NameSet & files_to_copy_instead_of_hardlinks) const;

    /// Native file access hooks for the base storage. On packed-part storage skp_idx.packed is a
    /// virtual file inside data.packed, so the base file-read overlay (which reads a standalone
    /// skp_idx.packed) is disabled here via getArchiveReaderForFile below; these hooks serve both the
    /// packed index substreams (through the inner-archive composition) and the native part files.
    bool existsFileImpl(const std::string & name) const override;
    size_t getFileSizeImpl(const std::string & file_name) const override;
    void prepareReadImpl(
        const std::string & name,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        ReadPipeline & pipeline) const override;
    std::unique_ptr<ReadBufferFromFileBase> readFileIfExistsImpl(
        const std::string & name,
        const ReadSettings & settings,
        std::optional<size_t> read_hint) const override;

    /// Override the base-class disk probe: on packed-part storage skp_idx.packed is a virtual
    /// file inside data.packed and can't be opened via disk->readFile against its part-relative
    /// path. We route the inner-archive header read through the outer reader instead. The archive
    /// helpers (copy/filter/seedFrom/hasSkipIndicesPackedArchive) rely on this returning the inner
    /// reader for packed source parts.
    std::shared_ptr<const PackedFilesReader> getSkipIndicesPackedReader() const override;

    /// Disable the base file-read overlay for packed storage: its standalone-archive read
    /// composition can't reach a skp_idx.packed that lives inside data.packed. The *Impl hooks above
    /// serve the index substreams instead (via the inner-archive composition).
    std::shared_ptr<const PackedFilesReader> getArchiveReaderForFile(const std::string &) const override { return nullptr; }

    void resetReader(const ReadSettings & read_settings);
    void resetWriterFromTransaction();
    void finalizeWriter();

    /// Modifying of files is possible only with transaction.

    template <typename Op>
    void executeUnaryWriteOperation(const String & file_name, Op && op);

    template <typename Op>
    void executeBinaryWriteOperation(const String & from_name, const String & to_name, Op && op);

    /// Private constructor to create a storage for projection with shared transaction.
    DataPartStorageOnDiskPacked(
        VolumePtr volume_,
        std::string root_path_,
        std::string part_dir_,
        DiskTransactionPtr transaction_,
        const ReadSettings & read_settings_,
        bool initialize = true);

    MutableDataPartStoragePtr create(VolumePtr volume_, std::string root_path_, std::string part_dir_, bool initialize_) const override;
    NameSet getActualFileNamesOnDisk(const NameSet & file_names) const override;
};

}
