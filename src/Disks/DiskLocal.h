#pragma once

#include <Disks/DiskLocalCheckThread.h>
#include <Disks/IDisk.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFile.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{

class DiskLocalReservation;

class DiskLocal : public IDisk
{
public:
    friend class DiskLocalCheckThread;
    friend class DiskLocalReservation;

    DiskLocal(const String & name_, const String & path_, UInt64 keep_free_space_bytes_,
              const Poco::Util::AbstractConfiguration & config, const String & config_prefix);

    DiskLocal(
        const String & name_,
        const String & path_,
        UInt64 keep_free_space_bytes_,
        ContextPtr context,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix);

    DiskLocal(const String & name_, const String & path_);

    const String & getPath() const override { return disk_path; }

    ReservationPtr reserve(UInt64 bytes) override;

    std::optional<UInt64> getTotalSpace() const override;
    std::optional<UInt64> getAvailableSpace() const override;
    std::optional<UInt64> getUnreservedSpace() const override;

    UInt64 getKeepingFreeSpace() const override { return keep_free_space_bytes; }

    bool existsFile(const String & path) const override;
    bool existsDirectory(const String & path) const override;
    bool existsFileOrDirectory(const String & path) const override;

    size_t getFileSize(const String & path) const override;

    void createDirectory(const String & path) override;

    void createDirectories(const String & path) override;

    void clearDirectory(const String & path) override;

    void moveDirectory(const String & from_path, const String & to_path) override;

    DirectoryIteratorPtr iterateDirectory(const String & path) const override;

    void createFile(const String & path) override;

    void moveFile(const String & from_path, const String & to_path) override;

    void replaceFile(const String & from_path, const String & to_path) override;

    void renameExchange(const std::string & old_path, const std::string & new_path) override;

    bool renameExchangeIfSupported(const std::string & old_path, const std::string & new_path) override;

    void copyDirectoryContent(
        const String & from_dir,
        const std::shared_ptr<IDisk> & to_disk,
        const String & to_dir,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        const std::function<void()> & cancellation_hook) override;

    void listFiles(const String & path, std::vector<String> & file_names) const override;

    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const String & path,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        std::optional<size_t> file_size) const override;

    std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const String & path,
        size_t buf_size,
        WriteMode mode,
        const WriteSettings & settings) override;

    Strings getBlobPath(const String & path) const override;
    bool areBlobPathsRandom() const override { return false; }
    void writeFileUsingBlobWritingFunction(const String & path, WriteMode mode, WriteBlobFunction && write_blob_function) override;

    void removeFile(const String & path) override;
    void removeFileIfExists(const String & path) override;
    void removeDirectory(const String & path) override;
    void removeDirectoryIfExists(const String & path) override;
    void removeRecursive(const String & path) override;

    void setLastModified(const String & path, const Poco::Timestamp & timestamp) override;

    Poco::Timestamp getLastModified(const String & path) const override;

    time_t getLastChanged(const String & path) const override;

    void setReadOnly(const String & path) override;

    void createHardLink(const String & src_path, const String & dst_path) override;

    bool isSymlinkSupported() const override { return true; }

    bool isSymlink(const String & path) const override;

    bool isSymlinkNoThrow(const String & path) const override;

    void createDirectorySymlink(const String & target, const String & link) override;

    String readSymlink(const fs::path & path) const override;

    bool equivalent(const String & p1, const String & p2) const override;

    bool equivalentNoThrow(const String & p1, const String & p2) const override;

    void truncateFile(const String & path, size_t size) override;

    DataSourceDescription getDataSourceDescription() const override;
    static DataSourceDescription getLocalDataSourceDescription(const String & path);

    bool isRemote() const override { return false; }

    bool supportZeroCopyReplication() const override { return false; }

    SyncGuardPtr getDirectorySyncGuard(const String & path) const override;

    void applyNewSettings(const Poco::Util::AbstractConfiguration & config, ContextPtr context, const String & config_prefix, const DisksMap &) override;

    bool isBroken() const override { return broken; }
    bool isReadOnly() const override { return readonly; }

    void startupImpl() override;

    void shutdown() override;

    /// Check if the disk is OK to proceed read/write operations. Currently the check is
    /// rudimentary. The more advanced choice would be using
    /// https://github.com/smartmontools/smartmontools. However, it's good enough for now.
    bool canRead() const noexcept;
    bool canWrite() noexcept;

    bool supportsStat() const override { return true; }
    struct stat stat(const String & path) const override;

    bool supportsChmod() const override { return true; }
    void chmod(const String & path, mode_t mode) override;

protected:
    void checkAccessImpl(const String & path) override;

private:
    std::optional<UInt64> tryReserve(UInt64 bytes);

    /// Setup disk for healthy check.
    /// Throw exception if it's not possible to setup necessary files and directories.
    void setup();

    /// Read magic number from disk checker file. Return std::nullopt if exception happens.
    std::optional<UInt32> readDiskCheckerMagicNumber() const noexcept;

    const String disk_path;
    const String disk_checker_path = ".disk_checker_file";
    std::atomic<UInt64> keep_free_space_bytes;
    LoggerPtr logger;
    DataSourceDescription data_source_description;

    UInt64 reserved_bytes = 0;
    UInt64 reservation_count = 0;

    static std::mutex reservation_mutex;

    std::atomic<bool> broken{false};
    std::atomic<bool> readonly{false};
    std::unique_ptr<DiskLocalCheckThread> disk_checker;
    /// A magic number to vaguely check if reading operation generates correct result.
    /// -1 means there is no available disk_checker_file yet.
    Int64 disk_checker_magic_number = -1;
    bool disk_checker_can_check_read = true;
};


}
