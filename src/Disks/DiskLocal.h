#pragma once

#include <base/logger_useful.h>
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

    DiskLocal(const String & name_, const String & path_, UInt64 keep_free_space_bytes_);
    DiskLocal(
        const String & name_,
        const String & path_,
        UInt64 keep_free_space_bytes_,
        ContextPtr context,
        UInt64 local_disk_check_period_ms);

    const String & getName() const override { return name; }

    const String & getPath() const override { return disk_path; }

    ReservationPtr reserve(UInt64 bytes) override;

    UInt64 getTotalSpace() const override;

    UInt64 getAvailableSpace() const override;

    UInt64 getUnreservedSpace() const override;

    UInt64 getKeepingFreeSpace() const override { return keep_free_space_bytes; }

    bool exists(const String & path) const override;

    bool isFile(const String & path) const override;

    bool isDirectory(const String & path) const override;

    size_t getFileSize(const String & path) const override;

    void createDirectory(const String & path) override;

    void createDirectories(const String & path) override;

    void clearDirectory(const String & path) override;

    void moveDirectory(const String & from_path, const String & to_path) override;

    DiskDirectoryIteratorPtr iterateDirectory(const String & path) override;

    void createFile(const String & path) override;

    void moveFile(const String & from_path, const String & to_path) override;

    void replaceFile(const String & from_path, const String & to_path) override;

    void copy(const String & from_path, const std::shared_ptr<IDisk> & to_disk, const String & to_path) override;

    void listFiles(const String & path, std::vector<String> & file_names) override;

    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const String & path,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        std::optional<size_t> file_size) const override;

    std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const String & path,
        size_t buf_size,
        WriteMode mode) override;

    void removeFile(const String & path) override;
    void removeFileIfExists(const String & path) override;
    void removeDirectory(const String & path) override;
    void removeRecursive(const String & path) override;

    void setLastModified(const String & path, const Poco::Timestamp & timestamp) override;

    Poco::Timestamp getLastModified(const String & path) override;

    void setReadOnly(const String & path) override;

    void createHardLink(const String & src_path, const String & dst_path) override;

    void truncateFile(const String & path, size_t size) override;

    DiskType getType() const override { return DiskType::Local; }
    bool isRemote() const override { return false; }

    bool supportZeroCopyReplication() const override { return false; }

    SyncGuardPtr getDirectorySyncGuard(const String & path) const override;

    void applyNewSettings(const Poco::Util::AbstractConfiguration & config, ContextPtr context, const String & config_prefix, const DisksMap &) override;

    bool isBroken() const override { return broken; }

    void startup() override;

    void shutdown() override;

    /// Check if the disk is OK to proceed read/write operations. Currently the check is
    /// rudimentary. The more advanced choice would be using
    /// https://github.com/smartmontools/smartmontools. However, it's good enough for now.
    bool canRead() const noexcept;
    bool canWrite() const noexcept;

private:
    bool tryReserve(UInt64 bytes);

    /// Setup disk for healthy check. Returns true if it's read-write, false if read-only.
    /// Throw exception if it's not possible to setup necessary files and directories.
    bool setup();

    /// Read magic number from disk checker file. Return std::nullopt if exception happens.
    std::optional<UInt32> readDiskCheckerMagicNumber() const noexcept;

    const String name;
    const String disk_path;
    const String disk_checker_path = ".disk_checker_file";
    std::atomic<UInt64> keep_free_space_bytes;
    Poco::Logger * logger;

    UInt64 reserved_bytes = 0;
    UInt64 reservation_count = 0;

    static std::mutex reservation_mutex;

    Poco::Logger * log = &Poco::Logger::get("DiskLocal");

    std::atomic<bool> broken{false};
    std::atomic<bool> readonly{false};
    std::unique_ptr<DiskLocalCheckThread> disk_checker;
    /// A magic number to vaguely check if reading operation generates correct result.
    /// -1 means there is no available disk_checker_file yet.
    Int64 disk_checker_magic_number = -1;
    bool disk_checker_can_check_read = true;
};


}
