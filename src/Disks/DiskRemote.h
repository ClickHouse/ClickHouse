#pragma once

#include <Client/RemoteFSConnectionPool.h>

#include <Common/logger_useful.h>
#include <Disks/DiskLocalCheckThread.h>
#include <Disks/IDisk.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFile.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{

class DiskRemote : public IDisk
{
public:
    friend class DiskRemoteReservation;

    DiskRemote(
        const String & name, 
        const String & host, UInt16 port, 
        const String & remote_disk_name, 
        unsigned max_connections_ = 20
    );

    const String & getPath() const override;

    ReservationPtr reserve(UInt64 bytes) override;

    UInt64 getTotalSpace() const override;

    UInt64 getAvailableSpace() const override;

    UInt64 getUnreservedSpace() const override;

    UInt64 getKeepingFreeSpace() const override { return 0; } // TODO implement this

    bool exists(const String & path) const override;

    bool isFile(const String & path) const override;

    bool isDirectory(const String & path) const override;

    size_t getFileSize(const String & path) const override;

    void createDirectory(const String & path) override;

    void createDirectories(const String & path) override;

    void clearDirectory(const String & path) override;

    void moveDirectory(const String & from_path, const String & to_path) override;

    DirectoryIteratorPtr iterateDirectory(const String & path) const override;

    void createFile(const String & path) override;

    void moveFile(const String & from_path, const String & to_path) override;

    void replaceFile(const String & from_path, const String & to_path) override;

    void copy(const String & from_path, const std::shared_ptr<IDisk> & to_disk, const String & to_path) override;

    void copyDirectoryContent(const String & from_dir, const std::shared_ptr<IDisk> & to_disk, const String & to_dir) override;

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
        const WriteSettings &) override;

    void removeFile(const String & path) override;
    void removeFileIfExists(const String & path) override;
    void removeDirectory(const String & path) override;
    void removeRecursive(const String & path) override;

    void setLastModified(const String & path, const Poco::Timestamp & timestamp) override;

    Poco::Timestamp getLastModified(const String & path) const override;

    time_t getLastChanged(const String & path) const override;

    void setReadOnly(const String & path) override;

    void createHardLink(const String & src_path, const String & dst_path) override;

    void truncateFile(const String & path, size_t size) override;

    DataSourceDescription getDataSourceDescription() const override;

    bool isRemote() const override { return true; }

    bool supportZeroCopyReplication() const override { return false; }

    bool isReadOnly() const override { return readonly; }
    bool isBroken() const override { return broken; }

    void shutdown() override;

    void startupImpl(ContextPtr context) override;

    void applyNewSettings(const Poco::Util::AbstractConfiguration & config, ContextPtr context, const String & config_prefix, const DisksMap &) override;

    MetadataStoragePtr getMetadataStorage() override {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "TODO: not implemented");
    }

    // getSerializedMetadata TODO подумать над метаданными

    // DiskObjectStoragePtr createDiskObjectStorage() override; // TODO подумать

    bool supportsStat() const override { return false; } // TODO подумать
//    struct stat stat(const String & path) const override;

    bool supportsChmod() const override { return false; } // TODO подумать
//    void chmod(const String & path, mode_t mode) override;

private:
    std::optional<UInt64> tryReserve(UInt64 bytes);

    const String host;
    const UInt16 port;
    const String remote_disk_name;

    const String disk_path;

    ConnectionTimeouts timeouts;

    mutable RemoteFSConnectionPool conn_pool;

    Poco::Logger * logger;
    DataSourceDescription data_source_description;

    UInt64 reserved_bytes = 0;
    UInt64 reservation_count = 0;

    static std::mutex reservation_mutex;

    std::atomic<bool> broken{false};
    std::atomic<bool> readonly{false};
};

} // DB

