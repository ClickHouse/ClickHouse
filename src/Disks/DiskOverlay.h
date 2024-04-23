#pragma once

#include <Disks/IDisk.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFile.h>
#include <Disks/ObjectStorages/IMetadataStorage.h>

namespace DB {

class DiskOverlay : public IDisk
{
public:
    DiskOverlay(const String & name_, DiskPtr disk_base_, DiskPtr disk_overlay_, MetadataStoragePtr metadata_, MetadataStoragePtr tracked_metadata_);
    DiskOverlay(const String & name_, const Poco::Util::AbstractConfiguration & config_, const String & config_prefix_, const DisksMap & map_);

    const String & getPath() const override { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "TODO"); }

    ReservationPtr reserve(UInt64 bytes) override;

    std::optional<UInt64> getTotalSpace() const override;
    std::optional<UInt64> getAvailableSpace() const override;
    std::optional<UInt64> getUnreservedSpace() const override;

    UInt64 getKeepingFreeSpace() const override { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "TODO"); }

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

    Strings getBlobPath(const String &  /*path*/) const override { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "TODO"); }
    void writeFileUsingBlobWritingFunction(const String &  /*path*/, WriteMode  /*mode*/, WriteBlobFunction &&  /*write_blob_function*/) override { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "TODO"); }

    void removeFile(const String & path) override;
    void removeFileIfExists(const String & path) override;
    void removeDirectory(const String & path) override;
    void removeRecursive(const String & path) override;

    void setLastModified(const String &  /*path*/, const Poco::Timestamp &  /*timestamp*/) override { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "TODO"); }

    Poco::Timestamp getLastModified(const String &  /*path*/) const override { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "TODO"); }

    time_t getLastChanged(const String &  /*path*/) const override { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "TODO"); }

    void setReadOnly(const String &  /*path*/) override { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "TODO"); }

    void createHardLink(const String &  /*src_path*/, const String &  /*dst_path*/) override { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "TODO"); }

    DataSourceDescription getDataSourceDescription() const override { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "TODO"); }

    /// Involves network interaction.
    bool isRemote() const override { return disk_overlay->isRemote() || disk_base->isRemote(); }

    /// Whether this disk support zero-copy replication.
    /// Overrode in remote fs disks.
    bool supportZeroCopyReplication() const override { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "TODO"); }


private:
    DiskPtr disk_base, disk_overlay;
    MetadataStoragePtr metadata, tracked_metadata;

    // A tracked file is a file that exists on the overlay disk (possibly under another name)
    // If a file is tracked, we don't need to list it from the base disk in calls to file listing functions
public:
    bool isTracked(const String& path) const;
private:
    void setTracked(const String& path);

    // When a file needs to be created on disk_overlay, we might be missing some parent directories that are
    // present on disk_base
    void ensureHaveDirectories(const String& path);

    // Get path to file in base disk
    std::optional<String> basePath(const String& path) const;

    struct OverlayInfo {
        // In case of rewrite we need to disregard the original path in disk_base
        // In case of add, the beginning of the file is in orig_path in disk_base
        // We only store the path in orig_path if the file/folder has been moved
        enum Type { add, rewrite };
        Type type;
        std::optional<String> orig_path;

        OverlayInfo() {
            type = Type::add;
            orig_path = {};
        }
    };

    std::unordered_map<String, OverlayInfo> overlay_info;
};

};
