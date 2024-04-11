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
    // DiskOverlay(const String & name_, std::shared_ptr<IDisk> disk_base_, std::shared_ptr<IDisk> disk_overlay_);
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

    void copyDirectoryContent(
        const String &  /*from_dir*/,
        const std::shared_ptr<IDisk> &  /*to_disk*/,
        const String &  /*to_dir*/,
        const ReadSettings &  /*read_settings*/,
        const WriteSettings &  /*write_settings*/,
        const std::function<void()> &  /*cancellation_hook*/) override { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "TODO"); }

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
    MetadataStoragePtr metadata;


    // Check if overlay overwrites data on base
    bool isReplaced(const String& path) const;

    void setReplaced(const String& path);

    // Update overlay_info when moving path
    void movePath(const String& path, const String& new_path);

    // Get path to file in base disk
    String basePath(const String& path) const;

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
