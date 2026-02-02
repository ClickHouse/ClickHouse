#pragma once

#include <Disks/IDisk.h>
#include <Storages/MergeTree/MergeTreeDataFormatVersion.h>
#include <Storages/MergeTree/Manifest/IManifestStorage.h>
#include <Storages/MergeTree/Manifest/BaseManifestEntry.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

class DiskManifest;
using DiskManifestPtr = std::shared_ptr<DiskManifest>;

class ManifestDirectoryIterator : public IDirectoryIterator
{
public:
    explicit ManifestDirectoryIterator(Strings names_);

    void next() override;
    bool isValid() const override;
    String name() const override;
    String path() const override;

private:
    Strings names;
    size_t pos = 0;
};

class DiskManifest : public IDisk
{
public:
    DiskManifest(
        const String & name_,
        const String & db_path,
        const String & relative_data_path_,
        std::weak_ptr<IManifestStorage> manifest_storage_,
        MergeTreeDataFormatVersion format_version_);

    DirectoryIteratorPtr iterateDirectory(const String & path) const override;

    const String & getPath() const override { return db_path; }
    std::optional<UInt64> getTotalSpace() const override { return 0; }
    std::optional<UInt64> getAvailableSpace() const override { return 0; }
    std::optional<UInt64> getUnreservedSpace() const override { return 0; }

    bool existsFile(const String &) const override { return false; }
    bool existsDirectory(const String &) const override { return false; }
    bool existsFileOrDirectory(const String &) const override { return false; }
    size_t getFileSize(const String &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
    }
    void createDirectory(const String &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
    }
    void createDirectories(const String &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
    }
    void moveDirectory(const String &, const String &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
    }
    void createFile(const String &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
    }
    void moveFile(const String &, const String &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
    }
    void replaceFile(const String &, const String &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
    }
    void listFiles(const String &, std::vector<String> &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
    }
    std::unique_ptr<ReadBufferFromFileBase>
    readFile(const String &, const ReadSettings &, std::optional<size_t>) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
    }
    std::unique_ptr<WriteBufferFromFileBase>
    writeFile(const String &, size_t, WriteMode, const WriteSettings &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
    }
    Strings getBlobPath(const String &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
    }
    bool areBlobPathsRandom() const override { return false; }
    void writeFileUsingBlobWritingFunction(const String &, WriteMode, WriteBlobFunction &&) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
    }
    void removeFile(const String &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
    }
    void removeFileIfExists(const String &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
    }
    void removeDirectory(const String &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
    }
    void removeRecursive(const String &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
    }
    void setLastModified(const String &, const Poco::Timestamp &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
    }
    Poco::Timestamp getLastModified(const String &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
    }
    time_t getLastChanged(const String &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
    }
    void setReadOnly(const String &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
    }
    void createHardLink(const String &, const String &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
    }
    void truncateFile(const String &, size_t) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
    }
    DataSourceDescription getDataSourceDescription() const override
    {
        return data_source_description;
    }
    bool isRemote() const override { return false; }
    bool supportZeroCopyReplication() const override { return false; }
    SyncGuardPtr getDirectorySyncGuard(const String &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
    }
    void applyNewSettings(const Poco::Util::AbstractConfiguration &, ContextPtr, const String &, const DisksMap &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
    }
    ReservationPtr reserve(UInt64 /* bytes */) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
    }
    bool isBroken() const override { return false; }
    bool isReadOnly() const override { return true; }

    mutable std::map<String, BaseManifestEntry> part_map;

private:
    String db_path;
    String relative_data_path;
    std::weak_ptr<IManifestStorage> manifest_storage;
    MergeTreeDataFormatVersion format_version;

    DataSourceDescription data_source_description;
};

}
