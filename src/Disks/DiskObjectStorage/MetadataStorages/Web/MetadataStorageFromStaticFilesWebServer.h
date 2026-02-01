#pragma once

#include <Disks/IDisk.h>
#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/MetadataStorageTransactionState.h>
#include <Common/SharedMutex.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Web/WebObjectStorage.h>

#include <filesystem>

namespace DB
{

struct PartitionCommand;

class MetadataStorageFromStaticFilesWebServer final : public IMetadataStorage
{
private:
    friend class MetadataStorageFromStaticFilesWebServerTransaction;

    const WebObjectStorage & object_storage;
    LoggerPtr log;

    enum class FileType : uint8_t
    {
        File,
        Directory
    };

    struct FileData;
    using FileDataPtr = std::shared_ptr<FileData>;

    struct FileData
    {
        FileData(FileType type_, size_t size_, bool loaded_children_ = false)
            : type(type_), size(size_), loaded_children(loaded_children_) {}

        static FileDataPtr createFileInfo(size_t size_)
        {
            return std::make_shared<FileData>(FileType::File, size_, false);
        }

        static FileDataPtr createDirectoryInfo(bool loaded_children_)
        {
            return std::make_shared<FileData>(FileType::Directory, 0, loaded_children_);
        }

        FileType type;
        size_t size;
        std::atomic<bool> loaded_children;
    };

    struct Files : public std::map<String, FileDataPtr>
    {
        auto find(const String & path, bool is_file) const
        {
            if (is_file)
                return std::map<String, FileDataPtr>::find(path);
            return std::map<String, FileDataPtr>::find(path.ends_with("/") ? path : path + '/');
        }

        auto add(const String & path, FileDataPtr data)
        {
            if (data->type == FileType::Directory)
                return emplace(path.ends_with("/") ? path : path + '/', data);
            return emplace(path, data);
        }
    };

    mutable Files files;
    mutable SharedMutex metadata_mutex;

    std::pair<FileDataPtr, std::vector<std::filesystem::path>>
    loadFiles(const String & path, const std::unique_lock<SharedMutex> &) const;

    FileDataPtr tryGetFileInfo(const String & path) const;
    std::vector<std::filesystem::path> listDirectoryInternal(const String & path) const;
    FileDataPtr getFileInfo(const String & path) const;

    void assertExists(const std::string & path) const;

public:
    explicit MetadataStorageFromStaticFilesWebServer(const WebObjectStorage & object_storage_);

    MetadataTransactionPtr createTransaction() override;

    const std::string & getPath() const override;

    MetadataStorageType getType() const override { return MetadataStorageType::StaticWeb; }

    bool existsFile(const std::string & path) const override;
    bool existsDirectory(const std::string & path) const override;
    bool existsFileOrDirectory(const std::string & path) const override;

    uint64_t getFileSize(const String & path) const override;
    std::optional<uint64_t> getFileSizeIfExists(const String & path) const override;

    std::vector<std::string> listDirectory(const std::string & path) const override;

    DirectoryIteratorPtr iterateDirectory(const std::string & path) const override;

    StoredObjects getStorageObjects(const std::string & path) const override;
    std::optional<StoredObjects> getStorageObjectsIfExist(const std::string & path) const override;

    struct stat stat(const String & /* path */) const override { return {}; }

    Poco::Timestamp getLastModified(const std::string & /* path */) const override
    {
        /// Required by MergeTree
        return {};
    }
    uint32_t getHardlinkCount(const std::string & /* path */) const override
    {
        return 1;
    }

    bool supportsChmod() const override { return false; }
    bool supportsStat() const override { return false; }
    bool isReadOnly() const override { return true; }
    bool areBlobPathsRandom() const override { return false; }
};

}
