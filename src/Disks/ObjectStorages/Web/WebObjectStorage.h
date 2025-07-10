#pragma once

#include "config.h"

#include <Disks/ObjectStorages/IObjectStorage.h>

#include <filesystem>
#include <shared_mutex>

namespace Poco
{
class Logger;
}

namespace DB
{

class WebObjectStorage : public IObjectStorage, WithContext
{
    friend class MetadataStorageFromStaticFilesWebServer;
    friend class MetadataStorageFromStaticFilesWebServerTransaction;

public:
    WebObjectStorage(const String & url_, ContextPtr context_);

    std::string getName() const override { return "WebObjectStorage"; }

    ObjectStorageType getType() const override { return ObjectStorageType::Web; }

    std::string getCommonKeyPrefix() const override { return url; }

    std::string getDescription() const override { return url; }

    bool exists(const StoredObject & object) const override;

    std::unique_ptr<ReadBufferFromFileBase> readObject( /// NOLINT
        const StoredObject & object,
        const ReadSettings & read_settings,
        std::optional<size_t> read_hint = {},
        std::optional<size_t> file_size = {}) const override;

    /// Open the file for write and return WriteBufferFromFileBase object.
    std::unique_ptr<WriteBufferFromFileBase> writeObject( /// NOLINT
        const StoredObject & object,
        WriteMode mode,
        std::optional<ObjectAttributes> attributes = {},
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        const WriteSettings & write_settings = {}) override;

    void removeObjectIfExists(const StoredObject & object) override;

    void removeObjectsIfExist(const StoredObjects & objects) override;

    ObjectMetadata getObjectMetadata(const std::string & path) const override;

    void copyObject( /// NOLINT
        const StoredObject & object_from,
        const StoredObject & object_to,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        std::optional<ObjectAttributes> object_to_attributes = {}) override;

    void shutdown() override;

    void startup() override;

    String getObjectsNamespace() const override { return ""; }

    ObjectStorageKey generateObjectKeyForPath(const std::string & path, const std::optional<std::string> & /* key_prefix */) const override
    {
        return ObjectStorageKey::createAsRelative(path);
    }

    bool areObjectKeysRandom() const override { return false; }

    bool isRemote() const override { return true; }

    bool isReadOnly() const override { return true; }

protected:
    [[noreturn]] static void throwNotAllowed();
    bool exists(const std::string & path) const;

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

        static FileDataPtr createDirectoryInfo(bool loaded_childrent_)
        {
            return std::make_shared<FileData>(FileType::Directory, 0, loaded_childrent_);
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
    mutable std::shared_mutex metadata_mutex;

    FileDataPtr tryGetFileInfo(const String & path) const;
    std::vector<std::filesystem::path> listDirectory(const String & path) const;
    FileDataPtr getFileInfo(const String & path) const;

private:
    std::pair<WebObjectStorage::FileDataPtr, std::vector<std::filesystem::path>>
    loadFiles(const String & path, const std::unique_lock<std::shared_mutex> &) const;

    const String url;
    LoggerPtr log;
    size_t min_bytes_for_seek;
};

}
