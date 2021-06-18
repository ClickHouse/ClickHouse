#pragma once

#include <Disks/IDiskRemote.h>
#include <IO/WriteBufferFromFile.h>
#include <Core/UUID.h>
#include <set>


namespace DB
{

struct DiskWebServerSettings
{
    /// Number of read attempts before throw that network is unreachable.
    size_t max_read_tries;
    /// Passed to SeekAvoidingReadBuffer.
    size_t min_bytes_for_seek;
    String files_prefix;

    DiskWebServerSettings(size_t max_read_tries_, size_t min_bytes_for_seek_, String files_prefix_)
        : max_read_tries(max_read_tries_) , min_bytes_for_seek(min_bytes_for_seek_), files_prefix(files_prefix_) {}
};


/// Storage to store data on a web server and metadata on the local disk.

class DiskWebServer : public IDisk, WithContext
{
using SettingsPtr = std::unique_ptr<DiskWebServerSettings>;

public:
    DiskWebServer(const String & disk_name_,
                  const String & files_root_path_uri_,
                  const String & metadata_path_,
                  ContextPtr context,
                  SettingsPtr settings_);

    using FileAndSize = std::pair<String, size_t>;
    using FilesInfo = std::unordered_map<String, size_t>;
    using FilesDirectory = std::map<String, FilesInfo>;

    struct Metadata
    {
        /// Fetch meta only when required.
        mutable FilesDirectory files;

        Metadata() {}
        void initialize(const String & uri_with_path, const String & files_prefix, ContextPtr context) const;
    };

    bool findFileInMetadata(const String & path, FileAndSize & file_info) const;

    String getFileName(const String & path) const;

    DiskType::Type getType() const override { return DiskType::Type::WebServer; }

    std::unique_ptr<ReadBufferFromFileBase> readFile(const String & path,
                                                     size_t buf_size,
                                                     size_t estimated_size,
                                                     size_t aio_threshold,
                                                     size_t mmap_threshold,
                                                     MMappedFileCache * mmap_cache) const override;
    /// Disk info

    const String & getName() const final override { return name; }

    /// ???
    const String & getPath() const final override { return metadata_path; }

    UInt64 getTotalSpace() const final override { return std::numeric_limits<UInt64>::max(); }

    UInt64 getAvailableSpace() const final override { return std::numeric_limits<UInt64>::max(); }

    UInt64 getUnreservedSpace() const final override { return std::numeric_limits<UInt64>::max(); }

    /// Read-only part

    bool exists(const String & path) const override;

    bool isFile(const String & path) const override;

    size_t getFileSize(const String & path) const override;

    void listFiles(const String & /* path */, std::vector<String> & /* file_names */) override { }

    void setReadOnly(const String & /* path */) override {}

    bool isDirectory(const String & path) const override;

    DiskDirectoryIteratorPtr iterateDirectory(const String & /* path */) override;

    Poco::Timestamp getLastModified(const String &) override { return Poco::Timestamp{}; }

    ReservationPtr reserve(UInt64 /*bytes*/) override { return nullptr; }

    /// Write and modification part

    std::unique_ptr<WriteBufferFromFileBase> writeFile(const String &, size_t, WriteMode) override;

    void moveFile(const String &, const String &) override {}

    void replaceFile(const String &, const String &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Disk {} is read-only", getName());
    }

    void removeFile(const String &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Disk {} is read-only", getName());
    }

    void removeFileIfExists(const String &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Disk {} is read-only", getName());
    }

    void removeRecursive(const String &) override {}

    void removeSharedFile(const String &, bool) override {}

    void removeSharedRecursive(const String &, bool) override {}

    void clearDirectory(const String &) override {}

    void moveDirectory(const String &, const String &) override {}

    void removeDirectory(const String &) override {}

    void setLastModified(const String &, const Poco::Timestamp &) override {}

    /// Create part

    void createFile(const String &) final override {}

    void createDirectory(const String &) override {}

    void createDirectories(const String &) override {}

    void createHardLink(const String &, const String &) override {}

private:

    Poco::Logger * log;
    String uri, name;
    const String metadata_path;
    SettingsPtr settings;

    Metadata metadata;
};

}
