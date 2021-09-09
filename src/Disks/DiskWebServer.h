#pragma once

#include <Disks/IDiskRemote.h>
#include <IO/WriteBufferFromFile.h>
#include <Core/UUID.h>
#include <set>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

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


/*
 * Quick ready test - you can try this disk, by using these queries (disk has two tables) and this endpoint:
 *
 *  ATTACH TABLE contributors UUID 'a563f7d8-fb00-4d50-a563-f7d8fb007d50' (good_person_name String) engine=MergeTree() order by good_person_name settings storage_policy='web';
 *  ATTACH TABLE test UUID '11c7a2f9-a949-4c88-91c7-a2f9a949ec88' (a Int32) engine=MergeTree() order by a settings storage_policy='web';
 *
 *   <storage_configuration>
 *       <disks>
 *           <web>
 *               <type>web</type>
 *               <endpoint>https://clickhouse-datasets.s3.yandex.net/kssenii-static-files-disk-test/kssenii-disk-tests/test1/</endpoint>
 *               <files_prefix>data</files_prefix>
 *           </web>
 *       </disks>
 *       <policies>
 *           <web>
 *               <volumes>
 *                   <main>
 *                       <disk>web</disk>
 *                   </main>
 *               </volumes>
 *           </web>
 *       </policies>
 *   </storage_configuration>
 *
 * To get files for upload run:
 * clickhouse static-files-disk-uploader --metadata-path <path> --output-dir <dir> --files-prefix data
 * (--metadata-path can be found in query: `select data_paths from system.tables where name='<table_name>';`)
 *
 * If url is not reachable on disk load when server is starting up tables, then all errors are caught.
 * If in this case there were errors, tables can be reloaded (become visible) via detach table table_name -> attach table table_name.
 * If metadata was successfully loaded at server startup, then tables are available straight away.
**/
class DiskWebServer : public IDisk, WithContext
{
using SettingsPtr = std::unique_ptr<DiskWebServerSettings>;

public:
    DiskWebServer(const String & disk_name_,
                  const String & files_root_path_uri_,
                  const String & metadata_path_,
                  ContextPtr context,
                  SettingsPtr settings_);

    struct File
    {
        String name;
        size_t size;
        File(const String & name_ = "", const size_t size_ = 0) : name(name_), size(size_) {}
    };

    using Directory = std::unordered_map<String, size_t>;

    /* Each root directory contains either directories like
     * all_x_x_x/{file}, detached/, etc, or root files like format_version.txt.
     */
    using RootDirectory = std::unordered_map<String, Directory>;

    /* Each table is attached via ATTACH TABLE table UUID <uuid> <def>.
     * Then there is a mapping: {table uuid} -> {root directory}
     */
    using TableDirectories = std::unordered_map<String, RootDirectory>;

    struct Metadata
    {
        /// Fetch meta only when required.
        mutable TableDirectories tables_data;

        Metadata() = default;

        void initialize(const String & uri_with_path, const String & files_prefix, const String & uuid, ContextPtr context) const;
    };

    using UUIDDirectoryListing = std::unordered_map<String, RootDirectory>;
    using RootDirectoryListing = std::unordered_map<String, Directory>;
    using DirectoryListing = std::unordered_map<String, size_t>;

    bool findFileInMetadata(const String & path, File & file_info) const;

    bool supportZeroCopyReplication() const override { return false; }

    String getFileName(const String & path) const;

    DiskType getType() const override { return DiskType::WebServer; }

    bool isRemote() const override { return true; }

    std::unique_ptr<ReadBufferFromFileBase> readFile(const String & path,
                                                     const ReadSettings & settings,
                                                     size_t estimated_size) const override;

    /// Disk info

    const String & getName() const final override { return name; }

    const String & getPath() const final override { return metadata_path; }

    bool isReadOnly() const override { return true; }

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

    /// Write and modification part

    std::unique_ptr<WriteBufferFromFileBase> writeFile(const String &, size_t, WriteMode) override;

    void moveFile(const String &, const String &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Disk {} is read-only", getName());
    }

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

    ReservationPtr reserve(UInt64 /*bytes*/) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Disk {} is read-only", getName());
    }

    void removeRecursive(const String &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Disk {} is read-only", getName());
    }

    void removeSharedFile(const String &, bool) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Disk {} is read-only", getName());
    }

    void removeSharedRecursive(const String &, bool) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Disk {} is read-only", getName());
    }

    void clearDirectory(const String &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Disk {} is read-only", getName());
    }

    void moveDirectory(const String &, const String &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Disk {} is read-only", getName());
    }

    void removeDirectory(const String &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Disk {} is read-only", getName());
    }

    void setLastModified(const String &, const Poco::Timestamp &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Disk {} is read-only", getName());
    }

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
