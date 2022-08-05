#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <Disks/IDisk.h>

namespace DB
{
class DiskMemory;
class ReadBufferFromFileBase;
class WriteBufferFromFileBase;


/** Implementation of Disk intended only for testing purposes.
  * All filesystem objects are stored in memory and lost on server restart.
  *
  * NOTE Work in progress. Currently the interface is not viable enough to support MergeTree or even StripeLog tables.
  * Please delete this interface if it will not be finished after 2020-06-18.
  */
class DiskMemory : public IDisk
{
public:
    explicit DiskMemory(const String & name_) : name(name_), disk_path("memory://" + name_ + '/') {}

    const String & getName() const override { return name; }

    const String & getPath() const override { return disk_path; }

    ReservationPtr reserve(UInt64 bytes) override;

    UInt64 getTotalSpace() const override;

    UInt64 getAvailableSpace() const override;

    UInt64 getUnreservedSpace() const override;

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

    void removeFile(const String & path) override;
    void removeFileIfExists(const String & path) override;
    void removeDirectory(const String & path) override;
    void removeRecursive(const String & path) override;

    void setLastModified(const String &, const Poco::Timestamp &) override {}

    Poco::Timestamp getLastModified(const String &) const override { return Poco::Timestamp(); }

    time_t getLastChanged(const String &) const override { return {}; }

    void setReadOnly(const String & path) override;

    void createHardLink(const String & src_path, const String & dst_path) override;

    bool isFilesHardLinked(const String & src_path, const String & dst_path) const override;

    uint32_t getFileHardLinkCount(const String & path) const override;

    void truncateFile(const String & path, size_t size) override;

    DiskType getType() const override { return DiskType::RAM; }
    bool isRemote() const override { return false; }

    bool supportZeroCopyReplication() const override { return false; }

private:
    void createDirectoriesImpl(const String & path);
    void replaceFileImpl(const String & from_path, const String & to_path);

    friend class WriteIndirectBuffer;

    enum class FileType
    {
        File,
        Directory
    };

    struct FileData
    {
        FileType type;
        String data;

        FileData(FileType type_, String data_) : type(type_), data(std::move(data_)) {}
        explicit FileData(FileType type_) : type(type_) {}
    };
    using Files = std::unordered_map<String, FileData>; /// file path -> file data

    const String name;
    const String disk_path;
    Files files;
    mutable std::mutex mutex;
};

}
