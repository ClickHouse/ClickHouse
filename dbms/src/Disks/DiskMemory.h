#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <Disks/IDisk.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

namespace DB
{
class DiskMemory;
class ReadBuffer;
class WriteBuffer;

/// Adapter with actual behaviour as ReadBufferFromString.
class ReadIndirectBuffer : public ReadBufferFromFileBase
{
public:
    ReadIndirectBuffer(String path_, const String & data_);

    std::string getFileName() const override { return path; }
    off_t seek(off_t off, int whence) override;
    off_t getPosition() override;

private:
    ReadBufferFromString buf;
    String path;
};

/// This class is responsible to update files metadata after buffer is finalized.
class WriteIndirectBuffer : public WriteBufferFromOwnString
{
public:
    WriteIndirectBuffer(DiskMemory * disk_, String path_, WriteMode mode_) : disk(disk_), path(std::move(path_)), mode(mode_) {}

    ~WriteIndirectBuffer() override;

    void finalize() override;

private:
    DiskMemory * disk;
    String path;
    WriteMode mode;
};

/** Implementation of Disk intended only for testing purposes.
  * All filesystem objects are stored in memory and lost on server restart.
  *
  * NOTE Work in progress. Currently the interface is not viable enough to support MergeTree or even StripeLog tables.
  * Please delete this interface if it will not be finished after 2020-06-18.
  */
class DiskMemory : public IDisk
{
public:
    DiskMemory(const String & name_) : name(name_), disk_path("memory://" + name_ + '/') {}

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

    DiskDirectoryIteratorPtr iterateDirectory(const String & path) override;

    void moveFile(const String & from_path, const String & to_path) override;

    void replaceFile(const String & from_path, const String & to_path) override;

    void copyFile(const String & from_path, const String & to_path) override;

    std::unique_ptr<ReadBufferFromFileBase> readFile(const String & path, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE) const override;

    std::unique_ptr<WriteBuffer>
    writeFile(const String & path, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE, WriteMode mode = WriteMode::Rewrite) override;

    void remove(const String & path) override;

    void removeRecursive(const String & path) override;

private:
    void createDirectoriesImpl(const String & path);
    void replaceFileImpl(const String & from_path, const String & to_path);

private:
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
        explicit FileData(FileType type_) : type(type_), data("") {}
    };
    using Files = std::unordered_map<String, FileData>; /// file path -> file data

    const String name;
    const String disk_path;
    Files files;
    mutable std::mutex mutex;
};

}
